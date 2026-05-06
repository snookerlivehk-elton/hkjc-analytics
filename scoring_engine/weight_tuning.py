import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy import func
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, log_loss

from database.models import Race, RaceEntry, RaceResult, ScoringFactor


def _is_missing_display(x: Any) -> bool:
    s = str(x or "").strip()
    return s == "" or s == "無數據"


def build_entry_factor_frame(
    session: Session,
    d1: date,
    d2: date,
    factor_names: List[str],
) -> pd.DataFrame:
    if not factor_names:
        return pd.DataFrame()
    if not isinstance(d1, date) or not isinstance(d2, date) or d1 > d2:
        return pd.DataFrame()

    rows = (
        session.query(
            RaceEntry.id.label("entry_id"),
            RaceEntry.race_id.label("race_id"),
            RaceEntry.horse_no.label("horse_no"),
            RaceResult.rank.label("rank"),
            ScoringFactor.factor_name.label("factor"),
            ScoringFactor.score.label("score"),
            ScoringFactor.raw_data_display.label("display"),
        )
        .join(Race, Race.id == RaceEntry.race_id)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .join(ScoringFactor, ScoringFactor.entry_id == RaceEntry.id)
        .filter(RaceResult.rank != None)
        .filter(ScoringFactor.factor_name.in_(list(factor_names)))
        .filter(func.date(Race.race_date) >= d1.isoformat())
        .filter(func.date(Race.race_date) <= d2.isoformat())
        .all()
    )
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["entry_id", "race_id", "horse_no", "rank", "factor", "score", "display"])
    df["missing"] = df["display"].apply(_is_missing_display).astype(int)

    sc = df.pivot_table(index="entry_id", columns="factor", values="score", aggfunc="first")
    ms = df.pivot_table(index="entry_id", columns="factor", values="missing", aggfunc="max")
    rank = df.groupby("entry_id", as_index=True)["rank"].max()
    race_id = df.groupby("entry_id", as_index=True)["race_id"].max()
    horse_no = df.groupby("entry_id", as_index=True)["horse_no"].max()

    out = pd.DataFrame(index=sc.index)
    out["rank"] = rank
    out["race_id"] = race_id
    out["horse_no"] = horse_no

    for fn in factor_names:
        col_s = sc[fn] if fn in sc.columns else pd.Series(index=out.index, dtype=float)
        col_m = ms[fn] if fn in ms.columns else pd.Series(index=out.index, dtype=float)
        out[f"{fn}__score"] = pd.to_numeric(col_s, errors="coerce")
        out[f"{fn}__missing"] = pd.to_numeric(col_m, errors="coerce").fillna(0.0)

    return out.reset_index(drop=False)


def _fit_logit(X: pd.DataFrame, y: np.ndarray, random_state: int = 0) -> Optional[LogisticRegression]:
    try:
        y0 = np.asarray(y).astype(int)
    except Exception:
        return None
    if len(y0) < 20:
        return None
    if len(set(list(y0))) < 2:
        return None
    model = LogisticRegression(
        penalty="l2",
        solver="liblinear",
        class_weight="balanced",
        random_state=int(random_state or 0),
        max_iter=500,
    )
    model.fit(X.values, y0)
    return model


def _coef_maps(model: LogisticRegression, X_cols: List[str], factor_names: List[str]) -> Tuple[Dict[str, float], Dict[str, float]]:
    coefs = model.coef_[0]
    coef_map_score: Dict[str, float] = {}
    coef_map_missing: Dict[str, float] = {}
    for i, col in enumerate(X_cols):
        if col.endswith("__score"):
            fn = col[: -len("__score")]
            if fn in factor_names:
                coef_map_score[fn] = float(coefs[i])
        elif col.endswith("__missing"):
            fn = col[: -len("__missing")]
            if fn in factor_names:
                coef_map_missing[fn] = float(coefs[i])
    return coef_map_score, coef_map_missing


def _eval_top3_focus_from_weights(
    df: pd.DataFrame,
    factor_names: List[str],
    weights: Dict[str, float],
) -> Dict[str, Any]:
    if df.empty:
        return {"races": 0, "w2_rate": 0.0, "top3_2in_rate": 0.0}
    dfx = df.copy()
    for fn in factor_names:
        s_col = f"{fn}__score"
        if s_col not in dfx.columns:
            dfx[s_col] = np.nan
        dfx[s_col] = pd.to_numeric(dfx[s_col], errors="coerce").fillna(5.0)

    races = 0
    w2 = 0
    t2 = 0
    for rid, g in dfx.groupby("race_id"):
        g2 = g.dropna(subset=["rank", "horse_no"], how="any")
        if g2.empty:
            continue
        try:
            g2["rank_i"] = g2["rank"].astype(int)
        except Exception:
            continue
        act = g2.sort_values(["rank_i", "horse_no"], ascending=[True, True])
        act_top4 = [int(x) for x in list(act["horse_no"].values)[:4] if int(x or 0) > 0]
        if len(act_top4) < 4:
            continue

        s = np.zeros(len(g2), dtype=float)
        for fn in factor_names:
            w = float(weights.get(fn) or 0.0)
            if abs(w) <= 1e-12:
                continue
            s += w * pd.to_numeric(g2[f"{fn}__score"], errors="coerce").fillna(5.0).astype(float).values
        g2 = g2.copy()
        g2["pred_score"] = s
        pred = g2.sort_values(["pred_score", "horse_no"], ascending=[False, True])
        pred_top3 = [int(x) for x in list(pred["horse_no"].values)[:3] if int(x or 0) > 0]
        pred_top2 = [int(x) for x in list(pred["horse_no"].values)[:2] if int(x or 0) > 0]

        winner = act_top4[0]
        races += 1
        if winner in set(pred_top2):
            w2 += 1
        if len(set(pred_top3) & set(act_top4)) >= 2:
            t2 += 1

    if races <= 0:
        return {"races": 0, "w2_rate": 0.0, "top3_2in_rate": 0.0}
    return {"races": int(races), "w2_rate": round(float(w2) / float(races) * 100.0, 1), "top3_2in_rate": round(float(t2) / float(races) * 100.0, 1)}


def tune_weights_top3_focus(
    session: Session,
    d1: date,
    d2: date,
    factor_names: List[str],
    max_suggest_weight: float = 3.0,
    objective: Optional[Dict[str, float]] = None,
    random_state: int = 0,
) -> Dict[str, Any]:
    factor_names = [str(x) for x in (factor_names or []) if str(x).strip()]
    factor_names = list(dict.fromkeys(factor_names))
    if not factor_names:
        return {"ok": False, "reason": "no_factors"}

    df = build_entry_factor_frame(session, d1=d1, d2=d2, factor_names=factor_names)
    if df.empty:
        return {"ok": False, "reason": "no_data"}

    X_cols = []
    for fn in factor_names:
        X_cols.append(f"{fn}__score")
        X_cols.append(f"{fn}__missing")

    X = df[X_cols].copy()
    for fn in factor_names:
        s_col = f"{fn}__score"
        m_col = f"{fn}__missing"
        s = pd.to_numeric(X[s_col], errors="coerce")
        X[s_col] = s.fillna(5.0)
        X[m_col] = pd.to_numeric(X[m_col], errors="coerce").fillna(0.0)

    rk = pd.to_numeric(df["rank"], errors="coerce").fillna(999).astype(int).values
    y_win = np.asarray([1 if int(x) == 1 else 0 for x in rk], dtype=int)
    y_top4 = np.asarray([1 if int(x) <= 4 else 0 for x in rk], dtype=int)

    obj = {"w2_weight": 0.7, "top3_2in_weight": 0.3}
    if isinstance(objective, dict):
        for k in list(obj.keys()):
            try:
                if k in objective:
                    obj[k] = float(objective.get(k))
            except Exception:
                pass
    w2w = float(obj.get("w2_weight") or 0.0)
    t2w = float(obj.get("top3_2in_weight") or 0.0)
    if (w2w + t2w) <= 0:
        w2w, t2w = 0.7, 0.3

    m_win = _fit_logit(X, y_win, random_state=random_state)
    m_t4 = _fit_logit(X, y_top4, random_state=random_state)
    if (m_win is None) or (m_t4 is None):
        return {"ok": False, "reason": "no_samples"}

    coef_win_score, coef_win_missing = _coef_maps(m_win, X_cols, factor_names)
    coef_t4_score, coef_t4_missing = _coef_maps(m_t4, X_cols, factor_names)

    pos_win = {fn: max(0.0, float(coef_win_score.get(fn) or 0.0)) for fn in factor_names}
    pos_t4 = {fn: max(0.0, float(coef_t4_score.get(fn) or 0.0)) for fn in factor_names}
    combined = {fn: (w2w * float(pos_win.get(fn) or 0.0)) + (t2w * float(pos_t4.get(fn) or 0.0)) for fn in factor_names}
    max_pos = max([float(v) for v in combined.values()] or [0.0])

    suggested: Dict[str, float] = {}
    cap = float(max_suggest_weight if max_suggest_weight is not None else 3.0)
    if cap < 0.1:
        cap = 0.1
    if max_pos <= 0.0:
        for fn in factor_names:
            suggested[fn] = 0.0
    else:
        for fn in factor_names:
            suggested[fn] = float(combined.get(fn) or 0.0) / max_pos * cap

    metrics = _eval_top3_focus_from_weights(df, factor_names=factor_names, weights=suggested)
    score = round((w2w * (float(metrics.get("w2_rate") or 0.0) / 100.0)) + (t2w * (float(metrics.get("top3_2in_rate") or 0.0) / 100.0)), 4)

    return {
        "ok": True,
        "rows": int(len(df)),
        "races": int(metrics.get("races") or 0),
        "objective": {"w2_weight": w2w, "top3_2in_weight": t2w},
        "in_sample": {"w2_rate": metrics.get("w2_rate"), "top3_2in_rate": metrics.get("top3_2in_rate"), "score": score},
        "coef_win_score": coef_win_score,
        "coef_win_missing": coef_win_missing,
        "coef_top4_score": coef_t4_score,
        "coef_top4_missing": coef_t4_missing,
        "suggested_weights": suggested,
    }


def build_topk_training_frame(
    session: Session,
    d1: date,
    d2: date,
    top_k: int,
    factor_names: List[str],
) -> pd.DataFrame:
    if not factor_names:
        return pd.DataFrame()
    if not isinstance(d1, date) or not isinstance(d2, date) or d1 > d2:
        return pd.DataFrame()
    if int(top_k or 0) <= 0:
        return pd.DataFrame()

    rows = (
        session.query(
            RaceEntry.id.label("entry_id"),
            RaceEntry.race_id.label("race_id"),
            RaceResult.rank.label("rank"),
            ScoringFactor.factor_name.label("factor"),
            ScoringFactor.score.label("score"),
            ScoringFactor.raw_data_display.label("display"),
        )
        .join(Race, Race.id == RaceEntry.race_id)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .join(ScoringFactor, ScoringFactor.entry_id == RaceEntry.id)
        .filter(RaceResult.rank != None)
        .filter(ScoringFactor.factor_name.in_(list(factor_names)))
        .filter(func.date(Race.race_date) >= d1.isoformat())
        .filter(func.date(Race.race_date) <= d2.isoformat())
        .all()
    )
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["entry_id", "race_id", "rank", "factor", "score", "display"])
    df["y"] = df["rank"].apply(lambda x: 1 if (x is not None and int(x) <= int(top_k)) else 0)
    df["missing"] = df["display"].apply(_is_missing_display).astype(int)

    sc = df.pivot_table(index="entry_id", columns="factor", values="score", aggfunc="first")
    ms = df.pivot_table(index="entry_id", columns="factor", values="missing", aggfunc="max")
    y = df.groupby("entry_id", as_index=True)["y"].max()
    race_id = df.groupby("entry_id", as_index=True)["race_id"].max()

    out = pd.DataFrame(index=sc.index)
    out["y"] = y
    out["race_id"] = race_id

    for fn in factor_names:
        col_s = sc[fn] if fn in sc.columns else pd.Series(index=out.index, dtype=float)
        col_m = ms[fn] if fn in ms.columns else pd.Series(index=out.index, dtype=float)
        out[f"{fn}__score"] = pd.to_numeric(col_s, errors="coerce")
        out[f"{fn}__missing"] = pd.to_numeric(col_m, errors="coerce").fillna(0.0)

    return out.reset_index(drop=False)


def tune_weights_topk(
    session: Session,
    d1: date,
    d2: date,
    top_k: int,
    factor_names: List[str],
    max_suggest_weight: float = 3.0,
    random_state: int = 0,
) -> Dict[str, Any]:
    factor_names = [str(x) for x in (factor_names or []) if str(x).strip()]
    factor_names = list(dict.fromkeys(factor_names))

    df = build_topk_training_frame(session, d1=d1, d2=d2, top_k=top_k, factor_names=factor_names)
    if df.empty:
        return {"ok": False, "reason": "no_data"}

    X_cols = []
    for fn in factor_names:
        X_cols.append(f"{fn}__score")
        X_cols.append(f"{fn}__missing")

    X = df[X_cols].copy()
    for fn in factor_names:
        s_col = f"{fn}__score"
        m_col = f"{fn}__missing"
        s = pd.to_numeric(X[s_col], errors="coerce")
        X[s_col] = s.fillna(5.0)
        X[m_col] = pd.to_numeric(X[m_col], errors="coerce").fillna(0.0)

    y = df["y"].astype(int).values

    model = LogisticRegression(
        penalty="l2",
        solver="liblinear",
        class_weight="balanced",
        random_state=int(random_state or 0),
        max_iter=500,
    )
    model.fit(X.values, y)

    prob = model.predict_proba(X.values)[:, 1]
    auc = None
    try:
        auc = float(roc_auc_score(y, prob))
    except Exception:
        auc = None
    ll = None
    try:
        ll = float(log_loss(y, prob, labels=[0, 1]))
    except Exception:
        ll = None

    coefs = model.coef_[0]
    coef_map_score: Dict[str, float] = {}
    coef_map_missing: Dict[str, float] = {}
    for i, col in enumerate(X_cols):
        if col.endswith("__score"):
            fn = col[: -len("__score")]
            coef_map_score[fn] = float(coefs[i])
        elif col.endswith("__missing"):
            fn = col[: -len("__missing")]
            coef_map_missing[fn] = float(coefs[i])

    pos = {fn: max(0.0, float(coef_map_score.get(fn) or 0.0)) for fn in factor_names}
    max_pos = max([float(v) for v in pos.values()] or [0.0])
    suggested: Dict[str, float] = {}
    if max_pos <= 0.0:
        for fn in factor_names:
            suggested[fn] = 0.0
    else:
        cap = float(max_suggest_weight if max_suggest_weight is not None else 3.0)
        if cap < 0.1:
            cap = 0.1
        for fn in factor_names:
            suggested[fn] = float(pos.get(fn) or 0.0) / max_pos * cap

    return {
        "ok": True,
        "top_k": int(top_k),
        "rows": int(len(df)),
        "pos_rate": float(np.mean(y)) if len(y) else None,
        "auc": auc,
        "log_loss": ll,
        "intercept": float(model.intercept_[0]) if hasattr(model, "intercept_") else None,
        "coef_score": coef_map_score,
        "coef_missing": coef_map_missing,
        "suggested_weights": suggested,
    }
