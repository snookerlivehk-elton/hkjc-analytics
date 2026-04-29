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

