from __future__ import annotations

import json
from datetime import date, datetime, time as dtime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import func
from sklearn.metrics import roc_auc_score

from database.models import Race, RaceEntry, RaceResult, ScoringFactor, SystemConfig
from scoring_engine.member_stats import HIT_METRICS, _calc_hits


def _is_missing_display(x: Any) -> bool:
    s = str(x if x is not None else "").strip()
    return s == "" or s == "無數據"


def _day_range(d1: date, d2: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(d1, dtime.min)
    end = datetime.combine(d2, dtime.min) + timedelta(days=1)
    return start, end


def _list_completed_race_ids(session: Session, d1: date, d2: date, min_finishers: int) -> List[int]:
    start, end = _day_range(d1, d2)
    rows = (
        session.query(Race.id)
        .join(RaceEntry, RaceEntry.race_id == Race.id)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceResult.rank != None)
        .filter(Race.race_date >= start)
        .filter(Race.race_date < end)
        .group_by(Race.id)
        .having(func.count(RaceResult.id) >= int(min_finishers or 0))
        .order_by(Race.id.asc())
        .all()
    )
    return [int(r[0]) for r in rows if r and r[0] is not None]


def _actual_topk_by_race(session: Session, race_ids: List[int], k: int) -> Dict[int, List[int]]:
    if not race_ids:
        return {}
    rows = (
        session.query(RaceEntry.race_id, RaceEntry.horse_no, RaceResult.rank)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceEntry.race_id.in_(list(race_ids)))
        .filter(RaceResult.rank != None)
        .order_by(RaceEntry.race_id.asc(), RaceResult.rank.asc(), RaceEntry.horse_no.asc())
        .all()
    )
    by_race: Dict[int, List[Tuple[int, int]]] = {}
    for rid, hn, rk in rows:
        try:
            rid_i = int(rid or 0)
            hn_i = int(hn or 0)
            rk_i = int(rk or 0)
        except Exception:
            continue
        if rid_i <= 0 or hn_i <= 0 or rk_i <= 0:
            continue
        by_race.setdefault(rid_i, []).append((rk_i, hn_i))
    out: Dict[int, List[int]] = {}
    for rid, items in by_race.items():
        items.sort(key=lambda x: (x[0], x[1]))
        out[rid] = [hn for _, hn in items[: int(k or 0)]]
    return out


def _auc_safe(y: List[int], s: List[float]) -> Optional[float]:
    if not y or not s or len(y) != len(s) or len(y) < 30:
        return None
    if len(set(y)) < 2:
        return None
    try:
        return float(roc_auc_score(np.asarray(y, dtype=int), np.asarray(s, dtype=float)))
    except Exception:
        return None


def evaluate_single_factor(
    session: Session,
    race_ids: List[int],
    actual_top5_by_race: Dict[int, List[int]],
    factor_name: str,
    top_k: int = 5,
) -> Dict[str, Any]:
    fn = str(factor_name or "").strip()
    if not fn or not race_ids:
        return {"factor_name": fn, "races": 0}

    rows = (
        session.query(RaceEntry.race_id, RaceEntry.horse_no, ScoringFactor.score, ScoringFactor.raw_data_display, RaceResult.rank)
        .join(ScoringFactor, ScoringFactor.entry_id == RaceEntry.id)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceEntry.race_id.in_(list(race_ids)))
        .filter(ScoringFactor.factor_name == fn)
        .all()
    )

    by_race: Dict[int, List[Tuple[float, int]]] = {}
    miss_disp = 0
    total_rows = 0
    scores_all: List[float] = []
    y_w2: List[int] = []
    y_top3: List[int] = []
    score_sum = 0.0
    score_sum2 = 0.0

    for rid, hn, sc, disp, rk in rows:
        try:
            rid_i = int(rid or 0)
            hn_i = int(hn or 0)
            sc_f = float(sc or 0.0)
            rk_i = int(rk or 0) if rk is not None else 0
        except Exception:
            continue
        if rid_i <= 0 or hn_i <= 0:
            continue
        total_rows += 1
        if _is_missing_display(disp):
            miss_disp += 1
        by_race.setdefault(rid_i, []).append((sc_f, hn_i))
        scores_all.append(sc_f)
        if rk_i > 0:
            y_w2.append(1 if rk_i <= 2 else 0)
            y_top3.append(1 if rk_i <= 3 else 0)
            score_sum += sc_f
            score_sum2 += sc_f * sc_f

    races = 0
    hit_sum = {str(k): 0 for k in list(HIT_METRICS)}
    for rid, act_top5 in actual_top5_by_race.items():
        if rid not in by_race:
            continue
        items = by_race.get(rid) or []
        if len(items) < int(top_k or 0) or not act_top5 or len(act_top5) < int(top_k or 0):
            continue
        items.sort(key=lambda x: (-x[0], x[1]))
        pred = [hn for _, hn in items[: int(top_k or 0)]]
        hits = _calc_hits(pred, act_top5[: int(top_k or 0)])
        if not hits:
            continue
        races += 1
        for k in list(HIT_METRICS):
            if k in hits:
                hit_sum[str(k)] = int(hit_sum.get(str(k)) or 0) + int(hits.get(k) or 0)

    auc_w2 = _auc_safe(y_w2, scores_all)
    auc_top3 = _auc_safe(y_top3, scores_all)
    mean_score = None
    std_score = None
    if total_rows >= 2:
        mean_score = score_sum / float(total_rows)
        var = (score_sum2 / float(total_rows)) - (mean_score * mean_score)
        std_score = float(max(0.0, var) ** 0.5)

    out = {
        "factor_name": fn,
        "races": int(races),
        "entries": int(total_rows),
        "coverage_pct": None,
        "missing_display_pct": (float(miss_disp) / float(total_rows) * 100.0) if total_rows else None,
        "w2_rate": (float(hit_sum.get("w2") or 0) / float(races) * 100.0) if races else None,
        "pq3_rate": (float(hit_sum.get("pq3") or 0) / float(races) * 100.0) if races else None,
        "p2_rate": (float(hit_sum.get("p2") or 0) / float(races) * 100.0) if races else None,
        "p3_rate": (float(hit_sum.get("p3") or 0) / float(races) * 100.0) if races else None,
        "auc_w2": auc_w2,
        "auc_top3": auc_top3,
        "mean_score": mean_score,
        "std_score": std_score,
    }
    return out


def evaluate_factors(
    session: Session,
    d1: date,
    d2: date,
    factor_names: List[str],
    top_k: int = 5,
    cache_key: str = "",
    save_cache: bool = True,
) -> Dict[str, Any]:
    if not isinstance(d1, date) or not isinstance(d2, date) or d1 > d2:
        return {"ok": False, "reason": "bad_range"}

    fnames = [str(x) for x in (factor_names or []) if str(x).strip()]
    fnames = list(dict.fromkeys(fnames))
    if not fnames:
        return {"ok": False, "reason": "no_factors"}

    key = str(cache_key or "").strip()
    if key:
        cfg = session.query(SystemConfig).filter_by(key=key).first()
        if cfg and isinstance(cfg.value, dict) and cfg.value.get("ok") is True:
            return cfg.value

    race_ids = _list_completed_race_ids(session, d1=d1, d2=d2, min_finishers=int(top_k or 0))
    if not race_ids:
        res = {"ok": True, "races": 0, "entries": 0, "rows": [], "date_range": {"from": d1.isoformat(), "to": d2.isoformat()}}
        if key and save_cache:
            cfg = session.query(SystemConfig).filter_by(key=key).first()
            if not cfg:
                cfg = SystemConfig(key=key, description="因子成效評估（單因子）")
                session.add(cfg)
            cfg.value = res
            session.commit()
        return res

    total_entries = int(session.query(RaceEntry.id).filter(RaceEntry.race_id.in_(list(race_ids))).count() or 0)
    actual_top5_by_race = _actual_topk_by_race(session, race_ids, int(top_k or 0))

    rows_out: List[Dict[str, Any]] = []
    for fn in fnames:
        r = evaluate_single_factor(session, race_ids=race_ids, actual_top5_by_race=actual_top5_by_race, factor_name=fn, top_k=int(top_k or 0))
        entries = int(r.get("entries") or 0)
        if total_entries > 0:
            r["coverage_pct"] = float(entries) / float(total_entries) * 100.0
        rows_out.append(r)

    rows_out.sort(
        key=lambda x: (
            -(float(x.get("pq3_rate") or -1e9)),
            -(float(x.get("w2_rate") or -1e9)),
            float(x.get("missing_display_pct") or 1e9),
            str(x.get("factor_name") or ""),
        )
    )

    res = {
        "ok": True,
        "policy": {"top_k": int(top_k or 0), "objectives": ["pq3", "w2"]},
        "date_range": {"from": d1.isoformat(), "to": d2.isoformat()},
        "races": int(len(race_ids)),
        "entries": int(total_entries),
        "rows": rows_out,
        "updated_at": datetime.utcnow().isoformat(),
    }

    if key and save_cache:
        cfg = session.query(SystemConfig).filter_by(key=key).first()
        if not cfg:
            cfg = SystemConfig(key=key, description="因子成效評估（單因子）")
            session.add(cfg)
        cfg.value = json.loads(json.dumps(res, ensure_ascii=False))
        session.commit()

    return res

