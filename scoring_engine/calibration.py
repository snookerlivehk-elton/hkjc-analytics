import math
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import func

from database.models import Race, RaceEntry, RaceResult, SystemConfig


def _zscore(scores: np.ndarray) -> np.ndarray:
    if scores.size == 0:
        return scores
    m = float(np.mean(scores))
    s = float(np.std(scores))
    if s <= 0.0:
        return np.zeros_like(scores, dtype=float)
    return (scores - m) / s


def _softmax(z: np.ndarray, temperature: float) -> np.ndarray:
    t = float(temperature) if temperature is not None else 1.0
    if t <= 0.0:
        t = 1.0
    x = z / t
    x = x - float(np.max(x)) if x.size else x
    ex = np.exp(x)
    denom = float(np.sum(ex))
    if denom <= 0.0:
        return np.ones_like(ex, dtype=float) / float(len(ex) or 1)
    return ex / denom


def _nll_for_races(session: Session, d1: date, d2: date, temperature: float) -> Tuple[Optional[float], int]:
    races = (
        session.query(Race.id)
        .join(RaceEntry, RaceEntry.race_id == Race.id)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceResult.rank != None)
        .filter(func.date(Race.race_date) >= d1.isoformat())
        .filter(func.date(Race.race_date) <= d2.isoformat())
        .distinct()
        .all()
    )
    race_ids = [int(r[0]) for r in races if r and r[0]]
    if not race_ids:
        return None, 0

    nll = 0.0
    used = 0
    for rid in race_ids:
        rows = (
            session.query(RaceEntry.id, RaceEntry.total_score, RaceResult.rank)
            .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
            .filter(RaceEntry.race_id == int(rid))
            .filter(RaceResult.rank != None)
            .all()
        )
        if not rows or len(rows) < 2:
            continue
        scores = np.array([float(r[1] or 0.0) for r in rows], dtype=float)
        ranks = [r[2] for r in rows]
        try:
            win_idx = next(i for i, rk in enumerate(ranks) if rk is not None and int(rk) == 1)
        except StopIteration:
            continue
        z = _zscore(scores)
        p = _softmax(z, temperature=float(temperature))
        pw = float(p[int(win_idx)] if int(win_idx) < len(p) else 0.0)
        pw = min(max(pw, 1e-12), 1.0)
        nll += -math.log(pw)
        used += 1
    if used <= 0:
        return None, 0
    return float(nll / float(used)), int(used)


def fit_winprob_temperature(
    session: Session,
    d1: date,
    d2: date,
    candidates: Optional[List[float]] = None,
) -> Dict[str, Any]:
    if not isinstance(d1, date) or not isinstance(d2, date) or d1 > d2:
        return {"ok": False, "reason": "bad_range"}

    cand = candidates
    if not cand:
        cand = [float(x) for x in np.concatenate([np.linspace(0.3, 1.5, 25), np.linspace(1.6, 4.0, 25)])]
    cand = [float(x) for x in cand if float(x) > 0.0]
    cand = sorted(set(cand))
    if not cand:
        return {"ok": False, "reason": "no_candidates"}

    best_t = None
    best_nll = None
    best_used = 0
    curve = []
    for t in cand:
        nll, used = _nll_for_races(session, d1=d1, d2=d2, temperature=float(t))
        if nll is None or used <= 0:
            continue
        curve.append({"t": float(t), "nll": float(nll), "races": int(used)})
        if best_nll is None or float(nll) < float(best_nll):
            best_nll = float(nll)
            best_t = float(t)
            best_used = int(used)

    if best_t is None or best_nll is None or best_used <= 0:
        return {"ok": False, "reason": "no_data"}

    return {
        "ok": True,
        "temperature": float(best_t),
        "nll": float(best_nll),
        "races": int(best_used),
        "date_range": {"from": d1.isoformat(), "to": d2.isoformat()},
        "curve": curve[-200:],
    }


def save_winprob_temperature(session: Session, value: Dict[str, Any]) -> None:
    cfg = session.query(SystemConfig).filter_by(key="winprob_temperature").first()
    if not cfg:
        cfg = SystemConfig(key="winprob_temperature", description="勝率校準溫度（softmax temperature）")
        session.add(cfg)
    cfg.value = value
    session.commit()


def load_winprob_temperature(session: Session) -> Optional[float]:
    cfg = session.query(SystemConfig).filter_by(key="winprob_temperature").first()
    if not cfg or not isinstance(cfg.value, dict):
        return None
    v = cfg.value.get("temperature")
    try:
        t = float(v)
    except Exception:
        return None
    if t <= 0.0:
        return None
    return float(t)

