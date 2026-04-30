from __future__ import annotations

from typing import Dict, List, Tuple

from sqlalchemy.orm import Session

from database.models import RaceEntry, ScoringFactor

TIE_BREAK = "horse_no"


def sort_desc(items: List[Tuple[int, float]]) -> List[Tuple[int, float]]:
    items.sort(key=lambda x: (-float(x[1] or 0.0), int(x[0] or 0)))
    return items


def sort_asc(items: List[Tuple[int, float]]) -> List[Tuple[int, float]]:
    items.sort(key=lambda x: (float(x[1] or 0.0), -int(x[0] or 0)))
    return items


def topk_from_scores(score_by_horse_no: Dict[int, float], k: int) -> List[int]:
    items = [(int(hn), float(sc or 0.0)) for hn, sc in (score_by_horse_no or {}).items() if int(hn or 0) > 0]
    sort_desc(items)
    return [hn for hn, _ in items[: int(k or 0)]]


def bottomk_from_scores(score_by_horse_no: Dict[int, float], k: int) -> List[int]:
    items = [(int(hn), float(sc or 0.0)) for hn, sc in (score_by_horse_no or {}).items() if int(hn or 0) > 0]
    sort_asc(items)
    return [hn for hn, _ in items[: int(k or 0)]]


def order_by_total_desc():
    return [RaceEntry.total_score.desc().nullslast(), RaceEntry.horse_no.asc().nullslast()]


def order_by_total_asc():
    return [RaceEntry.total_score.asc().nullsfirst(), RaceEntry.horse_no.desc().nullslast()]


def predicted_topk_by_total(session: Session, race_id: int, k: int) -> List[int]:
    rows = (
        session.query(RaceEntry.horse_no)
        .filter(RaceEntry.race_id == int(race_id))
        .order_by(*order_by_total_desc())
        .limit(int(k or 0))
        .all()
    )
    out: List[int] = []
    for (hn,) in rows:
        try:
            out.append(int(hn or 0))
        except Exception:
            out.append(0)
    return [x for x in out if x > 0]


def predicted_bottomk_by_total(session: Session, race_id: int, k: int) -> List[int]:
    rows = (
        session.query(RaceEntry.horse_no)
        .filter(RaceEntry.race_id == int(race_id))
        .order_by(*order_by_total_asc())
        .limit(int(k or 0))
        .all()
    )
    out: List[int] = []
    for (hn,) in rows:
        try:
            out.append(int(hn or 0))
        except Exception:
            out.append(0)
    return [x for x in out if x > 0]


def normalize_weights(weight_map: Dict[str, float]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for k, v in (weight_map or {}).items():
        kk = str(k or "").strip()
        if not kk:
            continue
        try:
            vv = float(v or 0.0)
        except Exception:
            vv = 0.0
        if abs(vv) < 1e-12:
            continue
        out[kk] = vv
    return out


def ranked_horses_by_weights(session: Session, race_id: int, weight_map: Dict[str, float]) -> List[int]:
    weights = normalize_weights(weight_map)
    if not weights:
        return []

    entries = session.query(RaceEntry.id, RaceEntry.horse_no).filter_by(race_id=int(race_id)).all()
    if not entries:
        return []

    entry_ids: List[int] = []
    entry_id_to_no: Dict[int, int] = {}
    for eid, hn in entries:
        try:
            eid_i = int(eid or 0)
        except Exception:
            eid_i = 0
        if eid_i <= 0:
            continue
        try:
            hn_i = int(hn or 0)
        except Exception:
            hn_i = 0
        if hn_i <= 0:
            continue
        entry_ids.append(eid_i)
        entry_id_to_no[eid_i] = hn_i
    if not entry_ids:
        return []

    factor_names = sorted(weights.keys())
    rows = (
        session.query(ScoringFactor.entry_id, ScoringFactor.factor_name, ScoringFactor.score)
        .filter(ScoringFactor.entry_id.in_(entry_ids))
        .filter(ScoringFactor.factor_name.in_(factor_names))
        .all()
    )

    score_map: Dict[int, Dict[str, float]] = {eid: {} for eid in entry_ids}
    for entry_id, factor_name, score in rows:
        try:
            eid = int(entry_id or 0)
        except Exception:
            continue
        if eid not in score_map:
            continue
        score_map[eid][str(factor_name)] = float(score or 0.0)

    items: List[Tuple[int, float]] = []
    for eid in entry_ids:
        hn = entry_id_to_no.get(int(eid))
        if hn is None:
            continue
        m = score_map.get(int(eid), {})
        total = 0.0
        for fn in factor_names:
            total += float(m.get(fn, 0.0)) * float(weights.get(fn, 0.0))
        items.append((int(hn), float(total)))

    sort_desc(items)
    return [hn for hn, _ in items]


def topk_by_weights(session: Session, race_id: int, weight_map: Dict[str, float], k: int) -> List[int]:
    ranked = ranked_horses_by_weights(session, race_id, weight_map)
    return ranked[: int(k or 0)]
