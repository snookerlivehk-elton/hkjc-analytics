from __future__ import annotations

from typing import Dict, List, Tuple

from sqlalchemy.orm import Session

from database.models import RaceEntry

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

