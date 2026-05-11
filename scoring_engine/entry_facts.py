from __future__ import annotations

from datetime import datetime, time as dtime, timedelta
from typing import Any, Dict, Optional, Tuple

from sqlalchemy.orm import Session

from database.models import EntryFact, Race, RaceEntry, RaceResult, RaceTrackCondition
from scoring_engine.top5_odds_stats import bucket_odds


def _day_range(date_str: str) -> Tuple[datetime, datetime]:
    d0 = datetime.strptime(str(date_str), "%Y/%m/%d").date()
    start = datetime.combine(d0, dtime.min)
    end = start + timedelta(days=1)
    return start, end


def build_entry_facts_for_race_date(session: Session, *, date_str: str, place_k: int = 3) -> Dict[str, Any]:
    start, end = _day_range(date_str)
    races = (
        session.query(Race.id, Race.race_date, Race.venue, Race.race_no, Race.race_class, Race.distance, Race.surface, Race.course_type)
        .filter(Race.race_date >= start)
        .filter(Race.race_date < end)
        .order_by(Race.race_no.asc(), Race.id.asc())
        .all()
    )
    if not races:
        return {"ok": True, "date": date_str, "races": 0, "rows": 0, "updated": 0, "created": 0, "skipped": 0}

    race_ids = [int(r[0]) for r in races]
    going_by_race: Dict[int, Optional[str]] = {}
    try:
        rows_tc = session.query(RaceTrackCondition.race_id, RaceTrackCondition.going_code).filter(RaceTrackCondition.race_id.in_(race_ids)).all()
        for rid, gc in rows_tc:
            try:
                going_by_race[int(rid)] = str(gc or "").strip() or None
            except Exception:
                continue
    except Exception:
        going_by_race = {}

    date_day = datetime.strptime(str(date_str), "%Y/%m/%d").date()

    q = (
        session.query(
            RaceEntry.id,
            RaceEntry.race_id,
            RaceEntry.horse_id,
            RaceEntry.jockey_id,
            RaceEntry.trainer_id,
            RaceEntry.draw,
            RaceEntry.rating,
            RaceResult.rank,
            RaceResult.win_odds,
            Race.race_no,
            Race.venue,
            Race.race_class,
            Race.distance,
            Race.surface,
            Race.course_type,
        )
        .join(Race, Race.id == RaceEntry.race_id)
        .outerjoin(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceEntry.race_id.in_(race_ids))
    )

    existing: Dict[int, EntryFact] = {}
    try:
        rows_existing = session.query(EntryFact).filter(EntryFact.race_id.in_(race_ids)).all()
        for ef in rows_existing:
            try:
                existing[int(ef.entry_id)] = ef
            except Exception:
                continue
    except Exception:
        existing = {}

    created = 0
    updated = 0
    skipped = 0
    rows = 0

    for (
        entry_id,
        race_id,
        horse_id,
        jockey_id,
        trainer_id,
        draw,
        rating,
        rank,
        win_odds,
        race_no,
        venue,
        race_class,
        distance,
        surface,
        course_type,
    ) in q.all():
        rows += 1
        try:
            eid = int(entry_id)
            rid = int(race_id)
        except Exception:
            skipped += 1
            continue

        try:
            rk = int(rank or 0)
        except Exception:
            rk = 0
        is_win = True if rk == 1 else False
        is_place = True if (rk > 0 and rk <= int(place_k)) else False

        sp_odds = None
        try:
            sp_odds = float(win_odds) if win_odds is not None else None
        except Exception:
            sp_odds = None
        bucket = bucket_odds(sp_odds)

        ef = existing.get(eid)
        if ef is None:
            ef = EntryFact(entry_id=eid, race_id=rid, race_date_day=date_day)
            session.add(ef)
            created += 1
            existing[eid] = ef
        else:
            updated += 1

        ef.venue = str(venue or "").strip() or None
        try:
            ef.race_no = int(race_no or 0) or None
        except Exception:
            ef.race_no = None

        ef.race_class = str(race_class or "").strip() or None
        try:
            ef.distance = int(distance or 0) or None
        except Exception:
            ef.distance = None
        ef.surface = str(surface or "").strip() or None
        ef.course_type = str(course_type or "").strip() or None
        ef.going_code = going_by_race.get(rid)

        try:
            ef.horse_id = int(horse_id) if horse_id is not None else None
        except Exception:
            ef.horse_id = None
        try:
            ef.jockey_id = int(jockey_id) if jockey_id is not None else None
        except Exception:
            ef.jockey_id = None
        try:
            ef.trainer_id = int(trainer_id) if trainer_id is not None else None
        except Exception:
            ef.trainer_id = None

        try:
            ef.draw = int(draw) if draw is not None else None
        except Exception:
            ef.draw = None
        try:
            ef.rating = int(rating) if rating is not None else None
        except Exception:
            ef.rating = None

        ef.rank = int(rk) if rk > 0 else None
        ef.is_win = bool(is_win) if rk > 0 else None
        ef.is_place = bool(is_place) if rk > 0 else None
        ef.sp_win_odds = sp_odds
        ef.odds_bucket_sp = str(bucket)

    session.commit()
    return {
        "ok": True,
        "date": date_str,
        "races": len(race_ids),
        "rows": rows,
        "created": created,
        "updated": updated,
        "skipped": skipped,
    }

