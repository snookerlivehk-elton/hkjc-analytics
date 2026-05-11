from __future__ import annotations

from datetime import datetime, time as dtime, timedelta
from typing import Any, Dict, Tuple

from sqlalchemy.orm import Session

from database.models import DrawStatsDaily, EntryFact


def _day_range(date_str: str) -> Tuple[datetime, datetime]:
    d0 = datetime.strptime(str(date_str), "%Y/%m/%d").date()
    start = datetime.combine(d0, dtime.min)
    end = start + timedelta(days=1)
    return start, end


def rebuild_draw_stats_daily_for_race_date(session: Session, *, date_str: str) -> Dict[str, Any]:
    date_day = datetime.strptime(str(date_str), "%Y/%m/%d").date()

    session.query(DrawStatsDaily).filter(DrawStatsDaily.race_date_day == date_day).delete(synchronize_session=False)
    session.commit()

    rows = session.query(EntryFact).filter(EntryFact.race_date_day == date_day).all()
    if not rows:
        return {"ok": True, "date": date_str, "rows": 0}

    acc: Dict[Tuple, Dict[str, int]] = {}
    for r in rows:
        draw = int(r.draw or 0)
        if draw <= 0:
            continue
        bucket = str(r.odds_bucket_sp or "UNKNOWN").strip() or "UNKNOWN"
        k = (
            date_day,
            str(r.venue or "").strip() or None,
            str(r.surface or "").strip() or None,
            str(r.course_type or "").strip() or None,
            str(r.going_code or "").strip() or None,
            str(r.race_class or "").strip() or None,
            int(r.distance or 0) or None,
            draw,
            bucket,
        )
        row = acc.get(k)
        if row is None:
            row = {"samples": 0, "win_cnt": 0, "place_cnt": 0}
            acc[k] = row
        row["samples"] += 1
        if r.is_win is True:
            row["win_cnt"] += 1
        if r.is_place is True:
            row["place_cnt"] += 1

    out_rows = []
    for (d, venue, surface, course_type, going_code, race_class, distance, draw, bucket), v in acc.items():
        out_rows.append(
            DrawStatsDaily(
                race_date_day=d,
                venue=venue,
                surface=surface,
                course_type=course_type,
                going_code=going_code,
                race_class=race_class,
                distance=distance,
                draw=int(draw),
                odds_bucket_sp=str(bucket),
                samples=int(v["samples"]),
                win_cnt=int(v["win_cnt"]),
                place_cnt=int(v["place_cnt"]),
            )
        )

    for row in out_rows:
        session.add(row)
    session.commit()
    return {"ok": True, "date": date_str, "rows": len(out_rows)}

