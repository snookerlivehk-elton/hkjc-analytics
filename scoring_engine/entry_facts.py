from __future__ import annotations

import re
from datetime import datetime, time as dtime, timedelta
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from database.models import EntryFact, Race, RaceEntry, RaceResult, RaceTrackCondition, SystemConfig
from scoring_engine.config_value import unwrap_value
from scoring_engine.normalization import surface_code
from scoring_engine.top5_odds_stats import bucket_odds


def _day_range(date_str: str) -> Tuple[datetime, datetime]:
    d0 = datetime.strptime(str(date_str), "%Y/%m/%d").date()
    start = datetime.combine(d0, dtime.min)
    end = start + timedelta(days=1)
    return start, end


def _parse_finish_time_to_seconds(s: Any) -> Optional[float]:
    v = str(s or "").strip().replace(" ", "").replace("．", ".").replace("：", ":")
    if not v:
        return None
    if ":" in v:
        parts = v.split(":")
        if len(parts) != 2:
            return None
        try:
            m = int(parts[0])
            sec = float(parts[1])
        except Exception:
            return None
        return (m * 60.0 + sec) if (m >= 0 and sec > 0) else None
    if v.count(".") >= 2:
        p = v.split(".")
        try:
            m = int(p[0])
            s2 = int(p[1])
            frac = int(p[2])
        except Exception:
            return None
        if s2 < 0 or s2 >= 60:
            return None
        return m * 60.0 + s2 + (frac / (100.0 if frac >= 10 else 10.0))
    try:
        sec = float(v)
    except Exception:
        return None
    return sec if sec > 0 else None


def _parse_positions(runpos: Any):
    s = str(runpos or "").strip()
    if not s:
        return []
    out = []
    for m in re.findall(r"\d{1,2}", s):
        try:
            v = int(m)
        except Exception:
            continue
        if v > 0:
            out.append(v)
    return out


def _runstyle_bucket(early_pos: Optional[int], field_size: Optional[int]) -> Optional[str]:
    try:
        p = int(early_pos or 0)
        fs = int(field_size or 0)
    except Exception:
        return None
    if p <= 0 or fs <= 0:
        return None
    ratio = float(p) / float(fs)
    if ratio <= 0.20:
        return "LEADER"
    if ratio <= 0.40:
        return "PROMINENT"
    if ratio <= 0.70:
        return "MIDFIELD"
    return "BACKMARKER"


def _class_candidates(race_class: Any):
    raw = str(race_class or "").strip()
    out = []
    if raw:
        out.append(raw)
    m = re.search(r"([1-5])", raw)
    if m:
        n = str(m.group(1))
        out.extend([f"Class {n}", f"第{n}班", n])
    return [x for x in out if x]


def _pace_bucket(delta_sec: Optional[float], standard_sec: Optional[float]) -> Optional[str]:
    if delta_sec is None or standard_sec is None:
        return None
    try:
        s = float(standard_sec)
        d = float(delta_sec)
    except Exception:
        return None
    if s <= 0:
        return None
    ratio = d / s
    if ratio <= -0.01:
        return "FAST"
    if ratio >= 0.01:
        return "SLOW"
    return "NORMAL"


def _load_course_time_reference(session: Session) -> Dict[str, Any]:
    row = session.query(SystemConfig).filter_by(key="course_time_reference:v1").first()
    payload, _ = unwrap_value(row.value) if row else (None, {})
    if isinstance(payload, dict):
        return payload
    return {}


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

    field_sizes: Dict[int, int] = {}
    try:
        fs_rows = (
            session.query(RaceEntry.race_id, func.count(RaceResult.id))
            .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
            .filter(RaceEntry.race_id.in_(race_ids))
            .filter(RaceResult.rank != None)
            .filter(RaceResult.rank > 0)
            .group_by(RaceEntry.race_id)
            .all()
        )
        field_sizes = {int(rid): int(cnt or 0) for rid, cnt in fs_rows if rid is not None}
    except Exception:
        field_sizes = {}

    runpos_by_race_no: Dict[int, Dict[str, str]] = {}
    try:
        rows_runpos = session.query(SystemConfig.key, SystemConfig.value).filter(SystemConfig.key.like(f"race_runpos:{date_str}:%")).all()
        for k, v in rows_runpos:
            kk = str(k or "").strip()
            try:
                rno = int(kk.split(":")[-1])
            except Exception:
                continue
            payload, _ = unwrap_value(v)
            m0 = {}
            if isinstance(payload, dict):
                if isinstance(payload.get("runpos"), dict):
                    m0 = payload.get("runpos") or {}
                else:
                    m0 = payload
            if isinstance(m0, dict) and m0:
                out0 = {str(hn): str(pos or "").strip() for hn, pos in m0.items() if hn is not None}
                runpos_by_race_no[rno] = out0
    except Exception:
        runpos_by_race_no = {}

    course_time_ref = _load_course_time_reference(session)
    standard_times = course_time_ref.get("standard_times") if isinstance(course_time_ref, dict) else {}
    if not isinstance(standard_times, dict):
        standard_times = {}

    q = (
        session.query(
            RaceEntry.id,
            RaceEntry.race_id,
            RaceEntry.horse_id,
            RaceEntry.jockey_id,
            RaceEntry.trainer_id,
            RaceEntry.horse_no,
            RaceEntry.draw,
            RaceEntry.rating,
            RaceResult.rank,
            RaceResult.win_odds,
            RaceResult.finish_time,
            RaceResult.finish_time_sec,
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
        horse_no,
        draw,
        rating,
        rank,
        win_odds,
        finish_time,
        finish_time_sec,
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

        early_pos = None
        rb = None
        try:
            hn = int(horse_no or 0)
        except Exception:
            hn = 0
        try:
            rno2 = int(race_no or 0)
        except Exception:
            rno2 = 0
        if hn > 0 and rno2 > 0:
            rp = (runpos_by_race_no.get(rno2) or {}).get(str(hn))
            pos = _parse_positions(rp)
            if pos:
                early_pos = int(pos[0])
                rb = _runstyle_bucket(early_pos, field_sizes.get(rid))
        ef.runpos_early = int(early_pos) if (early_pos is not None and int(early_pos) > 0) else None
        ef.runstyle_bucket = str(rb) if rb else "UNKNOWN"

        ft = None
        try:
            ft = float(finish_time_sec) if finish_time_sec is not None else None
        except Exception:
            ft = None
        if ft is None:
            ft = _parse_finish_time_to_seconds(finish_time)

        std_sec = None
        try:
            sc = surface_code(surface, course_type=course_type)
            if sc not in {"TURF", "AW"}:
                sc = "TURF"
            tkey = f"{str(venue or '').strip().upper()}:{sc}"
            dist_k = str(int(distance or 0))
            cls_map = (standard_times.get(tkey) or {}).get(dist_k) if isinstance(standard_times.get(tkey), dict) else None
            if isinstance(cls_map, dict):
                for ck in _class_candidates(race_class):
                    row = cls_map.get(ck)
                    if isinstance(row, dict) and row.get("standard_time_sec") is not None:
                        std_sec = float(row.get("standard_time_sec"))
                        break
                if std_sec is None:
                    for row in cls_map.values():
                        if isinstance(row, dict) and row.get("standard_time_sec") is not None:
                            std_sec = float(row.get("standard_time_sec"))
                            break
        except Exception:
            std_sec = None

        if ft is not None and std_sec is not None:
            delta = float(ft) - float(std_sec)
            ef.pace_delta_sec = float(delta)
            pb = _pace_bucket(delta, std_sec)
            ef.pace_bucket = str(pb) if pb else "UNKNOWN"
        else:
            ef.pace_delta_sec = None
            ef.pace_bucket = "UNKNOWN"

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
