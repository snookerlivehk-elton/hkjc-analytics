import hashlib
import json
from datetime import datetime, time as dtime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from database.models import Race, RaceEntry, SystemConfig


HK_TZ = ZoneInfo("Asia/Hong_Kong")


def _day_range(date_str: str) -> Tuple[datetime, datetime]:
    d0 = datetime.strptime(str(date_str), "%Y/%m/%d").date()
    start = datetime.combine(d0, dtime.min)
    end = start + timedelta(days=1)
    return start, end


def _get_cfg(session: Session, key: str) -> Optional[SystemConfig]:
    return session.query(SystemConfig).filter_by(key=key).first()


def _get_cfg_value(session: Session, key: str):
    cfg = _get_cfg(session, key)
    return cfg.value if cfg else None


def _sha256_json(obj: Any) -> str:
    s = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def get_race_day_anchor_dt(session: Session, date_str: str) -> datetime:
    start, end = _day_range(date_str)
    rows = (
        session.query(Race.race_no, Race.post_time_hk)
        .filter(and_(Race.race_date >= start, Race.race_date < end))
        .order_by(Race.race_no.asc(), Race.id.asc())
        .all()
    )
    for rn, pt in rows:
        if pt and isinstance(pt, str) and pt.strip():
            s = pt.strip()
            try:
                hh, mm = s.split(":")
                hh_i = int(hh)
                mm_i = int(mm)
                if 0 <= hh_i <= 23 and 0 <= mm_i <= 59:
                    d0 = datetime.strptime(str(date_str), "%Y/%m/%d").date()
                    return datetime.combine(d0, dtime(hh_i, mm_i)).replace(tzinfo=HK_TZ)
            except Exception:
                continue

    v = _get_cfg_value(session, "race_day_anchor_time_hk")
    t = "12:00"
    if isinstance(v, str) and v.strip():
        t = v.strip()
    try:
        hh, mm = t.split(":")
        hh_i = int(hh)
        mm_i = int(mm)
        if hh_i < 0 or hh_i > 23 or mm_i < 0 or mm_i > 59:
            raise ValueError("bad_time")
        tt = dtime(hh_i, mm_i)
    except Exception:
        tt = dtime(12, 0)
    d0 = datetime.strptime(str(date_str), "%Y/%m/%d").date()
    return datetime.combine(d0, tt).replace(tzinfo=HK_TZ)


def get_speedpro_readiness(
    session: Session,
    *,
    date_str: str,
    min_coverage: float = 0.85,
) -> Dict[str, Any]:
    if min_coverage > 1.0:
        min_coverage = min_coverage / 100.0
    if min_coverage < 0.0:
        min_coverage = 0.0
    if min_coverage > 1.0:
        min_coverage = 1.0

    start, end = _day_range(date_str)
    races = (
        session.query(Race.id, Race.race_no)
        .filter(and_(Race.race_date >= start, Race.race_date < end))
        .order_by(Race.race_no.asc(), Race.id.asc())
        .all()
    )
    if not races:
        return {
            "ok": False,
            "reason": "no_races",
            "min_coverage": min_coverage,
            "races": [],
        }

    out_races: List[Dict[str, Any]] = []
    ok_all = True
    for race_id, race_no in races:
        exp_cnt = (
            session.query(func.count(RaceEntry.id))
            .filter(RaceEntry.race_id == int(race_id))
            .scalar()
        )
        exp_cnt = int(exp_cnt or 0)
        if exp_cnt <= 0:
            out_races.append(
                {
                    "race_no": int(race_no or 0),
                    "expected": exp_cnt,
                    "both": 0,
                    "coverage": 0.0,
                    "ok": False,
                    "reason": "no_entries",
                }
            )
            ok_all = False
            continue

        snap_key = f"speedpro_energy:{date_str}:{int(race_no)}"
        snap = _get_cfg_value(session, snap_key)
        if not isinstance(snap, dict) or not snap:
            out_races.append(
                {
                    "race_no": int(race_no or 0),
                    "expected": exp_cnt,
                    "both": 0,
                    "coverage": 0.0,
                    "ok": False,
                    "reason": "missing_speedpro",
                }
            )
            ok_all = False
            continue

        entries = (
            session.query(RaceEntry.horse_no)
            .filter(RaceEntry.race_id == int(race_id))
            .all()
        )
        horse_nos = [int(x[0] or 0) for x in entries if int(x[0] or 0) > 0]
        both = 0
        for hn in horse_nos:
            row = snap.get(str(int(hn)))
            if not isinstance(row, dict):
                continue
            ea = row.get("energy_assess")
            sr = row.get("status_rating")
            if ea is not None and sr is not None:
                both += 1
        cov = both / float(exp_cnt) if exp_cnt else 0.0
        ok = cov >= float(min_coverage)
        if not ok:
            ok_all = False
        out_races.append(
            {
                "race_no": int(race_no or 0),
                "expected": exp_cnt,
                "both": both,
                "coverage": cov,
                "ok": ok,
                "reason": "ok" if ok else "low_coverage",
            }
        )

    return {
        "ok": bool(ok_all),
        "reason": "ok" if ok_all else "not_ready",
        "min_coverage": float(min_coverage),
        "races": out_races,
    }


def compute_speedpro_day_hash(session: Session, *, date_str: str) -> str:
    start, end = _day_range(date_str)
    races = (
        session.query(Race.race_no)
        .filter(and_(Race.race_date >= start, Race.race_date < end))
        .order_by(Race.race_no.asc())
        .all()
    )
    parts: List[str] = []
    for (race_no,) in races:
        info_key = f"speedpro_energy_info:{date_str}:{int(race_no)}"
        info = _get_cfg_value(session, info_key)
        raw_hash = ""
        if isinstance(info, dict):
            raw_hash = str(info.get("raw_hash") or "").strip()
        if not raw_hash:
            snap_key = f"speedpro_energy:{date_str}:{int(race_no)}"
            snap = _get_cfg_value(session, snap_key)
            if isinstance(snap, dict) and snap:
                raw_hash = _sha256_json(snap)
        parts.append(f"{int(race_no)}:{raw_hash}")
    return _sha256_json(parts)
