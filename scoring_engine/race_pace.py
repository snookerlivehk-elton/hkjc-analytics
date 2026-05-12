import math
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from database.models import Race, RaceDividend, RacePaceSnapshot, SystemConfig
from scoring_engine.config_value import unwrap_value
from scoring_engine.normalization import surface_code


PACE_UNKNOWN = "unknown"
PACE_VERY_FAST = "very_fast"
PACE_FAST = "fast"
PACE_MODERATE_FAST = "moderate_fast"
PACE_MODERATE = "moderate"
PACE_MODERATE_SLOW = "moderate_slow"
PACE_SLOW = "slow"
PACE_VERY_SLOW = "very_slow"


def _class_candidates(race_class: Any) -> List[str]:
    raw = str(race_class or "").strip()
    out = []
    if raw:
        out.append(raw)
    m = re.search(r"([1-5])", raw)
    if m:
        n = str(m.group(1))
        out.extend([f"Class {n}", f"第{n}班", n])
    return [x for x in out if x]


def _sum_first_k(vals: List[float], k: int) -> Optional[float]:
    if not isinstance(vals, list) or not vals:
        return None
    if k <= 0:
        return None
    if len(vals) < k:
        return None
    s = 0.0
    for i in range(k):
        try:
            v = float(vals[i])
        except Exception:
            return None
        if not math.isfinite(v) or v <= 0:
            return None
        s += v
    return float(s)


def _pick_k(distance: Optional[int], actual_segs: List[float], ref_segs: List[float]) -> Optional[int]:
    try:
        d = int(distance or 0)
    except Exception:
        d = 0
    has2 = (len(actual_segs) >= 2) and (len(ref_segs) >= 2)
    has3 = (len(actual_segs) >= 3) and (len(ref_segs) >= 3)
    if d >= 1400 and has3:
        return 3
    if has2:
        return 2
    if has3:
        return 3
    return None


def _pace_class_7(delta_sec: Optional[float], *, first_fast_flag: bool = False) -> str:
    if delta_sec is None:
        return PACE_UNKNOWN
    try:
        d = float(delta_sec)
    except Exception:
        return PACE_UNKNOWN
    if not math.isfinite(d):
        return PACE_UNKNOWN
    if d <= -1.0:
        return PACE_VERY_FAST
    if d <= -0.3:
        return PACE_VERY_FAST if first_fast_flag else PACE_FAST
    if d < -0.2:
        return PACE_MODERATE_FAST
    if d <= 0.2:
        return PACE_MODERATE
    if d < 0.5:
        return PACE_MODERATE_SLOW
    if d < 1.0:
        return PACE_SLOW
    return PACE_VERY_SLOW


def _load_course_time_reference(session: Session) -> Dict[str, Any]:
    row = session.query(SystemConfig).filter_by(key="course_time_reference:v1").first()
    payload, _ = unwrap_value(row.value) if row else (None, {})
    return payload if isinstance(payload, dict) else {}


def _lookup_ref_segment_times(
    course_time_ref: Dict[str, Any],
    *,
    venue: str,
    surface: str,
    distance: Optional[int],
    race_class: Any,
) -> Tuple[Optional[List[float]], Optional[str]]:
    if not isinstance(course_time_ref, dict):
        return None, None
    ref = course_time_ref.get("reference_sectionals") if isinstance(course_time_ref.get("reference_sectionals"), dict) else {}
    if not isinstance(ref, dict):
        return None, None
    try:
        d = int(distance or 0)
    except Exception:
        d = 0
    if d <= 0:
        return None, None
    tkey = f"{str(venue or '').strip().upper()}:{str(surface or '').strip().upper()}"
    dist_map = (ref.get(tkey) or {}).get(str(d)) if isinstance(ref.get(tkey), dict) else None
    if not isinstance(dist_map, dict) or not dist_map:
        return None, None
    chosen_cls = None
    node = None
    for ck in _class_candidates(race_class):
        v = dist_map.get(ck)
        if isinstance(v, dict) and isinstance(v.get("segment_times_sec"), list) and v.get("segment_times_sec"):
            node = v
            chosen_cls = ck
            break
    if node is None:
        for ck, v in dist_map.items():
            if isinstance(v, dict) and isinstance(v.get("segment_times_sec"), list) and v.get("segment_times_sec"):
                node = v
                chosen_cls = str(ck or "")
                break
    if not isinstance(node, dict):
        return None, None
    segs = node.get("segment_times_sec")
    if not isinstance(segs, list) or not segs:
        return None, None
    out = []
    for x in segs:
        try:
            f = float(x)
        except Exception:
            return None, None
        if not math.isfinite(f) or f <= 0:
            return None, None
        out.append(float(f))
    return out, chosen_cls


def compute_race_pace_for_race(session: Session, race_id: int) -> Dict[str, Any]:
    race = session.query(Race).filter_by(id=int(race_id)).first()
    if not race:
        return {"ok": False, "reason": "race_not_found"}
    div = session.query(RaceDividend).filter_by(race_id=int(race_id)).first()
    meta = div.meta if (div and isinstance(div.meta, dict)) else {}
    actual_segs = meta.get("sectional_times")
    if not isinstance(actual_segs, list) or not actual_segs:
        return {"ok": False, "reason": "no_sectional_times"}

    course_time_ref = _load_course_time_reference(session)
    sc = surface_code(getattr(race, "surface", None), course_type=getattr(race, "course_type", None))
    if sc not in {"TURF", "AW"}:
        sc = "TURF"
    ref_segs, ref_cls = _lookup_ref_segment_times(
        course_time_ref,
        venue=str(getattr(race, "venue", "") or ""),
        surface=sc,
        distance=getattr(race, "distance", None),
        race_class=getattr(race, "race_class", None),
    )
    if not isinstance(ref_segs, list) or not ref_segs:
        return {"ok": False, "reason": "no_reference_sectionals"}

    k = _pick_k(getattr(race, "distance", None), actual_segs, ref_segs)
    if not k:
        return {"ok": False, "reason": "not_enough_segments"}

    actual_sum = _sum_first_k(actual_segs, k)
    ref_sum = _sum_first_k(ref_segs, k)
    if actual_sum is None or ref_sum is None:
        return {"ok": False, "reason": "bad_segment_values"}

    delta = float(actual_sum) - float(ref_sum)
    first_fast_flag = False
    try:
        if len(actual_segs) >= 2 and len(ref_segs) >= 2:
            a1 = float(actual_segs[0])
            r1 = float(ref_segs[0])
            al = float(actual_segs[-1])
            rl = float(ref_segs[-1])
            first_delta = a1 - r1
            last_delta = al - rl
            if first_delta <= -0.3 and last_delta >= 0.3:
                first_fast_flag = True
    except Exception:
        first_fast_flag = False

    cls7 = _pace_class_7(delta, first_fast_flag=first_fast_flag)

    row = session.query(RacePaceSnapshot).filter_by(race_id=int(race_id)).first()
    if not row:
        row = RacePaceSnapshot(race_id=int(race_id))
        session.add(row)

    row.race_date_day = race.race_date.date()
    row.venue = str(getattr(race, "venue", "") or "").strip()
    row.race_no = int(getattr(race, "race_no", 0) or 0)
    row.distance = int(getattr(race, "distance", 0) or 0) or None
    row.surface_code = sc
    row.race_class = str(getattr(race, "race_class", "") or "").strip() or None
    row.k_segments = int(k)
    row.actual_sec = float(round(actual_sum, 4))
    row.ref_sec = float(round(ref_sum, 4))
    row.delta_sec = float(round(delta, 4))
    row.pace_class = str(cls7)
    row.meta = {
        "schema": "race_pace:v1",
        "k_segments": int(k),
        "ref_class_key": ref_cls,
        "actual_sectionals_sec": [float(x) for x in actual_segs],
        "ref_sectionals_sec": [float(x) for x in ref_segs],
        "first_fast_flag": bool(first_fast_flag),
        "computed_at": datetime.utcnow().isoformat(),
    }
    row.computed_at = datetime.utcnow()

    session.commit()
    return {"ok": True, "race_id": int(race_id), "pace_class": cls7, "delta_sec": float(delta), "k": int(k)}


def compute_race_pace_for_race_date(session: Session, *, date_str: str) -> Dict[str, Any]:
    try:
        d0 = datetime.strptime(str(date_str), "%Y/%m/%d").date()
    except Exception:
        return {"ok": False, "reason": "bad_date_str"}
    start = datetime(d0.year, d0.month, d0.day)
    end = datetime(d0.year, d0.month, d0.day, 23, 59, 59)
    races = (
        session.query(Race.id)
        .filter(Race.race_date >= start)
        .filter(Race.race_date <= end)
        .order_by(Race.race_no.asc(), Race.id.asc())
        .all()
    )
    race_ids = [int(x[0]) for x in races if x and x[0] is not None]
    ok = 0
    fail = 0
    last_err = None
    for rid in race_ids:
        res = compute_race_pace_for_race(session, int(rid))
        if res.get("ok"):
            ok += 1
        else:
            fail += 1
            last_err = res
    return {"ok": True, "date": str(date_str), "races": len(race_ids), "ok_races": ok, "failed": fail, "last_error": last_err}

