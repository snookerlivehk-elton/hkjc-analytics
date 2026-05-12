import math
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from database.models import EntryFact, Race, RaceEntry, RacePaceForecastSnapshot
from scoring_engine.normalization import surface_code


PACE_UNKNOWN = "unknown"
PACE_VERY_FAST = "very_fast"
PACE_FAST = "fast"
PACE_MODERATE_FAST = "moderate_fast"
PACE_MODERATE = "moderate"
PACE_MODERATE_SLOW = "moderate_slow"
PACE_SLOW = "slow"
PACE_VERY_SLOW = "very_slow"


PACE_ZH = {
    PACE_VERY_FAST: "極快步速",
    PACE_FAST: "快步速",
    PACE_MODERATE_FAST: "中等偏快步速",
    PACE_MODERATE: "中等步速",
    PACE_MODERATE_SLOW: "中等偏慢步速",
    PACE_SLOW: "慢步速",
    PACE_VERY_SLOW: "極慢步速",
    PACE_UNKNOWN: "—",
}


def _front_score(p_leader: float, p_prom: float) -> float:
    try:
        a = float(p_leader)
        b = float(p_prom)
    except Exception:
        return 0.0
    if not math.isfinite(a) or not math.isfinite(b):
        return 0.0
    a = max(0.0, min(1.0, a))
    b = max(0.0, min(1.0, b))
    return float(a + 0.6 * b)


def _calc_confidence(samples: List[int], field_size: int) -> str:
    if field_size <= 0 or not samples:
        return "low"
    ok = sum(1 for x in samples if int(x or 0) >= 3)
    coverage = ok / float(field_size)
    avg_n = sum(int(x or 0) for x in samples) / float(len(samples))
    if coverage >= 0.7 and avg_n >= 6:
        return "high"
    if coverage >= 0.45 and avg_n >= 4:
        return "mid"
    return "low"


def _pace_class_from_front(front_sum: float, leader_count: int, front_count: int) -> str:
    fs = float(front_sum or 0.0)
    lc = int(leader_count or 0)
    fc = int(front_count or 0)

    if lc >= 2 or fc >= 3 or fs >= 2.4:
        return PACE_VERY_FAST
    if lc >= 1 and (fc >= 2 or fs >= 1.7):
        return PACE_FAST
    if fc >= 1 or fs >= 1.1:
        return PACE_MODERATE_FAST
    if lc == 0 and fs <= 0.30:
        return PACE_VERY_SLOW
    if lc == 0 and fs <= 0.70:
        return PACE_SLOW
    if fs <= 0.95:
        return PACE_MODERATE_SLOW
    return PACE_MODERATE


def compute_race_pace_forecast_for_race(
    session: Session,
    *,
    race_id: int,
    sample_n: int = 10,
) -> Dict[str, Any]:
    race = session.query(Race).filter_by(id=int(race_id)).first()
    if not race or not getattr(race, "race_date", None):
        return {"ok": False, "reason": "race_not_found"}

    cutoff_day = race.race_date.date()

    entries = (
        session.query(RaceEntry.id, RaceEntry.horse_id, RaceEntry.horse_no)
        .filter(RaceEntry.race_id == int(race_id))
        .all()
    )
    if not entries:
        return {"ok": False, "reason": "no_entries"}

    horse_ids = [int(hid or 0) for _, hid, _ in entries if int(hid or 0) > 0]
    horse_ids = list(dict.fromkeys(horse_ids))
    field_size = int(len(entries))

    rows = (
        session.query(EntryFact.horse_id, EntryFact.runstyle_bucket)
        .filter(EntryFact.horse_id.in_(horse_ids))
        .filter(EntryFact.race_date_day < cutoff_day)
        .filter(EntryFact.runstyle_bucket != None)
        .filter(EntryFact.runstyle_bucket != "UNKNOWN")
        .order_by(EntryFact.horse_id.asc(), EntryFact.race_date_day.desc(), EntryFact.id.desc())
        .all()
    )

    seq_by_hid: Dict[int, List[str]] = {int(h): [] for h in horse_ids}
    for hid, b in rows:
        h = int(hid or 0)
        if h <= 0:
            continue
        seq = seq_by_hid.get(h)
        if seq is None:
            continue
        if len(seq) >= int(sample_n or 0):
            continue
        bb = str(b or "").strip()
        if not bb or bb == "UNKNOWN":
            continue
        seq.append(bb)

    entry_map = {int(hid): int(hno or 0) for _, hid, hno in entries if int(hid or 0) > 0}

    front_sum = 0.0
    front_count = 0
    leader_count = 0
    samples = []

    horses_out = []
    for hid in horse_ids:
        seq = seq_by_hid.get(int(hid)) or []
        n = int(len(seq))
        samples.append(n)
        if n <= 0:
            horses_out.append(
                {
                    "horse_id": int(hid),
                    "horse_no": int(entry_map.get(int(hid)) or 0) or None,
                    "n": 0,
                    "p_leader": 0.0,
                    "p_prominent": 0.0,
                    "front_score": 0.0,
                }
            )
            continue

        c_leader = sum(1 for x in seq if str(x) == "LEADER")
        c_prom = sum(1 for x in seq if str(x) == "PROMINENT")
        p_leader = c_leader / float(n)
        p_prom = c_prom / float(n)
        fs = _front_score(p_leader, p_prom)
        front_sum += float(fs)
        if fs >= 0.55:
            front_count += 1
        if p_leader >= 0.35:
            leader_count += 1

        horses_out.append(
            {
                "horse_id": int(hid),
                "horse_no": int(entry_map.get(int(hid)) or 0) or None,
                "n": int(n),
                "p_leader": float(round(p_leader, 4)),
                "p_prominent": float(round(p_prom, 4)),
                "front_score": float(round(fs, 4)),
            }
        )

    horses_out.sort(key=lambda x: float(x.get("front_score") or 0.0), reverse=True)
    top_push = [x for x in horses_out if float(x.get("front_score") or 0.0) > 0][:5]

    conf = _calc_confidence(samples, field_size=field_size)
    pace_class = _pace_class_from_front(front_sum, leader_count=leader_count, front_count=front_count)

    sc = surface_code(getattr(race, "surface", None), course_type=getattr(race, "course_type", None))
    if sc not in {"TURF", "AW"}:
        sc = "TURF"

    row = session.query(RacePaceForecastSnapshot).filter_by(race_id=int(race_id)).first()
    if not row:
        row = RacePaceForecastSnapshot(race_id=int(race_id))
        session.add(row)

    row.race_date_day = cutoff_day
    row.venue = str(getattr(race, "venue", "") or "").strip()
    row.race_no = int(getattr(race, "race_no", 0) or 0)
    row.distance = int(getattr(race, "distance", 0) or 0) or None
    row.surface_code = sc
    row.race_class = str(getattr(race, "race_class", "") or "").strip() or None

    row.sample_n = int(sample_n or 0)
    row.field_size = int(field_size)
    row.front_count = int(front_count)
    row.leader_count = int(leader_count)
    row.front_sum = float(round(float(front_sum), 6))
    row.pace_class = str(pace_class)
    row.confidence = str(conf)
    row.meta = {
        "schema": "pace_forecast:v1",
        "cutoff_day": cutoff_day.isoformat(),
        "sample_n": int(sample_n or 0),
        "horses": horses_out,
        "top_push": top_push,
        "computed_at": datetime.utcnow().isoformat(),
    }
    row.computed_at = datetime.utcnow()
    session.commit()

    return {
        "ok": True,
        "race_id": int(race_id),
        "pace_class": str(pace_class),
        "pace_label_zh": str(PACE_ZH.get(str(pace_class), "—")),
        "confidence": str(conf),
        "front_sum": float(front_sum),
        "front_count": int(front_count),
        "leader_count": int(leader_count),
        "field_size": int(field_size),
        "top_push": top_push,
    }
