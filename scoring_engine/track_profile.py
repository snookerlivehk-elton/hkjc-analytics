from __future__ import annotations

from datetime import datetime
from statistics import median
from typing import Any, Dict, List, Optional, Tuple
import re
from math import ceil

from sqlalchemy.orm import Session

from database.models import Race, RaceEntry, RaceResult, RaceTrackCondition, SystemConfig, RaceDividend
from scoring_engine.track_conditions import normalize_going


def _venue_code(venue: str) -> str:
    v = str(venue or "")
    if "跑馬地" in v or "HV" in v:
        return "HV"
    return "ST"


def _dist_bucket(distance: Optional[int]) -> str:
    try:
        d = int(distance or 0)
    except Exception:
        d = 0
    if d <= 0:
        return "U"
    if d <= 1200:
        return "S"
    if d <= 1600:
        return "M"
    return "L"


def _parse_positions(runpos: str) -> List[int]:
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


def _pos_to_band(pos: Optional[int], field_size: int) -> Optional[str]:
    if pos is None or field_size <= 0:
        return None
    fs = int(field_size)
    if fs <= 0:
        return None
    lead_th = max(1, int((fs * 0.25) + 0.9999))
    mid_th = max(1, int((fs * 0.60) + 0.9999))
    p = int(pos)
    if p <= lead_th:
        return "front"
    if p <= mid_th:
        return "mid"
    return "back"


STYLE_LABELS = {"front": "前領", "mid": "中置", "back": "後上"}
COMPOSITE_LABELS = {
    "front_hold": "前領續航",
    "stalk": "跟前",
    "mid": "中置",
    "closer": "後上",
    "fade": "早放後散",
}
PACE_LABELS = {"fast": "快步速", "normal": "正常步速", "slow": "慢步速"}


def _pct(counts: Dict[str, int], keys: List[str]) -> Dict[str, float]:
    total = sum(int(counts.get(k) or 0) for k in keys)
    if total <= 0:
        return {k: 0.0 for k in keys}
    out = {}
    for k in keys:
        out[k] = round((int(counts.get(k) or 0) / total) * 100.0, 1)
    return out


def _safe_avg(vals: List[float]) -> Optional[float]:
    v = [float(x) for x in vals if x is not None]
    if not v:
        return None
    return round(sum(v) / len(v), 2)


def _safe_median(vals: List[float]) -> Optional[float]:
    v = [float(x) for x in vals if x is not None]
    if not v:
        return None
    return round(float(median(v)), 2)

def _safe_mad(vals: List[float], med: float) -> Optional[float]:
    v = [abs(float(x) - float(med)) for x in vals if x is not None]
    if not v:
        return None
    return round(float(median(v)), 4)


def _load_runpos_snapshot(session: Session, date_str: str, race_no: int) -> Dict[str, str]:
    key = f"race_runpos:{date_str}:{int(race_no)}"
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if not cfg or not isinstance(cfg.value, dict):
        return {}
    runpos = cfg.value.get("runpos")
    if not isinstance(runpos, dict):
        return {}
    out = {}
    for k, v in runpos.items():
        kk = str(k).strip()
        vv = str(v or "").strip()
        if kk and vv:
            out[kk] = vv
    return out


def _trkprof_key(venue: str, going_code: str, course: str, dist_bucket: str) -> str:
    return f"trkprof:{venue}:{going_code}:{course}:{dist_bucket}"

def _pacebase_key(venue: str, surface: str, dist_bucket: str) -> str:
    return f"pacebase:{venue}:{surface}:{dist_bucket}"

def _surface_code(race: Race) -> str:
    s = str(getattr(race, "surface", "") or "").strip()
    if s:
        if "泥" in s or "全天候" in s:
            return "AW"
        if "草" in s:
            return "TURF"
    t = str(getattr(race, "track_type", "") or "").upper()
    if any(x in t for x in ["ALL WEATHER", "A/W", "AW"]):
        return "AW"
    if "TURF" in t:
        return "TURF"
    return "U"

def _classify_pace(first_split: Optional[float], baseline: Optional[Dict[str, Any]]) -> Optional[str]:
    if first_split is None or not baseline:
        return None
    try:
        med = float(baseline.get("median") or 0.0)
        mad = float(baseline.get("mad") or 0.0)
    except Exception:
        return None
    if med <= 0:
        return None
    thr = max(0.3, mad * 1.0)
    v = float(first_split)
    if v < (med - thr):
        return "fast"
    if v > (med + thr):
        return "slow"
    return "normal"

def _composite_style(early: Optional[str], late: Optional[str]) -> Optional[str]:
    if not early or not late:
        return None
    if early == "front" and late in ("front", "mid"):
        return "front_hold"
    if early in ("front", "mid") and late == "back":
        return "fade"
    if early == "back" and late in ("mid", "front"):
        return "closer"
    if early == "mid" and late in ("mid", "front"):
        return "stalk"
    return "mid"

def _style_parts_from_runpos(runpos: str, field_size: int) -> Dict[str, Optional[str]]:
    pos = _parse_positions(runpos)
    if not pos:
        return {"early": None, "mid": None, "late": None, "composite": None}
    early_pos = pos[0]
    mid_pos = pos[len(pos) // 2]
    late_pos = pos[-1]
    early = _pos_to_band(early_pos, field_size)
    mid = _pos_to_band(mid_pos, field_size)
    late = _pos_to_band(late_pos, field_size)
    comp = _composite_style(early, late)
    return {"early": early, "mid": mid, "late": late, "composite": comp}


def compute_track_profiles(
    session: Session,
    min_date: Optional[datetime] = None,
    max_date: Optional[datetime] = None,
    limit_races: int = 5000,
) -> Dict[str, Any]:
    q = session.query(Race).order_by(Race.race_date.asc(), Race.race_no.asc())
    if min_date is not None:
        q = q.filter(Race.race_date >= min_date)
    if max_date is not None:
        q = q.filter(Race.race_date <= max_date)
    races = q.limit(int(limit_races or 5000)).all()

    agg: Dict[str, Dict[str, Any]] = {}
    seen_races = 0
    pacebase_samples: Dict[str, List[float]] = {}
    race_rows: List[Dict[str, Any]] = []

    for race in races:
        seen_races += 1
        venue = _venue_code(race.venue)
        surface = _surface_code(race)
        raw_course = str(race.course_type or "").strip()
        course = raw_course or ("AWT" if surface == "AW" else "U")
        dist_b = _dist_bucket(race.distance)

        tc = session.query(RaceTrackCondition).filter_by(race_id=race.id).first()
        going_code = str(getattr(tc, "going_code", "") or "").strip()
        if not going_code:
            _, going_code2 = normalize_going(str(race.going or ""))
            going_code = str(going_code2 or "").strip()
        if not going_code:
            continue

        date_str = race.race_date.strftime("%Y/%m/%d")
        runpos_map = _load_runpos_snapshot(session, date_str, int(race.race_no))

        entries = session.query(RaceEntry).filter_by(race_id=race.id).all()
        ranked = []
        for e in entries:
            rr = getattr(e, "result", None)
            if rr is None:
                continue
            try:
                rk = int(getattr(rr, "rank", 0) or 0)
            except Exception:
                rk = 0
            if rk <= 0:
                continue
            ranked.append((rk, e, rr))
        if not ranked:
            continue

        field_size = len(ranked)
        ranked.sort(key=lambda x: x[0])
        winner = ranked[0] if ranked else None
        top4 = [x for x in ranked if x[0] <= 4]
        if not winner or not top4:
            continue

        div0 = session.query(RaceDividend).filter_by(race_id=int(race.id)).first()
        meta0 = div0.meta if (div0 and isinstance(div0.meta, dict)) else {}
        sec = meta0.get("sectional_times")
        first_split = None
        if isinstance(sec, list) and sec:
            try:
                first_split = float(sec[0])
            except Exception:
                first_split = None
        if first_split is not None and first_split > 0:
            pbk = _pacebase_key(venue, surface, dist_b)
            pacebase_samples.setdefault(pbk, []).append(float(first_split))

        race_rows.append(
            {
                "race": race,
                "venue": venue,
                "surface": surface,
                "course": course,
                "dist_b": dist_b,
                "going_code": going_code,
                "date_str": date_str,
                "runpos_map": runpos_map,
                "field_size": field_size,
                "winner": winner,
                "top4": top4,
                "first_split": first_split,
            }
        )

    pacebase = {}
    for k, vals in pacebase_samples.items():
        v = [float(x) for x in vals if x is not None and float(x) > 0]
        if len(v) < 12:
            continue
        med = float(median(v))
        mad = _safe_mad(v, med)
        pacebase[k] = {"n": int(len(v)), "median": float(round(med, 4)), "mad": float(mad or 0.0)}

    for row in race_rows:
        venue = row["venue"]
        course = row["course"]
        dist_b = row["dist_b"]
        going_code = row["going_code"]
        surface = row["surface"]
        runpos_map = row["runpos_map"]
        field_size = int(row["field_size"] or 0)
        winner = row["winner"]
        top4 = row["top4"]

        key = _trkprof_key(venue, going_code, course, dist_b)
        if key not in agg:
            agg[key] = {
                "venue": venue,
                "going_code": going_code,
                "course_type": course,
                "dist_bucket": dist_b,
                "surface": surface,
                "n_races": 0,
                "winner_early": {"front": 0, "mid": 0, "back": 0},
                "winner_mid": {"front": 0, "mid": 0, "back": 0},
                "winner_late": {"front": 0, "mid": 0, "back": 0},
                "top4_early": {"front": 0, "mid": 0, "back": 0},
                "top4_mid": {"front": 0, "mid": 0, "back": 0},
                "top4_late": {"front": 0, "mid": 0, "back": 0},
                "winner_comp": {"front_hold": 0, "stalk": 0, "mid": 0, "closer": 0, "fade": 0},
                "top4_comp": {"front_hold": 0, "stalk": 0, "mid": 0, "closer": 0, "fade": 0},
                "pace_winner": {"fast": 0, "normal": 0, "slow": 0},
                "pace_top4": {"fast": 0, "normal": 0, "slow": 0},
                "pace_races": 0,
                "winner_win_odds": [],
                "top4_win_odds": [],
                "updated_at": None,
            }

        st = agg[key]
        st["n_races"] = int(st.get("n_races") or 0) + 1

        pbk = _pacebase_key(venue, surface, dist_b)
        pace_tag = _classify_pace(row.get("first_split"), pacebase.get(pbk))
        if pace_tag:
            st["pace_races"] = int(st.get("pace_races") or 0) + 1
            st["pace_winner"][pace_tag] = int(st["pace_winner"].get(pace_tag) or 0) + 1
            for _rk, _e, _rr in top4:
                st["pace_top4"][pace_tag] = int(st["pace_top4"].get(pace_tag) or 0) + 1

        def add_styles(e: RaceEntry, prefix: str):
            hn = str(int(getattr(e, "horse_no", 0) or 0))
            pos = runpos_map.get(hn) or ""
            parts = _style_parts_from_runpos(pos, field_size)
            for k2 in ("early", "mid", "late"):
                v2 = parts.get(k2)
                if v2:
                    st[f"{prefix}_{k2}"][v2] = int(st[f"{prefix}_{k2}"].get(v2) or 0) + 1
            comp = parts.get("composite")
            if comp:
                st[f"{prefix}_comp"][comp] = int(st[f"{prefix}_comp"].get(comp) or 0) + 1

        add_styles(winner[1], "winner")
        for _rk, e, _rr in top4:
            add_styles(e, "top4")

        def add_odds(rr: RaceResult, target: str):
            try:
                o = float(getattr(rr, "win_odds", None))
            except Exception:
                o = None
            if o is not None and o > 0:
                st[target].append(o)

        add_odds(winner[2], "winner_win_odds")
        for _rk, _e, rr in top4:
            add_odds(rr, "top4_win_odds")

        st["updated_at"] = datetime.utcnow().isoformat()

    index = []
    for key, st in agg.items():
        w_early = _pct(st["winner_early"], ["front", "mid", "back"])
        w_mid = _pct(st["winner_mid"], ["front", "mid", "back"])
        w_late = _pct(st["winner_late"], ["front", "mid", "back"])
        t_early = _pct(st["top4_early"], ["front", "mid", "back"])
        t_mid = _pct(st["top4_mid"], ["front", "mid", "back"])
        t_late = _pct(st["top4_late"], ["front", "mid", "back"])

        w_comp = _pct(st["winner_comp"], ["front_hold", "stalk", "mid", "closer", "fade"])
        t_comp = _pct(st["top4_comp"], ["front_hold", "stalk", "mid", "closer", "fade"])

        pw = _pct(st["pace_winner"], ["fast", "normal", "slow"])
        pt = _pct(st["pace_top4"], ["fast", "normal", "slow"])

        val = {
            "venue": st["venue"],
            "going_code": st["going_code"],
            "course_type": st["course_type"],
            "dist_bucket": st["dist_bucket"],
            "surface": st.get("surface"),
            "n_races": int(st.get("n_races") or 0),
            "winner_style_pct": {STYLE_LABELS[k]: v for k, v in w_early.items()},
            "top4_style_pct": {STYLE_LABELS[k]: v for k, v in t_early.items()},
            "winner_style_early_pct": {STYLE_LABELS[k]: v for k, v in w_early.items()},
            "winner_style_mid_pct": {STYLE_LABELS[k]: v for k, v in w_mid.items()},
            "winner_style_late_pct": {STYLE_LABELS[k]: v for k, v in w_late.items()},
            "top4_style_early_pct": {STYLE_LABELS[k]: v for k, v in t_early.items()},
            "top4_style_mid_pct": {STYLE_LABELS[k]: v for k, v in t_mid.items()},
            "top4_style_late_pct": {STYLE_LABELS[k]: v for k, v in t_late.items()},
            "winner_style_composite_pct": {COMPOSITE_LABELS[k]: v for k, v in w_comp.items()},
            "top4_style_composite_pct": {COMPOSITE_LABELS[k]: v for k, v in t_comp.items()},
            "pace_races": int(st.get("pace_races") or 0),
            "winner_pace_pct": {PACE_LABELS[k]: v for k, v in pw.items()},
            "top4_pace_pct": {PACE_LABELS[k]: v for k, v in pt.items()},
            "winner_win_odds_avg": _safe_avg(st["winner_win_odds"]),
            "winner_win_odds_median": _safe_median(st["winner_win_odds"]),
            "top4_win_odds_avg": _safe_avg(st["top4_win_odds"]),
            "top4_win_odds_median": _safe_median(st["top4_win_odds"]),
            "updated_at": st.get("updated_at"),
        }
        cfg = session.query(SystemConfig).filter_by(key=key).first()
        if not cfg:
            cfg = SystemConfig(key=key, description="跑道/場地狀態統計（跑法分布＋賠率）")
            session.add(cfg)
        cfg.value = val
        index.append({"key": key, "n_races": val["n_races"], "updated_at": val["updated_at"]})

    idx_cfg = session.query(SystemConfig).filter_by(key="trkprof_index").first()
    if not idx_cfg:
        idx_cfg = SystemConfig(key="trkprof_index", description="跑道/場地狀態統計索引")
        session.add(idx_cfg)
    idx_cfg.value = {"updated_at": datetime.utcnow().isoformat(), "seen_races": int(seen_races), "items": index}
    session.commit()

    return {"ok": True, "groups": len(agg), "seen_races": int(seen_races), "index": index}


def load_track_profile(
    session: Session, venue: str, going_code: str, course_type: str, distance: Optional[int]
) -> Optional[Dict[str, Any]]:
    key = _trkprof_key(_venue_code(venue), str(going_code or "").strip(), str(course_type or "").strip() or "U", _dist_bucket(distance))
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if not cfg or not isinstance(cfg.value, dict):
        return None
    val = dict(cfg.value)
    val["key"] = key
    return val
