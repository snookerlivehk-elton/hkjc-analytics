from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from statistics import median
from typing import Any, Dict, List, Optional, Tuple
import re

from sqlalchemy.orm import Session

from database.models import Race, RaceEntry, RaceResult, RaceTrackCondition, SystemConfig
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


def _parse_first_pos(runpos: str) -> Optional[int]:
    s = str(runpos or "").strip()
    if not s:
        return None
    m = re.search(r"\d+", s)
    if not m:
        return None
    try:
        v = int(m.group(0))
    except Exception:
        return None
    return v if v > 0 else None


def _style_from_runpos(runpos: str, field_size: int) -> Optional[str]:
    fp = _parse_first_pos(runpos)
    if fp is None or field_size <= 0:
        return None
    fs = int(field_size)
    if fs <= 0:
        return None
    lead_th = max(1, int((fs * 0.25) + 0.9999))
    mid_th = max(1, int((fs * 0.60) + 0.9999))
    if fp <= lead_th:
        return "front"
    if fp <= mid_th:
        return "mid"
    return "back"


STYLE_LABELS = {"front": "前領", "mid": "中置", "back": "後上"}


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

    for race in races:
        seen_races += 1
        venue = _venue_code(race.venue)
        course = str(race.course_type or "").strip() or "U"
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

        key = _trkprof_key(venue, going_code, course, dist_b)
        if key not in agg:
            agg[key] = {
                "venue": venue,
                "going_code": going_code,
                "course_type": course,
                "dist_bucket": dist_b,
                "n_races": 0,
                "winner_style_counts": {"front": 0, "mid": 0, "back": 0},
                "top4_style_counts": {"front": 0, "mid": 0, "back": 0},
                "winner_win_odds": [],
                "top4_win_odds": [],
                "updated_at": None,
            }

        st = agg[key]
        st["n_races"] = int(st.get("n_races") or 0) + 1

        def add_style(e: RaceEntry, target: str):
            hn = str(int(getattr(e, "horse_no", 0) or 0))
            pos = runpos_map.get(hn) or ""
            style = _style_from_runpos(pos, field_size)
            if style:
                st[target][style] = int(st[target].get(style) or 0) + 1

        add_style(winner[1], "winner_style_counts")
        for _, e, _rr in top4:
            add_style(e, "top4_style_counts")

        def add_odds(rr: RaceResult, target: str):
            try:
                o = float(getattr(rr, "win_odds", None))
            except Exception:
                o = None
            if o is not None and o > 0:
                st[target].append(o)

        add_odds(winner[2], "winner_win_odds")
        for _, _, rr in top4:
            add_odds(rr, "top4_win_odds")

        st["updated_at"] = datetime.utcnow().isoformat()

    index = []
    for key, st in agg.items():
        w_pct = _pct(st["winner_style_counts"], ["front", "mid", "back"])
        t_pct = _pct(st["top4_style_counts"], ["front", "mid", "back"])
        val = {
            "venue": st["venue"],
            "going_code": st["going_code"],
            "course_type": st["course_type"],
            "dist_bucket": st["dist_bucket"],
            "n_races": int(st.get("n_races") or 0),
            "winner_style_pct": {STYLE_LABELS[k]: v for k, v in w_pct.items()},
            "top4_style_pct": {STYLE_LABELS[k]: v for k, v in t_pct.items()},
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
