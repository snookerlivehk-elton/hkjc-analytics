from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, Tuple, TYPE_CHECKING
from scoring_engine.track_conditions import normalize_going


if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from database.models import Race


_ZH_NUM = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5}


def normalize_race_class(raw: Any) -> str:
    t = str(raw or "").strip()
    if not t:
        return ""
    if ("新馬賽" in t) or ("新馬" in t):
        return "新馬賽"
    import re

    m = re.search(r"(?:Class\s*([1-5])|第\s*([1-5])\s*班|第\s*([一二三四五])\s*班)", t, re.IGNORECASE)
    if m:
        n = m.group(1) or m.group(2) or m.group(3)
        if n in _ZH_NUM:
            return f"第{n}班"
        try:
            return f"第{int(n)}班"
        except Exception:
            return f"第{str(n).strip()}班"
    m2 = re.search(r"(?:國際)?([一二三])級賽|Group\s*([1-3])|\bG\s*([1-3])\b", t, re.IGNORECASE)
    if m2:
        g = m2.group(1) or m2.group(2) or m2.group(3)
        if g in {"一", "二", "三"}:
            return f"{g}級賽"
        try:
            gi = int(g)
            return f"{['一', '二', '三'][gi - 1]}級賽" if 1 <= gi <= 3 else f"{gi}級賽"
        except Exception:
            return f"{str(g).strip()}級賽"
    if "平磅賽" in t:
        return "平磅賽"
    return ""


def venue_code(venue: Any, track_type: Any = None) -> str:
    v = str(venue or "").strip().upper()
    t = str(track_type or "").strip()
    if v == "HV" or ("跑馬地" in v) or ("跑馬地" in t) or ("HAPPY VALLEY" in t.upper()):
        return "HV"
    if v == "ST" or ("沙田" in v) or ("沙田" in t) or ("SHA TIN" in t.upper()):
        return "ST"
    if "HV" in v:
        return "HV"
    return "ST"


def venue_label(code_or_venue: Any, track_type: Any = None) -> str:
    c = str(code_or_venue or "").strip().upper()
    if c in {"HV", "HAPPY VALLEY"}:
        return "跑馬地"
    if c in {"ST", "SHA TIN"}:
        return "沙田"
    c2 = venue_code(code_or_venue, track_type=track_type)
    return "跑馬地" if c2 == "HV" else "沙田"


def surface_code(surface: Any = None, track_type: Any = None, course_type: Any = None) -> str:
    s = str(surface or "").strip()
    if s:
        if ("泥" in s) or ("全天候" in s):
            return "AW"
        if "草" in s:
            return "TURF"
    ct = str(course_type or "").strip().upper()
    if ct in {"AWT", "A/W", "AW"}:
        return "AW"
    t = str(track_type or "").strip().upper()
    if any(x in t for x in ["ALL WEATHER", "A/W", "AW", "AWT"]):
        return "AW"
    if "TURF" in t or "草" in t:
        return "TURF"
    return "U"


def normalize_course_type(course_type: Any, surface_code_: str = "") -> str:
    c = str(course_type or "").strip()
    cu = c.upper()
    sc = str(surface_code_ or "").strip().upper()
    if cu in {"AWT", "A/W", "AW"}:
        return "AWT"
    if not c:
        return "AWT" if sc == "AW" else "U"
    return c


def dist_bucket(distance: Any) -> str:
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


def going_code_for_race(session: Session, race: Race, override: Optional[str] = None) -> str:
    gc = str(override or "").strip()
    if gc:
        return gc
    from database.models import RaceTrackCondition
    try:
        tc = session.query(RaceTrackCondition).filter_by(race_id=int(getattr(race, "id", 0) or 0)).first()
    except Exception:
        tc = None
    code = str(getattr(tc, "going_code", "") or "").strip() if tc else ""
    if code:
        return code
    _, code2 = normalize_going(str(getattr(race, "going", "") or ""))
    return str(code2 or "").strip()


def bucket_parts(session: Session, race: Race, going_override: Optional[str] = None) -> Optional[Tuple[str, str, str, str]]:
    if not race:
        return None
    v = venue_code(getattr(race, "venue", ""), track_type=getattr(race, "track_type", None))
    g = going_code_for_race(session, race, override=going_override)
    if not g:
        return None
    sc = surface_code(getattr(race, "surface", None), track_type=getattr(race, "track_type", None), course_type=getattr(race, "course_type", None))
    c = normalize_course_type(getattr(race, "course_type", None), surface_code_=sc)
    d = dist_bucket(getattr(race, "distance", None))
    return v, g, c, d


def race_date_str(race: Race) -> str:
    rd = getattr(race, "race_date", None)
    if rd is not None and hasattr(rd, "strftime"):
        try:
            return rd.strftime("%Y/%m/%d")
        except Exception:
            pass
    return ""


def utc_now_iso() -> str:
    return datetime.utcnow().isoformat()
