from __future__ import annotations

from datetime import date, datetime, time as dtime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy.orm import Session

from scoring_engine.track_conditions import normalize_going
from scoring_engine.config_value import unwrap_value
from scoring_engine.normalization import normalize_course_type, surface_code, venue_code


def race_key(d: Any, venue: Any, race_no: Any) -> str:
    ds = ""
    if isinstance(d, str):
        ds = d.strip().replace("-", "/")
    elif d is not None and hasattr(d, "strftime"):
        ds = d.strftime("%Y/%m/%d")
    v = venue_code(venue)
    try:
        rn = int(race_no or 0)
    except Exception:
        rn = 0
    return f"{ds}:{v}:{rn}"


def race_day_key(d: Any, venue: Any) -> str:
    ds = ""
    if isinstance(d, str):
        ds = d.strip().replace("-", "/")
    elif d is not None and hasattr(d, "strftime"):
        ds = d.strftime("%Y/%m/%d")
    v = venue_code(venue)
    return f"{ds}:{v}"


def _clip_text(s: str, max_len: int = 5000) -> str:
    t = str(s or "")
    ml = int(max_len or 0)
    if ml > 0 and len(t) > ml:
        tail_len = min(2000, ml // 2)
        head_len = max(0, ml - tail_len)
        head = t[:head_len].rstrip()
        tail = t[-tail_len:].lstrip() if tail_len > 0 else ""
        if tail:
            return f"{head} ... {tail}"
        return head + "..."
    return t


def _day_range(d0: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(d0, dtime.min)
    end = start + timedelta(days=1)
    return start, end


def _race_day(race_date: Any) -> Optional[date]:
    if race_date is None:
        return None
    if isinstance(race_date, date) and not isinstance(race_date, datetime):
        return race_date
    if hasattr(race_date, "date"):
        try:
            return race_date.date()
        except Exception:
            return None
    return None


def _going_code_for_race(session: Session, race_id: int, race_going: Any) -> str:
    from database.models import RaceTrackCondition

    tc = session.query(RaceTrackCondition).filter_by(race_id=int(race_id)).first()
    code = str(getattr(tc, "going_code", "") or "").strip()
    if code:
        return code
    _, gc = normalize_going(str(race_going or ""))
    return str(gc or "").strip()


def upsert_search_document(
    session: Session,
    *,
    doc_type: str,
    ref_key: str,
    entity_type: str,
    entity_key: str,
    search_text: str,
    title: str = "",
    race_id: Optional[int] = None,
    race_date_day: Optional[date] = None,
    race_no: Optional[int] = None,
    venue: str = "",
    surface_code_: str = "",
    course_type: str = "",
    going_code: str = "",
    horse_name: str = "",
    jockey_name: str = "",
    trainer_name: str = "",
    payload_excerpt: Optional[Dict[str, Any]] = None,
) -> None:
    from database.models import SearchDocument

    dt = str(doc_type or "").strip()
    rk = str(ref_key or "").strip()
    if not dt or not rk:
        return
    row = session.query(SearchDocument).filter_by(doc_type=dt, ref_key=rk).first()
    if not row:
        row = SearchDocument(doc_type=dt, ref_key=rk, entity_type=str(entity_type or ""), entity_key=str(entity_key or ""))
        session.add(row)
    row.entity_type = str(entity_type or "")
    row.entity_key = str(entity_key or "")
    row.race_id = int(race_id) if race_id is not None else None
    row.race_date_day = race_date_day
    row.race_no = int(race_no) if race_no is not None else None
    row.venue = str(venue or "").strip() or None
    row.surface_code = str(surface_code_ or "").strip() or None
    row.course_type = str(course_type or "").strip() or None
    row.going_code = str(going_code or "").strip() or None
    row.horse_name = str(horse_name or "").strip() or None
    row.jockey_name = str(jockey_name or "").strip() or None
    row.trainer_name = str(trainer_name or "").strip() or None
    row.title = str(title or "").strip() or None
    row.search_text = _clip_text(search_text, max_len=5000)
    if isinstance(payload_excerpt, dict):
        row.payload_excerpt = payload_excerpt


def index_race_entry_bundle(session: Session, race_id: int) -> None:
    from database.models import Horse, Jockey, Race, RaceEntry, RaceResult, ScoringFactor, Trainer

    race = session.query(Race).filter_by(id=int(race_id)).first()
    if not race:
        return
    rd = _race_day(getattr(race, "race_date", None))
    rk = race_key(getattr(race, "race_date", None), getattr(race, "venue", ""), getattr(race, "race_no", 0))
    v = venue_code(getattr(race, "venue", ""), track_type=getattr(race, "track_type", None))
    sc = surface_code(getattr(race, "surface", None), track_type=getattr(race, "track_type", None), course_type=getattr(race, "course_type", None))
    ct = normalize_course_type(getattr(race, "course_type", None), surface_code_=sc)
    gc = _going_code_for_race(session, int(race.id), getattr(race, "going", None))
    title_r = f"{rk} R{int(getattr(race,'race_no',0) or 0)}"

    upsert_search_document(
        session,
        doc_type="race",
        ref_key=rk,
        entity_type="race",
        entity_key=rk,
        race_id=int(race.id),
        race_date_day=rd,
        race_no=int(getattr(race, "race_no", 0) or 0),
        venue=v,
        surface_code_=sc,
        course_type=ct,
        going_code=gc,
        title=title_r,
        search_text=" ".join(
            [
                rk,
                str(getattr(race, "race_class", "") or ""),
                str(getattr(race, "distance", "") or ""),
                str(getattr(race, "track_type", "") or ""),
                str(getattr(race, "going", "") or ""),
            ]
        ),
        payload_excerpt={
            "race_id": int(race.id),
            "race_class": str(getattr(race, "race_class", "") or ""),
            "distance": int(getattr(race, "distance", 0) or 0),
            "track_type": str(getattr(race, "track_type", "") or ""),
            "course_type": str(getattr(race, "course_type", "") or ""),
            "surface": str(getattr(race, "surface", "") or ""),
            "going": str(getattr(race, "going", "") or ""),
            "going_code": gc,
        },
    )

    entries = (
        session.query(RaceEntry, Horse, Jockey, Trainer)
        .join(Horse, Horse.id == RaceEntry.horse_id)
        .outerjoin(Jockey, Jockey.id == RaceEntry.jockey_id)
        .outerjoin(Trainer, Trainer.id == RaceEntry.trainer_id)
        .filter(RaceEntry.race_id == int(race.id))
        .all()
    )

    entry_ids = [int(getattr(e, "id", 0) or 0) for e, _, _, _ in entries if int(getattr(e, "id", 0) or 0) > 0]
    rr_by_entry_id: Dict[int, Any] = {}
    if entry_ids:
        rr_rows = session.query(RaceResult).filter(RaceResult.entry_id.in_(entry_ids)).all()
        rr_by_entry_id = {int(getattr(r, "entry_id", 0) or 0): r for r in rr_rows}

    factors_by_entry_id: Dict[int, List[Tuple[Any, Any]]] = {}
    if entry_ids:
        fac_rows = (
            session.query(ScoringFactor.entry_id, ScoringFactor.factor_name, ScoringFactor.raw_data_display)
            .filter(ScoringFactor.entry_id.in_(entry_ids))
            .order_by(ScoringFactor.entry_id.asc(), ScoringFactor.factor_name.asc())
            .all()
        )
        for eid, fn, disp in fac_rows:
            k = int(eid or 0)
            if k <= 0:
                continue
            factors_by_entry_id.setdefault(k, []).append((fn, disp))

    for e, h, j, t in entries:
        hn = int(getattr(e, "horse_no", 0) or 0)
        horse_nm = str(getattr(h, "name_ch", "") or "").strip()
        jockey_nm = str(getattr(j, "name_ch", "") or "").strip() if j else ""
        trainer_nm = str(getattr(t, "name_ch", "") or "").strip() if t else ""
        draw = int(getattr(e, "draw", 0) or 0)

        eid = int(getattr(e, "id", 0) or 0)
        rr = rr_by_entry_id.get(eid)
        rk2 = int(getattr(rr, "rank", 0) or 0) if rr else 0
        wo = getattr(rr, "win_odds", None) if rr else None

        fac_lines = []
        for fn, disp in factors_by_entry_id.get(eid, []):
            s = str(disp or "").strip()
            if not s:
                continue
            fac_lines.append(f"{str(fn)}:{s}")

        ref = f"{rk}:{hn}"
        title = f"{rk} [{hn}] {horse_nm}"
        parts = [
            rk,
            str(hn),
            horse_nm,
            jockey_nm,
            trainer_nm,
            str(getattr(race, "race_class", "") or ""),
            str(getattr(race, "distance", "") or ""),
            str(getattr(race, "going", "") or ""),
            str(draw),
        ]
        if rk2 > 0:
            parts.append(f"名次{rk2}")
        if wo is not None:
            try:
                parts.append(f"win_odds={float(wo):.2f}")
            except Exception:
                pass
        if fac_lines:
            parts.append(" ".join(fac_lines[:80]))

        upsert_search_document(
            session,
            doc_type="race_entry",
            ref_key=ref,
            entity_type="race",
            entity_key=rk,
            race_id=int(race.id),
            race_date_day=rd,
            race_no=int(getattr(race, "race_no", 0) or 0),
            venue=v,
            surface_code_=sc,
            course_type=ct,
            going_code=gc,
            horse_name=horse_nm,
            jockey_name=jockey_nm,
            trainer_name=trainer_nm,
            title=title,
            search_text=" ".join([p for p in parts if str(p or "").strip()]),
            payload_excerpt={
                "race_id": int(race.id),
                "entry_id": int(e.id),
                "horse_no": hn,
                "horse_name": horse_nm,
                "jockey": jockey_nm,
                "trainer": trainer_nm,
                "draw": draw,
                "rank": rk2 or None,
                "win_odds": wo,
            },
        )


def index_system_config_doc(session: Session, key: str, doc_type: str, title: str = "") -> None:
    from database.models import Race, SystemConfig

    cfg = session.query(SystemConfig).filter_by(key=str(key)).first()
    if not cfg:
        return
    payload, meta = unwrap_value(cfg.value)
    s = ""
    if isinstance(payload, dict):
        if doc_type in {"ai_report", "ai_reflection"}:
            s = str(payload.get("report") or payload.get("reflection") or "")
        else:
            try:
                s = str(payload)
            except Exception:
                s = ""
    elif isinstance(payload, list):
        try:
            s = " ".join([str(x) for x in payload[:50]])
        except Exception:
            s = ""
    else:
        s = str(payload or "")

    race_id = None
    race_no = None
    rd = None
    v = ""
    sc = ""
    ct = ""
    gc = ""
    ent_key = str(key)

    if isinstance(payload, dict):
        rid = payload.get("race_id")
        try:
            race_id = int(rid) if rid is not None else None
        except Exception:
            race_id = None
    if race_id:
        race = session.query(Race).filter_by(id=int(race_id)).first()
        if race:
            rd = _race_day(getattr(race, "race_date", None))
            race_no = int(getattr(race, "race_no", 0) or 0)
            ent_key = race_key(getattr(race, "race_date", None), getattr(race, "venue", ""), race_no)
            v = venue_code(getattr(race, "venue", ""), track_type=getattr(race, "track_type", None))
            sc = surface_code(getattr(race, "surface", None), track_type=getattr(race, "track_type", None), course_type=getattr(race, "course_type", None))
            ct = normalize_course_type(getattr(race, "course_type", None), surface_code_=sc)
            gc = _going_code_for_race(session, int(race.id), getattr(race, "going", None))

    txt = " ".join([str(key), str(title or ""), s, str(meta or "")])
    excerpt = _clip_text(s, max_len=1200)
    upsert_search_document(
        session,
        doc_type=str(doc_type or "system_config"),
        ref_key=str(key),
        entity_type="system_config",
        entity_key=str(ent_key),
        race_id=race_id,
        race_date_day=rd,
        race_no=race_no,
        venue=v,
        surface_code_=sc,
        course_type=ct,
        going_code=gc,
        title=str(title or str(key)),
        search_text=txt,
        payload_excerpt={"key": str(key), "excerpt": excerpt, "meta": meta, "race_id": race_id},
    )

