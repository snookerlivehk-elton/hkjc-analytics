from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_

from database.models import SystemConfig, Race, RaceEntry, RaceResult, ScoringFactor


STATS_START_DATE = datetime(2026, 4, 8)
STATS_WINDOW_DAYS = 720
CURRENT_POLICY = {
    "start_date": STATS_START_DATE.date().isoformat(),
    "window_days": int(STATS_WINDOW_DAYS),
    "cmp": "date",
    "v": 3,
    "metrics": ["WIN", "P", "Q1", "PQ", "T3E", "T3", "F4", "F4Q", "B5W", "B5P"],
}


def _cutoff_date(now: Optional[datetime] = None) -> datetime:
    n = now or datetime.now()
    w = n - timedelta(days=STATS_WINDOW_DAYS)
    return max(STATS_START_DATE, w)


def load_member_preset_stats(session: Session, email: str) -> Dict[str, Any]:
    e = str(email or "").strip().lower()
    if not e:
        return {}
    key = f"member_weight_preset_stats:{e}"
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if cfg and isinstance(cfg.value, dict):
        out = {}
        for k, v in cfg.value.items():
            if isinstance(v, dict) and v.get("policy") == CURRENT_POLICY:
                out[k] = v
        return out
    return {}


def save_member_preset_stats(session: Session, email: str, stats: Dict[str, Any]) -> None:
    e = str(email or "").strip().lower()
    if not e:
        return
    key = f"member_weight_preset_stats:{e}"
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if not cfg:
        cfg = SystemConfig(key=key, description="會員權重配置命中率統計（累積）")
        session.add(cfg)
    cfg.value = stats
    session.commit()

def delete_member_preset_stats(session: Session, email: str, preset_name: str) -> None:
    e = str(email or "").strip().lower()
    n = str(preset_name or "").strip()
    if not e or not n:
        return
    stats = load_member_preset_stats(session, e)
    if n in stats:
        stats.pop(n, None)
        save_member_preset_stats(session, e, stats)


def _list_completed_races(
    session: Session,
    cutoff: datetime,
    last_date: Optional[datetime],
    last_race_no: Optional[int],
    last_race_id: Optional[int],
    limit: int,
) -> List[Race]:
    cutoff_s = cutoff.date().isoformat()
    q = (
        session.query(Race)
        .join(RaceEntry, RaceEntry.race_id == Race.id)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceResult.rank != None)
        .filter(func.date(Race.race_date) >= cutoff_s)
        .group_by(Race.id)
        .having(func.count(RaceResult.id) >= 5)
        .order_by(func.date(Race.race_date).asc(), Race.race_no.asc(), Race.id.asc())
    )

    if last_date is not None and last_race_no is not None and last_race_id is not None:
        last_date_s = last_date.date().isoformat() if hasattr(last_date, "date") else str(last_date)
        q = q.filter(
            or_(
                func.date(Race.race_date) > last_date_s,
                and_(func.date(Race.race_date) == last_date_s, Race.race_no > last_race_no),
                and_(func.date(Race.race_date) == last_date_s, Race.race_no == last_race_no, Race.id > last_race_id),
            )
        )

    return q.limit(limit).all()


def _predict_top4_for_race(session: Session, race_id: int, weight_map: Dict[str, float]) -> List[int]:
    weights = {k: float(v) for k, v in (weight_map or {}).items()}
    if not weights:
        return []

    entries = session.query(RaceEntry.id, RaceEntry.horse_no).filter_by(race_id=race_id).all()
    if not entries:
        return []

    entry_ids = [e[0] for e in entries]
    entry_id_to_no = {e[0]: int(e[1]) for e in entries}
    factor_names = list(weights.keys())

    factors = (
        session.query(ScoringFactor.entry_id, ScoringFactor.factor_name, ScoringFactor.score)
        .filter(ScoringFactor.entry_id.in_(entry_ids))
        .filter(ScoringFactor.factor_name.in_(factor_names))
        .all()
    )

    totals: Dict[int, float] = {eid: 0.0 for eid in entry_ids}
    for entry_id, factor_name, score in factors:
        totals[int(entry_id)] += float(score or 0.0) * float(weights.get(factor_name, 0.0))

    ranked = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    top4 = [entry_id_to_no[eid] for eid, _ in ranked[:4] if eid in entry_id_to_no]
    return top4


def _predict_topk_for_race(session: Session, race_id: int, weight_map: Dict[str, float], k: int) -> List[int]:
    weights = {k2: float(v2) for k2, v2 in (weight_map or {}).items()}
    if not weights or k <= 0:
        return []

    entries = session.query(RaceEntry.id, RaceEntry.horse_no).filter_by(race_id=race_id).all()
    if not entries:
        return []

    entry_ids = [e[0] for e in entries]
    entry_id_to_no = {e[0]: int(e[1]) for e in entries}
    factor_names = list(weights.keys())

    factors = (
        session.query(ScoringFactor.entry_id, ScoringFactor.factor_name, ScoringFactor.score)
        .filter(ScoringFactor.entry_id.in_(entry_ids))
        .filter(ScoringFactor.factor_name.in_(factor_names))
        .all()
    )

    totals: Dict[int, float] = {eid: 0.0 for eid in entry_ids}
    for entry_id, factor_name, score in factors:
        totals[int(entry_id)] += float(score or 0.0) * float(weights.get(factor_name, 0.0))

    ranked = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    return [entry_id_to_no[eid] for eid, _ in ranked[:k] if eid in entry_id_to_no]


def _actual_topk_for_race(session: Session, race_id: int, k: int) -> List[int]:
    rows = (
        session.query(RaceEntry.horse_no, RaceResult.rank)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceEntry.race_id == race_id)
        .filter(RaceResult.rank != None)
        .order_by(RaceResult.rank.asc())
        .limit(k)
        .all()
    )
    return [int(r[0]) for r in rows]


def _calc_hits(pred: List[int], act: List[int]) -> Dict[str, int]:
    p2 = pred[:2]
    p3 = pred[:3]
    p4 = pred[:4]
    p5 = pred[:5]
    a1 = act[:1]
    a2 = act[:2]
    a3 = act[:3]
    a4 = act[:4]

    if len(act) < 5 or len(pred) < 5:
        return {}

    winner = a1[0]
    runner_up = a2[1]
    top3 = set(a3)
    top4 = set(a4)

    return {
        "win": int(winner in p2),
        "p": int(len(set(p3) & top3) >= 1),
        "q1": int((winner in p2) and (runner_up in p3)),
        "pq": int(len(set(p3) & top3) >= 2),
        "t3e": int((winner in p2) and (a2[1] in p4) and (a3[2] in p4)),
        "t3": int(set(a3).issubset(set(p4))),
        "f4": int((winner in p2) and (a2[1] in p5) and (a3[2] in p5) and (a4[3] in p5)),
        "f4q": int(top4.issubset(set(p5))),
        "b5w": int(winner in p5),
        "b5p": int(len(set(p5) & top3) >= 1),
    }


def update_member_preset_stats_incremental(
    session: Session,
    email: str,
    presets: List[Dict[str, Any]],
    per_preset_max_new_races: int = 30,
) -> Dict[str, Any]:
    cutoff = _cutoff_date()
    stats = load_member_preset_stats(session, email)
    now = datetime.now().isoformat()
    changed_any = False
    policy = CURRENT_POLICY

    for p in (presets or [])[:3]:
        name = str(p.get("name") or "").strip()
        weights = p.get("weights") if isinstance(p.get("weights"), dict) else {}
        if not name:
            continue

        st = stats.get(name)
        if not isinstance(st, dict):
            st = {
                "races": 0,
                "win": 0,
                "p": 0,
                "q1": 0,
                "pq": 0,
                "t3e": 0,
                "t3": 0,
                "f4": 0,
                "f4q": 0,
                "b5w": 0,
                "b5p": 0,
                "last_date": None,
                "last_race_no": None,
                "last_race_id": None,
                "updated_at": None,
                "policy": policy,
            }
            changed_any = True
        else:
            if st.get("policy") != policy:
                st = {
                    "races": 0,
                    "win": 0,
                    "p": 0,
                    "q1": 0,
                    "pq": 0,
                    "t3e": 0,
                    "t3": 0,
                    "f4": 0,
                    "f4q": 0,
                    "b5w": 0,
                    "b5p": 0,
                    "last_date": None,
                    "last_race_no": None,
                    "last_race_id": None,
                    "updated_at": None,
                    "policy": policy,
                }
                changed_any = True

        last_date = None
        if st.get("last_date"):
            try:
                last_date = datetime.fromisoformat(st["last_date"])
            except Exception:
                last_date = None
        last_race_no = st.get("last_race_no")
        last_race_id = st.get("last_race_id")

        if last_date is not None and last_date < cutoff:
            st["races"] = 0
            st["win"] = 0
            st["p"] = 0
            st["q1"] = 0
            st["pq"] = 0
            st["t3e"] = 0
            st["t3"] = 0
            st["f4"] = 0
            st["f4q"] = 0
            st["b5w"] = 0
            st["b5p"] = 0
            st["last_date"] = None
            st["last_race_no"] = None
            st["last_race_id"] = None
            last_date = None
            last_race_no = None
            last_race_id = None
            changed_any = True

        races = _list_completed_races(
            session=session,
            cutoff=cutoff,
            last_date=last_date,
            last_race_no=int(last_race_no) if last_race_no is not None else None,
            last_race_id=int(last_race_id) if last_race_id is not None else None,
            limit=per_preset_max_new_races,
        )

        processed = 0
        for race in races:
            act = _actual_topk_for_race(session, race.id, 5)
            pred = _predict_topk_for_race(session, race.id, weights, 5)
            hits = _calc_hits(pred, act)
            if not hits:
                continue

            st["races"] = int(st.get("races") or 0) + 1
            for k2, v2 in hits.items():
                st[k2] = int(st.get(k2) or 0) + int(v2)

            st["last_date"] = race.race_date.date().isoformat() if hasattr(race.race_date, "date") else str(race.race_date)
            st["last_race_no"] = int(race.race_no or 0)
            st["last_race_id"] = int(race.id)
            st["updated_at"] = now
            processed += 1

        stats[name] = st
        if processed > 0:
            changed_any = True

    if changed_any:
        save_member_preset_stats(session, email, stats)
    return stats


def update_all_members_preset_stats_for_race_date(session: Session, race_date_str: str) -> Dict[str, Any]:
    try:
        target_date = datetime.strptime(str(race_date_str), "%Y/%m/%d").date()
    except ValueError:
        return {"ok": False, "error": "race_date_str 格式應為 YYYY/MM/DD"}

    cutoff = _cutoff_date().date()
    if target_date < cutoff:
        return {"ok": True, "skipped": True, "reason": "date_before_cutoff"}

    races = (
        session.query(Race)
        .join(RaceEntry, RaceEntry.race_id == Race.id)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(func.date(Race.race_date) == target_date.isoformat())
        .filter(RaceResult.rank != None)
        .group_by(Race.id)
        .having(func.count(RaceResult.id) >= 5)
        .order_by(Race.race_no.asc(), Race.id.asc())
        .all()
    )
    if not races:
        return {"ok": True, "races": 0, "members": 0, "presets": 0}

    member_cfgs = (
        session.query(SystemConfig)
        .filter(SystemConfig.key.like("member_weight_presets:%"))
        .all()
    )

    members = 0
    presets_n = 0
    for cfg in member_cfgs:
        if not isinstance(cfg.value, list) or not cfg.value:
            continue
        email = cfg.key.split(":", 1)[1] if ":" in cfg.key else ""
        email = str(email or "").strip().lower()
        if not email:
            continue

        stats = load_member_preset_stats(session, email)
        now = datetime.now().isoformat()
        members += 1

        for p in cfg.value[:3]:
            if not isinstance(p, dict):
                continue
            name = str(p.get("name") or "").strip()
            weights = p.get("weights") if isinstance(p.get("weights"), dict) else {}
            if not name:
                continue
            presets_n += 1

            st = stats.get(name)
            if not isinstance(st, dict) or st.get("policy") != CURRENT_POLICY:
                st = {
                    "races": 0,
                    "win": 0,
                    "p": 0,
                    "q1": 0,
                    "pq": 0,
                    "t3e": 0,
                    "t3": 0,
                    "f4": 0,
                    "f4q": 0,
                    "b5w": 0,
                    "b5p": 0,
                    "last_date": None,
                    "last_race_no": None,
                    "last_race_id": None,
                    "updated_at": None,
                    "policy": CURRENT_POLICY,
                }

            last_date = None
            if st.get("last_date"):
                try:
                    last_date = datetime.fromisoformat(st["last_date"]).date()
                except Exception:
                    last_date = None
            last_race_no = int(st.get("last_race_no") or 0) if st.get("last_race_no") is not None else None
            last_race_id = int(st.get("last_race_id") or 0) if st.get("last_race_id") is not None else None

            for race in races:
                race_d = race.race_date.date() if hasattr(race.race_date, "date") else race.race_date
                if last_date is not None and last_race_no is not None and last_race_id is not None:
                    if race_d < last_date:
                        continue
                    if race_d == last_date:
                        if int(race.race_no or 0) < last_race_no:
                            continue
                        if int(race.race_no or 0) == last_race_no and int(race.id) <= last_race_id:
                            continue

                act = _actual_topk_for_race(session, race.id, 5)
                pred = _predict_topk_for_race(session, race.id, weights, 5)
                hits = _calc_hits(pred, act)
                if not hits:
                    continue

                st["races"] = int(st.get("races") or 0) + 1
                for k2, v2 in hits.items():
                    st[k2] = int(st.get(k2) or 0) + int(v2)

                st["last_date"] = race_d.isoformat() if hasattr(race_d, "isoformat") else str(race_d)
                st["last_race_no"] = int(race.race_no or 0)
                st["last_race_id"] = int(race.id)
                st["updated_at"] = now

            stats[name] = st

        save_member_preset_stats(session, email, stats)

    return {"ok": True, "races": len(races), "members": members, "presets": presets_n, "date": target_date.isoformat()}
