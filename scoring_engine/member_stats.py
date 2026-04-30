from __future__ import annotations

from datetime import datetime, timedelta
from math import ceil
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_

from database.models import SystemConfig, Race, RaceEntry, RaceResult, ScoringFactor
from scoring_engine import ranking


STATS_START_DATE = datetime(2026, 4, 8)
STATS_WINDOW_DAYS = 720
LEGACY_POLICY = {
    "start_date": STATS_START_DATE.date().isoformat(),
    "window_days": int(STATS_WINDOW_DAYS),
    "cmp": "date",
    "v": 3,
    "metrics": ["WIN", "P", "Q1", "PQ", "T3E", "T3", "F4", "F4Q", "B5W", "B5P"],
}
CURRENT_POLICY = {
    "start_date": STATS_START_DATE.date().isoformat(),
    "window_days": int(STATS_WINDOW_DAYS),
    "cmp": "date",
    "v": 5,
    "metrics": ["WIN", "P", "Q1", "PQ", "T3E", "T3", "F4", "F4Q", "B5W", "B5P"],
    "tie_break": ranking.TIE_BREAK,
    "sum_order": "sorted_weight_keys",
}

METRIC_LABELS = {
    "WIN": "獨贏(2)",
    "P": "位置(3)",
    "Q1": "正Q(2+3)",
    "PQ": "PQ(3)",
    "T3E": "三重(2+4)",
    "T3": "三重(4)",
    "F4": "四連(2+5)",
    "F4Q": "四連(5)",
    "B5W": "獨贏(45)",
    "B5P": "位置(5)",
}

LEGACY_ELIM_BOTTOM_PCTS = [10, 15, 20, 25, 30, 35, 40, 50]
ELIM_BOTTOM_PCTS = [35]
LEGACY_ELIM_POLICY = {
    "start_date": STATS_START_DATE.date().isoformat(),
    "window_days": int(STATS_WINDOW_DAYS),
    "cmp": "date",
    "v": 2,
    "top_k": 5,
    "bottom_pcts": LEGACY_ELIM_BOTTOM_PCTS,
    "metrics": ["pred", "tn", "fp"],
}
CURRENT_ELIM_POLICY = {
    "start_date": STATS_START_DATE.date().isoformat(),
    "window_days": int(STATS_WINDOW_DAYS),
    "cmp": "date",
    "v": 8,
    "top_k": 4,
    "bottom_pcts": ELIM_BOTTOM_PCTS,
    "metrics": ["pred", "tn", "fp"],
    "daily": True,
    "tie_break": ranking.TIE_BREAK,
    "sum_order": "sorted_weight_keys",
}


def _cutoff_date(now: Optional[datetime] = None) -> datetime:
    n = now or datetime.now()
    w = n - timedelta(days=STATS_WINDOW_DAYS)
    return max(STATS_START_DATE, w)


def _compute_elim_n(field_size: int, bottom_pct: float) -> int:
    n = int(field_size or 0)
    if n <= 0:
        return 0
    p = float(bottom_pct or 0.0)
    if p <= 0:
        return 0
    if p >= 100:
        return n
    out = int(ceil(n * (p / 100.0)))
    return max(1, min(out, n))


def _ranked_horses_for_race(session: Session, race_id: int, weight_map: Dict[str, float]) -> List[int]:
    return ranking.ranked_horses_by_weights(session, int(race_id), weight_map)


def load_member_preset_stats(session: Session, email: str) -> Dict[str, Any]:
    e = str(email or "").strip().lower()
    if not e:
        return {}
    key = f"member_weight_preset_stats:{e}"
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if cfg and isinstance(cfg.value, dict):
        out = {}
        for k, v in cfg.value.items():
            if isinstance(v, dict) and v.get("policy") in (CURRENT_POLICY, LEGACY_POLICY):
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


def rebuild_member_preset_stats(
    session: Session,
    email: str,
    presets: List[Dict[str, Any]],
    d1: datetime,
    d2: datetime,
) -> Dict[str, Any]:
    e = str(email or "").strip().lower()
    if not e:
        return {}
    now = datetime.now().isoformat()

    races = (
        session.query(Race)
        .join(RaceEntry, RaceEntry.race_id == Race.id)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceResult.rank != None)
        .filter(func.date(Race.race_date) >= d1.date().isoformat())
        .filter(func.date(Race.race_date) <= d2.date().isoformat())
        .group_by(Race.id)
        .having(func.count(RaceResult.id) >= 5)
        .order_by(func.date(Race.race_date).asc(), Race.race_no.asc(), Race.id.asc())
        .all()
    )

    out: Dict[str, Any] = {}
    for p in (presets or [])[:3]:
        name = str(p.get("name") or "").strip()
        weights = p.get("weights") if isinstance(p.get("weights"), dict) else {}
        if not name:
            continue
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
            "updated_at": now,
            "policy": CURRENT_POLICY,
        }

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

        out[name] = st

    save_member_preset_stats(session, e, out)
    return out


def load_member_preset_elim_stats(session: Session, email: str) -> Dict[str, Any]:
    e = str(email or "").strip().lower()
    if not e:
        return {}
    key = f"member_weight_preset_elim_stats:{e}"
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if cfg and isinstance(cfg.value, dict):
        out = {}
        for k, v in cfg.value.items():
            if isinstance(v, dict) and v.get("policy") in (CURRENT_ELIM_POLICY, LEGACY_ELIM_POLICY):
                out[k] = v
        return out
    return {}


def save_member_preset_elim_stats(session: Session, email: str, stats: Dict[str, Any]) -> None:
    e = str(email or "").strip().lower()
    if not e:
        return
    key = f"member_weight_preset_elim_stats:{e}"
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if not cfg:
        cfg = SystemConfig(key=key, description="會員權重配置反向表現統計（淘汰準確率/錯殺率）")
        session.add(cfg)
    cfg.value = stats
    session.commit()


def delete_member_preset_elim_stats(session: Session, email: str, preset_name: str) -> None:
    e = str(email or "").strip().lower()
    n = str(preset_name or "").strip()
    if not e or not n:
        return
    stats = load_member_preset_elim_stats(session, e)
    if n in stats:
        stats.pop(n, None)
        save_member_preset_elim_stats(session, e, stats)


def update_member_preset_elim_stats_incremental(
    session: Session,
    email: str,
    presets: List[Dict[str, Any]],
    per_preset_max_new_races: int = 60,
) -> Dict[str, Any]:
    cutoff = _cutoff_date()
    stats = load_member_preset_elim_stats(session, email)
    now = datetime.now().isoformat()
    changed_any = False
    policy = CURRENT_ELIM_POLICY
    top_k = int(policy.get("top_k") or 0)

    for p in (presets or [])[:3]:
        name = str(p.get("name") or "").strip()
        weights = p.get("weights") if isinstance(p.get("weights"), dict) else {}
        if not name:
            continue

        st = stats.get(name)
        if not isinstance(st, dict):
            st = {
                "pcts": {str(int(x)): {"races": 0, "pred": 0, "tn": 0, "fp": 0} for x in ELIM_BOTTOM_PCTS},
                "days": {},
                "last_date": None,
                "last_race_no": None,
                "last_race_id": None,
                "updated_at": None,
                "range_from": None,
                "range_to": None,
                "policy": policy,
            }
            changed_any = True
        else:
            if st.get("policy") != policy:
                st = {
                    "pcts": {str(int(x)): {"races": 0, "pred": 0, "tn": 0, "fp": 0} for x in ELIM_BOTTOM_PCTS},
                    "days": {},
                    "last_date": None,
                    "last_race_no": None,
                    "last_race_id": None,
                    "updated_at": None,
                    "range_from": None,
                    "range_to": None,
                    "policy": policy,
                }
                changed_any = True
            if not isinstance(st.get("days"), dict):
                st["days"] = {}
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
            st["pcts"] = {str(int(x)): {"races": 0, "pred": 0, "tn": 0, "fp": 0} for x in ELIM_BOTTOM_PCTS}
            st["last_date"] = None
            st["last_race_no"] = None
            st["last_race_id"] = None
            last_date = None
            last_race_no = None
            last_race_id = None
            changed_any = True

        pcts_map = st.get("pcts") if isinstance(st.get("pcts"), dict) else {}
        days_map = st.get("days") if isinstance(st.get("days"), dict) else {}
        for pct in ELIM_BOTTOM_PCTS:
            k = str(int(pct))
            if k not in pcts_map or not isinstance(pcts_map.get(k), dict):
                pcts_map[k] = {"races": 0, "pred": 0, "tn": 0, "fp": 0}

        races = _list_completed_races(
            session=session,
            cutoff=cutoff,
            last_date=last_date,
            last_race_no=(int(last_race_no) if last_race_no is not None else None),
            last_race_id=(int(last_race_id) if last_race_id is not None else None),
            limit=int(per_preset_max_new_races or 0),
            min_finishers=top_k,
        )
        processed = 0
        for r in races:
            rid = int(getattr(r, "id") or 0)
            if rid <= 0:
                continue
            act = _actual_topk_for_race(session, rid, top_k)
            if len(act) < top_k:
                continue
            ranked = _ranked_horses_for_race(session, rid, weights)
            if not ranked:
                continue
            n_field = int(session.query(func.count(RaceEntry.id)).filter(RaceEntry.race_id == rid).scalar() or 0)
            used_any = False
            day_key = r.race_date.date().isoformat() if hasattr(r.race_date, "date") else str(r.race_date or "")
            if day_key and day_key not in days_map:
                days_map[day_key] = {str(int(x)): {"races": 0, "pred": 0, "tn": 0, "fp": 0} for x in ELIM_BOTTOM_PCTS}
            for pct in ELIM_BOTTOM_PCTS:
                elim_n = _compute_elim_n(n_field, float(pct))
                if elim_n <= 0:
                    continue
                pred_neg = ranked[-elim_n:]
                pred_set = set(int(x) for x in pred_neg if int(x or 0) > 0)
                act_set = set(int(x) for x in act if int(x or 0) > 0)
                if not pred_set:
                    continue
                fp = int(len(pred_set & act_set))
                tn = int(len(pred_set - act_set))
                kk = str(int(pct))
                a = pcts_map.get(kk) if isinstance(pcts_map.get(kk), dict) else {"races": 0, "pred": 0, "tn": 0, "fp": 0}
                a["races"] = int(a.get("races") or 0) + 1
                a["pred"] = int(a.get("pred") or 0) + int(elim_n)
                a["tn"] = int(a.get("tn") or 0) + tn
                a["fp"] = int(a.get("fp") or 0) + fp
                pcts_map[kk] = a
                if day_key and isinstance(days_map.get(day_key), dict):
                    da = days_map[day_key].get(kk)
                    if isinstance(da, dict):
                        da["races"] = int(da.get("races") or 0) + 1
                        da["pred"] = int(da.get("pred") or 0) + int(elim_n)
                        da["tn"] = int(da.get("tn") or 0) + tn
                        da["fp"] = int(da.get("fp") or 0) + fp
                        days_map[day_key][kk] = da
                used_any = True

            if used_any:
                st["last_date"] = r.race_date.date().isoformat() if hasattr(r.race_date, "date") else str(r.race_date)
                st["last_race_no"] = int(getattr(r, "race_no") or 0)
                st["last_race_id"] = int(getattr(r, "id") or 0)
                st["updated_at"] = now
                processed += 1

        st["pcts"] = pcts_map
        st["days"] = days_map
        stats[name] = st
        if processed > 0:
            changed_any = True

    if changed_any:
        save_member_preset_elim_stats(session, email, stats)
    return stats


def rebuild_member_preset_elim_stats(
    session: Session,
    email: str,
    presets: List[Dict[str, Any]],
    d1: datetime,
    d2: datetime,
    bottom_pcts: Optional[List[int]] = None,
) -> Dict[str, Any]:
    e = str(email or "").strip().lower()
    if not e:
        return {}
    pcts = bottom_pcts or ELIM_BOTTOM_PCTS
    top_k = int(CURRENT_ELIM_POLICY.get("top_k") or 0)
    now = datetime.now().isoformat()

    out: Dict[str, Any] = {}
    q = (
        session.query(Race)
        .join(RaceEntry, RaceEntry.race_id == Race.id)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceResult.rank != None)
        .filter(func.date(Race.race_date) >= d1.date().isoformat())
        .filter(func.date(Race.race_date) <= d2.date().isoformat())
        .group_by(Race.id)
        .having(func.count(RaceResult.id) >= top_k)
        .order_by(func.date(Race.race_date).asc(), Race.race_no.asc(), Race.id.asc())
    )
    races = q.all()

    for p in (presets or [])[:3]:
        name = str(p.get("name") or "").strip()
        weights = p.get("weights") if isinstance(p.get("weights"), dict) else {}
        if not name or not weights:
            continue
        st = {
            "pcts": {str(int(x)): {"races": 0, "pred": 0, "tn": 0, "fp": 0} for x in pcts},
            "days": {},
            "last_date": None,
            "last_race_no": None,
            "last_race_id": None,
            "updated_at": now,
            "range_from": d1.date().isoformat(),
            "range_to": d2.date().isoformat(),
            "policy": CURRENT_ELIM_POLICY,
        }

        for r in races:
            rid = int(getattr(r, "id") or 0)
            if rid <= 0:
                continue
            act = _actual_topk_for_race(session, rid, top_k)
            if len(act) < top_k:
                continue
            ranked = _ranked_horses_for_race(session, rid, weights)
            if not ranked:
                continue
            n_field = int(session.query(func.count(RaceEntry.id)).filter(RaceEntry.race_id == rid).scalar() or 0)
            day_key = r.race_date.date().isoformat() if hasattr(r.race_date, "date") else str(r.race_date or "")
            if day_key and day_key not in st["days"]:
                st["days"][day_key] = {str(int(x)): {"races": 0, "pred": 0, "tn": 0, "fp": 0} for x in pcts}
            for pct in pcts:
                elim_n = _compute_elim_n(n_field, float(pct))
                if elim_n <= 0:
                    continue
                pred_neg = ranked[-elim_n:]
                pred_set = set(int(x) for x in pred_neg if int(x or 0) > 0)
                act_set = set(int(x) for x in act if int(x or 0) > 0)
                if not pred_set:
                    continue
                fp = int(len(pred_set & act_set))
                tn = int(len(pred_set - act_set))
                kk = str(int(pct))
                a = st["pcts"].get(kk)
                a["races"] += 1
                a["pred"] += int(elim_n)
                a["tn"] += tn
                a["fp"] += fp
                if day_key and isinstance(st["days"].get(day_key), dict):
                    da = st["days"][day_key].get(kk)
                    if isinstance(da, dict):
                        da["races"] = int(da.get("races") or 0) + 1
                        da["pred"] = int(da.get("pred") or 0) + int(elim_n)
                        da["tn"] = int(da.get("tn") or 0) + tn
                        da["fp"] = int(da.get("fp") or 0) + fp
                        st["days"][day_key][kk] = da

            st["last_date"] = r.race_date.date().isoformat() if hasattr(r.race_date, "date") else str(r.race_date)
            st["last_race_no"] = int(getattr(r, "race_no") or 0)
            st["last_race_id"] = int(getattr(r, "id") or 0)

        out[name] = st

    save_member_preset_elim_stats(session, e, out)
    return out


def _list_completed_races(
    session: Session,
    cutoff: datetime,
    last_date: Optional[datetime],
    last_race_no: Optional[int],
    last_race_id: Optional[int],
    limit: int,
    min_finishers: int = 5,
) -> List[Race]:
    cutoff_s = cutoff.date().isoformat()
    q = (
        session.query(Race)
        .join(RaceEntry, RaceEntry.race_id == Race.id)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceResult.rank != None)
        .filter(func.date(Race.race_date) >= cutoff_s)
        .group_by(Race.id)
        .having(func.count(RaceResult.id) >= int(min_finishers or 0))
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
    return _predict_topk_for_race(session, race_id, weight_map, 4)


def _predict_topk_for_race(session: Session, race_id: int, weight_map: Dict[str, float], k: int) -> List[int]:
    return ranking.topk_by_weights(session, int(race_id), weight_map, int(k or 0))


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

    preset_list = [p for p in (presets or [])[:3] if isinstance(p, dict) and str(p.get("name") or "").strip()]
    need_rebuild = False
    for p in preset_list:
        name = str(p.get("name") or "").strip()
        st0 = stats.get(name)
        if not isinstance(st0, dict) or st0.get("policy") != policy:
            need_rebuild = True
            break
    if need_rebuild and preset_list:
        return rebuild_member_preset_stats(session, email, preset_list, d1=cutoff, d2=datetime.now())

    for p in preset_list:
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
                continue

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
