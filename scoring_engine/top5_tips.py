from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from scoring_engine.config_value import unwrap_value
from scoring_engine.top5_odds_stats import ODDS_BUCKETS, bucket_label, bucket_odds
from scoring_engine.top5_tip_config import load_tip_config


def _date_range_from_race(race_dt: Any, stats_days: int) -> Tuple[date, date]:
    d0 = None
    if isinstance(race_dt, date) and not isinstance(race_dt, datetime):
        d0 = race_dt
    elif hasattr(race_dt, "date"):
        try:
            d0 = race_dt.date()
        except Exception:
            d0 = None
    if not d0:
        d0 = datetime.utcnow().date()
    days = int(stats_days or 0)
    if days <= 0:
        days = 180
    d1 = d0 - timedelta(days=days)
    return d1, d0


def _odds_source_label(s: str) -> str:
    k = str(s or "").strip()
    if k == "result_win_odds":
        return "賽後 Win 賠率"
    if k == "latest_history":
        return "最新 OddsHistory"
    if k == "pre_race_latest":
        return "賽前賠率（PRE）"
    return k or "未知"


def _get_race_entry_map(session: Session, race_id: int) -> Dict[int, Dict[str, Any]]:
    from database.models import Horse, Jockey, RaceEntry, Trainer

    rows = (
        session.query(RaceEntry.horse_no, Horse.name_ch, Jockey.name_ch, Trainer.name_ch)
        .join(Horse, Horse.id == RaceEntry.horse_id)
        .outerjoin(Jockey, Jockey.id == RaceEntry.jockey_id)
        .outerjoin(Trainer, Trainer.id == RaceEntry.trainer_id)
        .filter(RaceEntry.race_id == int(race_id))
        .all()
    )
    out: Dict[int, Dict[str, Any]] = {}
    for hn, horse_nm, jockey_nm, trainer_nm in rows:
        try:
            hni = int(hn or 0)
        except Exception:
            continue
        if hni <= 0:
            continue
        out[hni] = {
            "horse_name": str(horse_nm or "").strip(),
            "jockey": str(jockey_nm or "").strip(),
            "trainer": str(trainer_nm or "").strip(),
        }
    return out


def _get_race_odds_map(session: Session, race_id: int, odds_source: str) -> Dict[int, Optional[float]]:
    from database.models import OddsHistory, RaceEntry, RaceResult

    src = str(odds_source or "").strip()
    rows = (
        session.query(RaceEntry.horse_no, RaceEntry.id, RaceResult.win_odds)
        .outerjoin(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceEntry.race_id == int(race_id))
        .all()
    )
    entry_ids: List[int] = []
    base: Dict[int, Dict[str, Any]] = {}
    for hn, eid, wo in rows:
        try:
            hni = int(hn or 0)
        except Exception:
            continue
        if hni <= 0:
            continue
        eid_i = int(eid) if eid is not None else 0
        if eid_i > 0:
            entry_ids.append(eid_i)
        base[hni] = {"entry_id": eid_i or None, "result_win_odds": wo}

    if src in {"latest_history", "pre_race_latest"} and entry_ids:
        q = session.query(OddsHistory.entry_id, OddsHistory.win_odds, OddsHistory.captured_at)
        q = q.filter(OddsHistory.entry_id.in_(entry_ids))
        if src == "pre_race_latest":
            q = q.filter(OddsHistory.odds_type == "PRE")
        q = q.order_by(OddsHistory.entry_id.asc(), OddsHistory.captured_at.desc())
        latest: Dict[int, Optional[float]] = {}
        for eid, wo, _cap in q.all():
            ei = int(eid or 0)
            if ei <= 0 or ei in latest:
                continue
            try:
                latest[ei] = float(wo) if wo is not None else None
            except Exception:
                latest[ei] = None
        out2: Dict[int, Optional[float]] = {}
        for hn, m in base.items():
            eid_i = int(m.get("entry_id") or 0)
            out2[int(hn)] = latest.get(eid_i)
        return out2

    out: Dict[int, Optional[float]] = {}
    for hn, m in base.items():
        wo = m.get("result_win_odds")
        try:
            out[int(hn)] = float(wo) if wo is not None else None
        except Exception:
            out[int(hn)] = None
    return out


def _get_race_rank_map(session: Session, race_id: int) -> Dict[int, int]:
    from database.models import RaceEntry, RaceResult

    rows = (
        session.query(RaceEntry.horse_no, RaceResult.rank)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceEntry.race_id == int(race_id))
        .all()
    )
    out: Dict[int, int] = {}
    for hn, rk in rows:
        try:
            hni = int(hn or 0)
            rki = int(rk or 0)
        except Exception:
            continue
        if hni > 0 and rki > 0:
            out[hni] = rki
    return out


def _predictor_labels(session: Session) -> Dict[str, Dict[str, str]]:
    from database.models import ScoringWeight

    factor_labels: Dict[str, str] = {}
    try:
        rows = session.query(ScoringWeight.factor_name, ScoringWeight.description).all()
        for fn, desc in rows:
            k = str(fn or "").strip()
            v = str(desc or "").strip()
            if k and v:
                factor_labels[k] = v
    except Exception:
        factor_labels = {}
    return {"factor": factor_labels}


def _load_ai_top5(session: Session, race_date: Any, race_no: Any) -> Optional[List[int]]:
    from database.models import SystemConfig

    ds = ""
    try:
        ds = race_date.strftime("%Y/%m/%d")
    except Exception:
        ds = ""
    rn = int(race_no or 0)
    if not ds or rn <= 0:
        return None
    key = f"ai_race_report:{ds}:{rn}"
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if not cfg:
        return None
    payload, _ = unwrap_value(cfg.value)
    if not isinstance(payload, dict):
        return None
    t5 = payload.get("top5_horse_nos")
    if not isinstance(t5, list):
        return None
    out = []
    for x in t5[:5]:
        try:
            out.append(int(x))
        except Exception:
            continue
    return out or None


def generate_top5_tips_for_race(
    session: Session,
    *,
    race_id: int,
    member_email: Optional[str] = None,
    preset_name: Optional[str] = None,
    override_config: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    from database.models import PredictionTop5, Race
    from scoring_engine.top5_odds_stats import compute_top5_odds_stats

    cfg0 = dict(override_config or load_tip_config(session))
    if not bool(cfg0.get("enabled")):
        return []

    race = session.query(Race).filter_by(id=int(race_id)).first()
    if not race:
        return []

    odds_source = str(cfg0.get("odds_source") or "pre_race_latest")
    d1, d2 = _date_range_from_race(getattr(race, "race_date", None), int(cfg0.get("stats_days") or 180))
    min_samples = int(cfg0.get("min_samples") or 10)
    min_place = float(cfg0.get("min_place_rate") or 0.0)
    min_win = float(cfg0.get("min_win_rate") or 0.0)
    max_tips = int(cfg0.get("max_tips") or 20)

    positions = [int(x) for x in (cfg0.get("positions") or [1, 2, 3, 4, 5]) if str(x).strip().isdigit()]
    positions = [x for x in positions if 1 <= x <= 5]
    if not positions:
        positions = [1, 2, 3, 4, 5]

    odds_buckets = [str(x) for x in (cfg0.get("odds_buckets") or []) if str(x).strip()]
    if not odds_buckets:
        odds_buckets = [b.key for b in ODDS_BUCKETS]

    ptypes = [str(x) for x in (cfg0.get("predictor_types") or []) if str(x).strip()]
    if not ptypes:
        ptypes = ["preset", "factor", "ai"]

    me = str(member_email or "").strip().lower() or None
    preset_name_s = str(preset_name or "").strip() or None
    df_stats = compute_top5_odds_stats(
        session,
        d1=d1,
        d2=d2,
        member_email=me,
        include_presets=("preset" in ptypes),
        include_factors=("factor" in ptypes),
        include_ai=("ai" in ptypes),
        place_k=3,
        top_k=5,
        odds_source=odds_source if odds_source != "pre_race_latest" else "pre_race_latest",
    )
    stats_map: Dict[Tuple[str, str, str, int, str], Dict[str, Any]] = {}
    if not df_stats.empty:
        for _, r in df_stats.iterrows():
            ptype = str(r.get("predictor_type") or "")
            pkey = str(r.get("predictor_key") or "")
            mem = str(r.get("member_email") or "").strip().lower() if ptype == "preset" else ""
            pos = int(r.get("position") or 0)
            bucket = str(r.get("odds_bucket") or "")
            stats_map[(ptype, pkey, mem, pos, bucket)] = dict(r)

    entry_info = _get_race_entry_map(session, int(race_id))
    odds_map = _get_race_odds_map(session, int(race_id), odds_source)

    labels = _predictor_labels(session)
    factor_labels = labels.get("factor") or {}

    preds: List[Tuple[str, str, str, List[int]]] = []
    if "preset" in ptypes:
        q = session.query(PredictionTop5).filter_by(race_id=int(race_id), predictor_type="preset")
        if me:
            q = q.filter(PredictionTop5.member_email == me)
        if preset_name_s:
            q = q.filter(PredictionTop5.predictor_key == preset_name_s)
        for p in q.all():
            top5 = getattr(p, "top5", None)
            if isinstance(top5, list) and top5:
                mem2 = me or str(getattr(p, "member_email", "") or "").strip().lower()
                preds.append(
                    (
                        "preset",
                        str(getattr(p, "predictor_key", "") or ""),
                        mem2,
                        [int(x) for x in top5 if str(x).strip().isdigit()][:5],
                    )
                )
    if "factor" in ptypes:
        q = session.query(PredictionTop5).filter_by(race_id=int(race_id), predictor_type="factor")
        for p in q.all():
            top5 = getattr(p, "top5", None)
            if isinstance(top5, list) and top5:
                preds.append(("factor", str(getattr(p, "predictor_key", "") or ""), "", [int(x) for x in top5 if str(x).strip().isdigit()][:5]))
    if "ai" in ptypes:
        t5 = _load_ai_top5(session, getattr(race, "race_date", None), getattr(race, "race_no", None))
        if t5:
            preds.append(("ai", "AI", "", t5))

    ds = ""
    try:
        ds = race.race_date.strftime("%Y/%m/%d")
    except Exception:
        ds = ""
    venue = str(getattr(race, "venue", "") or "")
    rn = int(getattr(race, "race_no", 0) or 0)

    tips: List[Dict[str, Any]] = []
    for ptype, pkey, mem, top5 in preds:
        for pos in positions:
            if pos <= 0 or pos > len(top5):
                continue
            hn = top5[pos - 1]
            info = entry_info.get(int(hn)) or {}
            odds_v = odds_map.get(int(hn))
            bucket = bucket_odds(odds_v)
            if bucket not in odds_buckets:
                continue
            st_row = stats_map.get((ptype, pkey, mem if ptype == "preset" else "", int(pos), bucket))
            if not st_row:
                continue
            appear = int(st_row.get("appear") or 0)
            if appear < min_samples:
                continue
            place_rate = st_row.get("place_rate")
            win_rate = st_row.get("win_rate")
            try:
                pr = float(place_rate) if place_rate is not None else None
            except Exception:
                pr = None
            try:
                wr = float(win_rate) if win_rate is not None else None
            except Exception:
                wr = None

            src_label = "會員組合" if ptype == "preset" else ("獨立條件" if ptype == "factor" else "AI")
            key_label = pkey
            if ptype == "factor":
                key_label = str(factor_labels.get(pkey) or pkey)
            if ptype == "ai":
                key_label = "🤖 AI 推介"

            base_tip = {
                "race_date": ds,
                "venue": venue,
                "race_no": rn,
                "race_id": int(race_id),
                "horse_no": int(hn),
                "horse_name": str(info.get("horse_name") or ""),
                "jockey": str(info.get("jockey") or ""),
                "trainer": str(info.get("trainer") or ""),
                "odds_source": odds_source,
                "odds_source_label": _odds_source_label(odds_source),
                "win_odds": odds_v,
                "odds_bucket": bucket,
                "odds_bucket_label": bucket_label(bucket),
                "position": int(pos),
                "predictor_type": ptype,
                "predictor_type_label": src_label,
                "predictor_key": pkey,
                "predictor_key_label": key_label,
                "member_email": (mem if ptype == "preset" else None),
                "appear": appear,
                "win": int(st_row.get("win") or 0),
                "place": int(st_row.get("place") or 0),
                "win_rate": wr,
                "place_rate": pr,
                "min_samples": min_samples,
                "min_place_rate": min_place,
                "min_win_rate": min_win,
            }

            if pr is not None and pr >= min_place:
                tips.append({**base_tip, "hit_type": "place", "hit_label": "入圍", "hit_rate": pr, "hit_threshold": min_place})
            if wr is not None and wr >= min_win:
                tips.append({**base_tip, "hit_type": "win", "hit_label": "勝出", "hit_rate": wr, "hit_threshold": min_win})

    tips.sort(key=lambda x: (-(float(x.get("hit_rate") or 0.0)), -int(x.get("appear") or 0), int(x.get("position") or 0)))
    return tips[:max_tips] if max_tips > 0 else tips
