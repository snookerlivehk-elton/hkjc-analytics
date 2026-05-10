from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from sqlalchemy.orm import Session

from scoring_engine.config_value import unwrap_value


@dataclass(frozen=True)
class OddsBucket:
    key: str
    label: str
    lo: Optional[float] = None
    hi: Optional[float] = None


ODDS_BUCKETS: List[OddsBucket] = [
    OddsBucket("LT7", "<7", None, 7.0),
    OddsBucket("B7_10", "7-9.99", 7.0, 10.0),
    OddsBucket("B10_15", "10-14.99", 10.0, 15.0),
    OddsBucket("B15_20", "15-19.99", 15.0, 20.0),
    OddsBucket("B20_35", "20-34.99", 20.0, 35.0),
    OddsBucket("GE35", ">=35", 35.0, None),
    OddsBucket("UNKNOWN", "未知", None, None),
]


def _bucket_odds(odds: Any) -> str:
    try:
        v = float(odds)
    except Exception:
        return "UNKNOWN"
    if not (v > 0):
        return "UNKNOWN"
    for b in ODDS_BUCKETS:
        if b.key == "UNKNOWN":
            continue
        if b.lo is None and b.hi is not None:
            if v < b.hi:
                return b.key
        elif b.lo is not None and b.hi is None:
            if v >= b.lo:
                return b.key
        elif b.lo is not None and b.hi is not None:
            if (v >= b.lo) and (v < b.hi):
                return b.key
    return "UNKNOWN"


def _bucket_label(bucket_key: str) -> str:
    for b in ODDS_BUCKETS:
        if b.key == bucket_key:
            return b.label
    return str(bucket_key)


def _day_range(d: date) -> Tuple[datetime, datetime]:
    start = datetime.combine(d, time.min)
    end = start + timedelta(days=1)
    return start, end


def _date_range(d1: date, d2: date) -> Tuple[datetime, datetime]:
    s = datetime.combine(d1, time.min)
    e = datetime.combine(d2, time.min) + timedelta(days=1)
    return s, e


def compute_top5_odds_stats(
    session: Session,
    *,
    d1: date,
    d2: date,
    member_email: Optional[str] = None,
    include_presets: bool = True,
    include_factors: bool = True,
    include_ai: bool = True,
    place_k: int = 3,
    top_k: int = 5,
    odds_source: str = "result_win_odds",
) -> pd.DataFrame:
    from database.models import OddsHistory, PredictionTop5, Race, RaceEntry, RaceResult, ScoringWeight, SystemConfig

    factor_labels: Dict[str, str] = {}
    try:
        rows0 = session.query(ScoringWeight.factor_name, ScoringWeight.description).all()
        for fn, desc in rows0:
            k = str(fn or "").strip()
            v = str(desc or "").strip()
            if k and v:
                factor_labels[k] = v
    except Exception:
        factor_labels = {}

    type_labels = {"preset": "會員組合", "factor": "獨立條件", "ai": "AI"}

    def _predictor_key_label(ptype: str, pkey: str) -> str:
        if ptype == "factor":
            return str(factor_labels.get(pkey) or pkey)
        if ptype == "ai":
            return "🤖 AI 推介"
        return str(pkey)

    start, end = _date_range(d1, d2)
    race_rows = (
        session.query(Race.id, Race.race_date, Race.race_no, Race.venue)
        .filter(Race.race_date >= start)
        .filter(Race.race_date < end)
        .order_by(Race.race_date.asc(), Race.race_no.asc())
        .all()
    )
    race_ids = [int(rid) for (rid, _, _, _) in race_rows if rid is not None]
    if not race_ids:
        return pd.DataFrame()

    odds_map: Dict[int, float] = {}
    if str(odds_source or "") == "latest_history":
        sub = (
            session.query(OddsHistory.entry_id.label("eid"), OddsHistory.captured_at.label("cap"))
            .filter(OddsHistory.entry_id.isnot(None))
            .group_by(OddsHistory.entry_id)
            .subquery()
        )
        orows = (
            session.query(OddsHistory.entry_id, OddsHistory.win_odds)
            .join(sub, (sub.c.eid == OddsHistory.entry_id) & (sub.c.cap == OddsHistory.captured_at))
            .all()
        )
        for eid, wo in orows:
            try:
                odds_map[int(eid)] = float(wo) if wo is not None else None
            except Exception:
                continue

    erows = (
        session.query(RaceEntry.race_id, RaceEntry.horse_no, RaceEntry.id, RaceResult.rank, RaceResult.win_odds)
        .outerjoin(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceEntry.race_id.in_(race_ids))
        .all()
    )
    entry_by_race_hn: Dict[Tuple[int, int], Dict[str, Any]] = {}
    for rid, hn, eid, rk, wo in erows:
        try:
            k = (int(rid), int(hn or 0))
        except Exception:
            continue
        if k in entry_by_race_hn:
            continue
        odds_v = None
        if str(odds_source or "") == "latest_history":
            try:
                odds_v = odds_map.get(int(eid))
            except Exception:
                odds_v = None
        else:
            odds_v = wo
        entry_by_race_hn[k] = {"entry_id": int(eid) if eid is not None else None, "rank": rk, "win_odds": odds_v}

    q = session.query(PredictionTop5)
    q = q.filter(PredictionTop5.race_id.in_(race_ids))
    types: List[str] = []
    if include_presets:
        types.append("preset")
    if include_factors:
        types.append("factor")
    if types:
        q = q.filter(PredictionTop5.predictor_type.in_(types))
    else:
        q = q.filter(PredictionTop5.id == -1)
    if member_email:
        me = str(member_email).strip().lower()
        q = q.filter((PredictionTop5.predictor_type != "preset") | (PredictionTop5.member_email == me))
    preds = q.all()

    ai_keys: List[str] = []
    race_meta: Dict[int, Dict[str, Any]] = {}
    for rid, rdt, rn, v in race_rows:
        try:
            ds = rdt.strftime("%Y/%m/%d")
        except Exception:
            ds = ""
        race_meta[int(rid)] = {"date_str": ds, "race_no": int(rn or 0), "venue": str(v or "")}
        if include_ai and ds and int(rn or 0) > 0:
            ai_keys.append(f"ai_race_report:{ds}:{int(rn)}")

    ai_cfgs: Dict[str, Any] = {}
    if include_ai and ai_keys:
        rows = session.query(SystemConfig.key, SystemConfig.value).filter(SystemConfig.key.in_(ai_keys)).all()
        for k, v in rows:
            ai_cfgs[str(k)] = v

    acc: Dict[Tuple[str, str, str, int, str], Dict[str, int]] = {}

    def _add_row(ptype: str, pkey: str, mem: str, pos: int, bucket: str, rk: Any):
        kk = (ptype, pkey, mem, pos, bucket)
        row = acc.get(kk)
        if not row:
            row = {"appear": 0, "win": 0, "place": 0}
            acc[kk] = row
        row["appear"] += 1
        try:
            rki = int(rk) if rk is not None else 0
        except Exception:
            rki = 0
        if rki == 1:
            row["win"] += 1
        if rki > 0 and rki <= int(place_k or 3):
            row["place"] += 1

    for p in preds:
        ptype = str(getattr(p, "predictor_type", "") or "").strip()
        pkey = str(getattr(p, "predictor_key", "") or "").strip()
        mem = str(getattr(p, "member_email", "") or "").strip().lower() if ptype == "preset" else ""
        rid = int(getattr(p, "race_id", 0) or 0)
        top5 = getattr(p, "top5", None)
        if not isinstance(top5, list) or not rid:
            continue
        for i, hn in enumerate(top5[: int(top_k or 5)], 1):
            try:
                hni = int(hn)
            except Exception:
                continue
            ent = entry_by_race_hn.get((rid, hni))
            if not ent:
                continue
            bucket = _bucket_odds(ent.get("win_odds"))
            _add_row(ptype, pkey, mem, i, bucket, ent.get("rank"))

    if include_ai:
        for rid, meta0 in race_meta.items():
            ds = str((meta0 or {}).get("date_str") or "")
            rn = int((meta0 or {}).get("race_no") or 0)
            if not ds or rn <= 0:
                continue
            key = f"ai_race_report:{ds}:{rn}"
            raw_val = ai_cfgs.get(key)
            payload, _ = unwrap_value(raw_val)
            if not isinstance(payload, dict):
                continue
            top5 = payload.get("top5_horse_nos")
            if not isinstance(top5, list) or not top5:
                continue
            for i, hn in enumerate(top5[: int(top_k or 5)], 1):
                try:
                    hni = int(hn)
                except Exception:
                    continue
                ent = entry_by_race_hn.get((int(payload.get("race_id") or rid), hni)) or entry_by_race_hn.get((rid, hni))
                if not ent:
                    continue
                bucket = _bucket_odds(ent.get("win_odds"))
                _add_row("ai", "AI", "", i, bucket, ent.get("rank"))

    rows_out = []
    for (ptype, pkey, mem, pos, bucket), v in acc.items():
        appear = int(v.get("appear") or 0)
        win = int(v.get("win") or 0)
        place = int(v.get("place") or 0)
        ptype_s = str(ptype)
        pkey_s = str(pkey)
        rows_out.append(
            {
                "predictor_type": ptype_s,
                "predictor_type_label": str(type_labels.get(ptype_s) or ptype_s),
                "predictor_key": pkey_s,
                "predictor_key_label": _predictor_key_label(ptype_s, pkey_s),
                "member_email": mem or None,
                "position": int(pos),
                "odds_bucket": bucket,
                "odds_bucket_label": _bucket_label(bucket),
                "appear": appear,
                "win": win,
                "place": place,
                "win_rate": (win / appear) if appear else None,
                "place_rate": (place / appear) if appear else None,
            }
        )
    df = pd.DataFrame(rows_out)
    if not df.empty:
        df = df.sort_values(
            by=["predictor_type", "member_email", "predictor_key_label", "position", "odds_bucket"],
            ascending=[True, True, True, True, True],
        )
    return df
