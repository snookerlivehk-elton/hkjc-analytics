from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import func

from database.models import Race, RaceEntry, SystemConfig


DEFAULT_RERANK_CONFIG = {
    "ai_prior_weight": 1.0,
    "total_score_weight": 2.0,
    "speedpro_weight": 0.3,
    "recent_weight": 0.2,
    "jt_weight": 0.2,
}


def load_ai_rerank_config(session: Session) -> Dict[str, float]:
    cfg = session.query(SystemConfig).filter_by(key="ai_rerank_config").first()
    val = cfg.value if (cfg and isinstance(cfg.value, dict)) else {}
    out = dict(DEFAULT_RERANK_CONFIG)
    for k in list(out.keys()):
        try:
            if k in val:
                out[k] = float(val.get(k))
        except Exception:
            pass
    return out


def save_ai_rerank_config(session: Session, cfg: Dict[str, Any]) -> None:
    c = session.query(SystemConfig).filter_by(key="ai_rerank_config").first()
    if not c:
        c = SystemConfig(key="ai_rerank_config", description="AI Top5 重排校準參數")
        session.add(c)
    out = {}
    for k, v in dict(cfg or {}).items():
        try:
            out[str(k)] = float(v)
        except Exception:
            continue
    c.value = out
    session.commit()


def rerank_top5(
    session: Session,
    race_id: int,
    top5_horse_nos: List[Any],
    factors_by_horse: Optional[Dict[str, Dict[str, Any]]] = None,
    cfg: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    top5 = []
    for x in list(top5_horse_nos or []):
        try:
            xi = int(x)
        except Exception:
            continue
        if xi > 0:
            top5.append(xi)
    if len(top5) <= 1:
        return {"ok": True, "top5": top5, "debug": {"reason": "too_short"}}

    cfg0 = dict(cfg or load_ai_rerank_config(session))
    fb = factors_by_horse if isinstance(factors_by_horse, dict) else {}

    entries = session.query(RaceEntry).filter(RaceEntry.race_id == int(race_id)).all()
    score_map = {}
    for e in entries:
        try:
            hn = int(getattr(e, "horse_no", 0) or 0)
        except Exception:
            hn = 0
        if hn <= 0:
            continue
        try:
            ts = float(getattr(e, "total_score", 0.0) or 0.0)
        except Exception:
            ts = 0.0
        score_map[hn] = ts

    ts_vals = [float(v) for v in score_map.values()]
    ts_min = min(ts_vals) if ts_vals else 0.0
    ts_max = max(ts_vals) if ts_vals else 0.0
    ts_rng = (ts_max - ts_min) if (ts_max - ts_min) > 1e-9 else 1.0

    def norm_ts(hn: int) -> float:
        v = float(score_map.get(hn) or 0.0)
        return max(0.0, min(1.0, (v - ts_min) / ts_rng))

    def norm_factor(hn: int, key: str) -> float:
        try:
            v = float((fb.get(str(hn)) or {}).get(key))
        except Exception:
            v = 0.0
        if v <= 0:
            return 0.0
        return max(0.0, min(1.0, v / 10.0))

    n = len(top5)
    orig_pos = {hn: i for i, hn in enumerate(top5)}

    def ai_prior(hn: int) -> float:
        i = int(orig_pos.get(hn, n))
        if i >= n:
            return 0.0
        return float(n - i) / float(n)

    scored = []
    for hn in top5:
        s = 0.0
        s += float(cfg0.get("ai_prior_weight") or 0.0) * ai_prior(hn)
        s += float(cfg0.get("total_score_weight") or 0.0) * norm_ts(hn)
        s += float(cfg0.get("speedpro_weight") or 0.0) * norm_factor(hn, "speedpro")
        s += float(cfg0.get("recent_weight") or 0.0) * norm_factor(hn, "recent")
        s += float(cfg0.get("jt_weight") or 0.0) * norm_factor(hn, "jt")
        scored.append((s, hn))

    scored.sort(key=lambda x: (-float(x[0]), int(orig_pos.get(x[1], 999)), int(x[1])))
    out = [hn for _, hn in scored]

    return {
        "ok": True,
        "top5": out,
        "debug": {
            "cfg": cfg0,
            "orig": top5,
            "scores": [{"horse_no": int(hn), "score": float(s), "ai_pos": int(orig_pos.get(hn, n))} for s, hn in scored],
        },
    }


def backtest_rerank(
    session: Session,
    d1: Optional[datetime] = None,
    d2: Optional[datetime] = None,
    max_races: int = 300,
    cfg: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    q = session.query(Race).order_by(Race.race_date.desc(), Race.race_no.asc())
    if d1 is not None:
        q = q.filter(Race.race_date >= d1)
    if d2 is not None:
        q = q.filter(Race.race_date <= d2)
    races = q.limit(int(max_races or 300)).all()

    from scoring_engine.member_stats import _actual_topk_for_race

    base = {"races": 0, "w2": 0, "top3_2in_top4": 0}
    rer = {"races": 0, "w2": 0, "top3_2in_top4": 0}

    for race in races:
        date_str = race.race_date.strftime("%Y/%m/%d")
        key = f"ai_race_report:{date_str}:{int(race.race_no)}"
        cfg0 = session.query(SystemConfig).filter_by(key=key).first()
        if not cfg0 or not isinstance(cfg0.value, dict):
            continue
        val = cfg0.value
        top5 = val.get("top5_horse_nos")
        if not isinstance(top5, list) or not top5:
            continue
        act_top4 = _actual_topk_for_race(session, int(race.id), 4)
        if len(act_top4) < 4:
            continue

        winner = act_top4[0]
        act_set = set(act_top4)

        def upd(bucket: Dict[str, int], t5: List[int]):
            bucket["races"] += 1
            p1 = t5[0] if len(t5) > 0 else None
            p2 = t5[1] if len(t5) > 1 else None
            if winner in set([p1, p2]):
                bucket["w2"] += 1
            pred_top3 = set([x for x in t5[:3] if x is not None])
            if len(pred_top3 & act_set) >= 2:
                bucket["top3_2in_top4"] += 1

        t5i = []
        for x in top5:
            try:
                xi = int(x)
            except Exception:
                continue
            if xi > 0:
                t5i.append(xi)
        if not t5i:
            continue

        upd(base, t5i)

        res = rerank_top5(session, int(race.id), t5i, factors_by_horse=None, cfg=cfg)
        t5r = res.get("top5") if isinstance(res, dict) else None
        if isinstance(t5r, list) and t5r:
            upd(rer, [int(x) for x in t5r if int(x or 0) > 0])

    def rate(v: int, n: int) -> float:
        if n <= 0:
            return 0.0
        return round((int(v or 0) / int(n)) * 100.0, 1)

    out = {
        "ok": True,
        "base": {"races": base["races"], "w2": base["w2"], "top3_2in_top4": base["top3_2in_top4"], "w2_rate": rate(base["w2"], base["races"]), "top3_2in_rate": rate(base["top3_2in_top4"], base["races"])},
        "rerank": {"races": rer["races"], "w2": rer["w2"], "top3_2in_top4": rer["top3_2in_top4"], "w2_rate": rate(rer["w2"], rer["races"]), "top3_2in_rate": rate(rer["top3_2in_top4"], rer["races"])},
    }
    return out

