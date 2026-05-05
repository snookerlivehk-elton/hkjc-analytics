from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import func

from database.models import Race, RaceEntry, SystemConfig, RaceTrackCondition, ScoringFactor
from scoring_engine.track_conditions import normalize_going


DEFAULT_RERANK_CONFIG = {
    "ai_prior_weight": 1.0,
    "total_score_weight": 2.0,
    "speedpro_weight": 0.3,
    "recent_weight": 0.2,
    "jt_weight": 0.2,
}

DEFAULT_BUCKET_TUNE = {
    "objective": {"w2_weight": 0.7, "top3_2in_weight": 0.3},
    "grid_preset": "fast",
}


def _dist_bucket(distance: Any) -> str:
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


def _venue_code(venue: Any) -> str:
    v = str(venue or "").strip().upper()
    if v == "HV" or "跑馬地" in v:
        return "HV"
    return "ST"


def _get_going_code(session: Session, race: Race) -> str:
    tc = session.query(RaceTrackCondition).filter_by(race_id=int(race.id)).first()
    code = str(getattr(tc, "going_code", "") or "").strip()
    if code:
        return code
    _, code2 = normalize_going(str(getattr(race, "going", "") or ""))
    return str(code2 or "").strip()


def _bucket_parts(session: Session, race: Race) -> Optional[Tuple[str, str, str, str]]:
    venue = _venue_code(getattr(race, "venue", ""))
    going_code = _get_going_code(session, race)
    course = str(getattr(race, "course_type", "") or "").strip() or "U"
    dist_b = _dist_bucket(getattr(race, "distance", None))
    if not going_code:
        return None
    return venue, going_code, course, dist_b


def _bucket_key(parts: Tuple[str, str, str, str]) -> str:
    v, g, c, d = parts
    return f"ai_rerank_cfg:{v}:{g}:{c}:{d}"


def load_bucket_rerank_config(session: Session, parts: Tuple[str, str, str, str]) -> Optional[Dict[str, float]]:
    key = _bucket_key(parts)
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if not cfg or not isinstance(cfg.value, dict):
        return None
    val = cfg.value.get("weights") if isinstance(cfg.value.get("weights"), dict) else cfg.value
    if not isinstance(val, dict):
        return None
    out = {}
    for k in list(DEFAULT_RERANK_CONFIG.keys()):
        try:
            if k in val:
                out[k] = float(val.get(k))
        except Exception:
            continue
    return out or None


def save_bucket_rerank_config(session: Session, parts: Tuple[str, str, str, str], weights: Dict[str, Any], meta: Optional[Dict[str, Any]] = None) -> None:
    key = _bucket_key(parts)
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if not cfg:
        cfg = SystemConfig(key=key, description="AI Top5 重排校準參數（分桶）")
        session.add(cfg)
    w = {}
    for k, v in dict(weights or {}).items():
        if str(k) not in DEFAULT_RERANK_CONFIG:
            continue
        try:
            w[str(k)] = float(v)
        except Exception:
            continue
    cfg.value = {
        "bucket": {"venue": parts[0], "going_code": parts[1], "course_type": parts[2], "dist_bucket": parts[3]},
        "weights": w,
        "meta": dict(meta or {}),
        "updated_at": datetime.utcnow().isoformat(),
    }
    session.commit()


def _factors_by_horse_for_race(session: Session, race_id: int) -> Dict[str, Dict[str, Any]]:
    entries = session.query(RaceEntry).filter(RaceEntry.race_id == int(race_id)).all()
    out: Dict[str, Dict[str, Any]] = {}
    for e in entries:
        hn = str(getattr(e, "horse_no", "") or "").strip()
        if not hn:
            continue
        out[hn] = {"total_score": getattr(e, "total_score", None)}
    s_factors = (
        session.query(ScoringFactor)
        .join(RaceEntry)
        .filter(RaceEntry.race_id == int(race_id))
        .filter(ScoringFactor.factor_name.in_(["speedpro_energy", "recent_form", "jockey_trainer_bond"]))
        .all()
    )
    for f in s_factors:
        try:
            hn = str(getattr(getattr(f, "entry", None), "horse_no", "") or "").strip()
        except Exception:
            hn = ""
        if not hn:
            continue
        if hn not in out:
            out[hn] = {}
        if f.factor_name == "speedpro_energy":
            out[hn]["speedpro"] = f.score
        elif f.factor_name == "recent_form":
            out[hn]["recent"] = f.score
        elif f.factor_name == "jockey_trainer_bond":
            out[hn]["jt"] = f.score
    return out


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

    cfg0 = None
    parts = None
    try:
        race = session.query(Race).filter(Race.id == int(race_id)).first()
        if race:
            parts = _bucket_parts(session, race)
    except Exception:
        parts = None
    if cfg is not None:
        cfg0 = dict(cfg)
    else:
        if parts:
            bcfg = load_bucket_rerank_config(session, parts)
            if bcfg:
                cfg0 = dict(DEFAULT_RERANK_CONFIG)
                cfg0.update(bcfg)
        if cfg0 is None:
            cfg0 = dict(load_ai_rerank_config(session))
    fb = factors_by_horse if isinstance(factors_by_horse, dict) else {}

    score_map = {}
    fb_items = [(k, v) for k, v in fb.items()] if fb else []
    for k, v in fb_items:
        try:
            hn = int(k)
        except Exception:
            continue
        try:
            ts = float((v or {}).get("total_score") or 0.0)
        except Exception:
            ts = 0.0
        if hn > 0:
            score_map[hn] = ts
    if not score_map:
        entries = session.query(RaceEntry).filter(RaceEntry.race_id == int(race_id)).all()
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
            "bucket": {"venue": parts[0], "going_code": parts[1], "course_type": parts[2], "dist_bucket": parts[3]} if parts else None,
            "bucket_key": _bucket_key(parts) if parts else None,
        },
    }


def _eval_metrics(top5: List[int], act_top4: List[int]) -> Dict[str, int]:
    if not top5 or len(act_top4) < 4:
        return {"w2": 0, "top3_2in_top4": 0}
    winner = act_top4[0]
    act_set = set(act_top4)
    p1 = top5[0] if len(top5) > 0 else None
    p2 = top5[1] if len(top5) > 1 else None
    w2 = 1 if winner in set([p1, p2]) else 0
    top3 = set([x for x in top5[:3] if x is not None])
    t2 = 1 if len(top3 & act_set) >= 2 else 0
    return {"w2": w2, "top3_2in_top4": t2}


def _grid_values(preset: str) -> List[Dict[str, float]]:
    p = str(preset or "").strip().lower()
    if p == "thorough":
        ai_prior = [0.0, 0.5, 1.0]
        total_score = [1.0, 2.0, 3.0]
        speedpro = [0.0, 0.2, 0.4]
        recent = [0.0, 0.2, 0.4]
        jt = [0.0, 0.2, 0.4]
    else:
        ai_prior = [0.0, 1.0]
        total_score = [1.0, 2.0, 3.0]
        speedpro = [0.0, 0.3]
        recent = [0.0, 0.2]
        jt = [0.0, 0.2]

    out = []
    for a in ai_prior:
        for t in total_score:
            for sp in speedpro:
                for re0 in recent:
                    for j in jt:
                        out.append(
                            {
                                "ai_prior_weight": float(a),
                                "total_score_weight": float(t),
                                "speedpro_weight": float(sp),
                                "recent_weight": float(re0),
                                "jt_weight": float(j),
                            }
                        )
    return out


def tune_rerank_for_bucket(
    session: Session,
    parts: Tuple[str, str, str, str],
    d1: Optional[datetime] = None,
    d2: Optional[datetime] = None,
    max_races: int = 200,
    grid_preset: str = "fast",
    objective: Optional[Dict[str, float]] = None,
    save: bool = True,
) -> Dict[str, Any]:
    obj = dict(DEFAULT_BUCKET_TUNE["objective"])
    if isinstance(objective, dict):
        for k in list(obj.keys()):
            try:
                if k in objective:
                    obj[k] = float(objective.get(k))
            except Exception:
                pass
    w2w = float(obj.get("w2_weight") or 0.0)
    t2w = float(obj.get("top3_2in_weight") or 0.0)
    if (w2w + t2w) <= 0:
        w2w, t2w = 0.7, 0.3

    from scoring_engine.member_stats import _actual_topk_for_race

    venue, going_code, course, dist_b = parts

    q = session.query(Race).order_by(Race.race_date.desc(), Race.race_no.asc())
    if d1 is not None:
        q = q.filter(Race.race_date >= d1)
    if d2 is not None:
        q = q.filter(Race.race_date <= d2)
    q = q.filter(Race.venue == str(venue))
    q = q.filter(Race.course_type == str(course))
    if str(dist_b) == "S":
        q = q.filter(Race.distance != None).filter(Race.distance <= 1200)
    elif str(dist_b) == "M":
        q = q.filter(Race.distance != None).filter(Race.distance > 1200).filter(Race.distance <= 1600)
    elif str(dist_b) == "L":
        q = q.filter(Race.distance != None).filter(Race.distance > 1600)
    else:
        q = q.filter((Race.distance == None) | (Race.distance <= 0))

    races = q.limit(5000).all()

    scanned = 0
    in_bucket = 0
    with_report = 0
    with_results = 0
    cand = []
    missing_examples = {"no_ai_report": [], "no_results": []}

    for race in races:
        scanned += 1
        p2 = _bucket_parts(session, race)
        if not p2 or p2 != parts:
            continue
        in_bucket += 1

        date_str = race.race_date.strftime("%Y/%m/%d")
        key = f"ai_race_report:{date_str}:{int(race.race_no)}"
        cfg0 = session.query(SystemConfig).filter_by(key=key).first()
        if not cfg0 or not isinstance(cfg0.value, dict):
            if len(missing_examples["no_ai_report"]) < 5:
                missing_examples["no_ai_report"].append({"date": date_str, "race_no": int(race.race_no)})
            continue
        with_report += 1

        val = cfg0.value
        t5 = val.get("top5_horse_nos_original")
        if not isinstance(t5, list):
            t5 = val.get("top5_horse_nos")
        if not isinstance(t5, list) or not t5:
            continue

        act = _actual_topk_for_race(session, int(race.id), 4)
        if len(act) < 4:
            if len(missing_examples["no_results"]) < 5:
                missing_examples["no_results"].append({"date": date_str, "race_no": int(race.race_no)})
            continue
        with_results += 1

        cand.append((race, t5, act))
        if len(cand) >= int(max_races or 200):
            break

    if not cand:
        reason = "no_samples"
        if in_bucket <= 0:
            reason = "no_races_in_bucket"
        elif with_report <= 0:
            reason = "no_ai_reports"
        elif with_results <= 0:
            reason = "no_results"
        return {
            "ok": False,
            "reason": reason,
            "bucket": {"venue": venue, "going_code": going_code, "course_type": course, "dist_bucket": dist_b},
            "bucket_key": _bucket_key(parts),
            "debug": {
                "scanned": int(scanned),
                "in_bucket": int(in_bucket),
                "with_report": int(with_report),
                "with_results": int(with_results),
                "missing_examples": missing_examples,
                "hint": "需同時具備：AI 報告（ai_race_report）+ 已有賽果 Top4；並確保 RaceTrackCondition.going_code/跑道/距離分桶能對應分桶。",
            },
        }

    base = {"races": 0, "w2": 0, "top3_2in_top4": 0}

    pre = []
    for race, t5, act in cand:
        fb = _factors_by_horse_for_race(session, int(race.id))
        t5i = []
        for x in t5:
            try:
                xi = int(x)
            except Exception:
                continue
            if xi > 0:
                t5i.append(xi)
        if not t5i:
            continue
        m = _eval_metrics(t5i, act)
        base["races"] += 1
        base["w2"] += int(m.get("w2") or 0)
        base["top3_2in_top4"] += int(m.get("top3_2in_top4") or 0)
        pre.append({"race_id": int(race.id), "t5": t5i, "act": act, "fb": fb})

    if base["races"] <= 0:
        return {"ok": False, "reason": "no_valid_samples"}

    grid = _grid_values(grid_preset)
    best = None
    best_score = -1e18
    best_stats = None

    for g in grid:
        st = {"races": 0, "w2": 0, "top3_2in_top4": 0}
        for row in pre:
            rr = rerank_top5(session, int(row["race_id"]), row["t5"], factors_by_horse=row["fb"], cfg=g)
            t5r = rr.get("top5") if isinstance(rr, dict) else None
            if not isinstance(t5r, list) or not t5r:
                continue
            m = _eval_metrics([int(x) for x in t5r], row["act"])
            st["races"] += 1
            st["w2"] += int(m.get("w2") or 0)
            st["top3_2in_top4"] += int(m.get("top3_2in_top4") or 0)

        if st["races"] <= 0:
            continue
        w2_rate = float(st["w2"]) / float(st["races"])
        t2_rate = float(st["top3_2in_top4"]) / float(st["races"])
        score = (w2w * w2_rate) + (t2w * t2_rate)
        if score > best_score:
            best_score = score
            best = g
            best_stats = dict(st)

    def rate(v: int, n: int) -> float:
        if n <= 0:
            return 0.0
        return round((int(v or 0) / int(n)) * 100.0, 1)

    out = {
        "ok": True,
        "bucket": {"venue": parts[0], "going_code": parts[1], "course_type": parts[2], "dist_bucket": parts[3]},
        "bucket_key": _bucket_key(parts),
        "objective": {"w2_weight": w2w, "top3_2in_weight": t2w},
        "grid_preset": str(grid_preset),
        "samples": int(base["races"]),
        "base": {"races": base["races"], "w2_rate": rate(base["w2"], base["races"]), "top3_2in_rate": rate(base["top3_2in_top4"], base["races"])},
        "best": {"weights": best, "w2_rate": rate(best_stats["w2"], best_stats["races"]) if best_stats else 0.0, "top3_2in_rate": rate(best_stats["top3_2in_top4"], best_stats["races"]) if best_stats else 0.0},
    }

    if save and isinstance(best, dict):
        meta = {"tuned_at": datetime.utcnow().isoformat(), "samples": int(base["races"]), "base": out["base"], "best_rates": {"w2": out["best"]["w2_rate"], "top3_2in": out["best"]["top3_2in_rate"]}, "objective": out["objective"], "grid_preset": out["grid_preset"]}
        save_bucket_rerank_config(session, parts, best, meta=meta)
    return out


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
