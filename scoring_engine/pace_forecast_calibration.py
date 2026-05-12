import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from database.models import Race, RacePaceForecastSnapshot, RacePaceSnapshot, SystemConfig
from scoring_engine.config_value import build_meta, unwrap_value, wrap_value

PACE_UNKNOWN = "unknown"
PACE_VERY_FAST = "very_fast"
PACE_FAST = "fast"
PACE_MODERATE_FAST = "moderate_fast"
PACE_MODERATE = "moderate"
PACE_MODERATE_SLOW = "moderate_slow"
PACE_SLOW = "slow"
PACE_VERY_SLOW = "very_slow"


PACE_ORDER = [
    PACE_VERY_SLOW,
    PACE_SLOW,
    PACE_MODERATE_SLOW,
    PACE_MODERATE,
    PACE_MODERATE_FAST,
    PACE_FAST,
    PACE_VERY_FAST,
]
PACE_IDX = {k: i for i, k in enumerate(PACE_ORDER)}


def _parse_ymd(s: str) -> Optional[date]:
    t = str(s or "").strip().replace("-", "/")
    if not t:
        return None
    try:
        return datetime.strptime(t, "%Y/%m/%d").date()
    except Exception:
        return None


def _score_avg(front_sum: float, front_topk: int) -> float:
    try:
        fs = float(front_sum or 0.0)
    except Exception:
        fs = 0.0
    try:
        k = int(front_topk or 0)
    except Exception:
        k = 0
    k = max(1, k)
    v = fs / float(k)
    if v < 0.0:
        v = 0.0
    if v > 1.0:
        v = 1.0
    return float(v)


def _group_key(venue: str, surface_code: str, distance: int) -> Optional[str]:
    v = str(venue or "").strip().upper()
    sc = str(surface_code or "").strip().upper()
    try:
        d = int(distance or 0)
    except Exception:
        d = 0
    if not v or not sc or d <= 0:
        return None
    return f"{v}|{sc}|{d}"


def _learn_thresholds(items: List[Tuple[float, str]]) -> Optional[List[float]]:
    if not items:
        return None
    filtered = [(float(s), str(a)) for s, a in items if str(a) in PACE_IDX]
    if not filtered:
        return None
    filtered.sort(key=lambda x: x[0])

    counts = {k: 0 for k in PACE_ORDER}
    for _, a in filtered:
        counts[str(a)] = int(counts.get(str(a), 0)) + 1
    n = int(len(filtered))
    if n <= 5:
        return None

    scores = [float(s) for s, _ in filtered]
    thr: List[float] = []
    cum = 0
    last = float(scores[0])
    for k in PACE_ORDER[:-1]:
        cum += int(counts.get(k, 0))
        idx = int(cum - 1)
        if idx < 0:
            t = float(last)
        elif idx >= n:
            t = float(scores[-1])
        else:
            t = float(scores[idx])
        if t < last:
            t = float(last)
        last = float(t)
        thr.append(float(t))

    thr = [max(0.0, min(1.0, float(x))) for x in thr]
    for i in range(1, len(thr)):
        if thr[i] < thr[i - 1]:
            thr[i] = float(thr[i - 1])
    return thr


def _blend_thresholds(new_thr: List[float], old_thr: Optional[List[float]], alpha: float) -> List[float]:
    a = float(alpha or 0.0)
    a = max(0.0, min(1.0, a))
    if not old_thr or len(old_thr) != len(new_thr):
        return [float(x) for x in new_thr]
    out = []
    for n, o in zip(new_thr, old_thr):
        out.append(float(a) * float(n) + (1.0 - float(a)) * float(o))
    out = [max(0.0, min(1.0, float(x))) for x in out]
    for i in range(1, len(out)):
        if out[i] < out[i - 1]:
            out[i] = float(out[i - 1])
    return out


def _bucketize(score: float, thresholds: List[float]) -> str:
    try:
        s = float(score)
    except Exception:
        return PACE_UNKNOWN
    if not thresholds or len(thresholds) != (len(PACE_ORDER) - 1):
        return PACE_UNKNOWN
    for i, t in enumerate(thresholds):
        if float(s) <= float(t):
            return str(PACE_ORDER[int(i)])
    return str(PACE_ORDER[-1])


def load_pace_forecast_calibration(session: Session) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    cfg = session.query(SystemConfig).filter_by(key="pace_forecast_calibration:v1").first()
    if not cfg:
        return None, {}
    payload, meta = unwrap_value(cfg.value)
    if not isinstance(payload, dict):
        return None, dict(meta or {})
    return dict(payload), dict(meta or {})


def save_pace_forecast_calibration(session: Session, payload: Dict[str, Any]) -> None:
    cfg = session.query(SystemConfig).filter_by(key="pace_forecast_calibration:v1").first()
    if not cfg:
        cfg = SystemConfig(key="pace_forecast_calibration:v1", description="步速預測校準（由賽後步速學習）")
        session.add(cfg)
    cfg.value = wrap_value(
        payload,
        build_meta(
            source="learn_pace_forecast_calibration",
            schema=str(payload.get("schema") or ""),
            extra={"trained_range": payload.get("trained_range"), "n_pairs": payload.get("n_pairs")},
        ),
    )
    session.commit()


def learn_pace_forecast_calibration(
    session: Session,
    *,
    start_date: date,
    end_date: date,
    min_samples: int = 80,
    alpha: float = 0.25,
) -> Dict[str, Any]:
    d1 = start_date
    d2 = end_date
    if d1 > d2:
        d1, d2 = d2, d1

    start_dt = datetime.combine(d1, datetime.min.time())
    end_dt = datetime.combine(d2 + timedelta(days=1), datetime.min.time())

    old_payload, _ = load_pace_forecast_calibration(session)
    old_groups = {}
    old_global = None
    if isinstance(old_payload, dict):
        og = old_payload.get("groups")
        if isinstance(og, dict):
            old_groups = dict(og)
        gg = old_payload.get("global")
        if isinstance(gg, dict):
            old_global = dict(gg)

    pairs = (
        session.query(RacePaceForecastSnapshot, RacePaceSnapshot, Race)
        .join(RacePaceSnapshot, RacePaceSnapshot.race_id == RacePaceForecastSnapshot.race_id)
        .join(Race, Race.id == RacePaceForecastSnapshot.race_id)
        .filter(Race.race_date >= start_dt)
        .filter(Race.race_date < end_dt)
        .all()
    )

    all_items: List[Tuple[float, str]] = []
    by_group: Dict[str, List[Tuple[float, str]]] = {}
    for pred, act, race in pairs:
        a = str(getattr(act, "pace_class", "") or "").strip()
        if a not in PACE_IDX:
            continue
        meta = getattr(pred, "meta", None)
        topk = 4
        if isinstance(meta, dict):
            try:
                topk = int(meta.get("front_topk") or 4)
            except Exception:
                topk = 4
        sc = str(getattr(pred, "surface_code", "") or "").strip()
        venue = str(getattr(pred, "venue", "") or "").strip()
        dist = int(getattr(pred, "distance", 0) or 0)
        score = _score_avg(getattr(pred, "front_sum", 0.0), front_topk=int(topk))
        all_items.append((float(score), str(a)))
        gk = _group_key(venue, sc, dist)
        if gk:
            by_group.setdefault(str(gk), []).append((float(score), str(a)))

    min_s = max(30, int(min_samples or 0))
    thr_global = _learn_thresholds(all_items)
    global_out = None
    if thr_global:
        global_out = {
            "n": int(len(all_items)),
            "thresholds": _blend_thresholds(
                list(thr_global),
                old_global.get("thresholds") if isinstance(old_global, dict) else None,
                float(alpha),
            )
            if float(alpha) > 0.0
            else list(thr_global),
        }

    groups_out: Dict[str, Any] = {}
    for gk, items in by_group.items():
        if len(items) < min_s:
            continue
        thr = _learn_thresholds(items)
        if not thr:
            continue
        old = old_groups.get(gk) if isinstance(old_groups, dict) else None
        old_thr = None
        if isinstance(old, dict):
            old_thr = old.get("thresholds")
        groups_out[gk] = {
            "n": int(len(items)),
            "thresholds": _blend_thresholds(list(thr), old_thr if isinstance(old_thr, list) else None, float(alpha))
            if float(alpha) > 0.0
            else list(thr),
        }

    return {
        "schema": "pace_forecast_calibration:v1",
        "trained_range": {"from": d1.isoformat(), "to": d2.isoformat()},
        "n_pairs": int(len(all_items)),
        "min_samples": int(min_s),
        "alpha": float(alpha),
        "global": global_out,
        "groups": groups_out,
        "updated_at": datetime.utcnow().isoformat(),
    }


def apply_pace_forecast_calibration(
    session: Session,
    *,
    venue: str,
    surface_code: str,
    distance: int,
    score_avg: float,
    raw_pace_class: str,
) -> Tuple[str, Dict[str, Any]]:
    enabled = str(os.environ.get("PACE_FORECAST_USE_CALIBRATION") or "1").strip().lower() in ("1", "true", "yes")
    if not enabled:
        return str(raw_pace_class), {"enabled": False}
    payload, meta = load_pace_forecast_calibration(session)
    if not isinstance(payload, dict):
        return str(raw_pace_class), {"enabled": True, "reason": "no_calibration"}

    gk = _group_key(str(venue), str(surface_code), int(distance or 0))
    thresholds = None
    used = "global"
    prior_n = 0
    if gk and isinstance(payload.get("groups"), dict):
        g = payload.get("groups", {}).get(str(gk))
        if isinstance(g, dict) and isinstance(g.get("thresholds"), list):
            thresholds = list(g.get("thresholds"))
            used = str(gk)
            prior_n = int(g.get("n") or 0)
    if thresholds is None:
        gg = payload.get("global")
        if isinstance(gg, dict) and isinstance(gg.get("thresholds"), list):
            thresholds = list(gg.get("thresholds"))
            prior_n = int(gg.get("n") or 0)

    if thresholds is None:
        return str(raw_pace_class), {"enabled": True, "reason": "no_thresholds"}

    adj = _bucketize(float(score_avg), thresholds=thresholds)
    if adj == PACE_UNKNOWN:
        adj = str(raw_pace_class)
    return str(adj), {
        "enabled": True,
        "used": str(used),
        "prior_n": int(prior_n),
        "score_avg": float(round(float(score_avg), 6)),
        "raw": str(raw_pace_class),
        "meta": dict(meta or {}),
    }
