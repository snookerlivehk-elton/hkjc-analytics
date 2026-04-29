from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from database.models import RaceEntry, RaceResult, ScoringFactor, ScoringWeight
from scoring_engine.constants import DISABLED_FACTORS
from scoring_engine.factors import get_available_factors


def actual_ranks_by_horse_no(session: Session, race_id: int) -> Dict[int, int]:
    rows = (
        session.query(RaceEntry.horse_no, RaceResult.rank)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceEntry.race_id == int(race_id))
        .filter(RaceResult.rank != None)
        .all()
    )
    out: Dict[int, int] = {}
    for hn, rk in rows:
        try:
            out[int(hn or 0)] = int(rk or 0)
        except Exception:
            continue
    return out


def actual_topk(session: Session, race_id: int, k: int) -> List[int]:
    m = actual_ranks_by_horse_no(session, race_id)
    items = [(hn, rk) for hn, rk in m.items() if rk and rk > 0]
    items.sort(key=lambda x: x[1])
    return [hn for hn, _ in items[: int(k or 0)]]


def predicted_topk_by_total(session: Session, race_id: int, k: int) -> List[int]:
    rows = (
        session.query(RaceEntry.horse_no)
        .filter(RaceEntry.race_id == int(race_id))
        .order_by(RaceEntry.total_score.desc().nullslast(), RaceEntry.id.asc())
        .limit(int(k or 0))
        .all()
    )
    out: List[int] = []
    for (hn,) in rows:
        try:
            out.append(int(hn or 0))
        except Exception:
            out.append(0)
    return [x for x in out if x > 0]


def predicted_bottomk_by_total(session: Session, race_id: int, k: int) -> List[int]:
    rows = (
        session.query(RaceEntry.horse_no)
        .filter(RaceEntry.race_id == int(race_id))
        .order_by(RaceEntry.total_score.asc().nullslast(), RaceEntry.id.asc())
        .limit(int(k or 0))
        .all()
    )
    out: List[int] = []
    for (hn,) in rows:
        try:
            out.append(int(hn or 0))
        except Exception:
            out.append(0)
    return [x for x in out if x > 0]


def _factor_rows(session: Session, race_id: int, factor_name: str) -> List[Tuple[int, float]]:
    rows = (
        session.query(RaceEntry.horse_no, ScoringFactor.score)
        .join(ScoringFactor, ScoringFactor.entry_id == RaceEntry.id)
        .filter(RaceEntry.race_id == int(race_id))
        .filter(ScoringFactor.factor_name == str(factor_name))
        .all()
    )
    out: List[Tuple[int, float]] = []
    for hn, sc in rows:
        try:
            out.append((int(hn or 0), float(sc or 0.0)))
        except Exception:
            continue
    return [(hn, sc) for hn, sc in out if hn > 0]


def predicted_topk_by_factor(session: Session, race_id: int, factor_name: str, k: int) -> List[int]:
    rows = _factor_rows(session, race_id, factor_name)
    rows.sort(key=lambda x: (-x[1], x[0]))
    return [hn for hn, _ in rows[: int(k or 0)]]


def predicted_bottomk_by_factor(session: Session, race_id: int, factor_name: str, k: int) -> List[int]:
    rows = _factor_rows(session, race_id, factor_name)
    rows.sort(key=lambda x: (x[1], x[0]))
    return [hn for hn, _ in rows[: int(k or 0)]]


def active_factor_names(session: Session) -> List[str]:
    available = get_available_factors()
    rows = (
        session.query(ScoringWeight.factor_name)
        .filter(ScoringWeight.is_active == True)
        .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
        .order_by(ScoringWeight.factor_name.asc())
        .all()
    )
    out: List[str] = []
    for (n,) in rows:
        n = str(n or "").strip()
        if n and n in available:
            out.append(n)
    return out


def reverse_stats_for_race(
    actual_positive: List[int],
    predicted_negative: List[int],
) -> Dict[str, Any]:
    pos = {int(x) for x in (actual_positive or []) if int(x or 0) > 0}
    neg = {int(x) for x in (predicted_negative or []) if int(x or 0) > 0}

    total_pred_neg = len(neg)
    tn = len([x for x in neg if x not in pos])
    fp = len([x for x in neg if x in pos])
    return {
        "pred_neg": total_pred_neg,
        "tn": tn,
        "fp": fp,
        "neg_accuracy": (tn / total_pred_neg) if total_pred_neg else None,
        "false_elim_rate": (fp / total_pred_neg) if total_pred_neg else None,
    }


def factor_contributions_for_entry(session: Session, entry_id: int) -> List[Dict[str, Any]]:
    rows = (
        session.query(
            ScoringFactor.factor_name,
            ScoringFactor.raw_value,
            ScoringFactor.score,
            ScoringFactor.weight_at_time,
            ScoringFactor.raw_data_display,
        )
        .filter(ScoringFactor.entry_id == int(entry_id))
        .all()
    )
    out: List[Dict[str, Any]] = []
    for fn, raw_value, score, weight, display in rows:
        name = str(fn or "").strip()
        if not name:
            continue
        sc = float(score or 0.0)
        wt = float(weight or 0.0)
        contrib = sc * wt
        disp = (str(display) if display is not None else "")
        missing = (disp.strip() in ("", "無數據")) or (raw_value is None)
        out.append(
            {
                "factor": name,
                "raw_value": raw_value,
                "score": sc,
                "weight": wt,
                "contrib": contrib,
                "display": disp,
                "missing": bool(missing),
            }
        )
    out.sort(key=lambda x: (-(x.get("contrib") or 0.0), x.get("factor") or ""))
    return out


def summarize_entry_reason(session: Session, entry_id: int, top_n: int = 3) -> str:
    items = factor_contributions_for_entry(session, entry_id)
    tops = items[: int(top_n or 0)]
    if not tops:
        return ""
    parts = []
    for it in tops:
        parts.append(f"{it.get('factor')}({round(float(it.get('contrib') or 0.0), 2)})")
    missing_cnt = sum(1 for it in items if it.get("missing") is True)
    if missing_cnt:
        parts.append(f"缺{missing_cnt}")
    return " | ".join(parts)

