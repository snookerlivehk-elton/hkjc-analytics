from datetime import datetime
from typing import Dict, List, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import func

from database.models import Race, RaceEntry, ScoringFactor, ScoringWeight, SystemConfig, PredictionTop5
from scoring_engine.constants import DISABLED_FACTORS


def _race_ids_for_date(session: Session, target_date_str: str) -> List[int]:
    try:
        d = datetime.strptime(str(target_date_str), "%Y/%m/%d").date()
    except Exception:
        return []

    races = (
        session.query(Race.id)
        .filter(func.date(Race.race_date) == d.isoformat())
        .order_by(Race.race_no.asc(), Race.id.asc())
        .all()
    )
    return [r[0] for r in races]


def _load_member_presets(session: Session) -> List[Tuple[str, str, Dict[str, float]]]:
    cfgs = session.query(SystemConfig).filter(SystemConfig.key.like("member_weight_presets:%")).all()
    out: List[Tuple[str, str, Dict[str, float]]] = []
    for cfg in cfgs:
        if not isinstance(cfg.value, list):
            continue
        email = cfg.key.split(":", 1)[1] if ":" in cfg.key else ""
        email = str(email or "").strip().lower()
        if not email:
            continue
        for p in cfg.value[:3]:
            if not isinstance(p, dict):
                continue
            name = str(p.get("name") or "").strip()
            weights = p.get("weights") if isinstance(p.get("weights"), dict) else {}
            if not name or not weights:
                continue
            w = {}
            for k, v in weights.items():
                try:
                    w[str(k)] = float(v)
                except Exception:
                    continue
            if w:
                out.append((email, name, w))
    return out


def _active_factors(session: Session) -> List[Tuple[str, str]]:
    rows = (
        session.query(ScoringWeight.factor_name, ScoringWeight.description)
        .filter(ScoringWeight.is_active == True)
        .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
        .order_by(ScoringWeight.factor_name.asc())
        .all()
    )
    return [(str(a), str(b or a)) for a, b in rows]


def _fetch_factor_scores_for_race(session: Session, race_id: int, factor_names: List[str]):
    entries = session.query(RaceEntry.id, RaceEntry.horse_no).filter(RaceEntry.race_id == race_id).all()
    if not entries:
        return [], {}, {}

    entry_ids = [e[0] for e in entries]
    entry_id_to_no = {int(e[0]): int(e[1]) for e in entries}

    factors = (
        session.query(ScoringFactor.entry_id, ScoringFactor.factor_name, ScoringFactor.score)
        .filter(ScoringFactor.entry_id.in_(entry_ids))
        .filter(ScoringFactor.factor_name.in_(factor_names))
        .all()
    )

    score_map: Dict[int, Dict[str, float]] = {int(eid): {} for eid in entry_ids}
    for entry_id, factor_name, score in factors:
        score_map[int(entry_id)][str(factor_name)] = float(score or 0.0)

    return entry_ids, entry_id_to_no, score_map


def _topk_by_factor(entry_ids: List[int], entry_id_to_no: Dict[int, int], score_map: Dict[int, Dict[str, float]], factor_name: str, k: int = 5):
    items = []
    for eid in entry_ids:
        hn = entry_id_to_no.get(int(eid))
        if hn is None:
            continue
        s = float(score_map.get(int(eid), {}).get(factor_name, 0.0))
        items.append((hn, s))
    items.sort(key=lambda x: (-x[1], x[0]))
    return [hn for hn, _ in items[:k]]


def _topk_by_weights(entry_ids: List[int], entry_id_to_no: Dict[int, int], score_map: Dict[int, Dict[str, float]], weights: Dict[str, float], k: int = 5):
    items = []
    for eid in entry_ids:
        hn = entry_id_to_no.get(int(eid))
        if hn is None:
            continue
        m = score_map.get(int(eid), {})
        total = 0.0
        for fn, w in weights.items():
            total += float(m.get(fn, 0.0)) * float(w)
        items.append((hn, total))
    items.sort(key=lambda x: (-x[1], x[0]))
    return [hn for hn, _ in items[:k]]


def generate_prediction_top5_for_race_date(session: Session, target_date_str: str) -> Dict[str, int]:
    race_ids = _race_ids_for_date(session, target_date_str)
    if not race_ids:
        return {"races": 0, "factor_rows": 0, "preset_rows": 0}

    factors = _active_factors(session)
    presets = _load_member_presets(session)

    factor_names = [f[0] for f in factors]
    preset_factor_names = sorted({k for _, _, w in presets for k in w.keys()})
    all_factor_names = sorted(set(factor_names) | set(preset_factor_names))

    existing_rows = (
        session.query(PredictionTop5)
        .filter(PredictionTop5.race_id.in_(race_ids))
        .all()
    )
    existing_map = {}
    for row in existing_rows:
        key = (
            int(row.race_id),
            str(row.predictor_type),
            str(row.predictor_key),
            (str(row.member_email).lower() if row.member_email else None),
        )
        existing_map[key] = row

    factor_rows = 0
    preset_rows = 0

    races = session.query(Race).filter(Race.id.in_(race_ids)).all()
    race_by_id = {int(r.id): r for r in races}

    for rid in race_ids:
        race = race_by_id.get(int(rid))
        if not race:
            continue

        entry_ids, entry_id_to_no, score_map = _fetch_factor_scores_for_race(session, int(rid), all_factor_names)
        if not entry_ids:
            continue

        for factor_name, factor_desc in factors:
            top5 = _topk_by_factor(entry_ids, entry_id_to_no, score_map, factor_name, 5)
            key = (int(rid), "factor", factor_name, None)
            meta = {"desc": factor_desc, "generated_at": datetime.now().isoformat(), "target_date": str(target_date_str), "source": "draw"}
            row = existing_map.get(key)
            if row is None:
                row = PredictionTop5(
                    race_id=int(rid),
                    race_date=race.race_date,
                    race_no=int(race.race_no or 0),
                    predictor_type="factor",
                    predictor_key=factor_name,
                    member_email=None,
                    top5=top5,
                    meta=meta,
                )
                session.add(row)
                existing_map[key] = row
                factor_rows += 1
            else:
                row.race_date = race.race_date
                row.race_no = int(race.race_no or 0)
                row.top5 = top5
                row.meta = meta

        for email, preset_name, weights in presets:
            top5 = _topk_by_weights(entry_ids, entry_id_to_no, score_map, weights, 5)
            email_k = str(email or "").strip().lower()
            key = (int(rid), "preset", preset_name, email_k)
            meta = {"generated_at": datetime.now().isoformat(), "target_date": str(target_date_str), "source": "draw"}
            row = existing_map.get(key)
            if row is None:
                row = PredictionTop5(
                    race_id=int(rid),
                    race_date=race.race_date,
                    race_no=int(race.race_no or 0),
                    predictor_type="preset",
                    predictor_key=preset_name,
                    member_email=email_k,
                    top5=top5,
                    meta=meta,
                )
                session.add(row)
                existing_map[key] = row
                preset_rows += 1
            else:
                row.race_date = race.race_date
                row.race_no = int(race.race_no or 0)
                row.member_email = email_k
                row.top5 = top5
                row.meta = meta

    session.commit()
    return {"races": len(race_ids), "factor_rows": factor_rows, "preset_rows": preset_rows}


def finalize_prediction_top5_hits_for_race_date(session: Session, target_date_str: str) -> Dict[str, int]:
    race_ids = _race_ids_for_date(session, target_date_str)
    if not race_ids:
        return {"races": 0, "updated": 0, "skipped": 0}

    from database.models import RaceResult
    from scoring_engine.member_stats import _calc_hits

    rows = (
        session.query(RaceEntry.race_id, RaceEntry.horse_no, RaceResult.rank)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceEntry.race_id.in_(race_ids))
        .filter(RaceResult.rank != None)
        .order_by(RaceEntry.race_id.asc(), RaceResult.rank.asc())
        .all()
    )

    actual: Dict[int, List[int]] = {}
    for race_id, horse_no, rank in rows:
        rid = int(race_id)
        a = actual.get(rid)
        if a is None:
            a = []
            actual[rid] = a
        if len(a) < 5:
            a.append(int(horse_no))

    snaps = session.query(PredictionTop5).filter(PredictionTop5.race_id.in_(race_ids)).all()

    updated = 0
    skipped = 0
    now = datetime.now().isoformat()
    for s in snaps:
        act = actual.get(int(s.race_id)) or []
        pred = s.top5 if isinstance(s.top5, list) else []
        if len(act) < 5 or len(pred) < 5:
            skipped += 1
            continue
        hits = _calc_hits([int(x) for x in pred], act)
        if not hits:
            skipped += 1
            continue
        meta_old = s.meta if isinstance(s.meta, dict) else {}
        s.meta = {**meta_old, "actual_top5": act, "hits": hits, "results_at": now}
        updated += 1

    session.commit()
    return {"races": len(race_ids), "updated": updated, "skipped": skipped}
