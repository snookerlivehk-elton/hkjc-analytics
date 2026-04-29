import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import init_db, get_session
from database.models import Race, RaceEntry, RaceResult, ScoringFactor, ScoringWeight
from scoring_engine.constants import DISABLED_FACTORS
from scoring_engine.member_stats import _calc_hits
from scoring_engine.factors import get_available_factors


def _parse_date(s: str) -> Optional[datetime]:
    v = str(s or "").strip()
    if not v:
        return None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(v, fmt)
        except Exception:
            continue
    return None


def _date_range() -> Tuple[datetime, datetime]:
    d1 = _parse_date(os.environ.get("DATE_FROM", ""))  # inclusive
    d2 = _parse_date(os.environ.get("DATE_TO", ""))  # inclusive
    days = int(str(os.environ.get("DAYS", "") or "30").strip() or 30)
    if d1 and d2 and d1 > d2:
        d1, d2 = d2, d1
    if not d1 or not d2:
        end = datetime.now()
        start = end - timedelta(days=days)
        return start, end
    return d1, d2 + timedelta(days=1) - timedelta(seconds=1)


def _distance_bucket(m: int) -> str:
    v = int(m or 0)
    if v <= 1200:
        return "≤1200"
    if v <= 1600:
        return "1201-1600"
    if v <= 2000:
        return "1601-2000"
    return "2001+"


def _actual_topk(session, race_id: int, k: int = 5) -> List[int]:
    rows = (
        session.query(RaceEntry.horse_no)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceEntry.race_id == race_id)
        .filter(RaceResult.rank != None)
        .order_by(RaceResult.rank.asc())
        .limit(k)
        .all()
    )
    out: List[int] = []
    for (horse_no,) in rows:
        try:
            out.append(int(horse_no or 0))
        except Exception:
            out.append(0)
    return out


def _pred_topk(session, race_id: int, k: int = 5) -> List[int]:
    rows = (
        session.query(RaceEntry.horse_no)
        .filter(RaceEntry.race_id == race_id)
        .order_by(RaceEntry.total_score.desc().nullslast(), RaceEntry.id.asc())
        .limit(k)
        .all()
    )
    out: List[int] = []
    for (horse_no,) in rows:
        try:
            out.append(int(horse_no or 0))
        except Exception:
            out.append(0)
    return out


def _active_factor_names(session) -> List[str]:
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


def main():
    init_db()
    session = get_session()
    try:
        start, end = _date_range()
        races = (
            session.query(Race.id, Race.race_date, Race.venue, Race.distance)
            .filter(Race.race_date >= start)
            .filter(Race.race_date <= end)
            .order_by(Race.race_date.asc(), Race.race_no.asc())
            .all()
        )
        race_ids = [int(r[0]) for r in races]
        if not race_ids:
            print(f"找不到賽事：{start.date().isoformat()} ~ {end.date().isoformat()}")
            return

        factors = _active_factor_names(session)

        totals: Dict[str, int] = {"races": 0, "win": 0, "t3": 0, "f4": 0, "b5w": 0}
        seg: Dict[str, Dict[str, int]] = {}

        for race_id, race_date, venue, dist in races:
            actual_top5 = _actual_topk(session, int(race_id), k=5)
            if not actual_top5 or actual_top5[0] == 0:
                continue
            pred_top5 = _pred_topk(session, int(race_id), k=5)
            if not pred_top5 or pred_top5[0] == 0:
                continue

            h = _calc_hits(pred_top5, actual_top5)
            totals["races"] += 1
            totals["win"] += int(h.get("win") or 0)
            totals["t3"] += int(h.get("t3") or 0)
            totals["f4"] += int(h.get("f4") or 0)
            totals["b5w"] += int(h.get("b5w") or 0)

            key = f"{str(venue or '').upper()}|{_distance_bucket(int(dist or 0))}"
            g = seg.get(key)
            if g is None:
                g = {"races": 0, "win": 0, "t3": 0, "f4": 0, "b5w": 0}
                seg[key] = g
            g["races"] += 1
            g["win"] += int(h.get("win") or 0)
            g["t3"] += int(h.get("t3") or 0)
            g["f4"] += int(h.get("f4") or 0)
            g["b5w"] += int(h.get("b5w") or 0)

        print("=== Baseline (總分排序) 命中率 ===")
        print(f"Range: {start.date().isoformat()} ~ {end.date().isoformat()}")
        print(f"Races: {totals['races']}")
        if totals["races"]:
            r = float(totals["races"])
            print(f"Win: {totals['win']}/{totals['races']} ({totals['win']/r:.1%})")
            print(f"Top3: {totals['t3']}/{totals['races']} ({totals['t3']/r:.1%})")
            print(f"Top4: {totals['f4']}/{totals['races']} ({totals['f4']/r:.1%})")
            print(f"Top5: {totals['b5w']}/{totals['races']} ({totals['b5w']/r:.1%})")

        if seg:
            print("\n=== 分桶 (venue|distance) ===")
            for k in sorted(seg.keys()):
                g = seg[k]
                if not g["races"]:
                    continue
                r = float(g["races"])
                print(f"{k} races={g['races']} win={g['win']/r:.1%} t3={g['t3']/r:.1%} f4={g['f4']/r:.1%} t5={g['b5w']/r:.1%}")

        if factors:
            print("\n=== 因子覆蓋率（ScoringFactor） ===")
            for name in factors:
                q = (
                    session.query(func.count(ScoringFactor.id))
                    .join(RaceEntry, RaceEntry.id == ScoringFactor.entry_id)
                    .filter(RaceEntry.race_id.in_(race_ids))
                    .filter(ScoringFactor.factor_name == name)
                )
                total = int(q.scalar() or 0)
                if total == 0:
                    print(f"{name}: rows=0")
                    continue

                missing = (
                    session.query(func.count(ScoringFactor.id))
                    .join(RaceEntry, RaceEntry.id == ScoringFactor.entry_id)
                    .filter(RaceEntry.race_id.in_(race_ids))
                    .filter(ScoringFactor.factor_name == name)
                    .filter((ScoringFactor.raw_data_display == None) | (ScoringFactor.raw_data_display == "") | (ScoringFactor.raw_data_display == "無數據"))
                    .scalar()
                )
                missing = int(missing or 0)
                missing_raw = (
                    session.query(func.count(ScoringFactor.id))
                    .join(RaceEntry, RaceEntry.id == ScoringFactor.entry_id)
                    .filter(RaceEntry.race_id.in_(race_ids))
                    .filter(ScoringFactor.factor_name == name)
                    .filter(ScoringFactor.raw_value == None)
                    .scalar()
                )
                missing_raw = int(missing_raw or 0)
                print(f"{name}: rows={total} missing_raw={missing_raw/float(total):.1%} missing_display={missing/float(total):.1%}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
