import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from sqlalchemy import func

from database.connection import get_session, init_db
from database.models import Race, RacePaceForecastSnapshot, RacePaceSnapshot


def _parse_ymd(s: str) -> date | None:
    t = str(s or "").strip().replace("-", "/")
    if not t:
        return None
    try:
        return datetime.strptime(t, "%Y/%m/%d").date()
    except Exception:
        return None


def main():
    init_db()
    session = get_session()
    try:
        end_d = _parse_ymd(str(os.environ.get("END_DATE") or "")) or datetime.utcnow().date()
        start_d = _parse_ymd(str(os.environ.get("START_DATE") or "")) or (end_d - timedelta(days=120))
        if start_d > end_d:
            start_d, end_d = end_d, start_d

        start_dt = datetime.combine(start_d, datetime.min.time())
        end_dt = datetime.combine(end_d + timedelta(days=1), datetime.min.time())

        q = (
            session.query(
                RacePaceForecastSnapshot.pace_class.label("pred"),
                RacePaceSnapshot.pace_class.label("actual"),
                func.count().label("n"),
            )
            .join(RacePaceSnapshot, RacePaceSnapshot.race_id == RacePaceForecastSnapshot.race_id)
            .join(Race, Race.id == RacePaceForecastSnapshot.race_id)
            .filter(Race.race_date >= start_dt)
            .filter(Race.race_date < end_dt)
            .group_by(RacePaceForecastSnapshot.pace_class, RacePaceSnapshot.pace_class)
            .all()
        )

        if not q:
            print(f"no paired samples range={start_d.isoformat()}..{end_d.isoformat()}")
            return

        pred_cnt = (
            session.query(RacePaceForecastSnapshot.pace_class, func.count())
            .join(Race, Race.id == RacePaceForecastSnapshot.race_id)
            .filter(Race.race_date >= start_dt)
            .filter(Race.race_date < end_dt)
            .group_by(RacePaceForecastSnapshot.pace_class)
            .all()
        )
        act_cnt = (
            session.query(RacePaceSnapshot.pace_class, func.count())
            .join(Race, Race.id == RacePaceSnapshot.race_id)
            .filter(Race.race_date >= start_dt)
            .filter(Race.race_date < end_dt)
            .group_by(RacePaceSnapshot.pace_class)
            .all()
        )

        pred_cnt = sorted([(str(a or ""), int(b or 0)) for a, b in pred_cnt], key=lambda x: x[1], reverse=True)
        act_cnt = sorted([(str(a or ""), int(b or 0)) for a, b in act_cnt], key=lambda x: x[1], reverse=True)
        pairs = sorted([((str(a or ""), str(b or "")), int(c or 0)) for a, b, c in q], key=lambda x: x[1], reverse=True)

        print(f"range={start_d.isoformat()}..{end_d.isoformat()}")
        print("pred_dist", pred_cnt)
        print("actual_dist", act_cnt)
        print("top_pairs", pairs[:30])

        rows2 = (
            session.query(
                RacePaceForecastSnapshot.pace_class,
                RacePaceForecastSnapshot.meta,
                RacePaceSnapshot.pace_class,
            )
            .join(RacePaceSnapshot, RacePaceSnapshot.race_id == RacePaceForecastSnapshot.race_id)
            .join(Race, Race.id == RacePaceForecastSnapshot.race_id)
            .filter(Race.race_date >= start_dt)
            .filter(Race.race_date < end_dt)
            .all()
        )
        raw_pred_cnt_map = {}
        raw_pair_map = {}
        for p, m, a in rows2:
            pred = str(p or "").strip()
            meta = m if isinstance(m, dict) else {}
            raw = str(meta.get("raw_pace_class") or pred).strip()
            act = str(a or "").strip()
            if raw:
                raw_pred_cnt_map[raw] = int(raw_pred_cnt_map.get(raw, 0)) + 1
            if raw and act:
                k = (raw, act)
                raw_pair_map[k] = int(raw_pair_map.get(k, 0)) + 1

        raw_pred_cnt = sorted([(k, int(v)) for k, v in raw_pred_cnt_map.items()], key=lambda x: x[1], reverse=True)
        raw_pairs = sorted([((k[0], k[1]), int(v)) for k, v in raw_pair_map.items()], key=lambda x: x[1], reverse=True)
        print("raw_pred_dist", raw_pred_cnt)
        print("raw_top_pairs", raw_pairs[:30])

    finally:
        session.close()


if __name__ == "__main__":
    main()
