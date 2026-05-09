import os
import sys
from datetime import datetime, time as dtime, timedelta
from pathlib import Path

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import init_db, get_session
from database.models import Race
from scoring_engine.core import ScoringEngine


def main():
    init_db()
    session = get_session()
    try:
        target_date_str = os.environ.get("TARGET_DATE") or ""
        if not target_date_str:
            print("TARGET_DATE is required (YYYY/MM/DD)")
            return
        d = datetime.strptime(target_date_str, "%Y/%m/%d").date()
        start = datetime.combine(d, dtime.min)
        end = start + timedelta(days=1)

        races = (
            session.query(Race)
            .filter(Race.race_date >= start)
            .filter(Race.race_date < end)
            .order_by(Race.race_no.asc(), Race.id.asc())
            .all()
        )
        if not races:
            print(f"No races found for {target_date_str}")
            return

        engine = ScoringEngine(session)
        for r in races:
            print(f"Rescoring race_no={r.race_no} race_id={r.id}")
            engine.score_race(r.id)

        print(f"OK: rescored {len(races)} races for {target_date_str}")
    finally:
        session.close()


if __name__ == "__main__":
    main()

