import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.models import Race
from scoring_engine.pace_forecast import compute_race_pace_forecast_for_race


def _parse_ymd(s: str) -> datetime | None:
    t = str(s or "").strip().replace("-", "/")
    if not t:
        return None
    try:
        return datetime.strptime(t, "%Y/%m/%d")
    except Exception:
        return None


def main():
    init_db()
    session = get_session()
    try:
        d1 = _parse_ymd(os.environ.get("START_DATE") or "")
        d2 = _parse_ymd(os.environ.get("END_DATE") or "")
        if not d1 or not d2:
            print("請設定 START_DATE/END_DATE（YYYY/MM/DD）")
            return
        if d1 > d2:
            d1, d2 = d2, d1
        end = (d2 + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        start = d1.replace(hour=0, minute=0, second=0, microsecond=0)

        sample_s = str(os.environ.get("SAMPLE_N") or "").strip()
        try:
            sample_n = int(sample_s) if sample_s else 10
        except Exception:
            sample_n = 10

        races = (
            session.query(Race.id, Race.race_date, Race.race_no)
            .filter(Race.race_date >= start)
            .filter(Race.race_date < end)
            .order_by(Race.race_date.asc(), Race.race_no.asc(), Race.id.asc())
            .all()
        )
        if not races:
            print("range 無 races")
            return

        ok = 0
        fail = 0
        for rid, rdt, rno in races:
            try:
                res = compute_race_pace_forecast_for_race(session, race_id=int(rid), sample_n=int(sample_n))
                if isinstance(res, dict) and res.get("ok"):
                    ok += 1
                else:
                    fail += 1
            except Exception:
                fail += 1
            if (ok + fail) % 20 == 0:
                print(f"progress ok={ok} fail={fail}")

        print(f"done ok={ok} fail={fail} races={len(races)}")
    finally:
        session.close()


if __name__ == "__main__":
    main()

