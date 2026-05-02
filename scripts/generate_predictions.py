import os
import sys
from pathlib import Path

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import init_db, get_session
from scoring_engine.prediction_snapshots import generate_prediction_top5_for_race_date


def main():
    init_db()
    target_date = os.environ.get("TARGET_DATE", "").strip()
    if not target_date:
        print("缺少 TARGET_DATE（YYYY/MM/DD）")
        return

    session = get_session()
    try:
        res = generate_prediction_top5_for_race_date(session, target_date)
        print(f"完成：賽日 {target_date} races={res.get('races')} factor_rows={res.get('factor_rows')} preset_rows={res.get('preset_rows')}")
    finally:
        session.close()


if __name__ == "__main__":
    main()

