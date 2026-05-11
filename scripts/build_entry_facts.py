import os
import sys
from datetime import datetime
from pathlib import Path

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import init_db, get_session
from scoring_engine.entry_facts import build_entry_facts_for_race_date


def main():
    init_db()
    session = get_session()
    try:
        date_str = str(os.environ.get("TARGET_DATE") or "").strip() or datetime.now().strftime("%Y/%m/%d")
        res = build_entry_facts_for_race_date(session, date_str=date_str)
        print(res)
    finally:
        session.close()


if __name__ == "__main__":
    main()

