import os
import sys
from pathlib import Path

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from scripts.fetch_course_time_reference import main as fetch_course_time_reference_main
from scripts.fetch_racing_course import main as fetch_racing_course_main


if __name__ == "__main__":
    fetch_racing_course_main()
    if str(os.environ.get("ENABLE_COURSE_TIME_REFERENCE") or "").strip().lower() in ("1", "true", "yes"):
        fetch_course_time_reference_main()
