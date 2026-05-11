import sys
from pathlib import Path

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from scripts.fetch_racing_course import main


if __name__ == "__main__":
    main()
