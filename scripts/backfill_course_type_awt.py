import sys
from pathlib import Path

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.models import Race
from scoring_engine.track_profile import _surface_code


def main():
    init_db()
    session = get_session()
    try:
        q = session.query(Race)
        changed = 0
        seen = 0
        for r in q.all():
            seen += 1
            surface = _surface_code(r)
            ct0 = str(getattr(r, "course_type", "") or "").strip()
            if surface == "AW" and ((not ct0) or ct0.upper() == "U"):
                r.course_type = "AWT"
                changed += 1
        if changed:
            session.commit()
        print(f"ok={True} seen={seen} changed={changed}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
