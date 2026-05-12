import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import init_db, get_session
from database.models import SystemConfig
from data_scraper.course_time import COURSE_TIME_CFG_KEY, COURSE_TIME_URL, CourseTimeScraper
from scoring_engine.config_value import unwrap_value


def _min_interval_hours() -> int:
    v = str(os.environ.get("COURSE_TIME_MIN_INTERVAL_HOURS") or "").strip()
    if v.isdigit():
        return max(0, int(v))
    return 12


def _is_force() -> bool:
    return str(os.environ.get("FORCE") or "").strip().lower() in ("1", "true", "yes")


def _last_fetched_at_iso(session) -> Optional[str]:
    row = session.query(SystemConfig).filter_by(key=COURSE_TIME_CFG_KEY).first()
    if not row:
        return None
    payload, meta = unwrap_value(row.value)
    if isinstance(payload, dict):
        s = str(payload.get("fetched_at") or "").strip()
        if s:
            return s
    if isinstance(meta, dict):
        s2 = str(meta.get("fetched_at") or "").strip()
        if s2:
            return s2
    return None


def main():
    init_db()
    session = get_session()

    try:
        if not _is_force():
            last_iso = _last_fetched_at_iso(session)
            if last_iso:
                try:
                    last_dt = datetime.fromisoformat(last_iso)
                    if last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=timezone.utc)
                    now = datetime.now(timezone.utc)
                    mi = _min_interval_hours()
                    if mi > 0 and (now - last_dt) < timedelta(hours=mi):
                        print(f"skip: course_time_reference recently fetched_at={last_dt.isoformat()} min_interval_hours={mi}")
                        return
                except Exception:
                    pass

        scraper = CourseTimeScraper()
        res = scraper.update_system_config(session)
        if not isinstance(res, dict) or res.get("ok") is not True:
            raise RuntimeError(f"course_time_reference update failed: {res}")
        print(
            "ok: course_time_reference"
            f" key={COURSE_TIME_CFG_KEY}"
            f" url={COURSE_TIME_URL}"
            f" standard_tracks={res.get('standard_tracks')}"
            f" sectional_tracks={res.get('sectional_tracks')}"
        )
    finally:
        session.close()


if __name__ == "__main__":
    main()

