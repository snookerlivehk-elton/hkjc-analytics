import sys
from datetime import datetime
from pathlib import Path

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.models import SystemConfig
from data_scraper.racing_course import RacingCourseScraper
from scoring_engine.config_value import build_meta, wrap_value
from scoring_engine.raw_snapshots import upsert_raw_snapshot
from utils.logger import logger


def main():
    init_db()
    session = get_session()
    try:
        scraper = RacingCourseScraper()
        payload = scraper.scrape()
        url = str(scraper.url)
        key = "racing_course:v1"
        meta = build_meta(source="HKJC_RACING_COURSE", url=url, schema=key)
        wrapped = wrap_value(payload, meta)

        row = session.query(SystemConfig).filter_by(key=key).first()
        if not row:
            row = SystemConfig(key=key, value=wrapped, description="HKJC 跑道資料（跑道/場地狀況/量度點）")
            session.add(row)
        else:
            row.value = wrapped

        upsert_raw_snapshot(
            session,
            source="HKJC_RACING_COURSE",
            entity_type="global",
            entity_key=key,
            payload=payload,
            meta={"url": url, "schema": key},
            fetched_at=datetime.utcnow(),
        )

        session.commit()
        logger.info(f"fetch_racing_course_ok key={key}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
