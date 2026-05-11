import sys
from datetime import datetime
from pathlib import Path

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.models import SystemConfig
from data_scraper.windtracker import WindTrackerScraper
from scoring_engine.config_value import build_meta, wrap_value
from scoring_engine.search_index import upsert_search_document
from scoring_engine.raw_snapshots import upsert_raw_snapshot
from utils.logger import logger


def main():
    init_db()
    session = get_session()
    try:
        scraper = WindTrackerScraper()
        payload = scraper.scrape_latest()
        ds = str(payload.get("race_date") or "").strip()
        v = str(payload.get("venue") or "").strip()
        key = f"windtracker:{ds}:{v}" if (ds and v) else "windtracker:latest"

        meta = build_meta(source="HKJC_WINDTRACKER", url=str(scraper.url), schema=key, extra={"race_date": ds, "venue": v})
        wrapped = wrap_value(payload, meta)

        row = session.query(SystemConfig).filter_by(key=key).first()
        if not row:
            row = SystemConfig(key=key, value=wrapped, description="HKJC WindTracker（風向風速/天氣/場地）")
            session.add(row)
        else:
            row.value = wrapped

        race_date_day = None
        if ds:
            try:
                race_date_day = datetime.strptime(ds, "%Y/%m/%d").date()
            except Exception:
                race_date_day = None

        metrics = payload.get("metrics") if isinstance(payload, dict) else None
        winds = payload.get("winds") if isinstance(payload, dict) else None
        updated_at = str(payload.get("updated_at") or "").strip() if isinstance(payload, dict) else ""
        title = f"{ds} {v} WindTracker" if (ds and v) else "WindTracker"
        text = "\n".join(
            [
                title,
                f"最後更新 {updated_at}",
                f"氣溫 {str((metrics or {}).get('temperature_c') or '')}C",
                f"相對濕度 {str((metrics or {}).get('humidity_pct') or '')}%",
                f"雨量(總) {str((metrics or {}).get('rain_total_mm') or '')}mm",
                f"雨量(10min) {str((metrics or {}).get('rain_10min_mm') or '')}mm",
                f"土壤濕度 {str((metrics or {}).get('soil_moisture_pct') or '')}%",
                f"風 {str(winds or '')}",
            ]
        ).strip()
        upsert_search_document(
            session,
            doc_type="windtracker",
            ref_key=key,
            entity_type="race_day",
            entity_key=f"{ds}:{v}" if (ds and v) else "latest",
            search_text=text,
            title=title,
            race_date_day=race_date_day,
            venue=v,
            payload_excerpt={"updated_at": updated_at, "metrics": metrics, "winds": winds},
        )

        upsert_raw_snapshot(
            session,
            source="HKJC_WINDTRACKER",
            entity_type="race_day",
            entity_key=f"{ds}:{v}" if (ds and v) else "latest",
            payload=payload,
            meta={"url": str(scraper.url), "schema": key},
            fetched_at=datetime.utcnow(),
        )

        session.commit()
        logger.info(f"fetch_windtracker_ok key={key}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
