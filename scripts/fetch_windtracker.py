import os
import sys
from datetime import datetime, time as dtime, timedelta
from pathlib import Path

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.models import Race, RaceDayWeather, SystemConfig
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

        env_ds = str(os.environ.get("TARGET_DATE") or "").strip()
        if (not ds) and env_ds:
            ds = env_ds

        race_date_day = None
        if ds:
            try:
                race_date_day = datetime.strptime(ds, "%Y/%m/%d").date()
            except Exception:
                race_date_day = None

        env_v = str(os.environ.get("TARGET_VENUE") or "").strip()
        if (not v) and env_v:
            v = env_v
        if (not v) and race_date_day:
            try:
                start = datetime.combine(race_date_day, dtime.min)
                end = start + timedelta(days=1)
                rows_v = (
                    session.query(Race.venue)
                    .filter(Race.race_date >= start)
                    .filter(Race.race_date < end)
                    .filter(Race.venue != None)
                    .distinct()
                    .all()
                )
                venues = [str(x[0] or "").strip() for x in rows_v if x and str(x[0] or "").strip()]
                venues = list(dict.fromkeys(venues))
                if len(venues) == 1:
                    v = venues[0]
            except Exception:
                pass

        key = f"windtracker:{ds}:{v}" if (ds and v) else "windtracker:latest"

        meta = build_meta(source="HKJC_WINDTRACKER", url=str(scraper.url), schema=key, extra={"race_date": ds, "venue": v})
        wrapped = wrap_value(payload, meta)

        row = session.query(SystemConfig).filter_by(key=key).first()
        if not row:
            row = SystemConfig(key=key, value=wrapped, description="HKJC WindTracker（風向風速/天氣/場地）")
            session.add(row)
        else:
            row.value = wrapped

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

        if ds and v and race_date_day:
            metrics0 = metrics if isinstance(metrics, dict) else {}
            winds0 = winds if isinstance(winds, list) else []
            speeds = []
            dirs = []
            for w0 in winds0:
                if not isinstance(w0, dict):
                    continue
                d0 = str(w0.get("direction") or "").strip()
                if d0:
                    dirs.append(d0)
                try:
                    s0 = float(w0.get("speed_kmh")) if w0.get("speed_kmh") is not None else None
                except Exception:
                    s0 = None
                if s0 is not None:
                    speeds.append(float(s0))
            wind_avg = (sum(speeds) / float(len(speeds))) if speeds else None
            wind_max = max(speeds) if speeds else None
            wind_dir = dirs[0] if dirs else ""

            row_w = session.query(RaceDayWeather).filter_by(race_date_day=race_date_day, venue=v).first()
            if not row_w:
                row_w = RaceDayWeather(race_date_day=race_date_day, venue=v)
                session.add(row_w)
            row_w.updated_at_text = updated_at or None
            row_w.temperature_c = metrics0.get("temperature_c")
            row_w.humidity_pct = metrics0.get("humidity_pct")
            row_w.rain_total_mm = metrics0.get("rain_total_mm")
            row_w.rain_10min_mm = metrics0.get("rain_10min_mm")
            row_w.soil_moisture_pct = metrics0.get("soil_moisture_pct")
            row_w.wind_direction = wind_dir or None
            row_w.wind_speed_kmh_avg = wind_avg
            row_w.wind_speed_kmh_max = wind_max
            row_w.raw = {"metrics": metrics0, "winds": winds0, "updated_at": updated_at, "source_key": key}

        session.commit()
        logger.info(f"fetch_windtracker_ok key={key}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
