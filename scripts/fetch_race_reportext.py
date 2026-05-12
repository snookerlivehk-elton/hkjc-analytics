import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from zoneinfo import ZoneInfo

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.models import Horse, Jockey, Race, RaceEntry, Trainer
from data_scraper.race_report_ext import RaceReportExtScraper
from scoring_engine.normalization import going_code_for_race, normalize_course_type, surface_code
from scoring_engine.search_index import race_key, upsert_search_document
from scoring_engine.raw_snapshots import upsert_raw_snapshot
from utils.logger import logger


def _day_range(date_str: str):
    d0 = datetime.strptime(date_str, "%Y/%m/%d").date()
    start = datetime.combine(d0, datetime.min.time())
    end = start + timedelta(days=1)
    return start, end


def main():
    init_db()
    target_date_str = str(os.getenv("TARGET_DATE") or "").strip()
    if not target_date_str:
        hk = ZoneInfo("Asia/Hong_Kong")
        target_date_str = datetime.now(tz=hk).strftime("%Y/%m/%d")

    start, end = _day_range(target_date_str)
    session = get_session()
    scraper = RaceReportExtScraper()
    try:
        races = (
            session.query(Race)
            .filter(Race.race_date >= start)
            .filter(Race.race_date < end)
            .order_by(Race.race_no.asc(), Race.id.asc())
            .all()
        )
        if not races:
            logger.info(f"race_reportext no_races date={target_date_str}")
            return {"date": target_date_str, "races": 0, "ok_races": 0, "failed_races": 0, "items": 0}

        total_items = 0
        ok_races = 0
        failed_races = 0
        for r in races:
            rn = int(getattr(r, "race_no", 0) or 0)
            v = str(getattr(r, "venue", "") or "").strip()
            if not rn or not v:
                continue
            url = scraper.build_url(race_date=target_date_str, racecourse=v, race_no=rn)
            try:
                res = scraper.scrape_single_race(race_date=target_date_str, racecourse=v, race_no=rn)
            except Exception as e:
                logger.warning(f"race_reportext_fetch_failed date={target_date_str} venue={v} race_no={rn} err={str(e)}")
                session.rollback()
                failed_races += 1
                continue

            rk = race_key(getattr(r, "race_date", None), v, rn)
            upsert_raw_snapshot(
                session,
                source="HKJC_RACEREPORTEXT",
                entity_type="race",
                entity_key=rk,
                race_id=int(getattr(r, "id")),
                payload=res,
                meta={"url": url},
                fetched_at=datetime.utcnow(),
            )

            items = res.get("items") if isinstance(res, dict) else None
            if not isinstance(items, list):
                items = []
            entries = (
                session.query(RaceEntry, Horse, Jockey, Trainer)
                .join(Horse, RaceEntry.horse_id == Horse.id)
                .join(Jockey, RaceEntry.jockey_id == Jockey.id)
                .join(Trainer, RaceEntry.trainer_id == Trainer.id)
                .filter(RaceEntry.race_id == int(getattr(r, "id")))
                .all()
            )
            entry_by_no = {int(getattr(e, "horse_no", 0) or 0): (e, h, j, t) for (e, h, j, t) in entries}

            for it in items:
                if not isinstance(it, dict):
                    continue
                try:
                    horse_no = int(it.get("horse_no") or 0)
                except Exception:
                    horse_no = 0
                if horse_no <= 0:
                    continue

                e_pack = entry_by_no.get(horse_no)
                horse_name = ""
                jockey_name = ""
                trainer_name = ""
                if e_pack is not None:
                    _, h, j, t = e_pack
                    horse_name = str(getattr(h, "name_ch", "") or "").strip()
                    jockey_name = str(getattr(j, "name_ch", "") or "").strip()
                    trainer_name = str(getattr(t, "name_ch", "") or "").strip()
                else:
                    horse_name = str(it.get("horse_name") or "").strip()

                prev_date = str(it.get("prev_date") or "").strip()
                prev_rn = str(it.get("prev_race_no") or "").strip()
                desc = str(it.get("desc") or "").strip()
                title = f"{target_date_str} {v} R{rn} 各駒上次競賽事件摘要"
                text = "\n".join([title, f"馬號 {horse_no} {horse_name}", f"騎師 {jockey_name}", f"練馬師 {trainer_name}", f"上次 {prev_date} {prev_rn}", desc]).strip()

                sc = surface_code(getattr(r, "surface", None), track_type=getattr(r, "track_type", None), course_type=getattr(r, "course_type", None))
                ct = normalize_course_type(getattr(r, "course_type", None), surface_code_=sc)
                gc = going_code_for_race(session, r)

                upsert_search_document(
                    session,
                    doc_type="race_reportext",
                    ref_key=f"{rk}:{horse_no}",
                    entity_type="race",
                    entity_key=rk,
                    search_text=text,
                    title=title,
                    race_id=int(getattr(r, "id")),
                    race_date_day=getattr(r, "race_date").date() if getattr(r, "race_date", None) else None,
                    race_no=rn,
                    venue=v,
                    surface_code_=sc,
                    course_type=ct,
                    going_code=gc,
                    horse_name=horse_name,
                    jockey_name=jockey_name,
                    trainer_name=trainer_name,
                    payload_excerpt={
                        "horse_no": horse_no,
                        "prev_date": prev_date,
                        "prev_race_no": prev_rn,
                        "desc": desc,
                        "url": url,
                    },
                )
                total_items += 1

            session.commit()
            ok_races += 1
            logger.info(f"race_reportext_ok date={target_date_str} venue={v} race_no={rn} items={len(items)} url={url}")

        logger.info(f"race_reportext_done date={target_date_str} items={total_items}")
        return {
            "date": target_date_str,
            "races": len(races),
            "ok_races": int(ok_races),
            "failed_races": int(failed_races),
            "items": int(total_items),
        }
    finally:
        session.close()


if __name__ == "__main__":
    main()
