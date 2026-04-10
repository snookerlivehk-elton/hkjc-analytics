import os
import sys
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import init_db, get_session
from database.models import SystemConfig, PredictionTop5
from scripts.fetch_fixture import main as fetch_fixture_main


HK_TZ = ZoneInfo("Asia/Hong_Kong")
LOCK_KEY = "job_lock:fixture_prepare_draw"
LOCK_TTL_MIN = 120


def _get_cfg(session, key: str):
    return session.query(SystemConfig).filter_by(key=key).first()


def _upsert_cfg(session, key: str, value, desc: str):
    cfg = _get_cfg(session, key)
    if not cfg:
        cfg = SystemConfig(key=key, description=desc)
        session.add(cfg)
    cfg.value = value
    session.commit()


def _acquire_lock(session) -> bool:
    now = datetime.now(HK_TZ)
    cfg = _get_cfg(session, LOCK_KEY)
    if cfg and isinstance(cfg.value, str):
        try:
            ts = datetime.fromisoformat(cfg.value)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=HK_TZ)
            if now - ts < timedelta(minutes=LOCK_TTL_MIN):
                return False
        except Exception:
            pass
    _upsert_cfg(session, LOCK_KEY, now.isoformat(), "Cron lock: fixture + prepare draw pipeline (HK)")
    return True


def _release_lock(session):
    cfg = _get_cfg(session, LOCK_KEY)
    if cfg:
        session.delete(cfg)
        session.commit()


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _racecard_hash(races_info) -> str:
    payload = []
    for r in (races_info or []):
        rn = int(r.get("race_no") or 0)
        entries = []
        for e in (r.get("entries") or []):
            entries.append(
                {
                    "horse_code": str(e.get("horse_code") or ""),
                    "horse_no": int(e.get("horse_no") or 0),
                    "draw": int(e.get("draw") or 0),
                    "rating": int(e.get("rating") or 0),
                    "actual_weight": int(e.get("actual_weight") or 0),
                    "jockey": str(e.get("jockey") or ""),
                    "trainer": str(e.get("trainer") or ""),
                }
            )
        entries.sort(key=lambda x: (x["horse_no"], x["horse_code"]))
        payload.append({"race_no": rn, "entries": entries})
    payload.sort(key=lambda x: x["race_no"])
    return _sha256(json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True))


def _has_snapshots(session, target_date_str: str) -> bool:
    try:
        d = datetime.strptime(target_date_str, "%Y/%m/%d").date()
    except Exception:
        return False
    cnt = (
        session.query(PredictionTop5.id)
        .filter(PredictionTop5.race_date >= datetime.combine(d, datetime.min.time()))
        .filter(PredictionTop5.race_date < datetime.combine(d, datetime.min.time()) + timedelta(days=1))
        .count()
    )
    return cnt > 0


def main():
    init_db()
    session = get_session()
    try:
        if not _acquire_lock(session):
            print("Skip: another fixture_prepare_draw job is running")
            return
    finally:
        session.close()

    try:
        print("Step 1/4: fetch fixture (this updates fixture_next_raceday)")
        fetch_fixture_main()

        session2 = get_session()
        try:
            next_day_cfg = _get_cfg(session2, "fixture_next_raceday")
            target_date_str = str(next_day_cfg.value or "").strip() if next_day_cfg else ""
            if not target_date_str:
                print("Skip: fixture_next_raceday is empty")
                return

            print(f"Target race day: {target_date_str}")

            from data_scraper.race_card import RaceCardScraper

            scraper = RaceCardScraper()
            races_info = scraper.get_all_races_info(race_date=target_date_str)
            if not races_info:
                print(f"Skip: no racecard/draw yet for {target_date_str}")
                return

            h = _racecard_hash(races_info)
            hash_key = f"draw_card_hash:{target_date_str}"
            old_hash = _get_cfg(session2, hash_key)
            old_val = str(old_hash.value or "") if old_hash else ""
            if old_val == h and _has_snapshots(session2, target_date_str):
                print(f"Skip: draw unchanged and snapshots exist ({target_date_str})")
                return

            print("Step 2/4: run draw scraper (populate races/entries)")
        finally:
            session2.close()

        os.environ["TARGET_DATE"] = target_date_str

        from scripts.run_scraper import run_daily_scraper
        import asyncio

        asyncio.run(run_daily_scraper())

        print("Step 3/4: backfill horse history for this race day (date mode)")
        os.environ["BACKFILL_MODE"] = "date"
        from scripts.fetch_history import backfill_horse_history

        asyncio.run(backfill_horse_history())

        print("Step 4/4: rescore all races for this date, then regenerate Top5 snapshots")
        from scripts.rescore_race_date import main as rescore_main

        rescore_main()

        session3 = get_session()
        try:
            from scoring_engine.prediction_snapshots import generate_prediction_top5_for_race_date

            res = generate_prediction_top5_for_race_date(session3, target_date_str)
            print(f"OK: regenerated snapshots races={res.get('races')} factor_rows={res.get('factor_rows')} preset_rows={res.get('preset_rows')}")

            _upsert_cfg(session3, f"draw_card_hash:{target_date_str}", h, f"Draw hash for {target_date_str}")
            _upsert_cfg(session3, f"draw_prepared_at:{target_date_str}", datetime.now(HK_TZ).isoformat(), f"Prepared draw/rescore/snapshots at (HK) for {target_date_str}")
        finally:
            session3.close()
    finally:
        session4 = get_session()
        try:
            _release_lock(session4)
        finally:
            session4.close()


if __name__ == "__main__":
    main()

