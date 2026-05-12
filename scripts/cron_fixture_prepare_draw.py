import os
import sys
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import init_db, get_session
from sqlalchemy import func

from database.models import Race, RacePaceSnapshot, SystemConfig, PredictionTop5
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
            from scoring_engine.pace_forecast import compute_race_pace_forecast_for_race
            from scoring_engine.pace_forecast_calibration import learn_pace_forecast_calibration, save_pace_forecast_calibration

            res = generate_prediction_top5_for_race_date(session3, target_date_str)
            print(f"OK: regenerated snapshots races={res.get('races')} factor_rows={res.get('factor_rows')} preset_rows={res.get('preset_rows')}")

            _upsert_cfg(session3, f"draw_card_hash:{target_date_str}", h, f"Draw hash for {target_date_str}")
            _upsert_cfg(session3, f"draw_prepared_at:{target_date_str}", datetime.now(HK_TZ).isoformat(), f"Prepared draw/rescore/snapshots at (HK) for {target_date_str}")

            enabled = str(os.environ.get("ENABLE_PACE_FORECAST_AUTO_LEARN") or "1").strip().lower() in ("1", "true", "yes")
            if enabled:
                force = str(os.environ.get("FORCE_PACE_FORECAST_LEARN") or "").strip().lower() in ("1", "true", "yes")
                state_key = "pace_forecast_calib_state:v1"
                cfg_state = _get_cfg(session3, state_key)
                last_end = str(cfg_state.value.get("last_end_date") or "") if (cfg_state and isinstance(cfg_state.value, dict)) else ""

                latest_actual = session3.query(func.max(RacePaceSnapshot.race_date_day)).scalar()
                if latest_actual and (force or str(latest_actual.isoformat()) != str(last_end)):
                    lookback_s = str(os.environ.get("PACE_FORECAST_LEARN_LOOKBACK_DAYS") or "").strip()
                    try:
                        lookback_days = int(lookback_s) if lookback_s else 180
                    except Exception:
                        lookback_days = 180
                    lookback_days = max(30, int(lookback_days))

                    min_s = str(os.environ.get("PACE_FORECAST_LEARN_MIN_SAMPLES") or "").strip()
                    try:
                        min_samples = int(min_s) if min_s else 80
                    except Exception:
                        min_samples = 80

                    alpha_s = str(os.environ.get("PACE_FORECAST_LEARN_ALPHA") or "").strip()
                    try:
                        alpha = float(alpha_s) if alpha_s else 0.25
                    except Exception:
                        alpha = 0.25
                    alpha = max(0.0, min(1.0, float(alpha)))

                    start_d = latest_actual - timedelta(days=int(lookback_days) - 1)
                    end_d = latest_actual
                    payload = learn_pace_forecast_calibration(
                        session3,
                        start_date=start_d,
                        end_date=end_d,
                        min_samples=int(min_samples),
                        alpha=float(alpha),
                    )
                    save_pace_forecast_calibration(session3, payload)
                    _upsert_cfg(
                        session3,
                        state_key,
                        {
                            "last_end_date": str(end_d.isoformat()),
                            "trained_range": payload.get("trained_range") if isinstance(payload, dict) else None,
                            "n_pairs": int(payload.get("n_pairs") or 0) if isinstance(payload, dict) else 0,
                            "updated_at": datetime.utcnow().isoformat(),
                        },
                        "步速預測校準學習狀態",
                    )
                    print(f"OK: auto-learn pace calibration end={end_d.isoformat()} n_pairs={int(payload.get('n_pairs') or 0) if isinstance(payload, dict) else 0}")
                else:
                    print("Skip: no new actual pace data for learning")

                try:
                    sample_n = int(str(os.environ.get("PACE_FORECAST_SAMPLE_N") or "").strip() or 10)
                except Exception:
                    sample_n = 10
                d = datetime.strptime(target_date_str, "%Y/%m/%d").date()
                start_dt = datetime.combine(d, datetime.min.time())
                end_dt = start_dt + timedelta(days=1)
                race_ids = (
                    session3.query(Race.id)
                    .filter(Race.race_date >= start_dt)
                    .filter(Race.race_date < end_dt)
                    .order_by(Race.race_no.asc(), Race.id.asc())
                    .all()
                )
                for (rid,) in race_ids:
                    try:
                        compute_race_pace_forecast_for_race(session3, race_id=int(rid), sample_n=int(sample_n))
                    except Exception:
                        pass
                print(f"OK: refreshed pace forecasts for {target_date_str}")
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

