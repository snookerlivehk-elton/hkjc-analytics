import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.models import RaceEntry, SystemConfig
from database.repository import RacingRepository
from data_scraper.race_card import RaceCardScraper


def _parse_ymd(s: str) -> Optional[date]:
    v = str(s or "").strip()
    if not v:
        return None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            continue
    return None


def _fmt_ymd(d: date) -> str:
    return d.strftime("%Y/%m/%d")


def _get_cfg(session, key: str):
    return session.query(SystemConfig).filter_by(key=key).first()


def _get_cfg_value(session, key: str):
    cfg = _get_cfg(session, key)
    return cfg.value if cfg else None


def _upsert_cfg(session, key: str, value, description: str):
    cfg = _get_cfg(session, key)
    if not cfg:
        cfg = SystemConfig(key=key, description=description)
        session.add(cfg)
    cfg.value = value
    session.commit()


def _dates_from_fixture(session, start_date: date, end_date: date) -> Optional[List[date]]:
    v = _get_cfg_value(session, "fixture_dates")
    if not isinstance(v, list) or not v:
        return None
    out: List[date] = []
    for x in v:
        d0 = _parse_ymd(str(x or ""))
        if not d0:
            continue
        if start_date <= d0 <= end_date:
            out.append(d0)
    out.sort()
    return out


def main():
    init_db()
    session = get_session()
    try:
        start_date = _parse_ymd(os.environ.get("START_DATE") or "")
        end_date = _parse_ymd(os.environ.get("END_DATE") or "")
        if not start_date or not end_date:
            print("請設定環境變數 START_DATE/END_DATE（格式 YYYY/MM/DD 或 YYYY-MM-DD）")
            return
        if start_date > end_date:
            start_date, end_date = end_date, start_date

        resume = str(os.environ.get("RESUME") or "").strip().lower() in ("1", "true", "yes")
        sleep_sec = float(os.environ.get("SLEEP_SEC") or 0.6)
        sleep_day_sec = float(os.environ.get("SLEEP_DAY_SEC") or 1.2)

        progress_key = "backfill_racecard_progress"
        if resume:
            cfg = _get_cfg(session, progress_key)
            last_done = None
            if cfg and isinstance(cfg.value, dict):
                last_done = _parse_ymd(str(cfg.value.get("last_date") or ""))
            if last_done and last_done >= start_date:
                start_date = min(end_date, last_done + timedelta(days=1))

        use_fixture = str(os.environ.get("USE_FIXTURE_DATES") or "").strip().lower() in ("1", "true", "yes")
        days: Optional[List[date]] = None
        if use_fixture:
            days = _dates_from_fixture(session, start_date, end_date)
        if not days:
            days = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

        print(f">>> 批量回填排位表：{_fmt_ymd(start_date)} -> {_fmt_ymd(end_date)} （days={len(days)} use_fixture={bool(use_fixture)}）")

        scraper = RaceCardScraper()
        repo = RacingRepository(session)

        total_races = 0
        total_entries = 0
        for d in days:
            date_str = _fmt_ymd(d)
            print(f">>> 賽日：{date_str}")

            races_info = scraper.get_all_races_info(race_date=date_str)
            if not races_info:
                print("    - 無賽事或無法抓取（跳過）")
                _upsert_cfg(session, progress_key, {"last_date": date_str, "updated_at": datetime.now(timezone.utc).isoformat()}, "批量回填排位表進度")
                time.sleep(max(0.0, sleep_day_sec))
                continue

            race_dt = datetime.combine(d, dtime.min)
            for race_info in races_info:
                if not isinstance(race_info, dict):
                    continue
                rn = int(race_info.get("race_no") or 0)
                venue = str(race_info.get("venue") or "").strip() or "ST"
                if rn <= 0:
                    continue

                race = repo.create_race(
                    race_date=race_dt,
                    venue=venue,
                    race_no=rn,
                    post_time_hk=str(race_info.get("post_time_hk") or "").strip(),
                    race_class=str(race_info.get("race_class") or "").strip(),
                    distance=int(race_info.get("distance") or 0),
                    going=str(race_info.get("going") or "").strip(),
                    track_type=str(race_info.get("track_type") or "").strip(),
                    surface=str(race_info.get("surface") or "").strip(),
                    course_type=str(race_info.get("course_type") or "").strip(),
                )
                total_races += 1

                entries = race_info.get("entries") if isinstance(race_info.get("entries"), list) else []
                n_ok = 0
                for e in entries:
                    if not isinstance(e, dict):
                        continue
                    try:
                        horse_no = int(e.get("horse_no") or 0)
                    except Exception:
                        horse_no = 0
                    if horse_no <= 0:
                        continue

                    horse = repo.get_or_create_horse(str(e.get("horse_code") or "").strip().upper(), str(e.get("horse_name") or "").strip() or "未知")
                    jockey = repo.get_or_create_jockey(str(e.get("jockey") or "").strip() or "未知")
                    trainer = repo.get_or_create_trainer(str(e.get("trainer") or "").strip() or "未知")

                    entry = session.query(RaceEntry).filter_by(race_id=int(race.id), horse_no=int(horse_no)).first()
                    if not entry:
                        entry = RaceEntry(
                            race_id=int(race.id),
                            horse_id=int(horse.id),
                            jockey_id=int(jockey.id),
                            trainer_id=int(trainer.id),
                            horse_no=int(horse_no),
                            draw=int(e.get("draw") or 0) or None,
                            actual_weight=int(e.get("actual_weight") or 0) or None,
                            rating=int(e.get("rating") or 0) or None,
                        )
                        session.add(entry)
                        session.flush()
                    else:
                        if int(getattr(entry, "horse_id", 0) or 0) <= 0:
                            entry.horse_id = int(horse.id)
                        if int(getattr(entry, "jockey_id", 0) or 0) <= 0:
                            entry.jockey_id = int(jockey.id)
                        if int(getattr(entry, "trainer_id", 0) or 0) <= 0:
                            entry.trainer_id = int(trainer.id)
                        if getattr(entry, "draw", None) is None and int(e.get("draw") or 0) > 0:
                            entry.draw = int(e.get("draw") or 0)
                        if getattr(entry, "actual_weight", None) is None and int(e.get("actual_weight") or 0) > 0:
                            entry.actual_weight = int(e.get("actual_weight") or 0)
                        if getattr(entry, "rating", None) is None and int(e.get("rating") or 0) > 0:
                            entry.rating = int(e.get("rating") or 0)

                    n_ok += 1
                total_entries += n_ok
                session.commit()
                print(f"    - {venue} R{rn}: entries={n_ok}")
                time.sleep(max(0.0, sleep_sec))

            _upsert_cfg(session, progress_key, {"last_date": date_str, "updated_at": datetime.now(timezone.utc).isoformat()}, "批量回填排位表進度")
            time.sleep(max(0.0, sleep_day_sec))

        print(f">>> 完成：days={len(days)} races={total_races} entries={total_entries}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
