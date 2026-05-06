import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from sqlalchemy import func

from database.connection import get_session, init_db
from database.models import Horse, Race, RaceDividend, RaceEntry, RaceResult, RaceTrackCondition, SystemConfig
from data_scraper.local_results import LocalResultsScraper
from scoring_engine.track_conditions import normalize_going


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


def parse_finish_time_to_seconds(s: str):
    v = str(s or "").strip().replace(" ", "").replace("．", ".").replace("：", ":")
    if not v:
        return None
    if ":" in v:
        parts = v.split(":")
        if len(parts) != 2:
            return None
        try:
            m = int(parts[0])
            sec = float(parts[1])
        except ValueError:
            return None
        return m * 60.0 + sec
    if v.count(".") >= 2:
        p = v.split(".")
        try:
            m = int(p[0])
            s2 = int(p[1])
            frac = int(p[2])
        except ValueError:
            return None
        if s2 < 0 or s2 >= 60:
            return None
        return m * 60.0 + s2 + (frac / (100.0 if frac >= 10 else 10.0))
    try:
        sec = float(v)
    except ValueError:
        return None
    return sec if sec > 0 else None


def _race_id_for(d: date, race_no: int) -> str:
    return f"{d.strftime('%Y%m%d')}-{int(race_no)}"


def _upsert_race(session, d: date, venue: str, race_no: int, meta: Dict[str, Any]) -> Race:
    rid = _race_id_for(d, race_no)
    race_dt = datetime.combine(d, datetime.min.time())
    race = session.query(Race).filter(Race.race_id == rid).first()
    if not race:
        race = Race(race_date=race_dt, venue=str(venue or "").strip() or "ST", race_no=int(race_no), race_id=rid)
        session.add(race)
        session.flush()

    dist = meta.get("distance")
    try:
        dist_i = int(dist or 0)
    except Exception:
        dist_i = 0
    if dist_i > 0 and int(getattr(race, "distance", 0) or 0) <= 0:
        race.distance = dist_i

    going = str(meta.get("going") or "").strip()
    if going and not str(getattr(race, "going", "") or "").strip():
        race.going = going

    surface = str(meta.get("surface") or "").strip()
    if surface and not str(getattr(race, "surface", "") or "").strip():
        race.surface = surface

    course_type = str(meta.get("course_type") or "").strip()
    if course_type and not str(getattr(race, "course_type", "") or "").strip():
        race.course_type = course_type

    track_type = str(meta.get("track_type") or "").strip()
    if track_type and not str(getattr(race, "track_type", "") or "").strip():
        race.track_type = track_type

    return race


def _get_or_create_horse(session, code: str, name_ch: str) -> Optional[Horse]:
    c = str(code or "").strip().upper()
    n = str(name_ch or "").strip() or c
    if not c:
        return None
    h = session.query(Horse).filter(Horse.code == c).first()
    if not h:
        h = Horse(code=c, name_ch=n)
        session.add(h)
        session.flush()
        return h
    if (not str(getattr(h, "name_ch", "") or "").strip()) or (str(getattr(h, "name_ch", "") or "").strip() == "未知"):
        h.name_ch = n
        session.flush()
    return h


def _upsert_track_condition(session, race_id: int, meta: Dict[str, Any]) -> None:
    going_raw, going_code = normalize_going(str(meta.get("going") or ""))
    track_raw = str(meta.get("track") or "").strip()
    if not (going_raw or track_raw):
        return
    tc = session.query(RaceTrackCondition).filter_by(race_id=int(race_id)).first()
    if not tc:
        tc = RaceTrackCondition(race_id=int(race_id), source="HKJC_LOCALRESULTS")
        session.add(tc)
    tc.going_raw = going_raw or None
    tc.going_code = going_code or None
    tc.track_raw = track_raw or None


def _upsert_dividend(session, race_id: int, payload: Dict[str, Any]) -> None:
    div = session.query(RaceDividend).filter_by(race_id=int(race_id)).first()
    if not div:
        div = RaceDividend(race_id=int(race_id), source="HKJC")
        session.add(div)
    div.meta = payload.get("meta") or {}
    div.dividends = payload.get("dividends") or []


def _upsert_results(session, race: Race, payload: Dict[str, Any]) -> bool:
    results = payload.get("results") or []
    if not isinstance(results, list) or not results:
        return False

    runpos_by_horse_no: Dict[str, str] = {}
    changed = 0
    for r in results:
        if not isinstance(r, dict):
            continue

        try:
            horse_no = int(r.get("horse_no") or 0)
        except Exception:
            horse_no = 0
        if horse_no <= 0:
            continue

        pos = str(r.get("running_position") or "").strip()
        if pos:
            runpos_by_horse_no[str(horse_no)] = pos

        h = _get_or_create_horse(session, str(r.get("horse_code") or ""), str(r.get("horse_name") or ""))
        if h is None:
            continue

        entry = session.query(RaceEntry).filter_by(race_id=int(race.id), horse_no=int(horse_no)).first()
        if not entry:
            entry = RaceEntry(race_id=int(race.id), horse_id=int(h.id), horse_no=int(horse_no))
            session.add(entry)
            session.flush()
            changed += 1
        else:
            if int(getattr(entry, "horse_id", 0) or 0) <= 0:
                entry.horse_id = int(h.id)
                session.flush()
                changed += 1

        rr = session.query(RaceResult).filter_by(entry_id=int(entry.id)).first()
        if not rr:
            rr = RaceResult(entry_id=int(entry.id))
            session.add(rr)
            changed += 1

        try:
            rk = int(r.get("rank") or 0)
        except Exception:
            rk = 0
        rr.rank = rk or None
        rr.finish_time = str(r.get("finish_time") or "").strip()
        rr.finish_time_sec = parse_finish_time_to_seconds(rr.finish_time)
        rr.win_odds = r.get("win_odds")
        rr.margin = str(r.get("margin") or "").strip()

    if runpos_by_horse_no:
        date_str = str(payload.get("race_date") or "").strip()
        race_no = int(getattr(race, "race_no", 0) or 0)
        if date_str and race_no:
            key = f"race_runpos:{date_str}:{race_no}"
            cfg = session.query(SystemConfig).filter_by(key=key).first()
            if not cfg:
                cfg = SystemConfig(key=key, description="賽果沿途走位（running_position）快照")
                session.add(cfg)
            cfg.value = {"race_id": int(race.id), "race_date": date_str, "race_no": race_no, "runpos": runpos_by_horse_no}

    return True


def _try_scrape(scraper: LocalResultsScraper, date_str: str, racecourse: str, race_no: int) -> Optional[Dict[str, Any]]:
    try:
        payload = scraper.scrape_single_race(date_str, racecourse, race_no)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    results = payload.get("results") if isinstance(payload.get("results"), list) else []
    divs = payload.get("dividends") if isinstance(payload.get("dividends"), list) else []
    if not results and not divs:
        return None
    return payload


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
        max_races_per_day = int(os.environ.get("MAX_RACES_PER_DAY") or 12)
        sleep_sec = float(os.environ.get("SLEEP_SEC") or 0.6)
        sleep_day_sec = float(os.environ.get("SLEEP_DAY_SEC") or 1.2)

        progress_key = "backfill_local_results_progress"
        if resume:
            cfg = session.query(SystemConfig).filter_by(key=progress_key).first()
            last_done = None
            if cfg and isinstance(cfg.value, dict):
                last_done = _parse_ymd(str(cfg.value.get("last_date") or ""))
            if last_done and last_done >= start_date:
                start_date = min(end_date, last_done + timedelta(days=1))

        print(f">>> 批量回填賽果：{_fmt_ymd(start_date)} -> {_fmt_ymd(end_date)}")

        scraper = LocalResultsScraper()
        total_races = 0
        ok_races = 0
        for d in (start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)):
            date_str = _fmt_ymd(d)
            print(f">>> 賽日：{date_str}")

            chosen_course = None
            p1 = _try_scrape(scraper, date_str, "ST", 1)
            if p1:
                chosen_course = "ST"
            else:
                p1 = _try_scrape(scraper, date_str, "HV", 1)
                if p1:
                    chosen_course = "HV"
            if not chosen_course:
                print("    - 無賽事或無法抓取（跳過）")
                cfg = session.query(SystemConfig).filter_by(key=progress_key).first()
                if not cfg:
                    cfg = SystemConfig(key=progress_key, description="批量回填賽果進度")
                    session.add(cfg)
                cfg.value = {"last_date": date_str, "updated_at": datetime.now(timezone.utc).isoformat()}
                session.commit()
                time.sleep(max(0.0, sleep_day_sec))
                continue

            empty_streak = 0
            for rn in range(1, max_races_per_day + 1):
                total_races += 1
                payload = p1 if (rn == 1 and p1) else _try_scrape(scraper, date_str, chosen_course, rn)
                if not payload:
                    empty_streak += 1
                    if rn >= 2 and empty_streak >= 2:
                        break
                    continue
                empty_streak = 0

                meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
                results = payload.get("results") if isinstance(payload.get("results"), list) else []
                dividends = payload.get("dividends") if isinstance(payload.get("dividends"), list) else []
                n_results = 0
                n_runpos = 0
                for r in results:
                    if not isinstance(r, dict):
                        continue
                    try:
                        hn = int(r.get("horse_no") or 0)
                    except Exception:
                        hn = 0
                    if hn <= 0:
                        continue
                    n_results += 1
                    if str(r.get("running_position") or "").strip():
                        n_runpos += 1
                venue = str(meta.get("venue") or chosen_course).strip() or chosen_course
                race = _upsert_race(session, d, venue, rn, meta)
                _upsert_dividend(session, int(race.id), payload)
                _upsert_track_condition(session, int(race.id), meta)

                ok_rows = _upsert_results(session, race, payload)
                session.commit()
                ok_races += 1 if ok_rows else 0
                print(
                    f"    - {chosen_course} R{rn}: ok={bool(ok_rows)} results={n_results} runpos={n_runpos} dividends={len(dividends)} going={str(meta.get('going') or '').strip()} dist={int(meta.get('distance') or 0)} course={str(meta.get('course_type') or '').strip()}"
                )
                time.sleep(max(0.0, sleep_sec))

            cfg = session.query(SystemConfig).filter_by(key=progress_key).first()
            if not cfg:
                cfg = SystemConfig(key=progress_key, description="批量回填賽果進度")
                session.add(cfg)
            cfg.value = {"last_date": date_str, "updated_at": datetime.now(timezone.utc).isoformat()}
            session.commit()
            time.sleep(max(0.0, sleep_day_sec))

        print(f">>> 完成：days={(end_date - start_date).days + 1} total_races={total_races} ok_races={ok_races}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
