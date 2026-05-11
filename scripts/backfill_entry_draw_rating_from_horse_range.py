import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from sqlalchemy import or_

from database.connection import get_session, init_db
from database.models import Horse, Race, RaceEntry, SystemConfig
from data_scraper.horse import HorseScraper


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


def _to_int(v) -> Optional[int]:
    s = str(v or "").strip()
    if not s or s in ("---", "—"):
        return None
    try:
        return int(s)
    except Exception:
        return None


def _get_cfg(session, key: str):
    return session.query(SystemConfig).filter_by(key=key).first()


def _upsert_cfg(session, key: str, value, description: str):
    cfg = _get_cfg(session, key)
    if not cfg:
        cfg = SystemConfig(key=key, description=description)
        session.add(cfg)
    cfg.value = value
    session.commit()


def _build_mapping(history) -> Dict[Tuple[str, str, int], Tuple[Optional[int], Optional[int]]]:
    out: Dict[Tuple[str, str, int], Tuple[Optional[int], Optional[int]]] = {}
    if not isinstance(history, list):
        return out
    for r in history:
        if not isinstance(r, dict):
            continue
        racedate = str(r.get("racedate") or "").strip()
        racecourse = str(r.get("racecourse") or "").strip().upper()
        try:
            raceno = int(r.get("raceno") or 0)
        except Exception:
            raceno = 0
        if not (racedate and racecourse and raceno > 0):
            continue
        out[(racedate, racecourse, raceno)] = (_to_int(r.get("draw")), _to_int(r.get("rating")))
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
        overwrite = str(os.environ.get("OVERWRITE") or "").strip().lower() in ("1", "true", "yes")
        sleep_sec = float(os.environ.get("SLEEP_SEC") or 0.6)
        sleep_horse_sec = float(os.environ.get("SLEEP_HORSE_SEC") or 0.2)

        start_dt = datetime.combine(start_date, datetime.min.time())
        end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time())

        progress_key = "backfill_entry_draw_rating_progress"
        last_code = ""
        if resume:
            cfg = _get_cfg(session, progress_key)
            if cfg and isinstance(cfg.value, dict):
                last_code = str(cfg.value.get("last_code") or "").strip().upper()

        q = (
            session.query(Horse.code)
            .join(RaceEntry, Horse.id == RaceEntry.horse_id)
            .join(Race, Race.id == RaceEntry.race_id)
            .filter(Race.race_date >= start_dt)
            .filter(Race.race_date < end_dt)
            .filter(or_(RaceEntry.draw.is_(None), RaceEntry.rating.is_(None)))
            .distinct()
            .order_by(Horse.code.asc())
        )
        horse_codes = [str(x[0] or "").strip().upper() for x in q.all() if str(x[0] or "").strip()]

        if last_code:
            horse_codes = [c for c in horse_codes if c > last_code]

        print(
            f">>> 回填檔位/評分（馬匹頁往績）：{_fmt_ymd(start_date)} -> {_fmt_ymd(end_date)} "
            f"horses={len(horse_codes)} resume={bool(resume)} overwrite={bool(overwrite)}"
        )

        scraper = HorseScraper()
        ok_horses = 0
        updated_entries = 0
        for idx0, code in enumerate(horse_codes, start=1):
            history = scraper.get_horse_past_performance(code)
            mapping = _build_mapping(history)
            if not mapping:
                _upsert_cfg(
                    session,
                    progress_key,
                    {"last_code": code, "ok_horses": ok_horses, "updated_entries": updated_entries, "updated_at": datetime.now(timezone.utc).isoformat()},
                    "回填檔位/評分（馬匹頁往績）進度",
                )
                time.sleep(max(0.0, sleep_horse_sec))
                continue

            rows = (
                session.query(RaceEntry, Race)
                .join(Race, Race.id == RaceEntry.race_id)
                .join(Horse, Horse.id == RaceEntry.horse_id)
                .filter(Horse.code == code)
                .filter(Race.race_date >= start_dt)
                .filter(Race.race_date < end_dt)
                .all()
            )

            u = 0
            for entry, race in rows:
                try:
                    d0 = race.race_date.date()
                except Exception:
                    continue
                k = (_fmt_ymd(d0), str(race.venue or "").strip().upper(), int(race.race_no or 0))
                v = mapping.get(k)
                if not v:
                    continue
                draw_v, rating_v = v
                changed = False
                if overwrite or getattr(entry, "draw", None) is None:
                    if draw_v is not None:
                        entry.draw = int(draw_v)
                        changed = True
                if overwrite or getattr(entry, "rating", None) is None:
                    if rating_v is not None:
                        entry.rating = int(rating_v)
                        changed = True
                if changed:
                    u += 1

            if u:
                session.commit()
            ok_horses += 1
            updated_entries += u
            print(f"    - [{idx0}/{len(horse_codes)}] {code}: updated_entries={u}")

            _upsert_cfg(
                session,
                progress_key,
                {"last_code": code, "ok_horses": ok_horses, "updated_entries": updated_entries, "updated_at": datetime.now(timezone.utc).isoformat()},
                "回填檔位/評分（馬匹頁往績）進度",
            )
            time.sleep(max(0.0, sleep_sec))

        print(f">>> 完成：horses={len(horse_codes)} ok_horses={ok_horses} updated_entries={updated_entries}")
    finally:
        session.close()


if __name__ == "__main__":
    main()

