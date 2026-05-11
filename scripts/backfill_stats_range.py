import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.models import Race, SystemConfig
from scoring_engine.draw_stats_daily import rebuild_draw_stats_daily_for_race_date
from scoring_engine.entry_facts import build_entry_facts_for_race_date


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


def _dates_from_db_races(session, start_date: date, end_date: date) -> List[date]:
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time())
    rows = (
        session.query(Race.race_date)
        .filter(Race.race_date >= start_dt)
        .filter(Race.race_date < end_dt)
        .order_by(Race.race_date.asc())
        .all()
    )
    seen = set()
    out: List[date] = []
    for (dt,) in rows:
        if not dt:
            continue
        try:
            d0 = dt.date()
        except Exception:
            continue
        if d0 in seen:
            continue
        seen.add(d0)
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
        sleep_day_sec = float(os.environ.get("SLEEP_DAY_SEC") or 0.2)

        progress_key = "backfill_stats_progress"
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
            days = _dates_from_db_races(session, start_date, end_date)
        if not days:
            days = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

        print(
            f">>> 批量重建統計快照：{_fmt_ymd(start_date)} -> {_fmt_ymd(end_date)} （days={len(days)} use_fixture={bool(use_fixture)}）"
        )

        ok_days = 0
        for d in days:
            date_str = _fmt_ymd(d)
            r1 = build_entry_facts_for_race_date(session, date_str=date_str)
            r2 = rebuild_draw_stats_daily_for_race_date(session, date_str=date_str)
            ok_days += 1
            print(f"    - {date_str}: entry_facts rows={int(r1.get('rows') or 0)} created={int(r1.get('created') or 0)} updated={int(r1.get('updated') or 0)} | draw_stats rows={int(r2.get('rows') or 0)}")
            _upsert_cfg(session, progress_key, {"last_date": date_str, "updated_at": datetime.now(timezone.utc).isoformat()}, "批量重建統計快照進度")
            time.sleep(max(0.0, sleep_day_sec))

        print(f">>> 完成：days={len(days)} ok_days={ok_days}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
