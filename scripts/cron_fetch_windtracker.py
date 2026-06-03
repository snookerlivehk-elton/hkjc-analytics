import os
import sys
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.models import Race, RaceDayWeather, SystemConfig
from scripts.fetch_windtracker import main
from scoring_engine.readiness import get_race_day_anchor_dt
from sqlalchemy import or_


HK_TZ = ZoneInfo("Asia/Hong_Kong")


def _target_date_str(session) -> str:
    env_date = str(os.environ.get("TARGET_DATE") or "").strip()
    if env_date:
        return env_date
    cfg = session.query(SystemConfig).filter_by(key="fixture_next_raceday").first()
    v = cfg.value if cfg else None
    if isinstance(v, str) and v.strip():
        return v.strip()
    return datetime.now(HK_TZ).strftime("%Y/%m/%d")


def _day_range(date_str: str):
    d0 = datetime.strptime(str(date_str), "%Y/%m/%d").date()
    start = datetime.combine(d0, dtime.min)
    end = start + timedelta(days=1)
    return start, end


def _parse_hhmm_dt(date_str: str, s: str) -> datetime | None:
    t = str(s or "").strip()
    if not t or ":" not in t:
        return None
    try:
        hh, mm = t.split(":")
        hh_i = int(hh)
        mm_i = int(mm)
        if hh_i < 0 or hh_i > 23 or mm_i < 0 or mm_i > 59:
            return None
        d0 = datetime.strptime(str(date_str), "%Y/%m/%d").date()
        return datetime.combine(d0, dtime(hh_i, mm_i)).replace(tzinfo=HK_TZ)
    except Exception:
        return None


def _get_race_times_hk(session, date_str: str) -> list[datetime]:
    start, end = _day_range(date_str)
    rows = (
        session.query(Race.post_time_hk)
        .filter(Race.race_date >= start)
        .filter(Race.race_date < end)
        .order_by(Race.race_no.asc(), Race.id.asc())
        .all()
    )
    out: list[datetime] = []
    for (pt,) in rows:
        dt = _parse_hhmm_dt(date_str, str(pt or ""))
        if dt is not None:
            out.append(dt)
    return out


def _has_any_weather_metrics(session, date_str: str) -> bool:
    try:
        start, end = _day_range(date_str)
        venues = (
            session.query(Race.venue)
            .filter(Race.race_date >= start)
            .filter(Race.race_date < end)
            .filter(Race.venue != None)
            .distinct()
            .all()
        )
        venues0 = [str(v or "").strip() for (v,) in venues if str(v or "").strip()]
        venues0 = list(dict.fromkeys(venues0))
        q = session.query(RaceDayWeather.id).filter(RaceDayWeather.race_date_day == start.date())
        if venues0:
            q = q.filter(RaceDayWeather.venue.in_(venues0))
        q = q.filter(
            or_(
                RaceDayWeather.temperature_c.isnot(None),
                RaceDayWeather.humidity_pct.isnot(None),
                RaceDayWeather.rain_total_mm.isnot(None),
                RaceDayWeather.rain_10min_mm.isnot(None),
                RaceDayWeather.soil_moisture_pct.isnot(None),
                RaceDayWeather.wind_direction.isnot(None),
                RaceDayWeather.wind_speed_kmh_avg.isnot(None),
                RaceDayWeather.wind_speed_kmh_max.isnot(None),
            )
        )
        return bool(q.first())
    except Exception:
        return False


def main_cron():
    init_db()
    session = get_session()
    try:
        date_str = _target_date_str(session)
        now_hk = datetime.now(HK_TZ)

        ignore_window = str(os.environ.get("WINDTRACKER_IGNORE_WINDOW") or "").strip().lower() in ("1", "true", "yes")
        before_h = str(os.environ.get("WINDTRACKER_WINDOW_BEFORE_HOURS") or "").strip()
        after_h = str(os.environ.get("WINDTRACKER_WINDOW_AFTER_HOURS") or "").strip()
        try:
            before_h_v = float(before_h) if before_h else 12.0
        except Exception:
            before_h_v = 12.0
        try:
            after_h_v = float(after_h) if after_h else 2.0
        except Exception:
            after_h_v = 2.0

        anchor = get_race_day_anchor_dt(session, date_str)
        race_times = _get_race_times_hk(session, date_str)
        last_dt = race_times[-1] if race_times else (anchor + timedelta(hours=6))
        w_start = anchor - timedelta(hours=before_h_v)
        w_end = last_dt + timedelta(hours=after_h_v)

        if (not ignore_window) and (not (w_start <= now_hk <= w_end)):
            print(f"outside window date={date_str} now={now_hk.isoformat()} window={w_start.isoformat()}..{w_end.isoformat()}")
            return

        near_m = str(os.environ.get("WINDTRACKER_NEAR_RACE_MINUTES") or "").strip()
        if near_m:
            try:
                near_v = int(near_m)
            except Exception:
                near_v = 0
            if near_v > 0 and race_times:
                ok = any(abs((now_hk - dt).total_seconds()) <= float(near_v) * 60.0 for dt in race_times)
                if not ok:
                    if _has_any_weather_metrics(session, date_str):
                        print(f"skip not_near_race date={date_str} now={now_hk.isoformat()} near_minutes={near_v}")
                        return
                    print(f"override_not_near_race date={date_str} now={now_hk.isoformat()} near_minutes={near_v} reason=no_weather_yet")

        os.environ["TARGET_DATE"] = date_str
        main()
    finally:
        session.close()


if __name__ == "__main__":
    main_cron()
