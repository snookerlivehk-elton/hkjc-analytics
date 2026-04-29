import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import init_db, get_session
from database.models import SystemConfig
from data_scraper.windtracker import WindTrackerScraper


HK_TZ = ZoneInfo("Asia/Hong_Kong")
RACECARD_URL = "https://racing.hkjc.com/zh-hk/local/information/racecard"


def _get_cfg(session, key: str) -> Optional[SystemConfig]:
    return session.query(SystemConfig).filter_by(key=key).first()


def _upsert_cfg(session, key: str, value, description: str):
    cfg = _get_cfg(session, key)
    if not cfg:
        cfg = SystemConfig(key=key, description=description, value=value)
        session.add(cfg)
    else:
        cfg.value = value
        if description and not cfg.description:
            cfg.description = description
    session.commit()


def _date_str(now_hk: datetime, session) -> str:
    env_date = str(os.environ.get("TARGET_DATE") or "").strip()
    if env_date:
        return env_date
    cfg = _get_cfg(session, "fixture_next_raceday")
    if cfg and isinstance(cfg.value, str) and cfg.value.strip():
        return cfg.value.strip()
    return now_hk.strftime("%Y/%m/%d")


def _fetch_race_count_and_race_nos(racedate: str) -> Tuple[int, Dict[int, str]]:
    s = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://racing.hkjc.com/",
    }
    r = s.get(RACECARD_URL, params={"racedate": racedate, "RaceNo": 1}, headers=headers, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    race_nos = set()
    for a in soup.select("a[href*='RaceNo=']"):
        m = re.search(r"RaceNo=(\d+)", a.get("href", ""), re.IGNORECASE)
        if m:
            race_nos.add(int(m.group(1)))
    race_count = max(race_nos) if race_nos else 9
    return race_count, headers


def _parse_off_time_from_racecard_html(html_text: str) -> Optional[str]:
    soup = BeautifulSoup(html_text, "lxml")
    t = soup.get_text(separator=" ", strip=True)
    m = re.search(r"(\d{1,2}:\d{2})\s*(草地|全天候跑道|泥地|全天候|泥)", t)
    if m:
        return m.group(1)
    m2 = re.search(r"\b(\d{1,2}:\d{2})\b", t)
    return m2.group(1) if m2 else None


def _fetch_off_times(racedate: str) -> Dict[int, str]:
    race_count, headers = _fetch_race_count_and_race_nos(racedate)
    s = requests.Session()
    out: Dict[int, str] = {}
    for race_no in range(1, race_count + 1):
        r = s.get(RACECARD_URL, params={"racedate": racedate, "RaceNo": race_no}, headers=headers, timeout=20)
        r.raise_for_status()
        off_time = _parse_off_time_from_racecard_html(r.text)
        if off_time:
            out[race_no] = off_time
    return out


def _get_off_times_cached(session, racedate: str) -> Dict[int, str]:
    key = f"race_off_times:{racedate}"
    cfg = _get_cfg(session, key)
    if cfg and isinstance(cfg.value, dict) and cfg.value.get("races"):
        races = cfg.value.get("races")
        if isinstance(races, dict):
            out: Dict[int, str] = {}
            for k, v in races.items():
                try:
                    rn = int(k)
                except Exception:
                    continue
                if isinstance(v, str) and v.strip():
                    out[rn] = v.strip()
            if out:
                return out
    return {}


def _cache_off_times(session, racedate: str, off_times: Dict[int, str]):
    key = f"race_off_times:{racedate}"
    payload = {
        "racedate": racedate,
        "captured_at": datetime.now(HK_TZ).isoformat(),
        "source": "racecard",
        "races": {str(k): v for k, v in sorted(off_times.items())},
    }
    _upsert_cfg(session, key, payload, f"賽事開跑時間 (racedate={racedate})")


def _to_off_dt(racedate: str, off_time_hhmm: str) -> Optional[datetime]:
    try:
        d = datetime.strptime(racedate, "%Y/%m/%d").date()
        t = datetime.strptime(off_time_hhmm, "%H:%M").time()
        return datetime.combine(d, t).replace(tzinfo=HK_TZ)
    except Exception:
        return None


def _should_capture(now_hk: datetime, off_dt: datetime) -> Tuple[bool, Optional[float]]:
    delta_min = (off_dt - now_hk).total_seconds() / 60.0
    if 8.0 <= delta_min <= 12.0:
        return True, round(delta_min, 1)
    return False, round(delta_min, 1)


def main():
    init_db()
    session = get_session()
    try:
        now_hk = datetime.now(HK_TZ)
        now_override = str(os.environ.get("NOW_HK") or "").strip()
        if now_override:
            try:
                now_hk = datetime.fromisoformat(now_override)
                if now_hk.tzinfo is None:
                    now_hk = now_hk.replace(tzinfo=HK_TZ)
            except Exception:
                now_hk = datetime.now(HK_TZ)
        racedate = _date_str(now_hk, session)
        dry_run = str(os.environ.get("DRY_RUN") or "").strip() in ("1", "true", "True", "yes", "YES")

        off_times = _get_off_times_cached(session, racedate)
        if not off_times:
            off_times = _fetch_off_times(racedate)
            if off_times:
                _cache_off_times(session, racedate, off_times)

        if not off_times:
            print(f"找不到開跑時間，略過：racedate={racedate}")
            return

        scraper = WindTrackerScraper()
        captured_any = False

        for race_no, hhmm in sorted(off_times.items(), key=lambda x: x[0]):
            snap_key = f"windtracker_t10:{racedate}:{int(race_no)}"
            if _get_cfg(session, snap_key):
                continue

            off_dt = _to_off_dt(racedate, hhmm)
            if not off_dt:
                continue

            ok, t_minus = _should_capture(now_hk, off_dt)
            if not ok:
                continue

            payload = scraper.scrape()
            payload["meta"] = {
                "racedate": racedate,
                "race_no": int(race_no),
                "off_time_hk": off_dt.isoformat(),
                "t_minus_minutes": t_minus,
            }

            if dry_run:
                print(f"[DRY_RUN] 命中：racedate={racedate} race_no={race_no} t_minus={t_minus}")
                captured_any = True
                continue

            _upsert_cfg(session, snap_key, payload, f"WindTracker 場地快照 (T-10) racedate={racedate} R{int(race_no)}")
            print(f"已保存：{snap_key} t_minus={t_minus}")
            captured_any = True

        if not captured_any:
            print(f"未命中任何場次：now_hk={now_hk.isoformat()} racedate={racedate}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
