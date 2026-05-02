import hashlib
import os
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import init_db, get_session
from database.models import SystemConfig


HK_TZ = ZoneInfo("Asia/Hong_Kong")


def _month_pairs(today: date):
    y1, m1 = today.year, today.month
    if m1 == 12:
        return [(y1, m1), (y1 + 1, 1)]
    return [(y1, m1), (y1, m1 + 1)]


def _fetch_fixture_html(year: int, month: int) -> str:
    url = "https://racing.hkjc.com/zh-hk/local/information/fixture"
    r = requests.get(url, params={"calyear": year, "calmonth": f"{month:02d}"}, timeout=30)
    r.raise_for_status()
    return r.text


def _parse_fixture_month(year: int, month: int, html_text: str):
    soup = BeautifulSoup(html_text, "lxml")
    out = set()
    for td in soup.find_all("td", class_="calendar"):
        day_el = td.select_one("span.f_fs14")
        if not day_el:
            continue
        try:
            d = int(day_el.get_text(strip=True))
        except Exception:
            continue
        try:
            out.add(date(year, month, d))
        except Exception:
            continue
    return out


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _upsert_config(session, key: str, value, description: str):
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if not cfg:
        cfg = SystemConfig(key=key, description=description)
        session.add(cfg)
    cfg.value = value
    session.commit()


def _get_config_value(session, key: str):
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    return cfg.value if cfg else None


def _compute_next_raceday(dates_list, today: date):
    for s in dates_list:
        try:
            d = datetime.strptime(s, "%Y/%m/%d").date()
        except Exception:
            continue
        if d >= today:
            return s
    return ""


def main():
    init_db()
    session = get_session()
    try:
        now_hk = datetime.now(HK_TZ)
        today = now_hk.date()

        manual_year = os.environ.get("FIXTURE_YEAR")
        manual_month = os.environ.get("FIXTURE_MONTH")
        if manual_year and manual_month:
            pairs = [(int(manual_year), int(manual_month))]
        else:
            pairs = _month_pairs(today)

        dates = set()
        for y, m in pairs:
            html_text = _fetch_fixture_html(y, m)
            dates |= _parse_fixture_month(y, m, html_text)

        date_strs = sorted({d.strftime("%Y/%m/%d") for d in dates})
        payload = "|".join(date_strs)
        h = _sha256(payload)

        old_hash = _get_config_value(session, "fixture_dates_hash")
        if old_hash == h:
            next_day = _compute_next_raceday(date_strs, today)
            _upsert_config(session, "fixture_next_raceday", next_day, "賽期表：下一個賽日")
            print(f"賽期表未變更，共 {len(date_strs)} 個賽日；next_raceday={next_day}")
            return

        _upsert_config(session, "fixture_dates", date_strs, "賽期表：賽日日期清單 (YYYY/MM/DD)")
        _upsert_config(session, "fixture_dates_hash", h, "賽期表：日期清單 Hash")
        _upsert_config(session, "fixture_dates_updated_at", now_hk.isoformat(), "賽期表：更新時間 (HK)")

        next_day = _compute_next_raceday(date_strs, today)
        _upsert_config(session, "fixture_next_raceday", next_day, "賽期表：下一個賽日")

        print(f"賽期表已更新，共 {len(date_strs)} 個賽日；next_raceday={next_day}")
    finally:
        session.close()


if __name__ == "__main__":
    main()

