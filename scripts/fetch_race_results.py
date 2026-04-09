import os
import sys
from datetime import datetime
from pathlib import Path

# 加入專案根目錄到路徑，避免在部署環境找不到 database 模組
root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import init_db, get_session
from database.models import Race, RaceEntry, RaceResult, RaceDividend
from data_scraper.local_results import LocalResultsScraper


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


def venue_to_racecourse(venue: str) -> str:
    v = str(venue or "")
    if "跑馬地" in v or "HV" in v:
        return "HV"
    return "ST"


def main():
    init_db()
    session = get_session()

    target_date = os.environ.get("TARGET_DATE") or datetime.now().strftime("%Y/%m/%d")
    try:
        race_date_dt = datetime.strptime(target_date, "%Y/%m/%d")
    except ValueError:
        print("TARGET_DATE 格式應為 YYYY/MM/DD")
        return

    races = (
        session.query(Race)
        .filter(Race.race_date >= race_date_dt, Race.race_date < race_date_dt.replace(hour=23, minute=59, second=59))
        .order_by(Race.race_no.asc())
        .all()
    )
    if not races:
        print(f"找不到 {target_date} 的賽事資料，請先抓取排位表")
        return

    scraper = LocalResultsScraper()
    ok = 0
    for race in races:
        racecourse = venue_to_racecourse(race.venue)
        print(f"抓取賽果/派彩：{target_date} {racecourse} 第{race.race_no}場")
        payload = scraper.scrape_single_race(target_date, racecourse, race.race_no)
        meta = payload.get("meta") or {}

        div = session.query(RaceDividend).filter_by(race_id=race.id).first()
        if not div:
            div = RaceDividend(race_id=race.id, source="HKJC")
            session.add(div)
        div.meta = meta
        div.dividends = payload.get("dividends") or []

        results = payload.get("results") or []
        for r in results:
            horse_no = r.get("horse_no") or 0
            entry = (
                session.query(RaceEntry)
                .filter_by(race_id=race.id, horse_no=int(horse_no))
                .first()
            )
            if not entry:
                continue
            rr = session.query(RaceResult).filter_by(entry_id=entry.id).first()
            if not rr:
                rr = RaceResult(entry_id=entry.id)
                session.add(rr)
            rr.rank = int(r.get("rank") or 0) or None
            rr.finish_time = r.get("finish_time") or ""
            rr.finish_time_sec = parse_finish_time_to_seconds(rr.finish_time)
            rr.win_odds = r.get("win_odds")
            rr.margin = r.get("margin") or ""

        session.commit()
        ok += 1

    print(f"完成：已同步 {ok} 場賽果與派彩")


if __name__ == "__main__":
    main()
