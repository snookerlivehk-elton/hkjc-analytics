import os
import sys
from datetime import datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# 將專案根目錄加入路徑，以便導入 database 模組
root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import init_db, get_session
from database.models import Race, RaceEntry, RaceResult, RaceDividend, SystemConfig
from scoring_engine.job_queue import enqueue_job
from scripts.fetch_race_results import main as fetch_results_main


HK_TZ = ZoneInfo("Asia/Hong_Kong")
RUN_AT = time(23, 55)
CATCH_UP_UNTIL = time(3, 0)


def _get_latest_race_date(session):
    race = session.query(Race).order_by(Race.race_date.desc(), Race.race_no.desc()).first()
    if not race:
        return None
    rd = race.race_date
    return rd.date() if hasattr(rd, "date") else rd


def _mark_done(session, date_str: str):
    key = f"auto_results_fetched:{date_str}"
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if not cfg:
        cfg = SystemConfig(key=key, description="賽果自動爬取已完成（避免重覆）")
        session.add(cfg)
    cfg.value = True
    cfg.updated_at = datetime.now()
    session.commit()

def _mark_reflect_enqueued(session, date_str: str, job_id: str, top_n: int):
    key = f"auto_ai_reflect_enqueued:{date_str}"
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if not cfg:
        cfg = SystemConfig(key=key, description="賽後自動挑選最失準場次做批次反思：已排隊（避免重覆）")
        session.add(cfg)
    cfg.value = {"job_id": str(job_id or ""), "top_n": int(top_n or 0), "enqueued_at": datetime.utcnow().isoformat()}
    cfg.updated_at = datetime.now()
    session.commit()

def _already_reflect_enqueued(session, date_str: str) -> bool:
    key = f"auto_ai_reflect_enqueued:{date_str}"
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    return bool(cfg and cfg.value)


def _already_done(session, date_str: str) -> bool:
    key = f"auto_results_fetched:{date_str}"
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    return bool(cfg and cfg.value is True)


def _validate_date_fetched(session, race_date) -> bool:
    races = (
        session.query(Race.id)
        .filter(Race.race_date >= datetime.combine(race_date, datetime.min.time()))
        .filter(Race.race_date < datetime.combine(race_date, datetime.min.time()) + timedelta(days=1))
        .all()
    )
    race_ids = [r[0] for r in races]
    if not race_ids:
        return False

    div_cnt = session.query(RaceDividend).filter(RaceDividend.race_id.in_(race_ids)).count()
    res_cnt = (
        session.query(RaceResult)
        .join(RaceEntry, RaceEntry.id == RaceResult.entry_id)
        .filter(RaceEntry.race_id.in_(race_ids))
        .filter(RaceResult.rank != None)
        .count()
    )
    return div_cnt >= len(race_ids) and res_cnt >= (len(race_ids) * 4)

def _validate_event_report_fetched(session, race_date) -> bool:
    races = (
        session.query(Race.id, Race.race_no)
        .filter(Race.race_date >= datetime.combine(race_date, datetime.min.time()))
        .filter(Race.race_date < datetime.combine(race_date, datetime.min.time()) + timedelta(days=1))
        .order_by(Race.race_no.asc(), Race.id.asc())
        .all()
    )
    if not races:
        return False
    date_str = race_date.strftime("%Y/%m/%d")
    expected = {f"race_event_report:{date_str}:{int(rn or 0)}" for _, rn in races if int(rn or 0) > 0}
    if not expected:
        return False
    got = set(
        k
        for (k,) in session.query(SystemConfig.key)
        .filter(SystemConfig.key.in_(list(expected)))
        .all()
        if str(k or "").strip()
    )
    return len(got) >= len(expected)


def should_run(now_hk: datetime, race_date) -> bool:
    if not race_date:
        return False

    today = now_hk.date()
    if today == race_date and now_hk.time() >= RUN_AT:
        return True

    if today == (race_date + timedelta(days=1)) and now_hk.time() <= CATCH_UP_UNTIL:
        return True

    return False


def main():
    init_db()
    session = get_session()
    try:
        now_hk = datetime.now(HK_TZ)
        race_date = _get_latest_race_date(session)
        if not race_date:
            print("找不到任何賽事資料，略過。")
            return

        date_str = race_date.strftime("%Y/%m/%d")
        if not should_run(now_hk, race_date):
            print(f"未到執行時間：now_hk={now_hk.isoformat()} latest_race_date={date_str}")
            return

        if _already_done(session, date_str):
            print(f"已完成：{date_str}（避免重覆）")
            return
    finally:
        session.close()

    os.environ["TARGET_DATE"] = date_str
    print(f"開始自動抓取賽果：{date_str}（HK {RUN_AT.strftime('%H:%M')}）")
    fetch_results_main()

    session2 = get_session()
    try:
        if _validate_date_fetched(session2, race_date):
            _mark_done(session2, date_str)
            print(f"完成並已標記：{date_str}")
            enabled = str(os.environ.get("ENABLE_AUTO_AI_REFLECTION") or "0").strip().lower() in ("1", "true", "yes")
            if enabled and (not _already_reflect_enqueued(session2, date_str)):
                if not _validate_event_report_fetched(session2, race_date):
                    print(f"略過自動賽後反思：尚未齊「競賽事件報告（賽後）」 date={date_str}")
                    return
                top_n_s = str(os.environ.get("AUTO_AI_REFLECT_TOP_N") or "").strip()
                try:
                    top_n = int(top_n_s) if top_n_s else 3
                except Exception:
                    top_n = 3
                if top_n < 1:
                    top_n = 1
                if top_n > 5:
                    top_n = 5
                job = enqueue_job(session2, "ai_batch_reflect_day", {"date": date_str, "mode": "worst", "top_n": int(top_n)})
                jid = str(job.get("id") or "")
                _mark_reflect_enqueued(session2, date_str, jid, int(top_n))
                print(f"已排隊自動賽後反思：date={date_str} top_n={int(top_n)} job_id={jid}")
        else:
            print(f"抓取未達完成條件，保留重試：{date_str}")
    finally:
        session2.close()


if __name__ == "__main__":
    main()
