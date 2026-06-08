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
CATCH_UP_UNTIL = time(12, 0)


def _env_flag_default_true(name: str) -> bool:
    v = os.environ.get(str(name))
    if v is None:
        return True
    return str(v).strip().lower() in ("1", "true", "yes")


def _last_post_dt(session, race_date):
    if not race_date:
        return None
    start = datetime.combine(race_date, datetime.min.time())
    end = start + timedelta(days=1)
    rows = (
        session.query(Race.race_no, Race.post_time_hk)
        .filter(Race.race_date >= start)
        .filter(Race.race_date < end)
        .order_by(Race.race_no.desc(), Race.id.desc())
        .all()
    )
    for _, s in rows:
        if isinstance(s, str) and s.strip():
            try:
                hh, mm = s.strip().split(":")
                hh_i = int(hh)
                mm_i = int(mm)
                if 0 <= hh_i <= 23 and 0 <= mm_i <= 59:
                    return datetime.combine(race_date, time(hh_i, mm_i)).replace(tzinfo=HK_TZ)
            except Exception:
                continue
    return None


def _get_latest_race_date(session, now_hk: datetime):
    today = now_hk.date()
    end_dt = datetime.combine(today + timedelta(days=1), datetime.min.time())
    race = (
        session.query(Race)
        .filter(Race.race_date < end_dt)
        .order_by(Race.race_date.desc(), Race.race_no.desc())
        .first()
    )
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

def _clear_done(session, date_str: str):
    key = f"auto_results_fetched:{date_str}"
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if cfg:
        cfg.value = False
        cfg.updated_at = datetime.now()
        session.commit()


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

    ignore_time = str(os.environ.get("RESULTS_IGNORE_TIME") or "").strip().lower() in ("1", "true", "yes")
    if ignore_time:
        return True

    catch_until_s = str(os.environ.get("RESULTS_CATCH_UP_UNTIL") or "").strip()
    catch_until = CATCH_UP_UNTIL
    if catch_until_s:
        try:
            hh, mm = catch_until_s.split(":")
            hh_i = int(hh)
            mm_i = int(mm)
            if 0 <= hh_i <= 23 and 0 <= mm_i <= 59:
                catch_until = time(hh_i, mm_i)
        except Exception:
            pass

    catch_days_s = str(os.environ.get("RESULTS_CATCH_UP_DAYS") or "").strip()
    try:
        catch_days = int(catch_days_s) if catch_days_s else 2
    except Exception:
        catch_days = 2
    if catch_days < 0:
        catch_days = 0
    if catch_days > 7:
        catch_days = 7
    catch_mode = str(os.environ.get("RESULTS_CATCH_UP_MODE") or "anytime").strip().lower()

    today = now_hk.date()
    if today == race_date:
        try:
            session = get_session()
            try:
                last_dt = _last_post_dt(session, race_date)
            finally:
                session.close()
            if last_dt:
                if now_hk >= (last_dt + timedelta(minutes=40)):
                    return True
        except Exception:
            pass
        if now_hk.time() >= RUN_AT:
            return True

    if (today > race_date) and (today <= (race_date + timedelta(days=int(catch_days)))):
        if catch_mode in ("until", "morning"):
            return now_hk.time() <= catch_until
        return True

    return False


def main():
    init_db()
    session = get_session()
    try:
        now_hk = datetime.now(HK_TZ)
        race_date = _get_latest_race_date(session, now_hk)
        if not race_date:
            print("找不到任何賽事資料，略過。")
            return

        date_str = race_date.strftime("%Y/%m/%d")
        if not should_run(now_hk, race_date):
            print(
                f"未到執行時間：now_hk={now_hk.isoformat()} latest_finished_race_date={date_str} "
                f"run_at={RUN_AT.strftime('%H:%M')} "
                f"catch_up_mode={str(os.environ.get('RESULTS_CATCH_UP_MODE') or 'anytime')} "
                f"catch_up_until={str(os.environ.get('RESULTS_CATCH_UP_UNTIL') or CATCH_UP_UNTIL.strftime('%H:%M'))}"
            )
            return

        force = str(os.environ.get("RESULTS_FORCE") or "").strip().lower() in ("1", "true", "yes")
        if (not force) and _already_done(session, date_str):
            if _validate_date_fetched(session, race_date):
                print(f"已完成：{date_str}（避免重覆）")
                return
            _clear_done(session, date_str)
            print(f"偵測到完成標記但資料未齊，已解除標記並重跑：{date_str}")
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
            enabled = _env_flag_default_true("ENABLE_AUTO_AI_REFLECTION")
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
