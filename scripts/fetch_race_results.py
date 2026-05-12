import os
import sys
from datetime import datetime
from pathlib import Path

# 加入專案根目錄到路徑，避免在部署環境找不到 database 模組
root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import init_db, get_session
from database.models import Race, RaceEntry, RaceResult, RaceDividend, RaceTrackCondition, SystemConfig, RaceCoRunning
from data_scraper.local_results import LocalResultsScraper
from data_scraper.corunning import CoRunningScraper
from scoring_engine.member_stats import update_all_members_preset_stats_for_race_date
from scoring_engine.prediction_snapshots import finalize_prediction_top5_hits_for_race_date
from scoring_engine.entry_facts import build_entry_facts_for_race_date
from scoring_engine.draw_stats_daily import rebuild_draw_stats_daily_for_race_date
from scoring_engine.race_pace import compute_race_pace_for_race_date
from scoring_engine.track_conditions import normalize_going
from scoring_engine.config_value import build_meta, unwrap_value, wrap_value
from scoring_engine.normalization import venue_code


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
    return venue_code(venue)


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
    corunning = CoRunningScraper()
    ok = 0
    for race in races:
        racecourse = venue_to_racecourse(race.venue)
        print(f"抓取賽果/派彩：{target_date} {racecourse} 第{race.race_no}場")
        payload = scraper.scrape_single_race(target_date, racecourse, race.race_no)
        meta = payload.get("meta") or {}
        page_date = str(meta.get("race_date_page") or "").strip().replace("-", "/")
        if page_date and page_date != str(target_date):
            print(f"[略過] 賽果頁日期不符：expect={target_date} got={page_date}（通常表示該日未有賽果/網站回傳其他賽日）")
            continue
        page_venue = str(meta.get("venue") or "").strip().upper()
        if page_venue and page_venue != str(racecourse).strip().upper():
            print(f"[略過] 賽果頁場地不符：expect={racecourse} got={page_venue}")
            continue
        try:
            page_rn = int(meta.get("race_no_page")) if meta.get("race_no_page") is not None else None
        except Exception:
            page_rn = None
        if page_rn and int(page_rn) != int(race.race_no or 0):
            print(f"[略過] 賽果頁場次不符：expect={int(race.race_no or 0)} got={int(page_rn)}")
            continue

        div = session.query(RaceDividend).filter_by(race_id=race.id).first()
        if not div:
            div = RaceDividend(race_id=race.id, source="HKJC")
            session.add(div)
        div.meta = meta
        div.dividends = payload.get("dividends") or []

        going_raw, going_code = normalize_going(str(meta.get("going") or ""))
        track_raw = str(meta.get("track") or "").strip()
        if going_raw or track_raw:
            tc = session.query(RaceTrackCondition).filter_by(race_id=race.id).first()
            if not tc:
                tc = RaceTrackCondition(race_id=race.id, source="HKJC_LOCALRESULTS")
                session.add(tc)
            tc.going_raw = going_raw or None
            tc.going_code = going_code or None
            tc.track_raw = track_raw or None
            tc.updated_at = datetime.now()

        results = payload.get("results") or []
        has_any_time = False
        try:
            for r in results:
                if not isinstance(r, dict):
                    continue
                if str(r.get("finish_time") or "").strip():
                    has_any_time = True
                    break
        except Exception:
            has_any_time = False
        if results and (not has_any_time):
            print(f"[略過] 尚未有賽果（無完成時間）：{target_date} {racecourse} R{int(race.race_no or 0)}")
            continue
        runpos_by_horse_no = {}
        for r in results:
            horse_no = r.get("horse_no") or 0
            try:
                hn = int(horse_no)
            except Exception:
                hn = 0
            if hn:
                pos = str(r.get("running_position") or "").strip()
                if pos:
                    runpos_by_horse_no[str(hn)] = pos
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

        if runpos_by_horse_no:
            key = f"race_runpos:{target_date}:{int(race.race_no)}"
            cfg = session.query(SystemConfig).filter_by(key=key).first()
            if not cfg:
                cfg = SystemConfig(key=key, description="賽果沿途走位（running_position）快照")
                session.add(cfg)
            payload_runpos = {"race_id": int(race.id), "race_date": target_date, "race_no": int(race.race_no), "runpos": runpos_by_horse_no}
            m = build_meta(
                source="HKJC_LOCALRESULTS",
                fetched_at=datetime.utcnow().isoformat(),
                url=f"https://racing.hkjc.com/zh-hk/local/information/localresults?racedate={target_date}&Racecourse={racecourse}&RaceNo={int(race.race_no)}",
                schema="race_runpos:v1",
            )
            cfg.value = wrap_value(payload_runpos, m)

        try:
            date_yyyymmdd = race_date_dt.strftime("%Y%m%d")
            force = str(os.environ.get("FORCE_CORUNNING") or "").strip().lower() in ("1", "true", "yes")
            row2 = session.query(RaceCoRunning).filter_by(race_id=int(race.id)).first()
            has_items = bool(row2 and isinstance(row2.items, dict) and row2.items)
            if (not has_items) or force:
                res2 = corunning.scrape_single_race(date_yyyymmdd=date_yyyymmdd, race_no=int(race.race_no))
                items = res2.get("items") if isinstance(res2, dict) else None
                if isinstance(items, list) and items:
                    by_no = {str(int(x.get("horse_no"))): x for x in items if int(x.get("horse_no") or 0) > 0}
                    if not row2:
                        row2 = RaceCoRunning(
                            race_id=int(race.id),
                            race_date=race.race_date,
                            race_no=int(race.race_no or 0),
                            source="HKJC",
                            items={},
                        )
                        session.add(row2)
                    row2.items = by_no
                    row2.meta = {
                        "schema": "race_corunning:v1",
                        "date_yyyymmdd": str(date_yyyymmdd),
                        "url": f"https://racing.hkjc.com/zh-hk/local/information/corunning?date={date_yyyymmdd}&raceno={int(race.race_no)}",
                        "fetched_at": datetime.utcnow().isoformat(),
                    }
                    row2.fetched_at = datetime.utcnow()
        except Exception as e:
            print(f"[警告] 走勢評述抓取失敗：R{int(race.race_no)} {e}")

        try:
            from scoring_engine.search_index import index_corunning, index_race_entry_bundle, index_system_config_doc

            index_race_entry_bundle(session, int(race.id))
            index_corunning(session, int(race.id))
            index_system_config_doc(session, f"race_runpos:{target_date}:{int(race.race_no)}", doc_type="runpos", title=f"{target_date} R{int(race.race_no)} runpos")
        except Exception:
            pass

        session.commit()
        ok += 1

    print(f"完成：已同步 {ok} 場賽果與派彩")
    try:
        print(f"正在更新會員命中率統計（賽日 {target_date}）...")
        res = update_all_members_preset_stats_for_race_date(session, target_date)
        if isinstance(res, dict) and res.get("ok"):
            print(f"完成：已更新會員命中率（races={res.get('races')} members={res.get('members')} presets={res.get('presets')}）")
        else:
            print(f"會員命中率更新失敗: {res}")
    except Exception as e:
        print(f"會員命中率更新失敗: {e}")

    try:
        print(f"正在結算 Top5 快照命中（賽日 {target_date}）...")
        res2 = finalize_prediction_top5_hits_for_race_date(session, target_date)
        print(f"完成：Top5 命中結算 updated={res2.get('updated')} skipped={res2.get('skipped')} races={res2.get('races')}")
    except Exception as e:
        print(f"Top5 命中結算失敗: {e}")

    try:
        print(f"正在更新 entry_facts/draw_stats_daily（賽日 {target_date}）...")
        res3 = build_entry_facts_for_race_date(session, date_str=target_date)
        res4 = rebuild_draw_stats_daily_for_race_date(session, date_str=target_date)
        print(f"完成：entry_facts={res3} draw_stats_daily={res4}")
    except Exception as e:
        print(f"entry_facts/draw_stats_daily 更新失敗: {e}")

    try:
        print(f"正在更新每場步速分類（賽日 {target_date}）...")
        res5 = compute_race_pace_for_race_date(session, date_str=target_date)
        print(f"完成：race_pace={res5}")
    except Exception as e:
        print(f"race_pace 更新失敗: {e}")


if __name__ == "__main__":
    main()
