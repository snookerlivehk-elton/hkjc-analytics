import asyncio
import sys
import os
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

# 將專案根目錄加入路徑
root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.repository import RacingRepository
from data_scraper.race_card import RaceCardScraper
from data_scraper.odds import OddsScraper
from data_scraper.horse import HorseScraper
from scoring_engine.core import ScoringEngine
from scoring_engine.prediction_snapshots import generate_prediction_top5_for_race_date
from scoring_engine.track_conditions import normalize_going
from utils.logger import logger

async def run_daily_scraper():
    """執行每日自動抓取流程"""
    print(">>> 正在初始化資料庫結構...")
    init_db()
    
    # 顯示目前連線對象 (隱藏密碼)
    db_url = os.getenv("DATABASE_URL", "sqlite")
    target = db_url.split('@')[-1] if '@' in db_url else '本地 SQLite'
    print(f">>> 資料庫目標: {target}")
    
    session = get_session()
    repo = RacingRepository(session)
    engine = ScoringEngine(session)
    
    print(">>> 正在啟動穩定版爬蟲 (Requests Mode)...")
    scraper = RaceCardScraper()
    
    try:
        # 從環境變數獲取目標日期 (Y/m/d)，如果沒有則預設為當天
        target_date_str = os.getenv("TARGET_DATE", "")
        if target_date_str:
            print(f">>> 正在啟動穩定版爬蟲，目標日期: {target_date_str} ...")
        else:
            print(">>> 正在啟動穩定版爬蟲 (未指定日期，將自動抓取最新賽事)...")
            
        races_info = scraper.get_all_races_info(race_date=target_date_str)
        
        if not races_info:
            print(f">>> [失敗] 無法抓取 {target_date_str or '今日'} 的賽事資訊。這通常是因為該日無賽事或尚未出排位。")
            return

        from data_scraper.special_stats import SpecialStatsScraper
        special_scraper = SpecialStatsScraper()
        draw_stats_by_race = await special_scraper.get_draw_stats(racedate=target_date_str)
        enable_speedpro_inline = str(os.environ.get("ENABLE_SPEEDPRO_INLINE_FETCH") or "").strip().lower() in ("1", "true", "yes")
        speedpro_scraper = None
        formguide_scraper = None
        if enable_speedpro_inline:
            from data_scraper.speedpro_energy import SpeedProEnergyScraper
            from data_scraper.speedpro_formguide import SpeedProFormGuideScraper
            speedpro_scraper = SpeedProEnergyScraper()
            formguide_scraper = SpeedProFormGuideScraper()
        
        # 將檔位統計儲存到 SystemConfig 以供計分使用
        if draw_stats_by_race:
            from database.models import SystemConfig
            config_key = f"draw_stats_{target_date_str}" if target_date_str else f"draw_stats_{datetime.now().strftime('%Y/%m/%d')}"
            config = session.query(SystemConfig).filter_by(key=config_key).first()
            if not config:
                config = SystemConfig(key=config_key, description=f"當日檔位統計 ({target_date_str or '最新'})")
                session.add(config)
            config.value = draw_stats_by_race
            session.commit()
            print(f">>> 已將當日檔位統計存入資料庫 ({config_key})")

        print(f">>> 成功發現 {len(races_info)} 場賽事，開始同步數據...")
        for race_info in races_info:
            hk_now = datetime.now(ZoneInfo("Asia/Hong_Kong"))
            base_date_str = target_date_str or hk_now.strftime("%Y/%m/%d")
            race_date = datetime.strptime(base_date_str, "%Y/%m/%d")
            venue = race_info.get("venue", "ST")
            race = repo.create_race(
                race_date, 
                venue, 
                race_info["race_no"],
                race_class=race_info.get("race_class", ""),
                distance=race_info.get("distance", 0),
                going=race_info.get("going", ""),
                track_type=race_info.get("track_type", ""),
                surface=race_info.get("surface", ""),
                course_type=race_info.get("course_type", ""),
            )

            try:
                from database.models import RaceTrackCondition

                going_raw, going_code = normalize_going(str(race_info.get("going", "") or ""))
                track_raw = str(race_info.get("track_type", "") or "").strip()
                tc = session.query(RaceTrackCondition).filter_by(race_id=int(race.id)).first()
                if not tc:
                    tc = RaceTrackCondition(race_id=int(race.id), source="HKJC_RACECARD")
                    session.add(tc)
                if going_raw and (not str(getattr(tc, "going_raw", "") or "").strip()):
                    tc.going_raw = going_raw
                    tc.going_code = str(going_code or going_raw)
                if track_raw and (not str(getattr(tc, "track_raw", "") or "").strip()):
                    tc.track_raw = track_raw
            except Exception:
                pass
            
            print(f">>> 正在同步場次 {race.race_no} ({venue} | {race.distance}m | {race.going}) 的馬匹數據...")
            for entry_data in race_info["entries"]:
                horse = repo.get_or_create_horse(entry_data["horse_code"], entry_data["horse_name"])
                jockey = repo.get_or_create_jockey(entry_data["jockey"])
                trainer = repo.get_or_create_trainer(entry_data["trainer"])
                
                from database.models import RaceEntry
                entry = session.query(RaceEntry).filter_by(
                    race_id=race.id, 
                    horse_id=horse.id
                ).first()
                
                if not entry:
                    entry = RaceEntry(
                        race_id=race.id,
                        horse_id=horse.id,
                        jockey_id=jockey.id,
                        trainer_id=trainer.id,
                        horse_no=entry_data["horse_no"],
                        draw=entry_data["draw"],
                        actual_weight=entry_data["actual_weight"],
                        rating=entry_data["rating"] # 存入評分
                    )
                    session.add(entry)
                    session.flush()
                
                # 同步賠率 (如果有)
                if entry_data.get("win_odds"):
                    repo.update_odds(entry.id, entry_data["win_odds"], 0.0, "Live")
            
            session.commit()
            print(f">>> 場次 {race.race_no} 數據同步完成，執行計分中...")
            if speedpro_scraper is not None:
                try:
                    from database.models import SystemConfig

                    sp = speedpro_scraper.scrape(int(race.race_no or 0))
                    if isinstance(sp, dict) and sp:
                        key = f"speedpro_energy:{int(race.id)}"
                        cfg = session.query(SystemConfig).filter_by(key=key).first()
                        if not cfg:
                            cfg = SystemConfig(key=key, description=f"SpeedPRO 能量分（race_id={int(race.id)}）")
                            session.add(cfg)
                        cfg.value = sp
                        try:
                            date_str = (race.race_date.date().strftime("%Y/%m/%d") if race.race_date else None)
                        except Exception:
                            date_str = None
                        if date_str and race.race_no:
                            key2 = f"speedpro_energy:{date_str}:{int(race.race_no)}"
                            cfg2 = session.query(SystemConfig).filter_by(key=key2).first()
                            if not cfg2:
                                cfg2 = SystemConfig(key=key2, description=f"SpeedPRO 能量分（racedate={date_str} R{int(race.race_no)}）")
                                session.add(cfg2)
                            cfg2.value = {str(int(k)): v for k, v in sp.items()}
                        session.commit()
                        print(f">>> 已同步 SpeedPRO 能量分：race_no={race.race_no} entries={len(sp)}")
                except Exception as e:
                    print(f">>> [警告] SpeedPRO 能量分同步失敗（race_no={race.race_no}）: {e}")
                    session.rollback()

            if formguide_scraper is not None:
                try:
                    from database.models import SystemConfig
                    fg = formguide_scraper.scrape(int(race.race_no or 0))
                    if isinstance(fg, dict) and fg:
                        try:
                            date_str = (race.race_date.date().strftime("%Y/%m/%d") if race.race_date else None)
                        except Exception:
                            date_str = None
                        if date_str and race.race_no:
                            key_fg = f"speedpro_formguide:{date_str}:{int(race.race_no)}"
                            cfg_fg = session.query(SystemConfig).filter_by(key=key_fg).first()
                            if not cfg_fg:
                                cfg_fg = SystemConfig(key=key_fg, description=f"SpeedPRO 賽績指引（racedate={date_str} R{int(race.race_no)}）")
                                session.add(cfg_fg)
                            cfg_fg.value = {str(int(k)): v for k, v in fg.items()}
                            session.commit()
                            print(f">>> 已同步 SpeedPRO 賽績指引：race_no={race.race_no} entries={len(fg)}")
                except Exception as e:
                    print(f">>> [警告] SpeedPRO 賽績指引同步失敗（race_no={race.race_no}）: {e}")
                    session.rollback()

            engine.score_race(race.id)
            
        race_date_str = target_date_str or datetime.now().strftime("%Y/%m/%d")
        try:
            print(f">>> 正在生成預測 Top5 快照（賽日 {race_date_str}）...")
            res = generate_prediction_top5_for_race_date(session, race_date_str)
            print(f">>> 完成：Top5 快照 races={res.get('races')} factor_rows={res.get('factor_rows')} preset_rows={res.get('preset_rows')}")
        except Exception as e:
            print(f">>> [警告] 生成 Top5 快照失敗: {e}")
            session.rollback()

        print(">>> 每日抓取與計分流程全部完成！")
        
    except Exception as e:
        print(f">>> [錯誤] 抓取流程發生崩潰: {e}")
        session.rollback()
    finally:
        # 修正變數名稱錯誤
        if 'scraper' in locals() and hasattr(scraper, 'stop'):
            scraper.stop()
        session.close()

if __name__ == "__main__":
    asyncio.run(run_daily_scraper())
