import asyncio
import sys
import os
from pathlib import Path
from datetime import datetime

# 將專案根目錄加入路徑
root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import get_session, init_db
from database.repository import RacingRepository
from data_scraper.race_card import RaceCardScraper
from data_scraper.odds import OddsScraper
from data_scraper.horse import HorseScraper
from scoring_engine.core import ScoringEngine
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
        races_info = scraper.get_all_races_info()
        
        if not races_info:
            print(">>> [嘗試重試] 初始抓取無資料，正在嘗試備用路徑...")
            today_str = datetime.now().strftime("%Y/%m/%d")
            races_info = scraper.get_all_races_info(race_date=today_str)

        if not races_info:
            print(">>> [失敗] 仍無法抓取賽事資訊。這通常是因為 HKJC 封鎖了伺服器 IP 或今日確實無賽事。")
            return

        print(f">>> 成功發現 {len(races_info)} 場賽事，開始同步數據...")
        for race_info in races_info:
            race_date = datetime.now()
            venue = "HV" if "跑馬地" in race_info.get("header", "") else "ST"
            race = repo.create_race(race_date, venue, race_info["race_no"])
            
            print(f">>> 正在同步場次 {race.race_no} ({venue}) 的馬匹數據...")
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
                        actual_weight=entry_data["actual_weight"]
                    )
                    session.add(entry)
            
            session.commit()
            print(f">>> 場次 {race.race_no} 數據同步完成，執行計分中...")
            engine.score_race(race.id)
            
        print(">>> 每日抓取與計分流程全部完成！")
        
    except Exception as e:
        print(f">>> [錯誤] 抓取流程發生崩潰: {e}")
        session.rollback()
    finally:
        await race_card_scraper.stop()
        session.close()

if __name__ == "__main__":
    asyncio.run(run_daily_scraper())
