import asyncio
import sys
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
    print(">>> 正在初始化資料庫...")
    init_db()
    
    session = get_session()
    repo = RacingRepository(session)
    engine = ScoringEngine(session)
    
    print(">>> 正在啟動爬蟲...")
    race_card_scraper = RaceCardScraper()
    
    try:
        await race_card_scraper.start()
        print(">>> 瀏覽器啟動成功，開始抓取排位表...")
        races_info = await race_card_scraper.get_all_races_info()
        
        for race_info in races_info:
            # 建立賽事記錄 (修正場地邏輯)
            race_date = datetime.now()
            venue = "HV" if "跑馬地" in race_info.get("header", "") else "ST"
            race = repo.create_race(race_date, venue, race_info["race_no"])
            
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
            print(f">>> 場次 {race.race_no} 數據抓取完成，正在執行計分排名...")
            engine.score_race(race.id) # 立即執行計分
            logger.info(f"場次 {race.race_no} 計分完成")

        # 3. 抓取即時賠率 (分開處理以保持更新)
        logger.info("開始更新即時賠率...")
        # 賠率抓取邏輯 (這裡簡化，實務上需與 Entry ID 對接)
        # ...
        
    except Exception as e:
        logger.error(f"抓取流程發生錯誤: {e}")
        session.rollback()
    finally:
        await race_card_scraper.stop()
        session.close()

if __name__ == "__main__":
    asyncio.run(run_daily_scraper())
