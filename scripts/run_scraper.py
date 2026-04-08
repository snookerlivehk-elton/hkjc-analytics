import asyncio
import sys
from pathlib import Path
from datetime import datetime

# 將專案根目錄加入路徑
root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import get_session
from database.repository import RacingRepository
from data_scraper.race_card import RaceCardScraper
from data_scraper.odds import OddsScraper
from data_scraper.horse import HorseScraper
from utils.logger import logger

async def run_daily_scraper():
    """執行每日自動抓取流程"""
    session = get_session()
    repo = RacingRepository(session)
    
    race_card_scraper = RaceCardScraper()
    odds_scraper = OddsScraper()
    horse_scraper = HorseScraper()
    
    try:
        # 1. 啟動瀏覽器
        await race_card_scraper.start()
        
        # 2. 獲取當日排位表 (今日賽事)
        logger.info("開始抓取排位表...")
        races_info = await race_card_scraper.get_all_races_info()
        
        for race_info in races_info:
            # 建立賽事記錄
            race_date = datetime.now() # 這裡假設是今日
            race = repo.create_race(race_date, "ST", race_info["race_no"])
            
            for entry_data in race_info["entries"]:
                # 建立/獲取馬、騎、練
                horse = repo.get_or_create_horse(entry_data["horse_code"], entry_data["horse_name"])
                jockey = repo.get_or_create_jockey(entry_data["jockey"])
                trainer = repo.get_or_create_trainer(entry_data["trainer"])
                
                # 建立/獲取賽事排位 (Entry)
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
                        rating=entry_data["rating"],
                        gear=entry_data["gear"]
                    )
                    session.add(entry)
                    session.flush()
                
                # 抓取馬匹詳細資料 (如果需要)
                # profile = await horse_scraper.get_horse_profile(horse.code)
                # ...
                
            session.commit()
            logger.info(f"場次 {race_info['race_no']} 數據已同步至資料庫")

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
