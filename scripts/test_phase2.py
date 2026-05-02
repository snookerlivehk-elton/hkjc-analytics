import asyncio
import sys
from pathlib import Path
from datetime import datetime

# 加入專案路徑
root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from data_scraper.race_card import RaceCardScraper
from data_scraper.results import ResultsScraper
from utils.logger import logger

async def test_scrapers():
    """驗證階段 2 的抓取能力"""
    logger.info("=== 開始階段 2 驗證測試 ===")
    
    # 1. 初始化資料庫 (確保測試環境乾淨)
    init_db()
    
    # 2. 測試排位表抓取 (RaceCard)
    logger.info("測試 1: 抓取最新排位表...")
    card_scraper = RaceCardScraper()
    await card_scraper.start()
    
    try:
        # 獲取最近一場賽事資料 (不指定日期則抓取最新)
        races = await card_scraper.get_all_races_info()
        
        if not races:
            logger.error("❌ 無法獲取排位表數據，請檢查 HKJC 網站是否更換結構或今日無賽事")
        else:
            logger.info(f"✅ 成功獲取 {len(races)} 場賽事資訊")
            # 檢查第一場的第一匹馬
            first_race = races[0]
            if first_race['entries']:
                horse = first_race['entries'][0]
                logger.info(f"範例數據 (第1場): 馬名={horse['horse_name']}, 編號={horse['horse_code']}, 檔位={horse['draw']}")
            else:
                logger.warning("⚠️ 賽事資訊存在但無馬匹排位")

        # 3. 測試歷史賽果抓取 (Results)
        # 我們測試一個近期的日期，例如 2026/04/01 (根據賽曆有比賽)
        test_date = "2026/04/01"
        logger.info(f"測試 2: 抓取歷史賽果 ({test_date})...")
        res_scraper = ResultsScraper()
        # 修正：必須呼叫 start() 以初始化瀏覽器頁面
        await res_scraper.start()
        results = await res_scraper.get_results_by_date(test_date)
        
        if not results:
            logger.warning(f"⚠️ 日期 {test_date} 無結果，這可能是因為該日無賽事")
        else:
            logger.info(f"✅ 成功獲取 {len(results)} 場賽果")
            if results[0]['results']:
                first_res = results[0]['results'][0]
                logger.info(f"範例數據 (賽果): 名次={first_res['rank']}, 馬名={first_res['horse_name']}, 時間={first_res['finish_time']}")
        
        await res_scraper.stop()

    except Exception as e:
        logger.error(f"❌ 測試過程發生崩潰: {e}")
    finally:
        await card_scraper.stop()
        logger.info("=== 驗證測試結束 ===")

if __name__ == "__main__":
    asyncio.run(test_scrapers())
