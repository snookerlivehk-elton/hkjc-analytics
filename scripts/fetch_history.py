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
from database.models import Horse, HorseHistory
from data_scraper.horse import HorseScraper
from utils.logger import logger
import re

def parse_hkjc_date(date_str: str):
    """支援多種 HKJC 日期格式 (DD/MM/YY 或 DD/MM/YYYY)"""
    for fmt in ("%d/%m/%y", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

async def backfill_horse_history():
    """為資料庫中的馬匹回填歷史往績"""
    print(">>> 正在初始化資料庫結構...")
    init_db()
    
    session = get_session()
    scraper = HorseScraper()
    
    # 1. 獲取所有需要回填的馬匹
    horses = session.query(Horse).all()
    print(f">>> 發現 {len(horses)} 匹馬需要處理...")

    for horse in horses:
        print(f">>> 正在抓取馬匹 {horse.name_ch} ({horse.code}) 的歷史往績...")
        
        # 抓取往績
        history_records = scraper.get_horse_past_performance(horse.code)
        
        if not history_records:
            print(f"    - [警告] 未能抓取到 {horse.code} 的往績")
            continue

        # 儲存到資料庫
        new_count = 0
        for rec in history_records:
            try:
                # 使用強化版的日期解析
                race_date = parse_hkjc_date(rec["date"])
                if not race_date:
                    continue

                existing = session.query(HorseHistory).filter_by(
                    horse_id=horse.id, 
                    race_date=race_date
                ).first()

                if not existing:
                    # 處理場地與距離 (例如: ST / 草地 / "C" / 1200)
                    venue_parts = rec["venue"].split("/")
                    dist = 0
                    if venue_parts:
                        dist_match = re.search(r"\d+", venue_parts[-1])
                        if dist_match: dist = int(dist_match.group())

                    hh = HorseHistory(
                        horse_id=horse.id,
                        race_date=race_date,
                        venue=rec["venue"],
                        race_class=rec["class"],
                        distance=dist,
                        rank=int(rec["rank"]) if rec["rank"].isdigit() else 0,
                        draw=int(rec["draw"]) if rec["draw"].isdigit() else 0,
                        jockey_name=rec["jockey"],
                        weight=int(re.sub(r'\D', '', rec["weight"])) if re.sub(r'\D', '', rec["weight"]) else 0,
                        rating=int(re.sub(r'\D', '', rec["rating"])) if re.sub(r'\D', '', rec["rating"]) else 0,
                        finish_time=rec["finish_time"]
                    )
                    session.add(hh)
                    new_count += 1
            except Exception as e:
                print(f"    - [錯誤] 儲存紀錄失敗: {e}")
                continue
        
        session.commit()
        print(f"    - 成功同步 {new_count} 筆新往績。")
        
        # 溫柔爬取
        await asyncio.sleep(1)

    print(">>> 歷史往績回填完成！")
    session.close()

if __name__ == "__main__":
    asyncio.run(backfill_horse_history())
