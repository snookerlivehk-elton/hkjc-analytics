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
                    hh = HorseHistory(
                        horse_id=horse.id,
                        race_date=race_date,
                        venue=rec.get("venue", ""),
                        race_class=rec.get("race_class", ""),
                        distance=int(rec.get("distance", 0)) if str(rec.get("distance", "")).isdigit() else 0,
                        rank=int(str(rec.get("rank", "0"))) if str(rec.get("rank", "")).isdigit() else 0,
                        draw=int(str(rec.get("draw", "0"))) if str(rec.get("draw", "")).isdigit() else 0,
                        jockey_name=rec.get("jockey", ""),
                        trainer_name=rec.get("trainer", ""),
                        weight=int(re.sub(r'\D', '', str(rec.get("weight", "")))) if re.sub(r'\D', '', str(rec.get("weight", ""))) else 0,
                        rating=int(re.sub(r'\D', '', str(rec.get("rating", "")))) if re.sub(r'\D', '', str(rec.get("rating", ""))) else 0,
                        finish_time=rec.get("finish_time", "")
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
