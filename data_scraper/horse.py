import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from utils.logger import logger

class HorseScraper:
    """抓取馬匹基本資料、往績、晨操與獸醫報告 (穩定 Requests 版)"""

    def __init__(self):
        self.base_url = "https://racing.hkjc.com/racing/information/Chinese/Horse/Horse.aspx"
        self.workout_url = "https://racing.hkjc.com/racing/information/Chinese/Horse/Workout.aspx"
        self.vet_url = "https://racing.hkjc.com/racing/information/Chinese/Horse/VetReport.aspx"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    def get_horse_past_performance(self, horse_code: str) -> List[Dict[str, Any]]:
        """獲取馬匹歷史往績 (用於 Last 6 Runs)"""
        url = f"{self.base_url}?HorseId={horse_code}"
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            history = []
            # 尋找往績表格 (通常在 class="big_table" 或特定 ID 下)
            # 注意：馬匹 Profile 頁面下方有詳細往績表
            tables = soup.select("table.big_table")
            if not tables: return []
            
            # 通常第一個 big_table 是基本資料，第二個或後續是往績
            for table in tables:
                rows = table.select("tr")
                if len(rows) < 2: continue
                header_text = rows[0].get_text()
                if "場次" in header_text and "名次" in header_text:
                    # 這是往績表
                    for row in rows[1:]:
                        cols = row.select("td")
                        if len(cols) < 10: continue
                        
                        try:
                            record = {
                                "race_index": cols[0].text.strip(), # 季內場次
                                "rank": cols[1].text.strip(),       # 名次
                                "date": cols[2].text.strip(),       # 日期
                                "venue": cols[3].text.strip(),      # 場地/路程/跑道
                                "class": cols[4].text.strip(),      # 班次
                                "draw": cols[5].text.strip(),       # 檔位
                                "jockey": cols[6].text.strip(),     # 騎師
                                "weight": cols[8].text.strip(),     # 負磅
                                "rating": cols[10].text.strip(),    # 評分
                                "finish_time": cols[12].text.strip() # 完成時間
                            }
                            history.append(record)
                        except:
                            continue
                    break # 找到往績表就跳出
            return history
        except Exception as e:
            logger.error(f"抓取馬匹 {horse_code} 往績失敗: {e}")
            return []

    def get_horse_profile(self, horse_code: str) -> Dict[str, Any]:
        """獲取馬匹基本資料 (烙印、父系、母系等)"""
        url = f"{self.base_url}?HorseId={horse_code}"
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            profile = {"code": horse_code, "name": ""}
            name_tag = soup.select_one(".horseName")
            if name_tag: profile["name"] = name_tag.text.strip()
            
            # 解析基本資料表格
            profile_table = soup.select_one(".horseProfile")
            if profile_table:
                tds = [td.text.strip() for td in profile_table.select("td")]
                profile["details"] = tds
                
            return profile
        except:
            return {}

    def start(self): pass
    def stop(self): pass
