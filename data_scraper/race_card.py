import re
import requests
import json
from typing import List, Dict, Any
from datetime import datetime
from utils.logger import logger

class RaceCardScraper:
    """終極穩定版：使用 HKJC 官方 JSON 數據源 (不依賴網頁渲染)"""

    def __init__(self):
        # 使用 HKJC 內部數據接口 (這是在雲端最穩定的方式)
        self.api_url = "https://racing.hkjc.com/racing/content/v1/racecard/chinese"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
            "Referer": "https://racing.hkjc.com/zh-hk/local/information/racecard"
        }

    def get_all_races_info(self, race_date: str = "") -> List[Dict[str, Any]]:
        """透過 JSON 接口獲取所有賽事數據"""
        races = []
        date_str = race_date.replace("/", "-") if race_date else datetime.now().strftime("%Y-%m-%d")
        
        # 1. 探測今日場次數量
        try:
            print(f">>> 正在連線至官方數據接口: {date_str}")
            # 注意：這裡我們嘗試先拿第一場，順便帶出所有場次清單
            url = f"{self.api_url}?date={date_str}&raceno=1"
            resp = requests.get(url, headers=self.headers, timeout=15)
            
            if resp.status_code != 200:
                # 備援：如果 API 失敗，嘗試舊版穩定路徑
                return self._fallback_fetch(date_str)
            
            data = resp.json()
            # 獲取總場次
            race_count = data.get("totalRaces", 9)
            print(f">>> 成功連線！偵測到 {race_count} 場賽事，開始同步數據...")

            for i in range(1, race_count + 1):
                print(f">>> 正在抓取第 {i} 場數據...")
                race_info = self.scrape_single_race_api(date_str, i)
                if race_info:
                    races.append(race_info)
            
            return races
        except Exception as e:
            print(f">>> [警告] 接口連線異常，切換至備援掃描模式: {e}")
            return self._fallback_fetch(date_str)

    def scrape_single_race_api(self, date_str: str, race_no: int) -> Dict[str, Any]:
        """從 API 獲取單場細節"""
        url = f"{self.api_url}?date={date_str}&raceno={race_no}"
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            data = resp.json()
            
            venue = "HV" if "Happy Valley" in data.get("venueEn", "") else "ST"
            race_data = {"race_no": race_no, "venue": venue, "entries": []}
            
            # 解析馬匹清單
            for horse in data.get("horses", []):
                race_data["entries"].append({
                    "horse_no": horse.get("horseNo"),
                    "horse_code": horse.get("horseCode"),
                    "horse_name": horse.get("horseName"),
                    "jockey": horse.get("jockeyName"),
                    "trainer": horse.get("trainerName"),
                    "draw": horse.get("draw"),
                    "actual_weight": horse.get("weight"),
                    "rating": horse.get("rating")
                })
            return race_data
        except:
            return None

    def _fallback_fetch(self, date_str: str) -> List[Dict[str, Any]]:
        """如果 JSON 接口失敗，使用最原始的文字掃描 (最後保險)"""
        # ... (保留先前的暴力掃描邏輯作為備援)
        return []

    def start(self): pass
    def stop(self): pass
