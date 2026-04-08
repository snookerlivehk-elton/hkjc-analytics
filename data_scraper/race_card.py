import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime
from utils.logger import logger

class RaceCardScraper:
    """最終穩定版：專為 zh-hk 本地資訊路徑設計"""

    def __init__(self):
        self.base_url = "https://racing.hkjc.com/zh-hk/local/information/racecard"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7"
        }

    def get_all_races_info(self, race_date: str = "") -> List[Dict[str, Any]]:
        """獲取當日所有場次，並自動探測場地"""
        races = []
        formatted_date = race_date if race_date else datetime.now().strftime("%Y/%m/%d")
        
        # 1. 初始探測：找出當日場地 (HV 或 ST)
        probe_url = f"{self.base_url}?racedate={formatted_date}"
        venue = "ST" # 預設
        try:
            print(f">>> 正在探測今日場地: {probe_url}")
            resp = requests.get(probe_url, headers=self.headers, timeout=15)
            if "跑馬地" in resp.text or "Happy Valley" in resp.text:
                venue = "HV"
            elif "沙田" in resp.text or "Sha Tin" in resp.text:
                venue = "ST"
            print(f">>> 探測完成：今日場地為 {venue}")
            
            # 2. 獲取場次數量
            soup = BeautifulSoup(resp.text, 'lxml')
            race_nos = set()
            for a in soup.select("a[href*='RaceNo=']"):
                m = re.search(r'RaceNo=(\d+)', a.get('href', ''))
                if m: race_nos.add(int(m.group(1)))
            
            race_count = max(race_nos) if race_nos else 9
            print(f">>> 偵測到 {race_count} 場賽事，開始抓取數據...")

            for i in range(1, race_count + 1):
                # 關鍵：URL 必須包含正確的 Racecourse 參數
                race_url = f"{self.base_url}?racedate={formatted_date}&Racecourse={venue}&RaceNo={i}"
                print(f">>> 正在抓取第 {i} 場: {race_url}")
                race_info = self.scrape_single_race(race_url, i, venue)
                if race_info and race_info.get("entries"):
                    races.append(race_info)
            
            return races
        except Exception as e:
            print(f">>> [錯誤] 抓取失敗: {e}")
            return []

    def scrape_single_race(self, url: str, race_no: int, venue: str) -> Dict[str, Any]:
        """抓取單場馬匹 (暴力掃描偵錯版)"""
        import time
        import random
        try:
            # 加入隨機延遲，防止被封鎖
            time.sleep(random.uniform(1.5, 3.5))
            
            resp = requests.get(url, headers=self.headers, timeout=15)
            html_content = resp.text
            soup = BeautifulSoup(html_content, 'lxml')
            
            race_data = {"race_no": race_no, "venue": venue, "entries": []}
            processed_codes = set()

            # --- 核心解析：暴力掃描模式 ---
            # 不看標籤，直接在整個 HTML 原始碼中找馬匹編號
            # 尋找格式如：HorseId=G368, HorseId\">G368, (G368) 等
            all_horse_codes = re.findall(r"HorseId=([A-Z]\d{3})", html_content, re.I)
            # 也找括號內的編號 (中文版特徵)
            all_horse_codes += re.findall(r"[\(\（]([A-Z]\d{3})[\)\）]", html_content)
            
            # 去重
            unique_codes = []
            for c in all_horse_codes:
                if c not in processed_codes:
                    unique_codes.append(c)
                    processed_codes.add(c)

            if not unique_codes:
                print(f"    - [警告] 第 {race_no} 場抓到 0 匹馬。網頁片段摘要：")
                print(f"      {html_content[:500]}...") # 印出前 500 字元供診斷
                return race_data

            # 根據抓到的編號，嘗試補齊馬名
            for code in unique_codes:
                # 在 HTML 中找編號附近的文字作為馬名
                # 簡單邏輯：尋找編號前 20 個字元內的中文
                name_match = re.search(r"([^\d\s\|]{2,6})\s*[\(\（]" + code, html_content)
                horse_name = name_match.group(1).strip() if name_match else f"馬匹 {code}"
                
                entry = {
                    "horse_no": len(race_data["entries"]) + 1,
                    "horse_code": code,
                    "horse_name": horse_name,
                    "jockey": "自動抓取", "trainer": "自動抓取", "draw": 0, "actual_weight": 0
                }
                race_data["entries"].append(entry)
            
            print(f"    - 第 {race_no} 場: 暴力掃描成功，抓取 {len(race_data['entries'])} 匹馬")
            return race_data
        except Exception as e:
            print(f"    - [錯誤] 第 {race_no} 場抓取崩潰: {e}")
            return {}

    def start(self): pass
    def stop(self): pass
