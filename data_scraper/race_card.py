import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime
import time
import random

class RaceCardScraper:
    """終極穩定版：雙場地自動探測 + 寬鬆特徵解析"""

    def __init__(self):
        self.base_url = "https://racing.hkjc.com/zh-hk/local/information/racecard"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7"
        }

    def get_all_races_info(self, race_date: str = "") -> List[Dict[str, Any]]:
        """獲取當日所有場次 (雙場地自動嘗試)"""
        formatted_date = race_date if race_date else datetime.now().strftime("%Y/%m/%d")
        
        # 1. 嘗試找出今日到底是跑哪裡
        print(f">>> 正在探測今日賽事資訊 ({formatted_date})...")
        venue = self._detect_venue(formatted_date)
        print(f">>> 確定今日場地: {venue}")

        races = []
        # 2. 抓取場次 (通常 1-11 場)
        for i in range(1, 12):
            url = f"{self.base_url}?racedate={formatted_date}&Racecourse={venue}&RaceNo={i}"
            print(f">>> 正在同步第 {i} 場數據...")
            race_info = self.scrape_single_race(url, i, venue)
            
            if race_info and race_info.get("entries"):
                races.append(race_info)
            else:
                # 如果連第一場都抓不到，代表真的沒比賽了
                if i == 1: 
                    print(">>> [警告] 第一場無數據，今日可能無賽事。")
                    break
                # 如果是中間場次沒數據，可能是結束了
                break
        
        return races

    def _detect_venue(self, date_str: str) -> str:
        """透過嘗試連線來判斷場地"""
        for v in ["HV", "ST"]:
            url = f"{self.base_url}?racedate={date_str}&Racecourse={v}&RaceNo=1"
            try:
                resp = requests.get(url, headers=self.headers, timeout=10)
                # 只要網頁原始碼出現馬匹編號特徵 (如 G368)，就代表這個場地是對的
                if re.search(r"\([A-Z]\d{3}\)", resp.text):
                    return v
            except:
                continue
        return "HV" # 最終保底

    def scrape_single_race(self, url: str, race_no: int, venue: str) -> Dict[str, Any]:
        """抓取單場馬匹 (最寬鬆解析邏輯)"""
        try:
            time.sleep(random.uniform(0.5, 1.5))
            resp = requests.get(url, headers=self.headers, timeout=10)
            html = resp.text
            
            race_data = {"race_no": race_no, "venue": venue, "entries": []}
            
            # 終極正則：匹配馬名 + 編號 (不論中間夾雜什麼符號)
            # 格式：中文(2-6字) + ... + (字母+3位數字)
            matches = re.finditer(r"([^\d\s\<\>]{2,6})\s*[\(\（]([A-Z]\d{3})[\)\）]", html)
            
            processed_codes = set()
            for match in matches:
                name, code = match.group(1).strip(), match.group(2).strip()
                if "馬匹" in name or "編號" in name: continue
                if code in processed_codes: continue
                processed_codes.add(code)
                
                entry = {
                    "horse_no": len(race_data["entries"]) + 1,
                    "horse_code": code,
                    "horse_name": name,
                    "jockey": "自動獲取", "trainer": "自動獲取", "draw": 0, "actual_weight": 0
                }
                
                # 在 HTML 片段中找數字 (負磅與檔位)
                context = html[max(0, match.start()-100) : min(len(html), match.end()+300)]
                nums = re.findall(r">(\d+)<", context) # 只找被標籤包裹的純數字，準確率最高
                if not nums: nums = re.findall(r"\d+", context)
                
                for n in nums:
                    v = int(n)
                    if 100 <= v <= 145: entry["actual_weight"] = v
                    elif 1 <= v <= 14 and entry["draw"] == 0: entry["draw"] = v
                
                race_data["entries"].append(entry)
            
            if race_data["entries"]:
                print(f"    - 成功抓取 {len(race_data['entries'])} 匹馬")
            return race_data
        except:
            return {}

    def start(self): pass
    def stop(self): pass
