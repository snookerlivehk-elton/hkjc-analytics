import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime
import time
import random

class RaceCardScraper:
    """絕地求生版：不看 HTML 標籤，直接掃描文字流特徵"""

    def __init__(self):
        self.base_url = "https://racing.hkjc.com/racing/information/Chinese/Racing/RaceCard.aspx"
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://racing.hkjc.com/"
        }

    def get_all_races_info(self, race_date: str = "") -> List[Dict[str, Any]]:
        """獲取當日所有場次，自動識別場地"""
        date_str = race_date if race_date else datetime.now().strftime("%Y/%m/%d")
        print(f">>> 正在初始化連線: {date_str}")
        
        try:
            # 先嘗試獲取第一場以獲取 Cookie 和場次清單
            resp = self.session.get(f"{self.base_url}?RaceDate={date_str}&RaceNo=1", headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # 獲取場次數量
            race_nos = set()
            for a in soup.select("a[href*='RaceNo=']"):
                m = re.search(r'RaceNo=(\d+)', a.get('href', ''))
                if m: race_nos.add(int(m.group(1)))
            
            race_count = max(race_nos) if race_nos else 9
            print(f">>> 偵測到 {race_count} 場賽事，開始提取數據...")

            races = []
            for i in range(1, race_count + 1):
                url = f"{self.base_url}?RaceDate={date_str}&RaceNo={i}"
                print(f">>> 正在抓取第 {i} 場...")
                race_info = self.scrape_by_feature(url, i)
                if race_info and race_info.get("entries"):
                    races.append(race_info)
                time.sleep(random.uniform(1, 2))
            
            return races
        except Exception as e:
            print(f">>> [錯誤] 連線異常: {e}")
            return []

    def scrape_by_feature(self, url: str, race_no: int) -> Dict[str, Any]:
        """特徵解析法：直接從文字流提取數據"""
        try:
            resp = self.session.get(url, headers=self.headers, timeout=10)
            # 使用 BeautifulSoup 清理出純文字流
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # 1. 識別場地、班次、路程
            full_text = soup.get_text(separator=' ', strip=True)
            venue = "HV" if "跑馬地" in full_text else "ST"
            
            race_data = {
                "race_no": race_no, "venue": venue, 
                "race_class": "未知", "distance": 0, "going": "好地",
                "entries": []
            }
            
            # 提取班次路程
            class_m = re.search(r"(第[一二三四五]班|公開賽)", full_text)
            if class_m: race_data["race_class"] = class_m.group(1)
            dist_m = re.search(r"(\d{4})米", full_text)
            if dist_m: race_data["distance"] = int(dist_m.group(1))
            going_m = re.search(r"(好地|黏地|濕地|快地)", full_text)
            if going_m: race_data["going"] = going_m.group(1)

            # 2. 暴力掃描馬匹行
            # 我們找尋包含馬匹編號 (字母+3位數字) 的行
            rows = soup.find_all("tr")
            for row in rows:
                # 將整行轉為以 | 分割的文字，這能極大程度保留欄位感
                line = row.get_text(separator='|', strip=True)
                code_match = re.search(r"([A-Z]\d{3})", line)
                if not code_match: continue
                
                horse_code = code_match.group(1)
                parts = line.split('|')
                if len(parts) < 8: continue
                
                try:
                    # 這是馬會排位表的「物理規律」
                    # [0]是馬號, [2]或[3]是馬名, 緊接編號, 接著負磅, 騎師, 檔位, 練馬師, 評分
                    entry = {
                        "horse_no": 0,
                        "horse_code": horse_code,
                        "horse_name": "未知",
                        "jockey": "未知",
                        "trainer": "未知",
                        "draw": 0,
                        "actual_weight": 0,
                        "rating": 0
                    }
                    
                    # 遍歷分割出的片段，根據特徵填入
                    nums = []
                    for p in parts:
                        p = p.strip()
                        if p.isdigit(): nums.append(int(p))
                        # 識別馬名 (非數字、非編號、長度 2-4)
                        if not entry["horse_name"] != "未知" and 2 <= len(p) <= 4 and not any(c.isdigit() for c in p):
                            entry["horse_name"] = p
                    
                    # 從數字池中按邏輯提取
                    if nums:
                        entry["horse_no"] = nums[0]
                        for n in nums:
                            if 100 <= n <= 145: entry["actual_weight"] = n
                            elif 1 <= n <= 14 and entry["draw"] == 0 and n != entry["horse_no"]: entry["draw"] = n
                            elif 20 <= n <= 135 and entry["rating"] == 0: entry["rating"] = n
                    
                    # 提取騎師與練馬師 (通常在特定欄位附近)
                    # 這裡做一個保險：取馬名之後的兩個純文字片段
                    names = [p for p in parts if 2 <= len(p) <= 4 and not any(c.isdigit() for c in p) and p != entry["horse_name"]]
                    if len(names) >= 2:
                        entry["jockey"] = names[0]
                        entry["trainer"] = names[1]

                    if entry["horse_code"]:
                        race_data["entries"].append(entry)
                except:
                    continue
            
            if race_data["entries"]:
                print(f"    - 成功提取 {len(race_data['entries'])} 匹馬")
            return race_data
        except:
            return {}

    def start(self): pass
    def stop(self): pass
