import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime
import time

class RaceCardScraper:
    """最強韌版本：使用連結定位法，精確抓取馬名與各項數據"""

    def __init__(self):
        self.base_url = "https://racing.hkjc.com/zh-hk/local/information/racecard"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7"
        }

    def get_all_races_info(self, race_date: str = "") -> List[Dict[str, Any]]:
        """獲取當日所有場次，自動識別 HV/ST"""
        formatted_date = race_date if race_date else datetime.now().strftime("%Y/%m/%d")
        
        # 1. 探測今日場地
        print(f">>> 正在探測今日賽事場地 ({formatted_date})...")
        venue = "HV" # 預設
        for v in ["HV", "ST"]:
            url = f"{self.base_url}?racedate={formatted_date}&Racecourse={v}&RaceNo=1"
            resp = requests.get(url, headers=self.headers, timeout=10)
            if "馬名" in resp.text:
                venue = v
                break
        print(f">>> 確定今日場地為: {venue}")

        races = []
        for i in range(1, 12):
            race_url = f"{self.base_url}?racedate={formatted_date}&Racecourse={venue}&RaceNo={i}"
            print(f">>> 正在同步第 {i} 場數據...")
            race_info = self.scrape_single_race_precise(race_url, i, venue)
            
            if race_info and race_info.get("entries"):
                races.append(race_info)
            else:
                break
            time.sleep(1)
            
        return races

    def scrape_single_race_precise(self, url: str, race_no: int, venue: str) -> Dict[str, Any]:
        """精確解析 HTML：利用連結提取馬匹編號"""
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # 1. 解析 Header 資訊 (班次、路程、場況)
            header_text = soup.get_text(separator=' ', strip=True)
            race_class = "未知"; distance = 0; going = "好地"
            class_match = re.search(r"(第[一二三四五]班|公開賽|條件限制賽)", header_text)
            if class_match: race_class = class_match.group(1)
            dist_match = re.search(r"(\d{4})米", header_text)
            if dist_match: distance = int(dist_match.group(1))
            going_match = re.search(r"(好地|黏地|濕地|快地)", header_text)
            if going_match: going = going_match.group(1)

            race_data = {
                "race_no": race_no, "venue": venue, 
                "race_class": race_class, "distance": distance, "going": going,
                "entries": []
            }

            # 2. 核心解析：尋找所有馬名連結
            # 連結格式通常包含 HorseId=K416
            processed_codes = set()
            rows = soup.find_all("tr")
            for row in rows:
                tds = row.find_all("td")
                if len(tds) < 9: continue
                
                link = row.find("a", href=re.compile(r"[hH]orse[iI]d="))
                if not link: continue
                
                # 從連結提取編號
                code_match = re.search(r"horse[iI]d=([A-Z]\d{3})", link.get('href', ''), re.I)
                if not code_match: continue
                horse_code = code_match.group(1)
                
                if horse_code in processed_codes: continue
                processed_codes.add(horse_code)
                
                try:
                    # 垂直精確對位 (根據 HKJC 新版結構)
                    entry = {
                        "horse_no": int(re.sub(r'\D', '', tds[0].text.strip())) if tds[0].text.strip() else 0,
                        "horse_code": horse_code,
                        "horse_name": link.get_text(strip=True),
                        "jockey": tds[5].get_text(strip=True),
                        "trainer": tds[7].get_text(strip=True),
                        "draw": int(re.sub(r'\D', '', tds[6].text.strip())) if tds[6].text.strip().isdigit() else 0,
                        "actual_weight": int(re.sub(r'\D', '', tds[4].text.strip())) if tds[4].text.strip().isdigit() else 0,
                        "rating": int(re.sub(r'\D', '', tds[8].text.strip())) if tds[8].text.strip().isdigit() else 0
                    }
                    
                    # 清理騎師名 (拿掉減磅數字)
                    entry["jockey"] = re.sub(r"\s*\(.*?\)", "", entry["jockey"]).strip()
                    
                    race_data["entries"].append(entry)
                except:
                    continue
            
            if race_data["entries"]:
                print(f"    - 成功抓取 {len(race_data['entries'])} 匹馬")
            return race_data
        except Exception as e:
            print(f"    - [錯誤] 第 {race_no} 場解析崩潰: {e}")
            return {}

    def start(self): pass
    def stop(self): pass
