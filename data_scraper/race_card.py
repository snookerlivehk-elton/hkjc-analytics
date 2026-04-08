import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime
import time
import random

class RaceCardScraper:
    """終極強韌版：使用 Session 維持 Cookie 並支援自動場地切換"""

    def __init__(self):
        self.base_url = "https://racing.hkjc.com/racing/information/Chinese/Racing/RaceCard.aspx"
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://racing.hkjc.com/racing/information/Chinese/Racing/RaceCard.aspx"
        }

    def get_all_races_info(self, race_date: str = "") -> List[Dict[str, Any]]:
        """獲取當日所有場次，自動探測場地"""
        date_str = race_date if race_date else datetime.now().strftime("%Y/%m/%d")
        
        # 1. 建立 Session 並存取首頁以獲取 Cookie
        print(f">>> 正在初始化連線: {date_str}")
        try:
            init_url = f"{self.base_url}?RaceDate={date_str}&RaceNo=1"
            resp = self.session.get(init_url, headers=self.headers, timeout=15)
            
            # 2. 探測場次數量
            soup = BeautifulSoup(resp.text, 'lxml')
            race_tabs = soup.select(".race_tab_active, .race_tab_inactive, a[href*='RaceNo=']")
            race_nos = set()
            for tab in race_tabs:
                m = re.search(r'RaceNo=(\d+)', str(tab))
                if m: race_nos.add(int(m.group(1)))
            
            race_count = max(race_nos) if race_nos else 1
            print(f">>> 偵測到 {race_count} 場賽事，開始解析數據...")

            races = []
            for i in range(1, race_count + 1):
                race_url = f"{self.base_url}?RaceDate={date_str}&RaceNo={i}"
                print(f">>> 正在同步第 {i} 場...")
                race_info = self.scrape_single_race_html(race_url, i)
                if race_info and race_info.get("entries"):
                    races.append(race_info)
                time.sleep(random.uniform(1, 2))
            
            return races
        except Exception as e:
            print(f">>> [錯誤] 抓取流程中斷: {e}")
            return []

    def scrape_single_race_html(self, url: str, race_no: int) -> Dict[str, Any]:
        """精確解析 HTML 表格"""
        try:
            resp = self.session.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # 1. 解析 Header (對應截圖紅框)
            full_text = soup.get_text(separator=' ', strip=True)
            venue = "HV" if "跑馬地" in full_text else "ST"
            
            race_class = "未知"; distance = 0; going = "好地"
            # 班次
            class_match = re.search(r"(第[一二三四五]班|公開賽)", full_text)
            if class_match: race_class = class_match.group(1)
            # 路程
            dist_match = re.search(r"(\d{4})米", full_text)
            if dist_match: distance = int(dist_match.group(1))
            # 場況
            going_match = re.search(r"(好地|黏地|濕地|快地)", full_text)
            if going_match: going = going_match.group(1)

            race_data = {
                "race_no": race_no, "venue": venue, 
                "race_class": race_class, "distance": distance, "going": going,
                "entries": []
            }

            # 2. 解析表格
            table = soup.select_one("table.table_border_hide")
            if not table:
                for t in soup.find_all("table"):
                    if "馬名" in t.get_text():
                        table = t
                        break
            
            if not table: return {}

            rows = table.find_all("tr")
            processed_codes = set()
            for row in rows:
                tds = row.find_all("td")
                if len(tds) < 10: continue
                
                link = row.find("a", href=re.compile(r"HorseId="))
                if not link: continue
                
                code_match = re.search(r"HorseId=([A-Z]\d{3})", link.get('href', ''), re.I)
                if not code_match: continue
                horse_code = code_match.group(1)
                
                if horse_code in processed_codes: continue
                processed_codes.add(horse_code)
                
                try:
                    entry = {
                        "horse_no": int(re.sub(r'\D', '', tds[0].text.strip())) if tds[0].text.strip().isdigit() else 0,
                        "horse_code": horse_code,
                        "horse_name": re.sub(r"[\(\（].*?[\)\）]", "", link.get_text(strip=True)).strip(),
                        "actual_weight": int(re.sub(r'\D', '', tds[4].text.strip())) if tds[4].text.strip() else 0,
                        "jockey": tds[5].get_text(strip=True),
                        "draw": int(re.sub(r'\D', '', tds[6].text.strip())) if tds[6].text.strip() else 0,
                        "trainer": tds[7].get_text(strip=True),
                        "rating": int(re.sub(r'\D', '', tds[8].text.strip())) if tds[8].text.strip() else 0
                    }
                    entry["jockey"] = re.sub(r"\(.*?\)", "", entry["jockey"]).strip()
                    race_data["entries"].append(entry)
                except:
                    continue
            
            if race_data["entries"]:
                print(f"    - 成功抓取 {len(race_data['entries'])} 匹馬")
            return race_data
        except:
            return {}

    def start(self): pass
    def stop(self): pass
