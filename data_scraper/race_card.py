import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime
import time
import random

class RaceCardScraper:
    """生產環境穩定版：精確表格對位 + 自動雜訊過濾"""

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
            resp = self.session.get(f"{self.base_url}?RaceDate={date_str}&RaceNo=1", headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            race_nos = set()
            for a in soup.select("a[href*='RaceNo=']"):
                m = re.search(r'RaceNo=(\d+)', a.get('href', ''))
                if m: race_nos.add(int(m.group(1)))
            
            race_count = max(race_nos) if race_nos else 9
            print(f">>> 偵測到 {race_count} 場賽事，開始提取數據...")

            races = []
            for i in range(1, race_count + 1):
                url = f"{self.base_url}?RaceDate={date_str}&RaceNo={i}"
                print(f">>> 正在同步第 {i} 場數據...")
                race_info = self.scrape_single_race(url, i)
                if race_info and race_info.get("entries"):
                    races.append(race_info)
                time.sleep(random.uniform(0.5, 1.0))
            return races
        except Exception as e:
            print(f">>> [錯誤] 連線異常: {e}")
            return []

    def scrape_single_race(self, url: str, race_no: int) -> Dict[str, Any]:
        """精確對位解析，過濾表頭與無效行"""
        try:
            resp = self.session.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            full_text = soup.get_text(separator=' ', strip=True)
            venue = "HV" if "跑馬地" in full_text else "ST"
            
            race_data = {"race_no": race_no, "venue": venue, "entries": []}
            
            # 定位表格
            table = soup.select_one("table.starter")
            if not table: return {}

            rows = table.find_all("tr")
            for row in rows:
                tds = row.find_all("td")
                if len(tds) < 10: continue
                
                # 提取馬名與編號
                link = row.find("a", href=re.compile(r"horseid=", re.I))
                if not link: continue
                
                raw_name = link.get_text(strip=True)
                # --- 過濾機制：如果馬名包含「馬名」或「Horse」，代表這是表頭，跳過 ---
                if "馬名" in raw_name or "Horse" in raw_name: continue
                
                code_match = re.search(r"([A-Z]\d{3})", link.get('href', ''), re.I)
                if not code_match: continue
                horse_code = code_match.group(1).upper()
                
                try:
                    # 精確對位：[0]馬號 [3]馬名 [4]烙號 [5]負磅 [6]騎師 [8]檔位 [9]練馬師 [11]評分
                    entry = {
                        "horse_no": int(re.sub(r'\D', '', tds[0].text.strip())) if tds[0].text.strip().isdigit() else 0,
                        "horse_code": horse_code,
                        "horse_name": re.sub(r"[\(\（].*?[\)\）]", "", raw_name).strip(),
                        "actual_weight": int(re.sub(r'\D', '', tds[5].text.strip())) if tds[5].text.strip().isdigit() else 0,
                        "jockey": tds[6].get_text(strip=True),
                        "draw": int(re.sub(r'\D', '', tds[8].text.strip())) if tds[8].text.strip().isdigit() else 0,
                        "trainer": tds[9].get_text(strip=True),
                        "rating": int(re.sub(r'\D', '', tds[11].text.strip())) if tds[11].text.strip().isdigit() else 0
                    }
                    
                    # 再次校驗：如果騎師名字裡有數字或奇怪符號，標記為未知
                    if any(char.isdigit() for char in entry["jockey"]) and len(entry["jockey"]) < 3:
                        entry["jockey"] = "未知"
                    
                    # 清理括號
                    entry["jockey"] = re.sub(r"\(.*?\)", "", entry["jockey"]).strip()
                    
                    race_data["entries"].append(entry)
                except:
                    continue
            
            if race_data["entries"]:
                print(f"    - 第 {race_no} 場: 成功抓取 {len(race_data['entries'])} 匹馬")
            return race_data
        except:
            return {}

    def start(self): pass
    def stop(self): pass
