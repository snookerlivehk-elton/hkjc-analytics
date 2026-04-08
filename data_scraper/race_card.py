import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime
import time
import random

class RaceCardScraper:
    """絕地求生版：對準最原始桌面版 .aspx 網址，不依賴任何 API"""

    def __init__(self):
        # 桌面版網址是目前最穩定的數據源
        self.base_url = "https://racing.hkjc.com/racing/information/Chinese/Racing/RaceCard.aspx"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8",
            "Referer": "https://racing.hkjc.com/racing/information/Chinese/Racing/RaceCard.aspx"
        }

    def get_all_races_info(self, race_date: str = "") -> List[Dict[str, Any]]:
        """獲取當日所有場次"""
        races = []
        # 日期格式 YYYY/MM/DD
        date_str = race_date if race_date else datetime.now().strftime("%Y/%m/%d")
        
        try:
            print(f">>> 正在連線至馬會桌面版頁面: {date_str}")
            # 1. 先探測第一場，確認今日是否有賽事
            url = f"{self.base_url}?RaceDate={date_str}&RaceNo=1"
            resp = requests.get(url, headers=self.headers, timeout=15)
            
            if "馬名" not in resp.text and "Horse" not in resp.text:
                print(">>> [警告] 頁面未發現馬匹表格，今日可能無賽事。")
                return []
            
            soup = BeautifulSoup(resp.text, 'lxml')
            # 獲取總場次
            race_nos = set()
            for a in soup.select("a[href*='RaceNo=']"):
                m = re.search(r'RaceNo=(\d+)', a.get('href', ''))
                if m: race_nos.add(int(m.group(1)))
            
            race_count = max(race_nos) if race_nos else 1
            print(f">>> 成功！偵測到 {race_count} 場賽事，開始同步數據...")

            for i in range(1, race_count + 1):
                print(f">>> 正在同步第 {i} 場數據...")
                race_url = f"{self.base_url}?RaceDate={date_str}&RaceNo={i}"
                race_info = self.scrape_single_race_html(race_url, i)
                if race_info and race_info.get("entries"):
                    races.append(race_info)
                time.sleep(random.uniform(1, 2)) # 模擬真人翻頁
            
            return races
        except Exception as e:
            print(f">>> [錯誤] 連線異常: {e}")
            return []

    def scrape_single_race_html(self, url: str, race_no: int) -> Dict[str, Any]:
        """從桌面版 HTML 中暴力提取數據"""
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            html = resp.text
            soup = BeautifulSoup(html, 'lxml')
            
            # 1. 識別場地 (HV/ST)
            venue = "HV" if "跑馬地" in html else "ST"
            
            race_data = {
                "race_no": race_no, "venue": venue, 
                "race_class": "未知", "distance": 0, "going": "好地",
                "entries": []
            }
            
            # 2. 暴力解析馬匹行
            # 桌面版特徵：馬匹編號在括號內，如 (G368)
            rows = soup.find_all("tr")
            processed_codes = set()

            for row in rows:
                text = row.get_text(separator='|', strip=True)
                # 搜尋馬匹編號特徵 (字母+3位數字)
                code_match = re.search(r"([A-Z]\d{3})", text)
                if not code_match: continue
                
                horse_code = code_match.group(1)
                if horse_code in processed_codes: continue
                processed_codes.add(horse_code)
                
                # 分割欄位
                parts = text.split('|')
                if len(parts) < 5: continue
                
                try:
                    entry = {
                        "horse_no": len(race_data["entries"]) + 1,
                        "horse_code": horse_code,
                        "horse_name": re.sub(r"[\(\（].*?[\)\）]", "", parts[2]).strip() if len(parts) > 2 else "未知",
                        "jockey": parts[3] if len(parts) > 3 else "未知",
                        "trainer": parts[4] if len(parts) > 4 else "未知",
                        "actual_weight": 0, "draw": 0, "rating": 0
                    }
                    
                    # 識別負磅、檔位、評分
                    nums = re.findall(r"\d+", text)
                    for n in nums:
                        v = int(n)
                        if 100 <= v <= 145: entry["actual_weight"] = v
                        elif 1 <= v <= 14 and entry["draw"] == 0: entry["draw"] = v
                        elif 20 <= v <= 130 and entry["rating"] == 0: entry["rating"] = v
                    
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
