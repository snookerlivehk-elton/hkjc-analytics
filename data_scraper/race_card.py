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
        """精確表格對位解析：徹底解決騎練錯位問題"""
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # 1. 識別場地
            venue = "HV" if "跑馬地" in resp.text else "ST"
            race_data = {"race_no": race_no, "venue": venue, "entries": []}
            
            # 2. 定位排位表
            # 桌面版通常在 class="table_border_hide" 的表格中
            table = soup.select_one("table.table_border_hide")
            if not table:
                # 備用方案：尋找包含「馬名」文字的表格
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
                
                # 尋找馬名連結與 HorseId
                link = row.find("a", href=re.compile(r"HorseId="))
                if not link: continue
                
                code_match = re.search(r"HorseId=([A-Z]\d{3})", link.get('href', ''), re.I)
                if not code_match: continue
                horse_code = code_match.group(1)
                
                if horse_code in processed_codes: continue
                processed_codes.add(horse_code)
                
                try:
                    # --- 根據馬會桌面版標準索引對位 ---
                    # [0]馬號 [1]6次近績 [2]綵衣 [3]馬名 [4]負磅 [5]騎師 [6]檔位 [7]練馬師 [8]評分
                    entry = {
                        "horse_no": int(re.sub(r'\D', '', tds[0].text.strip())) if tds[0].text.strip().isdigit() else 0,
                        "horse_code": horse_code,
                        "horse_name": re.sub(r"[\(\（].*?[\)\）]", "", link.get_text(strip=True)).strip(),
                        "actual_weight": int(re.sub(r'\D', '', tds[4].text.strip())) if tds[4].text.strip().isdigit() else 0,
                        "jockey": tds[5].get_text(strip=True),
                        "draw": int(re.sub(r'\D', '', tds[6].text.strip())) if tds[6].text.strip().isdigit() else 0,
                        "trainer": tds[7].get_text(strip=True),
                        "rating": int(re.sub(r'\D', '', tds[8].text.strip())) if tds[8].text.strip().isdigit() else 0
                    }
                    
                    # 清理騎師名稱 (移除減磅括號如 -2)
                    entry["jockey"] = re.sub(r"\(.*?\)", "", entry["jockey"]).strip()
                    
                    race_data["entries"].append(entry)
                except Exception as e:
                    continue
            
            if race_data["entries"]:
                print(f"    - 第 {race_no} 場: 成功抓取 {len(race_data['entries'])} 匹馬 (精確對位)")
            return race_data
        except:
            return {}

    def start(self): pass
    def stop(self): pass
