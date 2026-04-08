import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime
from utils.logger import logger

class RaceCardScraper:
    """穩定版抓取器：使用最新 zh-hk 本地資訊路徑"""

    def __init__(self):
        # 使用你提供的穩定網址
        self.base_url = "https://racing.hkjc.com/zh-hk/local/information/racecard"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7"
        }

    def get_all_races_info(self, race_date: str = "") -> List[Dict[str, Any]]:
        """獲取當日所有場次"""
        races = []
        # 格式化日期：將 YYYY/MM/DD 轉換為 YYYY/MM/DD
        formatted_date = race_date if race_date else datetime.now().strftime("%Y/%m/%d")
        
        # 初始探測第一場
        url = f"{self.base_url}?racedate={formatted_date}&RaceNo=1"
        
        try:
            print(f">>> 正在連線至新版 HKJC: {url}")
            resp = requests.get(url, headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # 獲取場次數量
            race_nos = set()
            for a in soup.select("a[href*='RaceNo=']"):
                m = re.search(r'RaceNo=(\d+)', a.get('href', ''))
                if m: race_nos.add(int(m.group(1)))
            
            race_count = max(race_nos) if race_nos else 1
            print(f">>> 偵測到 {race_count} 場賽事，開始同步...")

            for i in range(1, race_count + 1):
                race_url = f"{self.base_url}?racedate={formatted_date}&RaceNo={i}"
                print(f">>> 正在抓取第 {i} 場...")
                race_info = self.scrape_single_race(race_url, i)
                if race_info and race_info.get("entries"):
                    races.append(race_info)
            
            return races
        except Exception as e:
            print(f">>> [錯誤] 連線異常: {e}")
            return []

    def scrape_single_race(self, url: str, race_no: int) -> Dict[str, Any]:
        """抓取單場馬匹"""
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # 識別場地
            page_text = soup.get_text(separator=' ', strip=True)
            venue = "HV" if "跑馬地" in page_text or "Happy Valley" in page_text else "ST"
            
            race_data = {"race_no": race_no, "venue": venue, "entries": []}
            processed_codes = set()

            # 解析馬匹 (新版通常在 table 結構中)
            all_text = soup.get_text(separator='|', strip=True)
            matches = list(re.finditer(r"([^\d\s\|]{2,6})\s*[\(\（]([A-Z]\d{3})[\)\）]", all_text))
            
            for match in matches:
                name, code = match.group(1).strip(), match.group(2).strip()
                if len(name) > 6 or "編號" in name: continue
                if code in processed_codes: continue
                processed_codes.add(code)
                
                entry = {
                    "horse_no": len(race_data["entries"]) + 1,
                    "horse_code": code, "horse_name": name,
                    "jockey": "自動抓取", "trainer": "自動抓取", "draw": 0, "actual_weight": 0
                }
                
                # 掃描上下文數字
                context = all_text[max(0, match.start()-20) : min(len(all_text), match.end()+100)]
                nums = re.findall(r"\d+", context)
                for n in nums:
                    v = int(n)
                    if 100 <= v <= 145: entry["actual_weight"] = v
                    elif 1 <= v <= 14 and entry["draw"] == 0: entry["draw"] = v
                
                race_data["entries"].append(entry)
            
            print(f"    - 第 {race_no} 場 ({venue}): 成功抓取 {len(race_data['entries'])} 匹馬")
            return race_data
        except:
            return {}

    def start(self): pass
    def stop(self): pass
