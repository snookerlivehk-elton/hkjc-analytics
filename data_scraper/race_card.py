import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from utils.logger import logger

class RaceCardScraper:
    """純 Requests 版抓取器：不依賴瀏覽器，保證雲端穩定度"""

    def __init__(self):
        self.base_url = "https://racing.hkjc.com/racing/information/Chinese/Racing/RaceCard.aspx"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7"
        }

    def get_all_races_info(self, race_date: str = "") -> List[Dict[str, Any]]:
        """獲取當日所有場次"""
        races = []
        url = self.base_url
        if race_date: url += f"?RaceDate={race_date}"
        
        try:
            print(f">>> 正在連線至 HKJC: {url}")
            resp = requests.get(url, headers=self.headers, timeout=15)
            if resp.status_code != 200:
                print(f">>> [失敗] HKJC 伺服器回傳錯誤代碼: {resp.status_code}")
                return []
            
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # 獲取場次數量
            race_nos = set()
            for a in soup.select("a[href*='RaceNo=']"):
                m = re.search(r'RaceNo=(\d+)', a.get('href', ''))
                if m: race_nos.add(int(m.group(1)))
            
            race_count = max(race_nos) if race_nos else 1
            print(f">>> 偵測到 {race_count} 場賽事，開始解析...")

            for i in range(1, race_count + 1):
                race_url = f"{self.base_url}?RaceNo={i}"
                if race_date: race_url += f"&RaceDate={race_date}"
                
                print(f">>> 正在抓取第 {i} 場...")
                race_info = self.scrape_single_race(race_url, i)
                if race_info and race_info.get("entries"):
                    races.append(race_info)
            
            return races
        except Exception as e:
            print(f">>> [錯誤] 網路連線異常: {e}")
            return []

    def scrape_single_race(self, url: str, race_no: int) -> Dict[str, Any]:
        """抓取單場馬匹 (純文字流解析)"""
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # 識別場地
            page_text = soup.get_text(separator=' ', strip=True)
            venue = "HV" if "跑馬地" in page_text or "Happy Valley" in page_text else "ST"
            
            race_data = {"race_no": race_no, "venue": venue, "entries": []}
            
            # 終極解析：直接掃描全網頁文字中的馬匹編號 (如 G368)
            all_text = soup.get_text(separator='|', strip=True)
            matches = list(re.finditer(r"([^\d\s\|]{2,6})\s*[\(\（]([A-Z]\d{3})[\)\）]", all_text))
            
            processed_codes = set()
            for match in matches:
                horse_name = match.group(1).strip()
                horse_code = match.group(2).strip()
                
                if horse_code in processed_codes: continue
                processed_codes.add(horse_code)
                
                entry = {
                    "horse_no": len(race_data["entries"]) + 1,
                    "horse_code": horse_code,
                    "horse_name": horse_name,
                    "jockey": "自動抓取", "trainer": "自動抓取", "draw": 0, "actual_weight": 0
                }
                
                # 簡單提取附近數字
                context = all_text[max(0, match.start()-20) : min(len(all_text), match.end()+100)]
                nums = re.findall(r"\d+", context)
                for n in nums:
                    v = int(n)
                    if 100 <= v <= 145: entry["actual_weight"] = v
                    elif 1 <= v <= 14 and entry["draw"] == 0: entry["draw"] = v
                
                race_data["entries"].append(entry)
            
            print(f"    - 第 {race_no} 場: 成功抓取 {len(race_data['entries'])} 匹馬")
            return race_data
        except:
            return {}

    def start(self): pass # 保持接口相容
    def stop(self): pass  # 保持接口相容
