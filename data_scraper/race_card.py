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
        """抓取單場馬匹 (連結屬性解析法)"""
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            race_data = {"race_no": race_no, "venue": venue, "entries": []}
            processed_codes = set()

            # 尋找所有包含馬匹編號的連結 (例如 href 包含 horse_id=H123)
            # 在新版網頁，這通常是最穩定的標誌
            links = soup.find_all("a", href=True)
            for link in links:
                href = link.get('href', '')
                # 匹配連結中的編號，如 horse_id=H123 或 HorseId=H123
                code_match = re.search(r"[hH]orse[iI]d=([A-Z]\d{3})", href)
                if not code_match: continue
                
                horse_code = code_match.group(1)
                if horse_code in processed_codes: continue
                processed_codes.add(horse_code)
                
                # 提取馬名 (過濾掉括號)
                raw_name = link.get_text(strip=True)
                horse_name = re.sub(r"[\(\（].*?[\)\）]", "", raw_name).strip()
                if not horse_name or len(horse_name) > 6: continue
                
                entry = {
                    "horse_no": len(race_data["entries"]) + 1,
                    "horse_code": horse_code,
                    "horse_name": horse_name,
                    "jockey": "自動抓取", "trainer": "自動抓取", "draw": 0, "actual_weight": 0
                }
                
                # 向上找表格行 TR，提取馬號、騎師、練馬師、負磅、檔位
                row = link.find_parent("tr")
                if row:
                    tds = row.find_all("td")
                    if len(tds) > 5:
                        # 馬號通常在第一格
                        h_no_text = tds[0].get_text(strip=True)
                        if h_no_text.isdigit(): entry["horse_no"] = int(h_no_text)
                        
                        # 騎練通常在固定位置 (根據 WebFetch 結果)
                        entry["jockey"] = tds[3].get_text(strip=True)
                        entry["trainer"] = tds[4].get_text(strip=True)
                        
                        # 掃描整行數字識別負磅與檔位
                        nums = re.findall(r"\d+", row.get_text())
                        for n in nums:
                            v = int(n)
                            if 100 <= v <= 145: entry["actual_weight"] = v
                            elif 1 <= v <= 14 and entry["draw"] == 0 and v != entry["horse_no"]: entry["draw"] = v

                race_data["entries"].append(entry)
            
            print(f"    - 第 {race_no} 場: 成功抓取 {len(race_data['entries'])} 匹馬")
            return race_data
        except Exception as e:
            print(f"    - 抓取異常: {e}")
            return {}

    def start(self): pass
    def stop(self): pass
