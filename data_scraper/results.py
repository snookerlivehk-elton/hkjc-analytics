import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime
from utils.logger import logger

class ResultsScraper:
    """穩定版賽果抓取器：使用最新 zh-hk 賽果路徑"""

    def __init__(self):
        self.base_url = "https://racing.hkjc.com/zh-hk/local/racing/results"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7"
        }

    def get_results_by_date(self, race_date: str) -> List[Dict[str, Any]]:
        """獲取指定日期的所有賽果"""
        results = []
        formatted_date = race_date if race_date else datetime.now().strftime("%Y/%m/%d")
        url = f"{self.base_url}?racedate={formatted_date}&RaceNo=1"
        
        try:
            print(f">>> 正在連線至歷史賽果: {url}")
            resp = requests.get(url, headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # 獲取場次數量
            race_nos = set()
            for a in soup.select("a[href*='RaceNo=']"):
                m = re.search(r'RaceNo=(\d+)', a.get('href', ''))
                if m: race_nos.add(int(m.group(1)))
            
            race_count = max(race_nos) if race_nos else 1
            print(f">>> 偵測到 {race_count} 場歷史賽果，開始同步...")

            for i in range(1, race_count + 1):
                race_url = f"{self.base_url}?racedate={formatted_date}&RaceNo={i}"
                print(f">>> 正在抓取第 {i} 場賽果...")
                race_res = self.scrape_single_race_result(race_url, i, formatted_date)
                if race_res and race_res.get("results"):
                    results.append(race_res)
            
            return results
        except Exception as e:
            print(f">>> [錯誤] 賽果連線異常: {e}")
            return []

    def scrape_single_race_result(self, url: str, race_no: int, race_date: str) -> Dict[str, Any]:
        """抓取單場歷史賽果與分段時間"""
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # 識別場地與場況
            page_text = soup.get_text(separator=' ', strip=True)
            going = "好地" # 預設
            going_match = re.search(r"場地狀況\s*:\s*(\w+)", page_text)
            if going_match: going = going_match.group(1)
            
            race_data = {"race_date": race_date, "race_no": race_no, "going": going, "results": []}
            
            # 解析馬匹名次 (新版通常在 .performance 或 table 中)
            # 這裡沿用強韌的連結掃描 + 上下文解析
            all_text = soup.get_text(separator='|', strip=True)
            matches = list(re.finditer(r"([^\d\s\|]{2,6})\s*[\(\（]([A-Z]\d{3})[\)\）]", all_text))
            
            for match in matches:
                name, code = match.group(1).strip(), match.group(2).strip()
                if len(name) > 6 or "編號" in name: continue
                
                res = {
                    "rank": 0, "horse_code": code, "horse_name": name,
                    "finish_time": "", "win_odds": 0.0, "sectional_times": []
                }
                
                # 掃描上下文獲取名次與時間
                context = all_text[max(0, match.start()-50) : min(len(all_text), match.end()+150)]
                # 名次通常在名字前
                rank_match = re.search(r"(\d+)\|" + re.escape(name), context)
                if rank_match: res["rank"] = int(rank_match.group(1))
                
                # 時間格式 1:23.45
                time_match = re.search(r"(\d:\d{2}\.\d{2})", context)
                if time_match: res["finish_time"] = time_match.group(1)
                
                race_data["results"].append(res)
            
            return race_data
        except:
            return {}

    def start(self): pass
    def stop(self): pass
