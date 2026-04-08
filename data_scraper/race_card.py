import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime
import time

class RaceCardScraper:
    """最終穩定版：使用 zh-hk 穩定路徑 + 垂直數據對位解析"""

    def __init__(self):
        # 對準你提供的穩定網址格式
        self.base_url = "https://racing.hkjc.com/zh-hk/local/information/racecard"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7"
        }

    def get_all_races_info(self, race_date: str = "") -> List[Dict[str, Any]]:
        """獲取當日所有場次，並自動探測 HV/ST"""
        formatted_date = race_date if race_date else datetime.now().strftime("%Y/%m/%d")
        
        # 1. 探測今日場地 (嘗試 HV 與 ST)
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
        # 2. 逐場抓取 (1-11 場)
        for i in range(1, 12):
            race_url = f"{self.base_url}?racedate={formatted_date}&Racecourse={venue}&RaceNo={i}"
            print(f">>> 正在同步第 {i} 場數據...")
            race_info = self.scrape_single_race_html(race_url, i, venue)
            
            if race_info and race_info.get("entries"):
                races.append(race_info)
            else:
                if i == 1: print(">>> [警告] 第一場無數據，請檢查網址或日期。")
                break
            time.sleep(1)
            
        return races

    def scrape_single_race_html(self, url: str, race_no: int, venue: str) -> Dict[str, Any]:
        """精確解析 HTML 表格數據"""
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'lxml')
            race_data = {"race_no": race_no, "venue": venue, "entries": []}
            
            # 尋找所有表格行
            rows = soup.find_all("tr")
            processed_horses = set()

            for row in rows:
                tds = row.find_all("td")
                if len(tds) < 8: continue
                
                # 特徵識別：馬號通常在第 1 格且為純數字
                h_no_raw = tds[0].get_text(strip=True)
                if not h_no_raw.isdigit(): continue
                
                # 提取馬名與編號
                # 新版結構：馬名可能在第 3 格，且包含 (G368) 這種編號
                name_cell = tds[2].get_text(separator=' ', strip=True)
                code_match = re.search(r"([A-Z]\d{3})", name_cell)
                if not code_match: continue
                
                horse_code = code_match.group(1)
                if horse_code in processed_horses: continue
                processed_horses.add(horse_code)
                
                # 清理馬名 (去掉編號)
                horse_name = re.sub(r"[\(\（].*?[\)\）]", "", name_cell).strip()
                
                entry = {
                    "horse_no": int(h_no_raw),
                    "horse_code": horse_code,
                    "horse_name": horse_name,
                    "jockey": tds[3].get_text(strip=True),
                    "trainer": tds[4].get_text(strip=True),
                    "actual_weight": 0,
                    "draw": 0
                }
                
                # 解析負磅 (通常在第 5 格)
                weight_raw = re.sub(r'\D', '', tds[5].get_text(strip=True))
                if weight_raw: entry["actual_weight"] = int(weight_raw)
                
                # 解析檔位 (通常在第 6 格)
                draw_raw = re.sub(r'\D', '', tds[6].get_text(strip=True))
                if draw_raw: entry["draw"] = int(draw_raw)
                
                race_data["entries"].append(entry)
            
            if race_data["entries"]:
                print(f"    - 成功抓取 {len(race_data['entries'])} 匹馬")
            return race_data
        except Exception as e:
            print(f"    - [錯誤] 解析異常: {e}")
            return {}

    def start(self): pass
    def stop(self): pass
