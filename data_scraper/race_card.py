import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime
import time
import random

class RaceCardScraper:
    """終極強韌版：使用 Session 維持與暴力特徵解析"""

    def __init__(self):
        self.base_url = "https://racing.hkjc.com/zh-hk/local/information/racecard"
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://racing.hkjc.com/zh-hk/local/information/racecard",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }

    def get_all_races_info(self, race_date: str = "") -> List[Dict[str, Any]]:
        """獲取當日所有場次，並自動識別場地"""
        formatted_date = race_date if race_date else datetime.now().strftime("%Y/%m/%d")
        
        # 1. 探測場地 (嘗試 HV 和 ST)
        print(f">>> 正在探測今日賽事場地 ({formatted_date})...")
        venue = "ST" 
        for v in ["HV", "ST"]:
            url = f"{self.base_url}?racedate={formatted_date}&Racecourse={v}&RaceNo=1"
            try:
                resp = self.session.get(url, headers=self.headers, timeout=15)
                # 只要網頁原始碼出現「馬名」或「編號」特徵，代表這個場地網址有效
                if "馬名" in resp.text or "Horse" in resp.text:
                    venue = v
                    print(f">>> 成功識別場地: {venue}")
                    break
            except:
                continue

        races = []
        # 2. 逐場抓取
        for i in range(1, 12):
            race_url = f"{self.base_url}?racedate={formatted_date}&Racecourse={venue}&RaceNo={i}"
            print(f">>> 正在同步第 {i} 場數據...")
            race_info = self.scrape_single_race_brute_force(race_url, i, venue)
            
            if race_info and race_info.get("entries"):
                races.append(race_info)
            else:
                if i == 1: print(">>> [警告] 第一場抓不到馬，可能今日無賽事或被封鎖。")
                break
            time.sleep(random.uniform(1, 2)) # 模擬真人翻網頁的間隔
            
        return races

    def scrape_single_race_brute_force(self, url: str, race_no: int, venue: str) -> Dict[str, Any]:
        """暴力解析：包含 Header 班次、路程、場況"""
        try:
            resp = self.session.get(url, headers=self.headers, timeout=10)
            html = resp.text
            soup = BeautifulSoup(html, 'lxml')
            
            # 1. 抓取 Header 資訊 (對應截圖紅框)
            # 尋找包含「班」、「米」、「地」的文字區塊
            header_text = soup.get_text(separator=' ', strip=True)
            
            race_class = "未知"
            distance = 0
            going = "好地"
            
            # 提取班次 (如: 第五班, 第一班)
            class_match = re.search(r"(第[一二三四五]班|公開賽|條件限制賽)", header_text)
            if class_match: race_class = class_match.group(1)
            
            # 提取路程 (如: 1200米)
            dist_match = re.search(r"(\d{4})米", header_text)
            if dist_match: distance = int(dist_match.group(1))
            
            # 提取場地狀況 (如: 好地, 黏地, 濕地)
            going_match = re.search(r"(好地|黏地|好至黏地|好至快地|濕地)", header_text)
            if going_match: going = going_match.group(1)

            race_data = {
                "race_no": race_no, 
                "venue": venue, 
                "race_class": race_class,
                "distance": distance,
                "going": going,
                "entries": []
            }
            
            processed_codes = set()
            rows = soup.find_all("tr")
            
            processed_codes = set()
            for row in rows:
                text = row.get_text(separator='|', strip=True)
                # 搜尋編號特徵 (字母+3位數字)
                code_match = re.search(r"([A-Z]\d{3})", text)
                if not code_match: continue
                
                horse_code = code_match.group(1)
                if horse_code in processed_codes: continue
                processed_codes.add(horse_code)
                
                # 分割文字流
                parts = text.split('|')
                if len(parts) < 5: continue
                
                try:
                    # 智能定位：馬號通常是第一個數字
                    h_no = int(re.search(r"\d+", parts[0]).group()) if re.search(r"\d+", parts[0]) else 0
                    # 馬名通常在編號所在的那個 part
                    h_name = re.sub(r"[\(\（].*?[\)\）]", "", parts[2]).strip() if len(parts) > 2 else "未知"
                    
                    entry = {
                        "horse_no": h_no,
                        "horse_code": horse_code,
                        "horse_name": h_name,
                        "jockey": parts[3] if len(parts) > 3 else "未知",
                        "trainer": parts[4] if len(parts) > 4 else "未知",
                        "actual_weight": 0,
                        "draw": 0
                    }
                    
                    # 掃描整行找出負磅 (100-140) 和 檔位 (1-14)
                    nums = re.findall(r"\d+", text)
                    for n in nums:
                        v = int(n)
                        if 100 <= v <= 145: entry["actual_weight"] = v
                        elif 1 <= v <= 14 and entry["draw"] == 0 and v != h_no: entry["draw"] = v
                    
                    race_data["entries"].append(entry)
                except:
                    continue
            
            if race_data["entries"]:
                print(f"    - 成功抓取 {len(race_data['entries'])} 匹馬")
            else:
                # 診斷：如果抓不到馬，印出 HTML 片段
                print(f"    - [診斷] 抓取失敗，HTML 前 200 字元: {html[:200]}")
                
            return race_data
        except Exception as e:
            print(f"    - [錯誤] 第 {race_no} 場連線崩潰: {e}")
            return {}

    def start(self): pass
    def stop(self): pass
