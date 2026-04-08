import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime
import time
import random

class RaceCardScraper:
    """絕地求生版：不看 HTML 標籤，直接掃描文字流特徵"""

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
            # 先嘗試獲取第一場以獲取 Cookie 和場次清單
            resp = self.session.get(f"{self.base_url}?RaceDate={date_str}&RaceNo=1", headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # 獲取場次數量
            race_nos = set()
            for a in soup.select("a[href*='RaceNo=']"):
                m = re.search(r'RaceNo=(\d+)', a.get('href', ''))
                if m: race_nos.add(int(m.group(1)))
            
            race_count = max(race_nos) if race_nos else 9
            print(f">>> 偵測到 {race_count} 場賽事，開始提取數據...")

            races = []
            for i in range(1, race_count + 1):
                url = f"{self.base_url}?RaceDate={date_str}&RaceNo={i}"
                print(f">>> 正在抓取第 {i} 場...")
                race_info = self.scrape_by_feature(url, i)
                if race_info and race_info.get("entries"):
                    races.append(race_info)
                time.sleep(random.uniform(1, 2))
            
            return races
        except Exception as e:
            print(f">>> [錯誤] 連線異常: {e}")
            return []

    def scrape_by_feature(self, url: str, race_no: int) -> Dict[str, Any]:
        """精確對位解析：利用表格索引鎖定欄位，徹底解決位移問題"""
        try:
            resp = self.session.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # 1. 識別基本資訊
            full_text = soup.get_text(separator=' ', strip=True)
            venue = "HV" if "跑馬地" in full_text else "ST"
            race_data = {
                "race_no": race_no, "venue": venue, 
                "race_class": "未知", "distance": 0, "going": "好地",
                "entries": []
            }
            
            # 提取班次、路程
            class_m = re.search(r"(第[一二三四五]班|公開賽)", full_text)
            if class_m: race_data["race_class"] = class_m.group(1)
            dist_m = re.search(r"(\d{4})米", full_text)
            if dist_m: race_data["distance"] = int(dist_m.group(1))

            # 2. 精確解析表格 TR
            rows = soup.find_all("tr")
            processed_codes = set()

            for row in rows:
                tds = row.find_all("td")
                # 關鍵：馬會標準排位表行至少有 10 個 td
                if len(tds) < 10: continue
                
                # 尋找包含馬匹編號的連結 (例如 HorseId=G368)
                link = row.find("a", href=re.compile(r"HorseId="))
                if not link: continue
                
                code_match = re.search(r"HorseId=([A-Z]\d{3})", link.get('href', ''), re.I)
                if not code_match: continue
                horse_code = code_match.group(1)
                
                if horse_code in processed_codes: continue
                processed_codes.add(horse_code)
                
                try:
                    # --- 根據馬會桌面版標準順序進行垂直對位 ---
                    # 索引 [0]: 馬號
                    # 索引 [3]: 馬名 (link 所在格)
                    # 索引 [4]: 負磅
                    # 索引 [5]: 騎師
                    # 索引 [6]: 檔位
                    # 索引 [7]: 練馬師
                    # 索引 [8]: 評分
                    
                    entry = {
                        "horse_no": int(re.sub(r'\D', '', tds[0].text.strip())) if tds[0].text.strip().isdigit() else 0,
                        "horse_code": horse_code,
                        "horse_name": re.sub(r"[\(\（].*?[\)\）]", "", link.get_text(strip=True)).strip(),
                        "jockey": tds[5].get_text(strip=True),
                        "trainer": tds[7].get_text(strip=True),
                        "actual_weight": int(re.sub(r'\D', '', tds[4].text.strip())) if tds[4].text.strip().isdigit() else 0,
                        "draw": int(re.sub(r'\D', '', tds[6].text.strip())) if tds[6].text.strip().isdigit() else 0,
                        "rating": int(re.sub(r'\D', '', tds[8].text.strip())) if tds[8].text.strip().isdigit() else 0
                    }
                    
                    # 清理騎師名稱 (有些會帶減磅數字，如「鍾易禮 (-2)」)
                    entry["jockey"] = re.sub(r"\(.*?\)", "", entry["jockey"]).strip()
                    
                    race_data["entries"].append(entry)
                except:
                    continue
            
            if race_data["entries"]:
                print(f"    - 第 {race_no} 場: 精確抓取 {len(race_data['entries'])} 匹馬")
            return race_data
        except:
            return {}
            
            if race_data["entries"]:
                print(f"    - 成功提取 {len(race_data['entries'])} 匹馬")
            return race_data
        except:
            return {}

    def start(self): pass
    def stop(self): pass
