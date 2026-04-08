import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime
import time

class RaceCardScraper:
    """傳奇穩定版：使用桌面版 .aspx 路徑 + 伺服器渲染解析"""

    def __init__(self):
        # 使用最原始、不依賴 JS 的桌面版網址
        self.base_url = "https://racing.hkjc.com/racing/information/Chinese/Racing/RaceCard.aspx"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://racing.hkjc.com/"
        }

    def get_all_races_info(self, race_date: str = "") -> List[Dict[str, Any]]:
        """獲取當日所有場次"""
        races = []
        formatted_date = race_date if race_date else datetime.now().strftime("%Y/%m/%d")
        
        # 初始探測
        url = f"{self.base_url}?RaceDate={formatted_date}&RaceNo=1"
        try:
            print(f">>> 正在連線至穩定版數據源: {url}")
            resp = requests.get(url, headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # 獲取場次數量
            race_tabs = soup.select(".race_tab_active, .race_tab_inactive, a[href*='RaceNo=']")
            race_nos = set()
            for tab in race_tabs:
                m = re.search(r'RaceNo=(\d+)', str(tab))
                if m: race_nos.add(int(m.group(1)))
            
            race_count = max(race_nos) if race_nos else 9
            print(f">>> 偵測到 {race_count} 場賽事，開始同步...")

            for i in range(1, race_count + 1):
                race_url = f"{self.base_url}?RaceDate={formatted_date}&RaceNo={i}"
                print(f">>> 正在抓取第 {i} 場...")
                race_info = self.scrape_single_race_aspx(race_url, i)
                if race_info and race_info.get("entries"):
                    races.append(race_info)
                time.sleep(1)
            
            return races
        except Exception as e:
            print(f">>> [錯誤] 連線異常: {e}")
            return []

    def scrape_single_race_aspx(self, url: str, race_no: int) -> Dict[str, Any]:
        """精確解析桌面版表格 (對應截圖紅框與表格)"""
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # 1. 識別場地與賽事細節 (對應截圖紅框)
            full_text = soup.get_text(separator=' ', strip=True)
            venue = "HV" if "跑馬地" in full_text else "ST"
            
            race_class = "未知"; distance = 0; going = "好地"
            # 班次
            class_match = re.search(r"(第[一二三四五]班|公開賽)", full_text)
            if class_match: race_class = class_match.group(1)
            # 路程
            dist_match = re.search(r"(\d{4})米", full_text)
            if dist_match: distance = int(dist_match.group(1))
            # 場況
            going_match = re.search(r"(好地|黏地|濕地|快地)", full_text)
            if going_match: going = going_match.group(1)

            race_data = {
                "race_no": race_no, "venue": venue, 
                "race_class": race_class, "distance": distance, "going": going,
                "entries": []
            }

            # 2. 解析排位表格
            # 桌面版特徵：馬匹編號在括號內，如 (K416)
            rows = soup.find_all("tr")
            processed_codes = set()

            for row in rows:
                tds = row.find_all("td")
                if len(tds) < 10: continue
                
                # 尋找馬匹連結與編號
                link = row.find("a", href=re.compile(r"HorseId="))
                if not link: continue
                
                code_match = re.search(r"HorseId=([A-Z]\d{3})", link.get('href', ''), re.I)
                if not code_match: continue
                horse_code = code_match.group(1)
                
                if horse_code in processed_codes: continue
                processed_codes.add(horse_code)
                
                try:
                    # 桌面版標準對位
                    entry = {
                        "horse_no": int(re.sub(r'\D', '', tds[0].text.strip())) if tds[0].text.strip().isdigit() else 0,
                        "horse_code": horse_code,
                        "horse_name": re.sub(r"[\(\（].*?[\)\）]", "", link.text).strip(),
                        "jockey": tds[3].get_text(strip=True),
                        "trainer": tds[4].get_text(strip=True),
                        "actual_weight": int(re.sub(r'\D', '', tds[5].text.strip())) if tds[5].text.strip() else 0,
                        "draw": int(re.sub(r'\D', '', tds[6].text.strip())) if tds[6].text.strip() else 0,
                        "rating": int(re.sub(r'\D', '', tds[8].text.strip())) if tds[8].text.strip() else 0
                    }
                    # 修正騎師名 (移除括號減磅)
                    entry["jockey"] = re.sub(r"\(.*?\)", "", entry["jockey"]).strip()
                    
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
