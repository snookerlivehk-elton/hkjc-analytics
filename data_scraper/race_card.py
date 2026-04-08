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
        """精確表格解析：解決欄位對位錯誤"""
        try:
            resp = self.session.get(url, headers=self.headers, timeout=10)
            html = resp.text
            soup = BeautifulSoup(html, 'lxml')
            
            # 1. 解析 Header (班次、路程、場況)
            header_text = soup.get_text(separator=' ', strip=True)
            race_class = "未知"; distance = 0; going = "好地"
            class_match = re.search(r"(第[一二三四五]班|公開賽|條件限制賽)", header_text)
            if class_match: race_class = class_match.group(1)
            dist_match = re.search(r"(\d{4})米", header_text)
            if dist_match: distance = int(dist_match.group(1))
            going_match = re.search(r"(好地|黏地|濕地|快地)", header_text)
            if going_match: going = going_match.group(1)

            race_data = {
                "race_no": race_no, "venue": venue, 
                "race_class": race_class, "distance": distance, "going": going,
                "entries": []
            }
            
            # 2. 精確表格解析
            # HKJC 的表格行通常包含馬匹編號連結
            rows = soup.find_all("tr")
            processed_codes = set()

            for row in rows:
                tds = row.find_all("td")
                if len(tds) < 10: continue # 正常的排位表行至少有 10 格
                
                # 尋找馬匹編號 (連結中)
                link = row.select_one("a[href*='HorseId=']")
                if not link: continue
                
                code_match = re.search(r"HorseId=([A-Z]\d{3})", link.get('href', ''))
                if not code_match: continue
                horse_code = code_match.group(1)
                
                if horse_code in processed_codes: continue
                processed_codes.add(horse_code)
                
                try:
                    # 根據 HKJC 標準排位表順序提取 (對應你的截圖)
                    # [0]馬號 [1]6次近績 [2]綵衣 [3]馬名 [4]負磅 [5]騎師 [6]檔位 [7]練馬師 [8]評分
                    entry = {
                        "horse_no": int(re.sub(r'\D', '', tds[0].text.strip())) if tds[0].text.strip() else 0,
                        "horse_code": horse_code,
                        "horse_name": re.sub(r"[\(\（].*?[\)\）]", "", link.text).strip(),
                        "jockey": tds[5].get_text(strip=True),
                        "trainer": tds[7].get_text(strip=True),
                        "draw": int(re.sub(r'\D', '', tds[6].text.strip())) if tds[6].text.strip().isdigit() else 0,
                        "actual_weight": int(re.sub(r'\D', '', tds[4].text.strip())) if tds[4].text.strip().isdigit() else 0,
                        "rating": int(re.sub(r'\D', '', tds[8].text.strip())) if tds[8].text.strip().isdigit() else 0
                    }
                    
                    # 清理騎師名 (拿掉減磅數字，如 "-2")
                    entry["jockey"] = re.sub(r"\s*\(.*?\)", "", entry["jockey"]).strip()
                    
                    race_data["entries"].append(entry)
                except:
                    continue
            
            if race_data["entries"]:
                print(f"    - 成功抓取 {len(race_data['entries'])} 匹馬 (精確模式)")
            return race_data
        except Exception as e:
            print(f"    - [錯誤] 第 {race_no} 場解析崩潰: {e}")
            return {}
            
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
