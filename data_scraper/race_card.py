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
        """抓取單場馬匹 (雙重掃描穩定版)"""
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # 識別場地
            page_text = soup.get_text(separator=' ', strip=True)
            venue = "HV" if "跑馬地" in page_text or "Happy Valley" in page_text else "ST"
            
            race_data = {"race_no": race_no, "venue": venue, "entries": []}
            processed_codes = set()

            # 策略 1: 尋找所有馬匹連結 (包含 HorseId=G368 這種格式)
            horse_links = soup.select("a[href*='HorseId=']")
            for link in horse_links:
                href = link.get('href', '')
                code_match = re.search(r"HorseId=([A-Z]\d{3})", href)
                if not code_match: continue
                
                horse_code = code_match.group(1)
                if horse_code in processed_codes: continue
                processed_codes.add(horse_code)
                
                # 取得馬名：拿掉括號
                raw_name = link.get_text(strip=True)
                horse_name = re.sub(r"[\(\（].*?[\)\）]", "", raw_name).strip()
                if not horse_name: continue # 跳過空名稱
                
                entry = {
                    "horse_no": len(race_data["entries"]) + 1,
                    "horse_code": horse_code,
                    "horse_name": horse_name,
                    "jockey": "自動獲取", "trainer": "自動獲取", "draw": 0, "actual_weight": 0
                }
                
                # 策略 2: 向上找 TR 行，提取更多數字 (負磅、檔位)
                row = link.find_parent("tr")
                if row:
                    tds = row.select("td")
                    td_texts = [td.get_text(strip=True) for td in tds]
                    # 提取馬號 (第一個數字)
                    if tds and tds[0].text.strip().isdigit():
                        entry["horse_no"] = int(tds[0].text.strip())
                    
                    # 掃描整行數字
                    nums = re.findall(r"\d+", row.get_text())
                    for n in nums:
                        v = int(n)
                        if 100 <= v <= 145: entry["actual_weight"] = v
                        elif 1 <= v <= 14 and entry["draw"] == 0 and v != entry["horse_no"]: entry["draw"] = v
                    
                    # 提取騎練 (通常在 3, 4 欄)
                    if len(td_texts) > 4:
                        entry["jockey"] = td_texts[3]
                        entry["trainer"] = td_texts[4]

                race_data["entries"].append(entry)
            
            # 如果還是沒抓到，嘗試策略 3: 文字流掃描 (備援)
            if not race_data["entries"]:
                all_text = soup.get_text(separator='|', strip=True)
                matches = re.finditer(r"([^\d\s\|]{2,6})\s*[\(\（]([A-Z]\d{3})[\)\）]", all_text)
                for match in matches:
                    name, code = match.group(1).strip(), match.group(2).strip()
                    if code not in processed_codes:
                        race_data["entries"].append({
                            "horse_no": len(race_data["entries"]) + 1,
                            "horse_code": code, "horse_name": name,
                            "jockey": "備援抓取", "trainer": "備援抓取", "draw": 0, "actual_weight": 0
                        })
                        processed_codes.add(code)

            print(f"    - 第 {race_no} 場: 成功抓取 {len(race_data['entries'])} 匹馬")
            return race_data
        except Exception as e:
            print(f"    - 第 {race_no} 場: 抓取錯誤: {e}")
            return {}

    def start(self): pass # 保持接口相容
    def stop(self): pass  # 保持接口相容
