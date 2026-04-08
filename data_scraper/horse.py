import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from utils.logger import logger

class HorseScraper:
    """抓取馬匹基本資料、往績、晨操與獸醫報告 (新版 zh-hk 穩定版)"""

    def __init__(self):
        # 更新為你提供的穩定網址格式
        self.base_url = "https://racing.hkjc.com/zh-hk/local/information/horse"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7"
        }

    def get_horse_past_performance(self, horse_code: str) -> List[Dict[str, Any]]:
        """獲取馬匹歷史往績 (支援自動前綴補齊)"""
        # 策略：嘗試不同的年份前綴，直到成功訪問
        # 例如 K 開頭通常是 2024，J 開頭是 2023
        current_year = datetime.now().year
        prefixes = [f"HK_{current_year}_", f"HK_{current_year-1}_", f"HK_{current_year-2}_", ""]
        
        for prefix in prefixes:
            full_id = f"{prefix}{horse_code}"
            url = f"{self.base_url}?horseid={full_id}"
            try:
                resp = requests.get(url, headers=self.headers, timeout=10)
                if "往績紀錄" not in resp.text: continue # 沒抓到往績，試下一個前綴
                
                soup = BeautifulSoup(resp.text, 'lxml')
                history = []
                
                # 新版解析邏輯：尋找包含賽績的表格
                rows = soup.find_all("tr")
                for row in rows:
                    tds = row.find_all("td")
                    if len(tds) < 10: continue
                    
                    # 特徵：第一格是 3 位數場次 (如 001)
                    idx_text = tds[0].get_text(strip=True)
                    if not re.match(r"^\d{3}$", idx_text): continue
                    
                    try:
                        record = {
                            "race_index": idx_text,
                            "rank": tds[1].get_text(strip=True),
                            "date": tds[2].get_text(strip=True),
                            "venue": tds[3].get_text(strip=True),
                            "class": tds[4].get_text(strip=True),
                            "draw": tds[7].get_text(strip=True),
                            "jockey": tds[6].get_text(strip=True),
                            "weight": tds[5].get_text(strip=True),
                            "rating": tds[8].get_text(strip=True),
                            "finish_time": tds[9].get_text(strip=True)
                        }
                        history.append(record)
                    except:
                        continue
                
                if history: 
                    print(f"    - [成功] 使用 ID: {full_id} 抓取到 {len(history)} 筆往績")
                    return history
            except:
                continue
        return []

    def get_horse_profile(self, horse_code: str) -> Dict[str, Any]:
        """獲取馬匹基本資料 (烙印、父系、母系等)"""
        url = f"{self.base_url}?HorseId={horse_code}"
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            profile = {"code": horse_code, "name": ""}
            name_tag = soup.select_one(".horseName")
            if name_tag: profile["name"] = name_tag.text.strip()
            
            # 解析基本資料表格
            profile_table = soup.select_one(".horseProfile")
            if profile_table:
                tds = [td.text.strip() for td in profile_table.select("td")]
                profile["details"] = tds
                
            return profile
        except:
            return {}

    def start(self): pass
    def stop(self): pass
