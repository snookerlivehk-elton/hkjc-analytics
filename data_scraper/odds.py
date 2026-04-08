import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime
from utils.logger import logger

class OddsScraper:
    """穩定版賠率抓取器：使用 bet.hkjc.com 投注版路徑"""

    def __init__(self):
        self.base_url = "https://bet.hkjc.com/ch/racing/wp"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7"
        }

    def get_win_place_odds(self, race_no: int, race_date: str = "", venue: str = "HV") -> List[Dict[str, Any]]:
        """獲取獨贏及位置賠率"""
        # 格式化日期：YYYY-MM-DD
        date_str = race_date.replace("/", "-") if race_date else datetime.now().strftime("%Y-%m-%d")
        url = f"{self.base_url}/{date_str}/{venue}/{race_no}"
        
        try:
            print(f">>> 正在抓取即時賠率: {url}")
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, 'lxml')
            
            odds_list = []
            # 解析賠率表格 (投注版通常包含 win-place-odds 相關 class)
            # 這裡我們使用文字識別作為保險
            rows = soup.find_all("tr")
            for row in rows:
                text = row.get_text(separator='|', strip=True)
                # 尋找包含賠率數字的行
                nums = re.findall(r"\d+\.\d+", text)
                if len(nums) >= 2:
                    # 假設第一個數字是 Win，第二個是 Place
                    horse_no_match = re.search(r"^(\d+)", text)
                    if horse_no_match:
                        odds_list.append({
                            "horse_no": int(horse_no_match.group(1)),
                            "win_odds": float(nums[0]),
                            "place_odds": float(nums[1])
                        })
            
            return odds_list
        except Exception as e:
            print(f">>> [警告] 賠率抓取失敗: {e}")
            return []

    def start(self): pass
    def stop(self): pass
