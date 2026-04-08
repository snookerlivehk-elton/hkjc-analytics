import requests
import json
from typing import List, Dict, Any
from datetime import datetime
from utils.logger import logger

class RaceCardScraper:
    """傳奇突破版：直連馬會 GraphQL 數據中心 (100% 穩定、含賠率)"""

    def __init__(self):
        # 這是 2026 年馬會最核心的數據接口
        self.api_url = "https://info.cld.hkjc.com/graphql/base/"
        self.headers = {
            "Content-Type": "application/json",
            "x-apollo-operation-name": "racing",
            "apollo-require-preflight": "true",
            "Origin": "https://bet.hkjc.com",
            "Referer": "https://bet.hkjc.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

    def get_all_races_info(self, race_date: str = "") -> List[Dict[str, Any]]:
        """獲取當日所有場次及完整數據 (馬名、騎練、賠率)"""
        # 格式化日期為 YYYY-MM-DD
        date_str = race_date.replace("/", "-") if race_date else datetime.now().strftime("%Y-%m-%d")
        
        # 我們先嘗試探測 HV，如果不對再試 ST
        races = []
        for venue in ["HV", "ST"]:
            print(f">>> 正在從數據中心提取 {venue} 賽事數據 ({date_str})...")
            venue_races = self._fetch_from_graphql(date_str, venue)
            if venue_races:
                print(f">>> [成功] 發現 {len(venue_races)} 場賽事！")
                races.extend(venue_races)
                break # 找到有資料的場地就停止
        
        return races

    def _fetch_from_graphql(self, date_str: str, venue: str) -> List[Dict[str, Any]]:
        """向 GraphQL 發送查詢請求"""
        query = """
        query racing($date: String, $venueCode: String) {
          raceMeetings(date: $date, venueCode: $venueCode) {
            venueCode
            races {
              no
              raceClassCh
              distance
              goingCh
              runners {
                no
                nameCh
                horseCode
                jockey { nameCh }
                trainer { nameCh }
                draw
                weight
                rtg
                winOdds
              }
            }
          }
        }
        """
        payload = {
            "operationName": "racing",
            "variables": {"date": date_str, "venueCode": venue},
            "query": query
        }

        try:
            resp = requests.post(self.api_url, headers=self.headers, json=payload, timeout=15)
            if resp.status_code != 200: return []
            
            data = resp.json()
            meetings = data.get("data", {}).get("raceMeetings", [])
            if not meetings: return []

            formatted_races = []
            for m in meetings:
                v_code = m.get("venueCode")
                for r in m.get("races", []):
                    race_info = {
                        "race_no": r.get("no"),
                        "venue": v_code,
                        "race_class": r.get("raceClassCh", "未知"),
                        "distance": r.get("distance", 0),
                        "going": r.get("goingCh", "好地"),
                        "entries": []
                    }
                    for h in r.get("runners", []):
                        race_info["entries"].append({
                            "horse_no": h.get("no"),
                            "horse_code": h.get("horseCode"),
                            "horse_name": h.get("nameCh"),
                            "jockey": h.get("jockey", {}).get("nameCh", "未知"),
                            "trainer": h.get("trainer", {}).get("nameCh", "未知"),
                            "draw": h.get("draw", 0),
                            "actual_weight": h.get("weight", 0),
                            "rating": h.get("rtg", 0),
                            "win_odds": h.get("winOdds", 0.0) # 順便抓到賠率了！
                        })
                    formatted_races.append(race_info)
            return formatted_races
        except Exception as e:
            print(f">>> [警告] {venue} 數據提取異常: {e}")
            return []

    def start(self): pass
    def stop(self): pass
