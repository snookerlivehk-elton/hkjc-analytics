from typing import List, Dict, Any
from bs4 import BeautifulSoup
from data_scraper.base import BaseScraper
from utils.logger import logger

class SpecialStatsScraper(BaseScraper):
    """抓取特殊統計數據：Draw Statistics, J/T Combo, SpeedPRO"""

    def __init__(self):
        super().__init__()
        self.draw_stats_url = "https://racing.hkjc.com/racing/information/Chinese/Racing/DrawStats.aspx"
        self.jt_combo_url = "https://racing.hkjc.com/racing/information/Chinese/Racing/JockeyTrainerCombo.aspx"
        self.speedpro_url = "https://racing.hkjc.com/racing/information/Chinese/Racing/SpeedPro.aspx"

    async def get_draw_stats(self, racedate: str = "") -> Dict[int, List[Dict[str, Any]]]:
        """獲取當日賽事的官方檔位統計 (Draw Statistics)
        回傳格式: { race_no: [ {draw, total_runs, win, win_rate, place_rate}, ... ] }
        """
        import requests
        
        # 賽日專用的檔位統計頁面，會列出當天所有場次的檔位數據
        url = f"https://racing.hkjc.com/zh-hk/local/information/draw"
        if racedate:
            url += f"?racedate={racedate}"
            
        print(f">>> 正在抓取當日檔位統計: {url}")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://racing.hkjc.com/"
        }
        
        stats_by_race = {}
        
        try:
            # 這裡我們可以直接用 requests 因為它不需要複雜的渲染
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'lxml')
            
            tables = soup.find_all("table")
            for table in tables:
                text = table.get_text(strip=True)
                if "檔位" in text and "勝出率" in text:
                    # 找場次號碼，通常在表頭第一行
                    rows = table.find_all("tr")
                    if not rows: continue
                    
                    header_text = rows[0].get_text(strip=True)
                    import re
                    m = re.search(r'第\s*(\d+)\s*場', header_text)
                    if not m: continue
                    
                    race_no = int(m.group(1))
                    stats_list = []
                    
                    # 跳過前兩行標題 (賽事資訊, 欄位名稱)
                    for row in rows[2:]:
                        cols = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                        if len(cols) >= 10 and cols[0].isdigit():
                            try:
                                stats_list.append({
                                    "draw": int(cols[0]),
                                    "total_runs": int(cols[1]),
                                    "win": int(cols[2]),
                                    "win_rate": float(cols[6]),
                                    "place_rate": float(cols[8])
                                })
                            except ValueError:
                                continue
                                
                    if stats_list:
                        stats_by_race[race_no] = stats_list
                        
            print(f">>> 成功抓取 {len(stats_by_race)} 場賽事的檔位統計")
            return stats_by_race
            
        except Exception as e:
            print(f">>> [錯誤] 抓取檔位統計失敗: {e}")
            return {}

    async def get_jt_combo(self) -> List[Dict[str, Any]]:
        """獲取騎師/練馬師合作統計 (J/T Combo)"""
        if not await self.navigate_with_retry(self.jt_combo_url):
            return []
            
        html = await self.get_content()
        soup = BeautifulSoup(html, 'lxml')
        
        combos = []
        # 解析 J/T Combo 表格
        return combos

    async def get_speedpro_data(self, race_no: int) -> List[Dict[str, Any]]:
        """獲取 SpeedPRO 官方能量分與分段分析"""
        url = f"{self.speedpro_url}?RaceNo={race_no}"
        if not await self.navigate_with_retry(url):
            return []
            
        html = await self.get_content()
        soup = BeautifulSoup(html, 'lxml')
        
        speedpro_list = []
        # 解析 SpeedPRO 表格，提取能量分等數據
        return speedpro_list
