import re
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from data_scraper.base import BaseScraper
from utils.logger import logger

class ResultsScraper(BaseScraper):
    """抓取歷史賽事結果與分段時間"""

    def __init__(self):
        super().__init__()
        self.base_url = "https://racing.hkjc.com/racing/information/Chinese/Racing/LocalResults.aspx"

    async def get_results_by_date(self, race_date: str) -> List[Dict[str, Any]]:
        """獲取指定日期的所有場次結果"""
        # race_date format: YYYY/MM/DD
        results = []
        
        # 1. 導向首頁獲取場次數量
        url = f"{self.base_url}?RaceDate={race_date}"
        if not await self.navigate_with_retry(url):
            return []

        html = await self.get_content()
        soup = BeautifulSoup(html, 'lxml')
        
        # 修正：更強健的場次按鈕選取器
        race_tabs = soup.select("img[src*='racecard_'], .race_tab_active, .race_tab_inactive, a[href*='RaceNo=']")
        race_nos = set()
        for tab in race_tabs:
            match = re.search(r'RaceNo=(\d+)', str(tab))
            if match:
                race_nos.add(int(match.group(1)))
        
        race_count = max(race_nos) if race_nos else 0
        
        if race_count == 0:
            text = soup.get_text()
            race_matches = re.findall(r'第\s*(\d+)\s*場', text)
            if race_matches:
                race_count = max(int(m) for m in race_matches)

        logger.info(f"日期 {race_date} 偵測到 {race_count} 場賽事結果")

        for i in range(1, race_count + 1):
            # 修正 URL 拼接邏輯
            separator = "&" if "?" in url else "?"
            race_url = f"{url}{separator}RaceNo={i}"
            race_result = await self.scrape_single_race_result(race_url, i, race_date)
            if race_result:
                results.append(race_result)
        
        return results

    async def scrape_single_race_result(self, url: str, race_no: int, race_date: str) -> Dict[str, Any]:
        """抓取單場賽事的結果、場地狀態與分段時間"""
        if not await self.navigate_with_retry(url):
            return {}

        html = await self.get_content()
        soup = BeautifulSoup(html, 'lxml')
        
        # 1. 解析場次基礎資訊
        # 尋找 "好地", "黏地" 等場地狀況 (Going)
        going = ""
        going_match = re.search(r"場地狀況\s*:\s*(\w+)", soup.get_text())
        if going_match:
            going = going_match.group(1)
            
        race_data = {
            "race_date": race_date,
            "race_no": race_no,
            "going": going,
            "results": []
        }

        # 2. 解析結果表
        performance_table = soup.select_one(".performance")
        if not performance_table:
            return {}

        rows = performance_table.select("tr")[1:]
        for row in rows:
            cols = row.select("td")
            if len(cols) < 10: continue
            
            try:
                time_str = cols[10].text.strip()
                time_sec = 0.0
                if ":" in time_str:
                    m, s = time_str.split(":")
                    time_sec = float(m) * 60 + float(s)
                
                # 提取馬匹編號 (例如: 爆熱 (G368) -> G368)
                horse_name_raw = cols[2].text.strip()
                horse_code = ""
                code_match = re.search(r"\((.*?)\)", horse_name_raw)
                if code_match:
                    horse_code = code_match.group(1)
                
                result = {
                    "rank": self.parse_int(cols[0].text),
                    "horse_no": self.parse_int(cols[1].text),
                    "horse_code": horse_code,
                    "horse_name": horse_name_raw.split('(')[0].strip(),
                    "jockey": cols[3].text.strip(),
                    "trainer": cols[4].text.strip(),
                    "actual_weight": self.parse_int(cols[5].text),
                    "draw": self.parse_int(cols[6].text),
                    "margin": cols[7].text.strip(),
                    "running_pos": [pos.strip() for pos in cols[8].text.split()],
                    "finish_time": time_str,
                    "finish_time_sec": time_sec,
                    "win_odds": self.parse_float(cols[11].text),
                    "sectional_times": []
                }
                
                # 3. 解析分段時間 (Sectional Times)
                # 在 HKJC 網頁中，分段時間通常在 class="sectionalTime" 的表格內
                # 或者直接在結果列中提取 (視網頁結構而定)
                # 這裡尋找所有分段時間單元格
                sec_tds = row.select("td.sectionalTime")
                for td in sec_tds:
                    val = self.parse_float(td.text)
                    if val > 0:
                        result["sectional_times"].append(val)
                
                race_data["results"].append(result)
            except Exception as e:
                logger.error(f"解析賽果行錯誤 (場次 {race_no}): {e}")

        return race_data

    def _parse_sectional_times(self, row_soup) -> List[float]:
        """解析單一馬匹的分段時間 (隱藏在特定的 TD 內)"""
        # 實作細節需根據 HKJC 具體 HTML 結構
        # 範例回傳: [23.5, 22.8, 24.1]
        return [] # 待補全
