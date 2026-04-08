from typing import Dict, Any, List
from bs4 import BeautifulSoup
from data_scraper.base import BaseScraper
from utils.logger import logger

class OddsScraper(BaseScraper):
    """抓取即時與早盤賠率 (Win/Place)"""

    def __init__(self):
        super().__init__()
        self.base_url = "https://racing.hkjc.com/racing/information/Chinese/Racing/OddsAll.aspx"

    async def get_win_place_odds(self, race_no: int, race_date: str = "") -> List[Dict[str, Any]]:
        """獲取單場賽事的獨贏與位置賠率"""
        url = f"{self.base_url}?RaceNo={race_no}"
        if race_date:
            url += f"&RaceDate={race_date}"
            
        if not await self.navigate_with_retry(url):
            return []

        html = await self.get_content()
        soup = BeautifulSoup(html, 'lxml')
        
        odds_data = []
        
        # 1. 尋找賠率表格 (通常在 class="table_border_hide" 內)
        # 賠率頁面通常是動態載入的，可能需要等候
        await self.page.wait_for_selector("table.table_border_hide", timeout=10000)
        
        rows = soup.select("table.table_border_hide tr")[1:] # 跳過表頭
        for row in rows:
            cols = row.select("td")
            if len(cols) < 5: continue
            
            try:
                # 獨贏與位置賠率 (Win/Place)
                horse_no = self.parse_int(cols[0].text)
                win_odds = self.parse_float(cols[3].text)
                place_odds = self.parse_float(cols[4].text)
                
                odds_data.append({
                    "horse_no": horse_no,
                    "win_odds": win_odds,
                    "place_odds": place_odds
                })
            except Exception as e:
                logger.error(f"解析賠率行錯誤 (場次 {race_no}): {e}")

        return odds_data

    async def get_early_odds(self, race_no: int, race_date: str = "") -> List[Dict[str, Any]]:
        """獲取早盤賠率 (通常由特定的 API 或早盤頁面提供)"""
        # 實作早盤抓取邏輯
        # 這裡簡化回傳空清單
        return []
