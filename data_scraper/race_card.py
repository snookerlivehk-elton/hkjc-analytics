import re
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from data_scraper.base import BaseScraper
from utils.logger import logger

class RaceCardScraper(BaseScraper):
    """抓取排位表 (今日賽事)"""

    def __init__(self):
        super().__init__()
        self.base_url = "https://racing.hkjc.com/racing/information/Chinese/Racing/RaceCard.aspx"

    async def get_all_races_info(self, race_date: str = "") -> List[Dict[str, Any]]:
        """獲取當日所有場次的基礎資訊與排位"""
        races = []
        
        # 1. 導向首頁獲取場次數量
        url = self.base_url if not race_date else f"{self.base_url}?RaceDate={race_date}"
        if not await self.navigate_with_retry(url):
            return []

        html = await self.get_content()
        soup = BeautifulSoup(html, 'lxml')
        
        # 獲取場次按鈕
        # 修正：HKJC 的場次按鈕可能使用不同的 class 或結構
        race_tabs = soup.select("img[src*='racecard_'], .race_tab_active, .race_tab_inactive, a[href*='RaceNo=']")
        
        # 提取場次數字並去重
        race_nos = set()
        for tab in race_tabs:
            match = re.search(r'RaceNo=(\d+)', str(tab))
            if match:
                race_nos.add(int(match.group(1)))
        
        race_count = max(race_nos) if race_nos else 0
        
        # 如果還是 0，嘗試尋找頁面上的 "第 X 場" 文字
        if race_count == 0:
            text = soup.get_text()
            race_matches = re.findall(r'第\s*(\d+)\s*場', text)
            if race_matches:
                race_count = max(int(m) for m in race_matches)

        logger.info(f"偵測到 {race_count} 場賽事")

        for i in range(1, race_count + 1):
            # 修正 URL 拼接邏輯：判斷是否已有 query string
            separator = "&" if "?" in url else "?"
            race_url = f"{url}{separator}RaceNo={i}"
            race_info = await self.scrape_single_race(race_url, i)
            if race_info:
                races.append(race_info)
        
        return races

    async def scrape_single_race(self, url: str, race_no: int) -> Dict[str, Any]:
        """抓取單場賽事的排位與基礎資訊"""
        if not await self.navigate_with_retry(url):
            return {}

        # 增加等待機制，確保動態表格已載入
        try:
            await self.page.wait_for_selector("table", timeout=5000)
        except:
            pass

        html = await self.get_content()
        soup = BeautifulSoup(html, 'lxml')
        
        # 1. 解析場次資訊 (Header)
        # 嘗試多種可能的 Header 選擇器
        header_tag = soup.select_one(".font_white.f_left.f_fs14.f_fwb, .race_tab td")
        header_text = header_tag.text.strip() if header_tag else f"第 {race_no} 場"
        
        race_data = {
            "race_no": race_no,
            "header": header_text,
            "entries": []
        }

        # 2. 解析排位表 (Main Table)
        # 尋找包含 "馬名" 或 "Horse Name" 的表格
        target_table = None
        for table in soup.select("table"):
            if "馬名" in table.get_text() or "Horse Name" in table.get_text():
                target_table = table
                break
        
        if not target_table:
            logger.warning(f"場次 {race_no}: 找不到排位表格")
            return race_data

        rows = target_table.select("tr")
        for row in rows:
            cols = row.select("td")
            # 排除表頭或過短的行
            if len(cols) < 8: continue
            
            # 判斷第一格是否為數字 (馬號)
            horse_no_text = cols[0].text.strip()
            if not horse_no_text.isdigit(): continue
            
            try:
                # 尋找馬匹編號 (通常在括號內)
                name_cell = ""
                for col in cols:
                    if "(" in col.text and ")" in col.text:
                        name_cell = col.text.strip()
                        break
                
                if not name_cell: name_cell = cols[2].text.strip()
                
                horse_code = ""
                code_match = re.search(r"\((.*?)\)", name_cell)
                if code_match:
                    horse_code = code_match.group(1)
                
                entry = {
                    "horse_no": self.parse_int(horse_no_text),
                    "horse_code": horse_code,
                    "horse_name": name_cell.split('(')[0].strip(),
                    "jockey": cols[3].text.strip() if len(cols) > 3 else "",
                    "trainer": cols[4].text.strip() if len(cols) > 4 else "",
                    "draw": self.parse_int(cols[6].text) if len(cols) > 6 else 0,
                    "actual_weight": self.parse_int(cols[5].text) if len(cols) > 5 else 0,
                    "rating": self.parse_int(cols[8].text) if len(cols) > 8 else 0,
                }
                race_data["entries"].append(entry)
            except Exception as e:
                logger.error(f"解析排位行錯誤 (場次 {race_no}): {e}")

        logger.info(f"場次 {race_no}: 成功抓取 {len(race_data['entries'])} 匹馬")
        return race_data
