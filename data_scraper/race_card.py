import re
import asyncio
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
        # 直接從第 1 場開始探測，避免首頁加載問題
        base_probe_url = f"{self.base_url}?RaceNo=1"
        if race_date: base_probe_url += f"&RaceDate={race_date}"
        
        if not await self.navigate_with_retry(base_probe_url):
            return []

        # 獲取場次數量 (從頁面上的場次按鈕)
        html = await self.get_content()
        soup = BeautifulSoup(html, 'lxml')
        race_tabs = soup.select("a[href*='RaceNo=']")
        race_nos = set()
        for tab in race_tabs:
            m = re.search(r'RaceNo=(\d+)', tab.get('href', ''))
            if m: race_nos.add(int(m.group(1)))
        
        race_count = max(race_nos) if race_nos else 9 # 預設探測 9 場
        logger.info(f"偵測到 {race_count} 場賽事")

        for i in range(1, race_count + 1):
            race_url = f"{self.base_url}?RaceNo={i}"
            if race_date: race_url += f"&RaceDate={race_date}"
            
            logger.info(f"正在處理第 {i} 場...")
            race_info = await self.scrape_single_race(race_url, i)
            if race_info and race_info.get("entries"):
                races.append(race_info)
        
        return races

    async def scrape_single_race(self, url: str, race_no: int) -> Dict[str, Any]:
        """抓取單場賽事的排位與基礎資訊 (雙模穩定版)"""
        if not await self.navigate_with_retry(url):
            return {}

        # 增加等待，確保 AJAX 載入
        await asyncio.sleep(5)
        
        html = await self.get_content()
        soup = BeautifulSoup(html, 'lxml')
        
        # 1. 識別場地
        page_text = soup.get_text()
        venue = "HV" if "跑馬地" in page_text or "Happy Valley" in page_text else "ST"
        
        race_data = {"race_no": race_no, "venue": venue, "entries": []}

        # 2. 核心解析邏輯：尋找所有馬匹連結 (不論是在排位表還是賽果頁)
        # 尋找 HorseId= 或 Horse.aspx?HorseId=
        links = soup.find_all("a", href=re.compile(r"HorseId=[A-Z]\d{3}"))
        
        processed_codes = set()
        for link in links:
            href = link.get('href', '')
            code_match = re.search(r"HorseId=([A-Z]\d{3})", href)
            if not code_match: continue
            
            horse_code = code_match.group(1)
            if horse_code in processed_codes: continue
            processed_codes.add(horse_code)
            
            try:
                row = link.find_parent("tr")
                if not row: continue
                
                tds = row.find_all("td")
                td_texts = [td.get_text(separator=' ', strip=True) for td in tds]
                
                # 智能提取：馬號 (通常是行內的第一個數字)
                horse_no_match = re.search(r"^\d+", " ".join(td_texts[:2]))
                horse_no = int(horse_no_match.group(0)) if horse_no_match else 0
                
                # 提取純中文馬名
                horse_name = re.sub(r"[\(\（].*?[\)\）]", "", link.get_text(strip=True)).strip()
                
                entry = {
                    "horse_no": horse_no,
                    "horse_code": horse_code,
                    "horse_name": horse_name,
                    "jockey": "",
                    "trainer": "",
                    "draw": 0,
                    "actual_weight": 0
                }
                
                # 遍歷 TD 識別負磅、檔位、騎練
                for i, txt in enumerate(td_texts):
                    # 識別騎師/練馬師 (長度 2-4 的純文字)
                    if not entry["jockey"] and 2 <= len(txt) <= 4 and i > 1:
                        entry["jockey"] = txt
                    elif not entry["trainer"] and 2 <= len(txt) <= 4 and i > 2:
                        entry["trainer"] = txt
                    
                    # 識別數字
                    if txt.isdigit():
                        v = int(txt)
                        if 100 <= v <= 140: entry["actual_weight"] = v
                        elif 1 <= v <= 14 and entry["draw"] == 0 and v != horse_no: entry["draw"] = v
                
                race_data["entries"].append(entry)
            except:
                continue

        if race_data["entries"]:
            logger.info(f"場次 {race_no}: 成功抓取 {len(race_data['entries'])} 匹馬")
        return race_data
