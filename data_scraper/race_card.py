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
        """抓取單場賽事的排位與基礎資訊 (終極強韌版)"""
        if not await self.navigate_with_retry(url):
            return {}

        # 1. 等待核心表格 ID 出現 (HKJC 專屬 ID)
        try:
            await self.page.wait_for_selector("#racecardlist, .table_border_hide", timeout=20000)
        except:
            logger.warning(f"場次 {race_no}: 等待表格 ID 超時，嘗試強制解析")
        
        await asyncio.sleep(3) # 給 AJAX 更多時間
        html = await self.get_content()
        soup = BeautifulSoup(html, 'lxml')
        
        # 2. 解析場地 (從頁面文字找)
        page_text = soup.get_text()
        header_text = f"第 {race_no} 場"
        header_tag = soup.select_one(".font_white.f_left.f_fs14.f_fwb")
        if header_tag: header_text = header_tag.text.strip()
        
        race_data = {"race_no": race_no, "header": header_text, "entries": []}

        # 3. 解析馬匹行 (尋找包含 HorseId 的連結)
        # 這是最穩定的方法，因為馬名連結一定包含 HorseId
        rows = soup.select("tr")
        for row in rows:
            link = row.select_one("a[href*='HorseId=']")
            if not link: continue
            
            href = link.get('href', '')
            code_match = re.search(r"HorseId=([A-Z]\d{3})", href)
            if not code_match: continue
            
            horse_code = code_match.group(1)
            # 避免重複
            if any(e["horse_code"] == horse_code for e in race_data["entries"]): continue
            
            try:
                tds = row.select("td")
                td_texts = [td.get_text(strip=True) for td in tds]
                
                # 智能提取馬名 (過濾括號)
                horse_name = re.sub(r"[\(\（].*?[\)\）]", "", link.get_text(strip=True)).strip()
                
                # 提取馬號 (通常在第一或二格)
                horse_no_raw = re.sub(r'\D', '', td_texts[0])
                horse_no = int(horse_no_raw) if horse_no_raw else 0
                
                entry = {
                    "horse_no": horse_no,
                    "horse_code": horse_code,
                    "horse_name": horse_name,
                    "jockey": td_texts[3] if len(td_texts) > 3 else "",
                    "trainer": td_texts[4] if len(td_texts) > 4 else "",
                    "draw": 0,
                    "actual_weight": 0
                }
                
                # 從所有 TD 中掃描負磅(100-140) 與 檔位(1-14)
                for txt in td_texts:
                    if txt.isdigit():
                        v = int(txt)
                        if 100 <= v <= 145: entry["actual_weight"] = v
                        elif 1 <= v <= 14 and entry["draw"] == 0 and v != horse_no: entry["draw"] = v
                
                race_data["entries"].append(entry)
            except:
                continue

        if race_data["entries"]:
            logger.info(f"場次 {race_no}: 成功抓取 {len(race_data['entries'])} 匹馬")
        else:
            logger.error(f"場次 {race_no}: 抓取失敗，HTML 長度: {len(html)}")
            
        return race_data
