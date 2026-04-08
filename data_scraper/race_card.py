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
        """抓取單場賽事的排位與基礎資訊 (強容錯版)"""
        # 在 URL 中加入 Default=1，強迫伺服器端渲染
        if "?" in url: url += "&Default=1"
        else: url += "?Default=1"
        
        if not await self.navigate_with_retry(url):
            return {}

        # 等待一點點時間讓頁面穩定
        await asyncio.sleep(2)
        
        html = await self.get_content()
        soup = BeautifulSoup(html, 'lxml')
        race_data = {"race_no": race_no, "entries": []}

        # 核心解析邏輯：改用「馬匹連結」定位法
        # 尋找所有包含 HorseId=XXX 的 <a> 標籤
        horse_links = soup.select("a[href*='HorseId=']")
        for link in horse_links:
            href = link.get('href', '')
            code_match = re.search(r"HorseId=([A-Z]\d{3})", href)
            if code_match:
                horse_code = code_match.group(1)
                # 避免重複處理同一匹馬
                if any(e["horse_code"] == horse_code for e in race_data["entries"]):
                    continue
                
                try:
                    # 向上尋找所在的 TR 行
                    row = link.find_parent("tr")
                    if not row: continue
                    
                    text = row.get_text(separator=' ', strip=True)
                    tds = row.select("td")
                    td_texts = [td.text.strip() for td in tds]
                    
                    # 提取馬號 (該行第一個數字)
                    horse_no_match = re.search(r"^\d+", text)
                    horse_no = int(horse_no_match.group(0)) if horse_no_match else 0
                    
                    # 提取馬名：拿掉括號及其內容，只保留純文字
                    raw_name = link.get_text(strip=True)
                    horse_name = re.sub(r"[\(\（].*?[\)\）]", "", raw_name).strip()
                    
                    entry = {
                        "horse_no": horse_no,
                        "horse_code": horse_code,
                        "horse_name": horse_name,
                        "jockey": td_texts[3] if len(td_texts) > 3 else "",
                        "trainer": td_texts[4] if len(td_texts) > 4 else "",
                        "draw": 0,
                        "actual_weight": 0
                    }
                    
                    # 識別負磅與檔位
                    nums = re.findall(r"\d+", text)
                    for n in nums:
                        v = int(n)
                        if 100 <= v <= 140: entry["actual_weight"] = v
                        elif 1 <= v <= 14 and entry["draw"] == 0 and v != horse_no: entry["draw"] = v
                    
                    race_data["entries"].append(entry)
                except:
                    continue

        if race_data["entries"]:
            logger.info(f"場次 {race_no}: 成功抓取 {len(race_data['entries'])} 匹馬")
        return race_data
