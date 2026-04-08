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
        """抓取單場賽事的排位與基礎資訊 (全能文字流掃描版)"""
        if not await self.navigate_with_retry(url):
            return {}

        # 增加等待
        await asyncio.sleep(5)
        html = await self.get_content()
        soup = BeautifulSoup(html, 'lxml')
        
        # 1. 識別場地
        page_text = soup.get_text(separator=' ', strip=True)
        venue = "HV" if "跑馬地" in page_text or "Happy Valley" in page_text else "ST"
        race_data = {"race_no": race_no, "venue": venue, "entries": []}

        # 2. 終極解析：直接找所有包含馬匹編號特徵的區塊
        # 匹配格式如 (G368), （H123）
        all_text = soup.get_text(separator='|', strip=True)
        # 正規表達式找馬：馬號(可選) + 馬名 + (編號)
        # 我們先找所有符合 (A123) 的編號，再往回推馬名
        matches = re.finditer(r"([^\d\s\|]{2,6})\s*[\(\（]([A-Z]\d{3})[\)\）]", all_text)
        
        processed_codes = set()
        for i, match in enumerate(matches):
            horse_name = match.group(1).strip()
            horse_code = match.group(2).strip()
            
            if horse_code in processed_codes: continue
            processed_codes.add(horse_code)
            
            # 建立馬匹資料
            entry = {
                "horse_no": i + 1, # 暫時用順序，後續再精確匹配
                "horse_code": horse_code,
                "horse_name": horse_name,
                "jockey": "未知",
                "trainer": "未知",
                "draw": 0,
                "actual_weight": 0
            }
            
            # 嘗試在附近找數字 (負磅與檔位)
            # 我們抓取 match 位置前後 100 個字元的片段
            context = all_text[max(0, match.start()-50) : min(len(all_text), match.end()+100)]
            nums = re.findall(r"\d+", context)
            for n in nums:
                v = int(n)
                if 100 <= v <= 145: entry["actual_weight"] = v
                elif 1 <= v <= 14 and entry["draw"] == 0: entry["draw"] = v
            
            race_data["entries"].append(entry)

        if race_data["entries"]:
            logger.info(f"場次 {race_no}: 成功抓取 {len(race_data['entries'])} 匹馬 (文字流模式)")
        else:
            # 備用方案：如果文字流沒抓到，嘗試找連結
            links = soup.find_all("a", href=re.compile(r"HorseId=[A-Z]\d{3}"))
            for link in links:
                code = re.search(r"HorseId=([A-Z]\d{3})", link.get('href', '')).group(1)
                if code not in processed_codes:
                    name = re.sub(r"[\(\（].*?[\)\）]", "", link.get_text(strip=True)).strip()
                    race_data["entries"].append({
                        "horse_no": len(race_data["entries"]) + 1,
                        "horse_code": code,
                        "horse_name": name,
                        "jockey": "未知", "trainer": "未知", "draw": 0, "actual_weight": 0
                    })
                    processed_codes.add(code)

        return race_data
