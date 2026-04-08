from typing import Dict, Any, List
from bs4 import BeautifulSoup
from data_scraper.base import BaseScraper
from utils.logger import logger

class HorseScraper(BaseScraper):
    """抓取馬匹基本資料、晨操與獸醫報告"""

    def __init__(self):
        super().__init__()
        self.profile_url = "https://racing.hkjc.com/racing/information/Chinese/Horse/Horse.aspx"
        self.workout_url = "https://racing.hkjc.com/racing/information/Chinese/Horse/Workout.aspx"
        self.vet_url = "https://racing.hkjc.com/racing/information/Chinese/Horse/VetReport.aspx"

    async def get_horse_profile(self, horse_code: str) -> Dict[str, Any]:
        """獲取馬匹基本資料"""
        url = f"{self.profile_url}?HorseId={horse_code}"
        if not await self.navigate_with_retry(url):
            return {}

        html = await self.get_content()
        soup = BeautifulSoup(html, 'lxml')
        
        # 解析基本資料表格 (通常在 class="horseProfile" 內)
        profile_table = soup.select_one(".horseProfile")
        if not profile_table:
            logger.warning(f"找不到馬匹 {horse_code} 的資料表")
            return {}

        # 這裡根據表格結構解析欄位
        # (這裡簡化)
        profile = {
            "code": horse_code,
            "name": soup.select_one(".horseName").text.strip() if soup.select_one(".horseName") else "",
            "details": [td.text.strip() for td in profile_table.select("td")]
        }
        
        return profile

    async def get_horse_workouts(self, horse_code: str) -> List[Dict[str, Any]]:
        """獲取馬匹晨操紀錄 (Trackwork)"""
        url = f"{self.workout_url}?HorseId={horse_code}"
        if not await self.navigate_with_retry(url):
            return []

        html = await self.get_content()
        soup = BeautifulSoup(html, 'lxml')
        
        workouts = []
        # 尋找晨操表格 (class="big_table")
        table = soup.select_one(".big_table")
        if table:
            rows = table.select("tr")[1:]
            for row in rows:
                cols = row.select("td")
                if len(cols) < 5: continue
                workouts.append({
                    "date": cols[0].text.strip(),
                    "type": cols[1].text.strip(),
                    "description": cols[2].text.strip(),
                    "rating": self.parse_int(cols[4].text) if len(cols) > 4 else 0
                })
        return workouts

    async def get_horse_vet_reports(self, horse_code: str) -> List[Dict[str, Any]]:
        """獲取馬匹獸醫報告紀錄 (Veterinary Records)"""
        url = f"{self.vet_url}?HorseId={horse_code}"
        if not await self.navigate_with_retry(url):
            return []

        html = await self.get_content()
        soup = BeautifulSoup(html, 'lxml')
        
        reports = []
        table = soup.select_one(".big_table")
        if table:
            rows = table.select("tr")[1:]
            for row in rows:
                cols = row.select("td")
                if len(cols) < 3: continue
                reports.append({
                    "date": cols[0].text.strip(),
                    "details": cols[1].text.strip(),
                    "passed_date": cols[2].text.strip()
                })
        return reports
