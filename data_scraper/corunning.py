import re
import requests
from bs4 import BeautifulSoup
from typing import Any, Dict, List, Optional


class CoRunningScraper:
    def __init__(self):
        self.base_url = "https://racing.hkjc.com/zh-hk/local/information/corunning"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://racing.hkjc.com/",
        }

    def fetch(self, date_yyyymmdd: str, race_no: int) -> str:
        url = f"{self.base_url}?date={date_yyyymmdd}&raceno={int(race_no)}"
        resp = requests.get(url, headers=self.headers, timeout=20)
        resp.raise_for_status()
        return resp.text

    def scrape_single_race(self, date_yyyymmdd: str, race_no: int) -> Dict[str, Any]:
        html = self.fetch(date_yyyymmdd=date_yyyymmdd, race_no=race_no)
        soup = BeautifulSoup(html, "lxml")
        items = self._parse_table(soup)
        return {"date": date_yyyymmdd, "race_no": int(race_no), "items": items}

    def _find_table(self, soup: BeautifulSoup):
        for table in soup.find_all("table"):
            first_tr = table.find("tr")
            if not first_tr:
                continue
            header_cells = [c.get_text(" ", strip=True) for c in first_tr.find_all(["th", "td"])]
            header_norm = "".join(header_cells).replace(" ", "")
            if ("走勢評述" in header_norm) and ("馬號" in header_norm) and ("馬名" in header_norm):
                return table
        return None

    def _parse_table(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        table = self._find_table(soup)
        if table is None:
            return []

        rows: List[Dict[str, Any]] = []
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            cols = [td.get_text(" ", strip=True) for td in tds]
            if len(cols) < 6:
                continue

            rank_s, horse_no_s, horse_name_s, jockey_s, gear_s, comment_s = cols[:6]
            rank = self._to_int(rank_s)
            horse_no = self._to_int(horse_no_s)
            if horse_no <= 0:
                continue

            horse_code = ""
            m = re.search(r"\(([A-Z]\d{3})\)", horse_name_s)
            if m:
                horse_code = m.group(1).strip()
                horse_name_s = re.sub(r"\s*\([A-Z]\d{3}\)\s*", "", horse_name_s).strip()

            rows.append(
                {
                    "rank": rank or None,
                    "horse_no": horse_no,
                    "horse_name": horse_name_s.strip(),
                    "horse_code": horse_code,
                    "jockey": str(jockey_s or "").strip(),
                    "gear": str(gear_s or "").strip(),
                    "commentary": str(comment_s or "").strip(),
                }
            )

        return rows

    def _to_int(self, s: str) -> int:
        try:
            return int(re.sub(r"\D", "", str(s)))
        except Exception:
            return 0

