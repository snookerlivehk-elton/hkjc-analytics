from __future__ import annotations

import re
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup

from scoring_engine.normalization import venue_code


class RaceReportFullScraper:
    def __init__(self):
        self.base_url = "https://racing.hkjc.com/zh-hk/local/information/racereportfull"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        }

    def build_url(self, *, race_date: str, racecourse: str, race_no: int) -> str:
        rc = venue_code(racecourse)
        return f"{self.base_url}?racedate={race_date}&Racecourse={rc}&RaceNo={int(race_no)}"

    def fetch(self, *, race_date: str, racecourse: str, race_no: int) -> str:
        url = self.build_url(race_date=race_date, racecourse=racecourse, race_no=race_no)
        resp = requests.get(url, headers=self.headers, timeout=30)
        resp.raise_for_status()
        return resp.text

    def scrape_single_race(self, *, race_date: str, racecourse: str, race_no: int) -> Dict[str, Any]:
        html = self.fetch(race_date=race_date, racecourse=racecourse, race_no=race_no)
        soup = BeautifulSoup(html, "lxml")
        items = self._parse_event_table_for_race(soup, race_no=int(race_no))
        return {
            "race_date": race_date,
            "racecourse": venue_code(racecourse),
            "race_no": int(race_no),
            "items": items,
        }

    def _parse_event_table_for_race(self, soup: BeautifulSoup, *, race_no: int) -> List[Dict[str, Any]]:
        race_no = int(race_no)
        tables: List[Any] = []
        for table in soup.find_all("table"):
            first_tr = table.find("tr")
            if not first_tr:
                continue
            headers = [c.get_text(" ", strip=True) for c in first_tr.find_all(["th", "td"])]
            norm = "".join(headers).replace(" ", "")
            if ("馬號" in norm) and ("競賽事件" in norm):
                tables.append(table)
        target = None
        if 1 <= race_no <= len(tables):
            target = tables[race_no - 1]
        if target is None:
            return []

        out: List[Dict[str, Any]] = []
        first_tr = target.find("tr")
        header_cells = first_tr.find_all(["th", "td"]) if first_tr is not None else []
        headers = [c.get_text(" ", strip=True) for c in header_cells]
        idx_hn = -1
        idx_name = -1
        idx_event = -1
        for i, h in enumerate(headers):
            hh = str(h or "").replace(" ", "")
            if idx_hn < 0 and "馬號" in hh:
                idx_hn = i
            if idx_name < 0 and "馬名" in hh:
                idx_name = i
            if idx_event < 0 and ("競賽事件" in hh or (("事件" in hh) and ("競賽" in hh))):
                idx_event = i
        if idx_hn < 0 or idx_event < 0:
            return []

        rows = target.find_all("tr")[1:]
        for tr in rows:
            cells = tr.find_all(["td", "th"])
            if not cells:
                continue
            if len(cells) <= max(idx_hn, idx_event):
                continue
            horse_no_txt = cells[idx_hn].get_text(" ", strip=True)
            try:
                horse_no = int(str(horse_no_txt or "").strip())
            except Exception:
                continue
            if horse_no <= 0:
                continue
            horse_name = ""
            if idx_name >= 0 and len(cells) > idx_name:
                for a in cells[idx_name].find_all("a"):
                    href = str(a.get("href") or "").strip().lower()
                    if "information/horse" in href and "horseid=" in href:
                        horse_name = a.get_text(" ", strip=True)
                        break
                if not horse_name:
                    horse_name = cells[idx_name].get_text(" ", strip=True)
            desc = cells[idx_event].get_text(" ", strip=True)
            desc = str(desc or "").strip()
            if not desc or desc in ("無特別報告。", "無特別報告"):
                continue
            out.append({"horse_no": int(horse_no), "horse_name": horse_name, "desc": desc})
        out.sort(key=lambda x: int(x.get("horse_no") or 0))
        return out
