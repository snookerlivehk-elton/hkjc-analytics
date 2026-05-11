from __future__ import annotations

import re
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup

from scoring_engine.normalization import venue_code


class RaceReportExtScraper:
    def __init__(self):
        self.base_url = "https://racing.hkjc.com/zh-hk/local/information/racereportext"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        }

    def build_url(self, *, race_date: str, racecourse: str, race_no: int) -> str:
        rc = venue_code(racecourse)
        return f"{self.base_url}?racedate={race_date}&Racecourse={rc}&RaceNo={int(race_no)}"

    def fetch(self, *, race_date: str, racecourse: str, race_no: int) -> str:
        url = self.build_url(race_date=race_date, racecourse=racecourse, race_no=race_no)
        resp = requests.get(url, headers=self.headers, timeout=25)
        resp.raise_for_status()
        return resp.text

    def scrape_single_race(self, *, race_date: str, racecourse: str, race_no: int) -> Dict[str, Any]:
        html = self.fetch(race_date=race_date, racecourse=racecourse, race_no=race_no)
        soup = BeautifulSoup(html, "lxml")
        meta = self._parse_meta(soup)
        items = self._parse_table(soup)
        return {
            "race_date": race_date,
            "racecourse": venue_code(racecourse),
            "race_no": int(race_no),
            "meta": meta,
            "items": items,
        }

    def _parse_meta(self, soup: BeautifulSoup) -> Dict[str, Any]:
        text = soup.get_text(separator=" ", strip=True)
        last_update = ""
        m = re.search(r"最後更新\s*[:：]\s*([0-9/:\s]+)", text)
        if m:
            last_update = str(m.group(1) or "").strip()
        return {"last_update": last_update}

    def _parse_table(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        target = None
        for table in soup.find_all("table"):
            first_tr = table.find("tr")
            if not first_tr:
                continue
            headers = [c.get_text(" ", strip=True) for c in first_tr.find_all(["th", "td"])]
            if any("馬號" in h for h in headers) and any("描述" in h for h in headers):
                target = table
                break
        if target is None:
            return []

        out: List[Dict[str, Any]] = []
        rows = target.find_all("tr")[1:]
        for tr in rows:
            cells = tr.find_all(["td", "th"])
            if not cells:
                continue
            c0 = cells[0].get_text(" ", strip=True)
            if not re.match(r"^\d+$", str(c0 or "").strip()):
                continue
            try:
                horse_no = int(c0)
            except Exception:
                continue
            horse_code = cells[1].get_text(" ", strip=True) if len(cells) > 1 else ""
            horse_name = ""
            horse_id = ""
            horse_link = ""
            if len(cells) > 2:
                a = cells[2].find("a")
                if a is not None:
                    horse_name = a.get_text(" ", strip=True)
                    horse_link = str(a.get("href") or "").strip()
                    m = re.search(r"horseid=([^&#]+)", horse_link)
                    if m:
                        horse_id = str(m.group(1) or "").strip()
                else:
                    horse_name = cells[2].get_text(" ", strip=True)
            prev_date = cells[3].get_text(" ", strip=True) if len(cells) > 3 else ""
            prev_race_no = ""
            prev_race_link = ""
            if len(cells) > 4:
                a2 = cells[4].find("a")
                if a2 is not None:
                    prev_race_no = a2.get_text(" ", strip=True)
                    prev_race_link = str(a2.get("href") or "").strip()
                else:
                    prev_race_no = cells[4].get_text(" ", strip=True)
            desc = cells[5].get_text(" ", strip=True) if len(cells) > 5 else ""
            out.append(
                {
                    "horse_no": horse_no,
                    "horse_code": horse_code,
                    "horse_name": horse_name,
                    "horse_id": horse_id,
                    "horse_link": horse_link,
                    "prev_date": prev_date,
                    "prev_race_no": prev_race_no,
                    "prev_race_link": prev_race_link,
                    "desc": desc,
                }
            )
        return out
