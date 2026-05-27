from __future__ import annotations

import re
import time
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup

from scoring_engine.normalization import venue_code
from utils.logger import logger


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
        html = ""
        for i in range(3):
            try:
                resp = requests.get(url, headers=self.headers, timeout=35)
                resp.raise_for_status()
                html = resp.text or ""
                break
            except Exception as e:
                if i < 2:
                    time.sleep(1.0 + i * 1.5)
                    continue
                raise
        return html

    def scrape_single_race(self, *, race_date: str, racecourse: str, race_no: int) -> Dict[str, Any]:
        url = self.build_url(race_date=race_date, racecourse=racecourse, race_no=race_no)
        html = self.fetch(race_date=race_date, racecourse=racecourse, race_no=race_no)
        soup = BeautifulSoup(html, "lxml")
        items = self._parse_event_table_for_race(soup, race_no=int(race_no))
        if not items:
            low = (html or "").lower()
            need_render = ("<!doctype html" in low) and ("競賽事件" not in html)
            if need_render:
                try:
                    from playwright.sync_api import sync_playwright

                    with sync_playwright() as p:
                        browser = p.chromium.launch(
                            headless=True,
                            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
                        )
                        context = browser.new_context(
                            user_agent=str(self.headers.get("User-Agent") or ""),
                            viewport={"width": 1920, "height": 1080},
                        )
                        page = context.new_page()
                        page.goto(url, wait_until="networkidle", timeout=45000)
                        page.wait_for_timeout(1200)
                        html2 = page.content() or ""
                        browser.close()
                    if html2:
                        soup2 = BeautifulSoup(html2, "lxml")
                        items2 = self._parse_event_table_for_race(soup2, race_no=int(race_no))
                        if items2:
                            items = items2
                except Exception as e:
                    logger.warning(f"[RaceReportFullScraper] playwright fallback failed url={url} err={type(e).__name__}: {e}")
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
        if len(tables) == 1:
            target = tables[0]
        elif 1 <= race_no <= len(tables):
            target = tables[race_no - 1]
        elif tables:
            target = tables[0]
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
