import re
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from utils.logger import logger
class OddsScraper:
    """穩定版賠率抓取器：使用 bet.hkjc.com 投注版路徑"""

    def __init__(self):
        self.base_url = "https://bet.hkjc.com/ch/racing/wp"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7"
        }

    def _fetch_wp_html(self, race_no: int, race_date: str = "", venue: str = "HV") -> Tuple[str, str]:
        date_str = race_date.replace("/", "-") if race_date else datetime.now().strftime("%Y-%m-%d")
        url = f"{self.base_url}/{date_str}/{venue}/{race_no}"
        resp = requests.get(url, headers=self.headers, timeout=15)
        resp.raise_for_status()

        html = resp.text or ""
        if ("<!doctype html" in html.lower()) and ("__NEXT_DATA__" not in html) and ("投注" not in html) and (len(html) < 20000):
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
                    html = page.content() or html
                    browser.close()
            except Exception:
                pass

        return url, html

    @staticmethod
    def _parse_money_to_int(s: str) -> Optional[int]:
        if not s:
            return None
        m = re.search(r"\$?\s*([0-9][0-9,]*)", str(s))
        if not m:
            return None
        try:
            return int(m.group(1).replace(",", ""))
        except Exception:
            return None

    @staticmethod
    def _parse_float(s: str) -> Optional[float]:
        if not s:
            return None
        m = re.search(r"([0-9]+(?:\.[0-9]+)?)", str(s))
        if not m:
            return None
        try:
            return float(m.group(1))
        except Exception:
            return None

    @staticmethod
    def _find_runner_odds_table(soup: BeautifulSoup):
        for table in soup.find_all("table"):
            header_cells = []
            tr = table.find("tr")
            if not tr:
                continue
            for c in tr.find_all(["th", "td"]):
                header_cells.append(c.get_text(strip=True))
            if not header_cells:
                continue
            header_text = "|".join(header_cells)
            if ("馬號" in header_text) and ("獨贏" in header_text) and ("位置" in header_text):
                return table, header_cells
        return None, None

    @staticmethod
    def _extract_update_time_hk(text: str) -> Optional[str]:
        if not text:
            return None
        m = re.search(r"更新時間[:：]\s*([0-9]{2}/[0-9]{2}/[0-9]{4}\s+[0-9]{2}:[0-9]{2})", text)
        if m:
            return m.group(1)
        return None

    def get_wp_snapshot(self, race_no: int, race_date: str = "", venue: str = "HV") -> Dict[str, Any]:
        url = ""
        try:
            url, html = self._fetch_wp_html(race_no=race_no, race_date=race_date, venue=venue)
            soup = BeautifulSoup(html, "lxml")

            page_text = soup.get_text(separator="\n", strip=True)
            update_time_hk = self._extract_update_time_hk(page_text)

            post_time_hk = None
            m = re.search(r",\s*([0-9]{1,2}:[0-9]{2})\s*,\s*第", page_text)
            if m:
                post_time_hk = m.group(1)

            odds_list: List[Dict[str, Any]] = []
            table, header_cells = self._find_runner_odds_table(soup)
            if table and header_cells:
                try:
                    hn_idx = header_cells.index("馬號")
                except ValueError:
                    hn_idx = 0
                try:
                    win_idx = header_cells.index("獨贏")
                except ValueError:
                    win_idx = None
                try:
                    place_idx = header_cells.index("位置")
                except ValueError:
                    place_idx = None

                for tr in table.find_all("tr")[1:]:
                    tds = tr.find_all(["td", "th"])
                    if not tds:
                        continue
                    if hn_idx >= len(tds):
                        continue
                    hn_raw = tds[hn_idx].get_text(strip=True)
                    if not hn_raw or (not hn_raw.isdigit()):
                        continue
                    hn = int(hn_raw)
                    win_odds = None
                    place_odds = None
                    if win_idx is not None and win_idx < len(tds):
                        win_odds = self._parse_float(tds[win_idx].get_text(strip=True))
                    if place_idx is not None and place_idx < len(tds):
                        place_odds = self._parse_float(tds[place_idx].get_text(strip=True))
                    if (win_odds is None) or (place_odds is None):
                        continue
                    odds_list.append({"horse_no": hn, "win_odds": float(win_odds), "place_odds": float(place_odds)})

            pools: Dict[str, int] = {}
            for table in soup.find_all("table"):
                for tr in table.find_all("tr"):
                    tds = tr.find_all(["td", "th"])
                    if len(tds) < 2:
                        continue
                    k = tds[0].get_text(strip=True)
                    v = tds[1].get_text(strip=True)
                    amt = self._parse_money_to_int(v)
                    if not k or amt is None:
                        continue
                    if ("獨贏" in k) or ("位置" in k) or ("連贏" in k) or ("位置Q" in k) or ("孖寶" in k) or ("此場總投注額" in k):
                        pools[k] = int(amt)

            return {
                "url": url,
                "venue": venue,
                "race_date": race_date,
                "race_no": int(race_no),
                "update_time_hk": update_time_hk,
                "post_time_hk": post_time_hk,
                "odds": odds_list,
                "pools": pools,
            }
        except Exception as e:
            logger.warning(f"[OddsScraper] wp snapshot failed url={url or '(n/a)'} err={e}")
            return {"url": url, "venue": venue, "race_date": race_date, "race_no": int(race_no), "odds": [], "pools": {}}

    def get_win_place_odds(self, race_no: int, race_date: str = "", venue: str = "HV") -> List[Dict[str, Any]]:
        """獲取獨贏及位置賠率"""
        snap = self.get_wp_snapshot(race_no=race_no, race_date=race_date, venue=venue)
        return list(snap.get("odds") or [])

    def start(self): pass
    def stop(self): pass
