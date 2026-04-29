import hashlib
import re
import asyncio
from datetime import datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from data_scraper.base import BaseScraper


HK_TZ = ZoneInfo("Asia/Hong_Kong")


def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _first_float(pattern: str, text: str) -> Optional[float]:
    m = re.search(pattern, text)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def _first_str(pattern: str, text: str) -> Optional[str]:
    m = re.search(pattern, text)
    return m.group(1).strip() if m else None


class WindTrackerScraper:
    def __init__(self):
        self.url = "https://racing.hkjc.com/zh-hk/local/info/windtracker"
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://racing.hkjc.com/",
        }

    def scrape(self) -> Dict[str, Any]:
        r = self.session.get(self.url, headers=self.headers, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        text = soup.get_text(separator=" ", strip=True)
        raw_html = r.text

        if not text.strip():
            try:
                rendered = self._scrape_rendered_text()
                if rendered and rendered.strip():
                    text = rendered
            except Exception:
                pass

        parsed = self._parse(text)
        captured_at = datetime.now(HK_TZ).isoformat()
        return {
            "source_url": self.url,
            "captured_at": captured_at,
            "raw_sha256": _sha256(text or raw_html),
            "raw_text": (text or "")[:20000],
            "raw_html_excerpt": (raw_html or "")[:20000],
            "parsed": parsed,
        }

    def _scrape_rendered_text(self) -> str:
        async def _run() -> str:
            base = BaseScraper()
            await base.start()
            try:
                ok = await base.navigate_with_retry(self.url, retries=2)
                if not ok or not base.page:
                    return ""
                try:
                    await base.page.wait_for_function(
                        "() => { const t = (document.body && document.body.innerText) ? document.body.innerText : ''; return /最後更新:\\s*\\d{2}\\/\\d{2}\\/\\d{4}\\s*\\d{2}:\\d{2}/.test(t); }",
                        timeout=15000,
                    )
                except Exception:
                    pass
                try:
                    body = await base.page.inner_text("body")
                    if body and body.strip():
                        return str(body).strip()
                except Exception:
                    pass

                html = await base.get_content()
                soup2 = BeautifulSoup(html, "lxml")
                return soup2.get_text(separator=" ", strip=True)
            finally:
                await base.stop()

        try:
            return asyncio.run(_run())
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(_run())
            finally:
                loop.close()

    def _parse(self, text: str) -> Dict[str, Any]:
        last_updated_raw = _first_str(r"最後更新:\s*([0-9]{2}/[0-9]{2}/[0-9]{4}\s*[0-9]{2}:[0-9]{2})", text)
        last_updated_at = None
        if last_updated_raw:
            try:
                last_updated_at = datetime.strptime(last_updated_raw, "%d/%m/%Y %H:%M").replace(tzinfo=HK_TZ).isoformat()
            except Exception:
                last_updated_at = None

        venue = None
        if "跑馬地" in text:
            venue = "HV"
        elif "沙田" in text:
            venue = "ST"

        meeting_date = _first_str(r"([0-9]{2}/[0-9]{2}/[0-9]{4})\s*\(", text)
        meeting_race_count = None
        try:
            meeting_race_count = int(_first_str(r"(\d+)\s*場賽事", text) or "")
        except Exception:
            meeting_race_count = None

        track_surface = _first_str(r"(草地|全天候跑道|泥地)\s*,\s*\"[A-Z0-9\+]+\"\s*賽道", text)
        course_code = _first_str(r"(\"[A-Z0-9\+]+\")\s*賽道", text)

        turf_condition = _first_str(r"(?:草地場地|場地)\s*([^\s]+)", text)
        penetrometer = _first_float(r"度地儀指數\s*([0-9.]+)", text)
        temperature_c = _first_float(r"氣溫\s*([0-9.]+)\s*°C", text)
        humidity_pct = _first_float(r"相對濕度\s*([0-9.]+)\s*%", text)
        rainfall_total_mm = _first_float(r"總雨量\s*([0-9.]+)\s*毫米", text)
        rainfall_10min_mm = _first_float(r"最近10分鐘雨量\s*([0-9.]+)\s*毫米", text)
        soil_moisture_pct = _first_float(r"土壤濕度\s*([0-9.]+)\s*%", text)

        return {
            "last_updated_at": last_updated_at,
            "last_updated_raw": last_updated_raw,
            "meeting_date": meeting_date,
            "meeting_race_count": meeting_race_count,
            "venue": venue,
            "track_surface": track_surface,
            "course_code": course_code.strip('"') if course_code else None,
            "turf_condition": turf_condition,
            "penetrometer": penetrometer,
            "temperature_c": temperature_c,
            "humidity_pct": humidity_pct,
            "rainfall_total_mm": rainfall_total_mm,
            "rainfall_10min_mm": rainfall_10min_mm,
            "soil_moisture_pct": soil_moisture_pct,
        }
