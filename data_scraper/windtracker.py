from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from scoring_engine.normalization import venue_code


class WindTrackerScraper:
    def __init__(self):
        self.url = "https://racing.hkjc.com/zh-hk/local/info/windtracker"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        }

    def fetch(self) -> str:
        resp = requests.get(self.url, headers=self.headers, timeout=25)
        resp.raise_for_status()
        return resp.text

    async def _fetch_rendered_async(self) -> str:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
            )
            context = await browser.new_context(
                user_agent=str(self.headers.get("User-Agent") or ""),
                viewport={"width": 1920, "height": 1080},
            )
            page = await context.new_page()
            await page.goto(self.url, wait_until="networkidle", timeout=45000)
            await page.wait_for_timeout(1500)
            html = await page.content()
            await browser.close()
            return html

    def scrape_latest(self) -> Dict[str, Any]:
        html = self.fetch()
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(separator=" ", strip=True)
        if ("最後更新" not in text) or ("風向" not in text):
            try:
                html = asyncio.run(self._fetch_rendered_async())
                soup = BeautifulSoup(html, "lxml")
                text = soup.get_text(separator=" ", strip=True)
            except Exception:
                pass

        ds = self._extract_date(text)
        venue = self._extract_venue(text)
        updated_at = self._extract_updated_at(text)
        metrics = self._extract_metrics(text)
        winds = self._extract_winds(text)

        return {
            "race_date": ds,
            "venue": venue,
            "updated_at": updated_at,
            "metrics": metrics,
            "winds": winds,
        }

    def _extract_updated_at(self, text: str) -> str:
        m = re.search(r"最後更新\s*[:：]\s*([0-9/]+\s*[0-9:]+)", text)
        return str(m.group(1) or "").strip() if m else ""

    def _extract_date(self, text: str) -> str:
        m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
        if not m:
            return ""
        dd, mm, yyyy = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        return f"{yyyy}/{mm}/{dd}"

    def _extract_venue(self, text: str) -> str:
        m = re.search(r",\s*(跑馬地|沙田)\s*,", text)
        if m:
            return venue_code("HV" if str(m.group(1) or "") == "跑馬地" else "ST")
        if "跑馬地" in text and "沙田" not in text:
            return venue_code("HV")
        if "沙田" in text and "跑馬地" not in text:
            return venue_code("ST")
        return ""

    def _extract_metrics(self, text: str) -> Dict[str, Any]:
        def f(pat: str) -> Optional[float]:
            m = re.search(pat, text)
            if not m:
                return None
            try:
                return float(str(m.group(1) or "").strip())
            except Exception:
                return None

        def mm(pat: str) -> Optional[float]:
            m = re.search(pat, text)
            if not m:
                return None
            try:
                return float(str(m.group(1) or "").strip())
            except Exception:
                return None

        return {
            "temperature_c": f(r"氣溫\s*([0-9.]+)\s*(?:°C|℃|度)"),
            "humidity_pct": f(r"(?:相對濕度|濕度)\s*([0-9.]+)\s*%"),
            "rain_total_mm": mm(r"總雨量\s*([0-9.]+)\s*(?:毫米|mm|MM)"),
            "rain_10min_mm": mm(r"(?:最近\s*10\s*分鐘雨量|10\s*分鐘雨量|最近10分鐘雨量)\s*([0-9.]+)\s*(?:毫米|mm|MM)"),
            "soil_moisture_pct": f(r"土壤濕度\s*([0-9.]+)\s*%"),
        }

    def _extract_winds(self, text: str) -> List[Dict[str, Any]]:
        pairs = re.findall(r"([東西南北]{1,2}(?:偏[東西南北])?)\s*([0-9.]+)\s*公里/小時", text)
        out: List[Dict[str, Any]] = []
        for i, (direction, speed) in enumerate(pairs, 1):
            try:
                v = float(speed)
            except Exception:
                v = None
            out.append({"idx": i, "direction": str(direction).strip(), "speed_kmh": v})
        return out
