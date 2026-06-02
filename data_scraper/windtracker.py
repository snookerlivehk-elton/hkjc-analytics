from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from scoring_engine.normalization import venue_code
from utils.logger import logger


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
            try:
                for i in range(2):
                    try:
                        await page.goto(self.url, wait_until="domcontentloaded", timeout=45000)
                        try:
                            await page.wait_for_function(
                                "document.body && document.body.innerText && document.body.innerText.includes('最後更新')",
                                timeout=15000,
                            )
                        except Exception:
                            pass
                        await page.wait_for_timeout(800)
                        return await page.content()
                    except PlaywrightTimeoutError as e:
                        if i >= 1:
                            raise e
                        await page.wait_for_timeout(1200)
                        continue
            finally:
                await browser.close()

    def scrape_latest(self) -> Dict[str, Any]:
        def _parse(html0: str):
            soup0 = BeautifulSoup(html0, "lxml")
            text0 = soup0.get_text(separator=" ", strip=True)
            ds0 = self._extract_date(text0)
            venue0 = self._extract_venue(text0)
            updated_at0 = self._extract_updated_at(text0)
            metrics0 = self._extract_metrics(text0)
            winds0 = self._extract_winds(text0)
            return text0, ds0, venue0, updated_at0, metrics0, winds0

        fetch_mode = "requests"
        render_error = ""

        html = self.fetch()
        text, ds, venue, updated_at, metrics, winds = _parse(html)

        need_render = False
        if ("最後更新" not in text) or ("風向" not in text) or (not ds) or (not venue):
            need_render = True
        else:
            try:
                has_metric = any(v is not None for v in (metrics or {}).values())
            except Exception:
                has_metric = False
            if not has_metric and not winds:
                need_render = True

        if need_render:
            try:
                html2 = asyncio.run(self._fetch_rendered_async())
                text, ds, venue, updated_at, metrics, winds = _parse(html2)
                fetch_mode = "playwright"
            except Exception as e:
                render_error = f"{type(e).__name__}: {e}"
                logger.warning(f"[WindTrackerScraper] rendered fetch failed err={render_error}")

        out = {
            "race_date": ds,
            "venue": venue,
            "updated_at": updated_at,
            "metrics": metrics,
            "winds": winds,
            "_fetch_mode": fetch_mode,
        }
        if render_error:
            out["_render_error"] = render_error
        return out

    def _extract_updated_at(self, text: str) -> str:
        m = re.search(r"最後更新\s*[:：]\s*([0-9/]+\s*[0-9:]+)", text)
        return str(m.group(1) or "").strip() if m else ""

    def _extract_date(self, text: str) -> str:
        m = re.search(r"\b(\d{4})[/-](\d{1,2})[/-](\d{1,2})\b", text)
        if m:
            yyyy, mm, dd = m.group(1), m.group(2).zfill(2), m.group(3).zfill(2)
            return f"{yyyy}/{mm}/{dd}"
        m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", text)
        if not m:
            return ""
        dd, mm, yyyy = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        return f"{yyyy}/{mm}/{dd}"

    def _extract_venue(self, text: str) -> str:
        m = re.search(r"(跑馬地|沙田)\s*(?:馬場)?\s*風(?:向|速)", text)
        if m:
            return venue_code("HV" if str(m.group(1) or "") == "跑馬地" else "ST")
        m = re.search(r",\s*(跑馬地|沙田)\s*,", text)
        if m:
            return venue_code("HV" if str(m.group(1) or "") == "跑馬地" else "ST")
        ds = self._extract_date(text)
        if ds:
            try:
                i = text.find(ds.replace("/", "-"))
                if i < 0:
                    i = text.find(ds)
                if i >= 0:
                    seg = text[max(0, i - 80) : i + 160]
                    has_hv = "跑馬地" in seg
                    has_st = "沙田" in seg
                    if has_hv and (not has_st):
                        return venue_code("HV")
                    if has_st and (not has_hv):
                        return venue_code("ST")
            except Exception:
                pass
        try:
            iu = text.find("最後更新")
            if iu >= 0:
                seg = text[max(0, iu - 200) : iu + 200]
                has_hv = "跑馬地" in seg
                has_st = "沙田" in seg
                if has_hv and (not has_st):
                    return venue_code("HV")
                if has_st and (not has_hv):
                    return venue_code("ST")
        except Exception:
            pass
        has_hv = "跑馬地" in text
        has_st = "沙田" in text
        if has_hv and (not has_st):
            return venue_code("HV")
        if has_st and (not has_hv):
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
            "temperature_c": f(r"氣溫\s*[:：]?\s*([0-9.]+)\s*(?:°C|℃|度)"),
            "humidity_pct": f(r"(?:相對濕度|濕度)\s*[:：]?\s*([0-9.]+)\s*%"),
            "rain_total_mm": mm(r"總雨量\s*[:：]?\s*([0-9.]+)\s*(?:毫米|mm|MM)"),
            "rain_10min_mm": mm(r"(?:最近\s*10\s*分鐘雨量|10\s*分鐘雨量|最近10分鐘雨量)\s*[:：]?\s*([0-9.]+)\s*(?:毫米|mm|MM)"),
            "soil_moisture_pct": f(r"土壤濕度\s*[:：]?\s*([0-9.]+)\s*%"),
        }

    def _extract_winds(self, text: str) -> List[Dict[str, Any]]:
        pairs = re.findall(r"([東西南北]{1,2}(?:偏[東西南北])?)\s*([0-9.]+)\s*(?:公里/小時|km/h|KM/H)", text)
        out: List[Dict[str, Any]] = []
        for i, (direction, speed) in enumerate(pairs, 1):
            try:
                v = float(speed)
            except Exception:
                v = None
            out.append({"idx": i, "direction": str(direction).strip(), "speed_kmh": v})
        return out
