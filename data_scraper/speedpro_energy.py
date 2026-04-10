import re
from typing import Dict, Any

import pandas as pd
import requests


class SpeedProEnergyScraper:
    def __init__(self):
        self.base_url = "https://racing.hkjc.com/zh-hk/local/info/speedpro/speedguide"

    def _parse_float(self, v):
        s = str(v or "").strip()
        if not s:
            return None
        s = s.replace(",", "")
        m = re.search(r"-?\d+(?:\.\d+)?", s)
        if not m:
            return None
        try:
            return float(m.group(0))
        except Exception:
            return None

    def _parse_horse_no(self, v):
        s = str(v or "").strip()
        m = re.search(r"\b(\d{1,2})\b", s)
        if not m:
            return None
        try:
            return int(m.group(1))
        except Exception:
            return None

    def _parse_status_rating(self, v):
        s = str(v or "").strip()
        if not s:
            return None
        f = self._parse_float(s)
        if f is not None:
            return float(f)
        m = re.search(r"[A-Ea-e]", s)
        if m:
            return {"A": 5.0, "B": 4.0, "C": 3.0, "D": 2.0, "E": 1.0}.get(m.group(0).upper(), None)
        return None

    def scrape(self, race_no: int) -> Dict[int, Dict[str, Any]]:
        r = requests.get(
            self.base_url,
            params={"raceno": int(race_no)},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30,
        )
        r.raise_for_status()
        html = r.text or ""

        if "排位日" in html and "當晚公佈" in html:
            return {}

        try:
            tables = pd.read_html(html)
        except Exception:
            return {}

        target = None
        for df in tables:
            cols = [str(c) for c in df.columns]
            if any("能量所需" in c for c in cols) and any(("速勢能量" in c) or ("能量評估" in c) for c in cols):
                target = df
                break
        if target is None:
            return {}

        col_horse = None
        col_need = None
        col_rating = None
        col_assess = None
        for c in target.columns:
            cs = str(c)
            if col_horse is None and ("馬號" in cs or cs.strip() in ("號", "馬")):
                col_horse = c
            if col_need is None and "能量所需" in cs:
                col_need = c
            if col_rating is None and ("狀態評級" in cs or "狀態" in cs):
                col_rating = c
            if col_assess is None and ("速勢能量評估" in cs or ("能量評估" in cs and "所需" not in cs)):
                col_assess = c

        if col_horse is None:
            col_horse = target.columns[0]

        out: Dict[int, Dict[str, Any]] = {}
        for _, row in target.iterrows():
            horse_no = self._parse_horse_no(row.get(col_horse))
            if not horse_no:
                continue
            energy_required = self._parse_float(row.get(col_need)) if col_need is not None else None
            status_rating = self._parse_status_rating(row.get(col_rating)) if col_rating is not None else None
            energy_assess = self._parse_float(row.get(col_assess)) if col_assess is not None else None
            diff = None
            if energy_required is not None and energy_assess is not None:
                diff = float(energy_required) - float(energy_assess)
            out[int(horse_no)] = {
                "energy_required": energy_required,
                "status_rating": status_rating,
                "energy_assess": energy_assess,
                "energy_diff": diff,
            }

        return out

