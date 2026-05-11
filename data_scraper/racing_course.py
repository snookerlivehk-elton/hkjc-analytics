from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup


class RacingCourseScraper:
    def __init__(self):
        self.url = "https://racing.hkjc.com/zh-hk/local/page/racing-course"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        }

    def fetch(self) -> str:
        resp = requests.get(self.url, headers=self.headers, timeout=25)
        resp.raise_for_status()
        return resp.text

    def scrape(self) -> Dict[str, Any]:
        html = self.fetch()
        soup = BeautifulSoup(html, "lxml")
        tables = soup.find_all("table")
        track_dims = self._parse_track_dimensions(tables)
        going_maps = self._parse_going_maps(tables)
        measurement_points = self._parse_measurement_points(tables)
        return {
            "track_dimensions": track_dims,
            "going_maps": going_maps,
            "measurement_points": measurement_points,
        }

    def _parse_track_dimensions(self, tables) -> Dict[str, List[Dict[str, Any]]]:
        out: Dict[str, List[Dict[str, Any]]] = {}
        for t in tables:
            header = self._table_headers(t)
            if not header:
                continue
            if ("跑道" not in header[0]) or (not any("直路" in h for h in header)) or (not any("闊度" in h for h in header)):
                continue
            venue = self._guess_venue_name(t) or "UNKNOWN"
            rows = []
            for tr in t.find_all("tr")[1:]:
                cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
                if len(cells) < 4:
                    continue
                code = str(cells[0] or "").strip().rstrip(":")
                label = str(cells[1] or "").strip()
                straight_m = self._to_int_meters(cells[2])
                width_m = self._to_float_meters(cells[3])
                if not code:
                    continue
                rows.append({"course_type": code, "label": label, "straight_m": straight_m, "width_m": width_m})
            if rows:
                out.setdefault(venue, []).extend(rows)
        return out

    def _parse_going_maps(self, tables) -> Dict[str, Any]:
        turf: List[Dict[str, Any]] = []
        awt: List[Dict[str, Any]] = []
        for t in tables:
            header = self._table_headers(t)
            if not header:
                continue
            if any("度地儀指數" in h for h in header) and any("場地狀況" in h for h in header):
                for tr in t.find_all("tr")[1:]:
                    cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
                    if len(cells) < 2:
                        continue
                    going = str(cells[0] or "").strip()
                    code = str(cells[1] or "").strip()
                    rng = str(cells[2] or "").strip() if len(cells) > 2 else ""
                    if not going:
                        continue
                    turf.append({"going": going, "going_code": code, "penetrometer_range": rng})
            elif any("全天候跑道" in h for h in header) and (len(header) <= 2 or any("快地" in x for x in " ".join(header))):
                text = t.get_text(" ", strip=True)
                pairs = re.findall(r"\b([A-Z]{2})\b\s*[:：]?\s*([^\s]+地|封地|受天雨影響|例常灑水)", text)
                for code, label in pairs:
                    awt.append({"going_code": str(code).strip(), "label": str(label).strip()})
        return {"turf": turf, "awt": awt}

    def _parse_measurement_points(self, tables) -> Dict[str, List[Dict[str, Any]]]:
        out: Dict[str, List[Dict[str, Any]]] = {}
        for t in tables:
            header = self._table_headers(t)
            if not header:
                continue
            if len(header) < 2:
                continue
            if not re.match(r"^\d+$", str(header[0] or "").strip()):
                continue
            if not any("m" in str(h or "") for h in header):
                continue

            name = self._guess_measurement_name(t) or "UNKNOWN"
            rows: List[Dict[str, Any]] = []
            for tr in t.find_all("tr")[1:]:
                cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
                if len(cells) < 3:
                    continue
                try:
                    idx = int(str(cells[0] or "").strip())
                except Exception:
                    continue
                loc = str(cells[1] or "").strip()
                dist_m = self._to_int_meters(cells[2])
                rows.append({"idx": idx, "location": loc, "distance_to_finish_m": dist_m})
            if rows:
                out[name] = rows
        return out

    def _table_headers(self, table) -> List[str]:
        tr = table.find("tr")
        if not tr:
            return []
        return [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]

    def _guess_venue_name(self, table) -> Optional[str]:
        prev = table.find_previous(["h1", "h2", "h3", "h4"])
        if prev is not None:
            t = prev.get_text(" ", strip=True)
            t = re.sub(r"\s+", " ", str(t or "")).strip()
            if t:
                return t
        return None

    def _guess_measurement_name(self, table) -> Optional[str]:
        prev = table.find_previous(["u", "strong", "h3", "h4", "h5"])
        if prev is not None:
            t = prev.get_text(" ", strip=True)
            t = re.sub(r"\s+", " ", str(t or "")).strip()
            if t:
                return t
        return None

    def _to_int_meters(self, s: Any) -> Optional[int]:
        t = str(s or "").strip()
        if t in {"-", "—", ""}:
            return None
        m = re.search(r"(\d+)", t.replace(",", ""))
        if not m:
            return None
        try:
            return int(m.group(1))
        except Exception:
            return None

    def _to_float_meters(self, s: Any) -> Optional[float]:
        t = str(s or "").strip()
        if t in {"-", "—", ""}:
            return None
        m = re.search(r"(\d+(?:\.\d+)?)", t.replace(",", ""))
        if not m:
            return None
        try:
            return float(m.group(1))
        except Exception:
            return None
