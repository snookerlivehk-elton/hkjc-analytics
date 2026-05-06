import re
import requests
from bs4 import BeautifulSoup
from typing import Any, Dict, List, Optional


class LocalResultsScraper:
    def __init__(self):
        self.base_url = "https://racing.hkjc.com/zh-hk/local/information/localresults"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        }

    def fetch(self, race_date: str, racecourse: str, race_no: int) -> str:
        url = f"{self.base_url}?racedate={race_date}&Racecourse={racecourse}&RaceNo={race_no}"
        resp = requests.get(url, headers=self.headers, timeout=20)
        resp.raise_for_status()
        return resp.text

    def scrape_single_race(self, race_date: str, racecourse: str, race_no: int) -> Dict[str, Any]:
        html = self.fetch(race_date=race_date, racecourse=racecourse, race_no=race_no)
        soup = BeautifulSoup(html, "lxml")

        meta = self._parse_meta(soup)
        results = self._parse_results_table(soup)
        dividends = self._parse_dividends(soup)

        return {
            "race_date": race_date,
            "racecourse": racecourse,
            "race_no": race_no,
            "meta": meta,
            "results": results,
            "dividends": dividends,
        }

    def _parse_meta(self, soup: BeautifulSoup) -> Dict[str, Any]:
        text = soup.get_text(separator=" ", strip=True)

        going = ""
        m = re.search(r"場地狀況\s*:\s*([^\s]+)", text)
        if m:
            going = m.group(1).strip()

        venue = ""
        if ("跑馬地" in text) or ("Happy Valley" in text):
            venue = "HV"
        elif ("沙田" in text) or ("Sha Tin" in text):
            venue = "ST"

        distance = 0
        m = re.search(r"(\d{3,4})\s*米", text)
        if m:
            try:
                distance = int(m.group(1))
            except Exception:
                distance = 0

        surface = ""
        course_type = ""
        track_type = ""
        if ("全天候" in text) or ("All Weather" in text) or ("A/W" in text) or ("AWT" in text):
            surface = "泥地"
            course_type = "AWT"
            if venue == "HV":
                track_type = "跑馬地全天候"
            elif venue == "ST":
                track_type = "沙田全天候"
            else:
                track_type = "全天候"
        else:
            surface = "草地"
            m = re.search(r"賽道\s*:\s*([^\s]+)\s*-\s*\"([^\"]+)\"\s*賽道", text)
            if m:
                venue_txt = m.group(1).strip()
                course_txt = m.group(2).strip()
                course_type = course_txt
                track_type = f"{venue_txt}草地\"{course_txt}\""
            else:
                track_type = ""

        times = re.findall(r"\(\s*(\d+:\d{2}\.\d{2}|\d+\.\d{2})\s*\)", text)
        race_time = ""
        if times:
            for v in reversed(times):
                if ":" in v:
                    race_time = v
                    break
            if not race_time:
                race_time = times[-1]

        sectional = []
        seg = re.search(r"分段時間\s*:\s*([0-9\.\s]+)", text)
        if seg:
            for v in re.findall(r"\d+\.\d{2}", seg.group(1)):
                try:
                    sectional.append(float(v))
                except ValueError:
                    pass

        return {
            "venue": venue,
            "distance": distance,
            "surface": surface,
            "course_type": course_type,
            "track_type": track_type,
            "going": going,
            "track": track_type,
            "race_time": race_time,
            "sectional_times": sectional,
        }

    def _find_results_table(self, soup: BeautifulSoup):
        cand = soup.select_one(".performance table")
        if cand is not None:
            return cand

        for table in soup.find_all("table"):
            first_tr = table.find("tr")
            if not first_tr:
                continue
            header_cells = [c.get_text(" ", strip=True) for c in first_tr.find_all(["th", "td"])]
            header_norm = "".join(header_cells).replace(" ", "")
            if ("名次" in header_norm) and ("馬號" in header_norm) and (("完成時間" in header_norm) or ("完成" in header_norm and "時間" in header_norm)):
                return table
        return None

    def _parse_results_table(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        table = self._find_results_table(soup)
        if table is None:
            return []

        header_row = None
        for tr in table.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            if not cells:
                continue
            norm = "".join(cells).replace(" ", "")
            if ("名次" in norm) and ("馬號" in norm):
                header_row = cells
                break

        headers = header_row or []
        header_norms = [h.replace(" ", "") for h in headers]
        idx = {}
        for i, h in enumerate(header_norms):
            if h == "名次":
                idx["rank"] = i
            elif h == "馬號":
                idx["horse_no"] = i
            elif h.startswith("馬名"):
                idx["horse_name"] = i
            elif ("頭馬" in h) and ("距離" in h):
                idx["margin"] = i
            elif ("沿途" in h) and ("走位" in h):
                idx["running_position"] = i
            elif ("完成" in h) and ("時間" in h):
                idx["finish_time"] = i
            elif ("獨贏" in h) and ("賠率" in h):
                idx["win_odds"] = i

        rows = []
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            cols = [td.get_text(" ", strip=True) for td in tds]

            def get_i(key: str) -> str:
                i = idx.get(key)
                if i is None or i >= len(cols):
                    return ""
                return cols[i]

            rank_s = get_i("rank") or (cols[0] if cols else "")
            horse_no_s = get_i("horse_no")
            horse_name_s = get_i("horse_name")
            margin_s = get_i("margin")
            pos_s = get_i("running_position")
            finish_time_s = get_i("finish_time")
            win_odds_s = get_i("win_odds")

            horse_code = ""
            m = re.search(r"\(([A-Z]\d{3})\)", horse_name_s)
            if m:
                horse_code = m.group(1)
                horse_name_s = re.sub(r"\s*\([A-Z]\d{3}\)\s*", "", horse_name_s).strip()

            rows.append(
                {
                    "rank": self._to_int(rank_s),
                    "horse_no": self._to_int(horse_no_s),
                    "horse_name": horse_name_s,
                    "horse_code": horse_code,
                    "margin": margin_s.strip(),
                    "running_position": pos_s.strip(),
                    "finish_time": finish_time_s.strip(),
                    "win_odds": self._to_float(win_odds_s),
                }
            )

        rows = [r for r in rows if r.get("horse_no")]
        return rows

    def _parse_dividends(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        table = soup.select_one(".dividend_tab table")
        if table is None:
            return []

        items: List[Dict[str, Any]] = []
        current_pool = ""
        for tr in table.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            if not cells:
                continue

            if len(cells) == 1:
                if "派彩備註" in cells[0]:
                    break
                continue

            norm0 = cells[0].replace(" ", "")
            if norm0 in ("彩池", "勝出組合", "派彩(HK$)", "派彩"):
                continue

            if len(cells) == 3:
                current_pool = cells[0].strip()
                combo = cells[1].strip()
                dividend = cells[2].strip()
            elif len(cells) == 2:
                if not current_pool:
                    continue
                combo = cells[0].strip()
                dividend = cells[1].strip()
            else:
                continue

            if not current_pool or not combo:
                continue

            items.append(
                {
                    "pool": current_pool,
                    "combination": combo,
                    "dividend": self._to_float(dividend),
                    "unit": "HK$",
                }
            )

        return items

    def _to_int(self, s: str) -> int:
        try:
            return int(re.sub(r"\D", "", str(s)))
        except Exception:
            return 0

    def _to_float(self, s: str) -> Optional[float]:
        v = str(s or "").strip().replace(",", "")
        if not v:
            return None
        m = re.search(r"(\d+(?:\.\d+)?)", v)
        if not m:
            return None
        try:
            return float(m.group(1))
        except ValueError:
            return None
