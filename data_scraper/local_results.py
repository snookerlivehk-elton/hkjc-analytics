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

        track = ""
        m = re.search(r"賽道\s*:\s*([^\s]+)\s*-\s*\"([^\"]+)\"\s*賽道", text)
        if m:
            track = f"{m.group(1).strip()} - \"{m.group(2).strip()}\""

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

        return {"going": going, "track": track, "race_time": race_time, "sectional_times": sectional}

    def _find_results_table(self, soup: BeautifulSoup):
        for table in soup.find_all("table"):
            header_text = " ".join(th.get_text(strip=True) for th in table.find_all("th"))
            if ("名次" in header_text) and ("馬號" in header_text) and ("完成時間" in header_text):
                return table
        return None

    def _parse_results_table(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        table = self._find_results_table(soup)
        if table is None:
            return []

        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        header_idx = {h: i for i, h in enumerate(headers)}

        rows = []
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds or len(tds) < 5:
                continue
            cols = [td.get_text(" ", strip=True) for td in tds]

            def get_col(name: str) -> str:
                i = header_idx.get(name)
                if i is None or i >= len(cols):
                    return ""
                return cols[i]

            rank_s = get_col("名次") or cols[0]
            horse_no_s = get_col("馬號") or ""
            horse_name_s = get_col("馬名") or ""
            margin_s = get_col("頭馬距離") or ""
            pos_s = get_col("沿途走位") or ""
            finish_time_s = get_col("完成時間") or ""
            win_odds_s = get_col("獨贏賠率") or ""

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
        tables = soup.find_all("table")
        dividend_tables = []
        for table in tables:
            header_text = " ".join(th.get_text(strip=True) for th in table.find_all("th"))
            if ("彩池" in header_text and "派彩" in header_text) or ("派彩" in header_text and "組合" in header_text):
                dividend_tables.append(table)

        if not dividend_tables:
            return []

        items: List[Dict[str, Any]] = []
        for table in dividend_tables:
            headers = [th.get_text(strip=True) for th in table.find_all("th")]
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if not tds:
                    continue
                cols = [td.get_text(" ", strip=True) for td in tds]
                if len(cols) < 2:
                    continue

                row = {headers[i]: cols[i] for i in range(min(len(headers), len(cols)))}

                pool = row.get("彩池") or row.get("池") or cols[0]
                combo = row.get("組合") or row.get("馬號") or cols[1]
                dividend = row.get("派彩") or row.get("派彩($)") or (cols[2] if len(cols) > 2 else "")
                unit = row.get("每注") or row.get("每注($)") or ""

                pool = pool.strip()
                combo = combo.strip()
                if not pool or not combo:
                    continue

                items.append(
                    {
                        "pool": pool,
                        "combination": combo,
                        "dividend": self._to_float(dividend),
                        "unit": unit.strip(),
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
