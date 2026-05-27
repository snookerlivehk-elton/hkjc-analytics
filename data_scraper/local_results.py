import re
import time
import requests
from bs4 import BeautifulSoup
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs

from scoring_engine.normalization import normalize_course_type, surface_code, venue_code
from utils.logger import logger


class LocalResultsScraper:
    def __init__(self):
        self.base_url = "https://racing.hkjc.com/zh-hk/local/information/localresults"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        self.last_url = ""
        self.last_event_report_found = False

    def fetch(self, race_date: str, racecourse: str, race_no: int) -> str:
        url = f"{self.base_url}?racedate={race_date}&Racecourse={racecourse}&RaceNo={race_no}"
        html = ""
        for i in range(3):
            try:
                resp = requests.get(url, headers=self.headers, timeout=25)
                resp.raise_for_status()
                html = resp.text or ""
                try:
                    self.last_url = str(getattr(resp, "url", "") or "")
                except Exception:
                    self.last_url = ""
                break
            except Exception as e:
                if i < 2:
                    time.sleep(1.0 + i * 1.5)
                    continue
                raise

        low = html.lower()
        need_render = False
        if ("<!doctype html" in low) and (("名次" not in html) or ("馬號" not in html)):
            need_render = True
        if ("enable javascript" in low) or ("access denied" in low):
            need_render = True

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
                    if html2:
                        html = html2
                    try:
                        self.last_url = str(page.url or "")
                    except Exception:
                        pass
                    browser.close()
            except Exception as e:
                logger.warning(f"[LocalResultsScraper] playwright fallback failed url={url} err={type(e).__name__}: {e}")

        return html

    def scrape_single_race(self, race_date: str, racecourse: str, race_no: int) -> Dict[str, Any]:
        html = self.fetch(race_date=race_date, racecourse=racecourse, race_no=race_no)
        soup = BeautifulSoup(html, "lxml")

        meta = self._parse_meta(soup)
        if isinstance(meta, dict) and self.last_url:
            meta["final_url"] = str(self.last_url)
        results = self._parse_results_table(soup)
        dividends = self._parse_dividends(soup)
        event_report = self._parse_event_report(soup)

        return {
            "race_date": race_date,
            "racecourse": racecourse,
            "race_no": race_no,
            "meta": meta,
            "results": results,
            "dividends": dividends,
            "event_report": event_report,
            "event_report_found": bool(self.last_event_report_found),
        }

    def _url_params(self) -> Dict[str, str]:
        u = str(self.last_url or "").strip()
        if not u:
            return {}
        try:
            q = parse_qs(urlparse(u).query)
        except Exception:
            return {}
        out: Dict[str, str] = {}
        for k, v in (q or {}).items():
            kk = str(k or "").strip().lower()
            if not kk:
                continue
            if not isinstance(v, list) or not v:
                continue
            out[kk] = str(v[0] or "").strip()
        return out

    def _parse_meta(self, soup: BeautifulSoup) -> Dict[str, Any]:
        text = soup.get_text(separator=" ", strip=True)

        race_date_page = ""
        m = re.search(r"(賽事日期|賽事日子|racedate)\s*:\s*(\d{4}[/-]\d{2}[/-]\d{2}|\d{2}[/-]\d{2}[/-]\d{4})", text, re.IGNORECASE)
        if m:
            s = str(m.group(2) or "").strip().replace("-", "/")
            if s:
                try:
                    if re.match(r"^\d{4}/\d{2}/\d{2}$", s):
                        race_date_page = s
                    elif re.match(r"^\d{2}/\d{2}/\d{4}$", s):
                        dd, mm, yyyy = s.split("/")
                        race_date_page = f"{yyyy}/{mm}/{dd}"
                except Exception:
                    race_date_page = ""
        if not race_date_page:
            try:
                for tag in soup.find_all(["input", "select"]):
                    key = f"{str(tag.get('name') or '')} {str(tag.get('id') or '')}".lower()
                    if "racedate" not in key:
                        continue
                    cand = ""
                    if tag.name == "input":
                        cand = str(tag.get("value") or "").strip()
                    elif tag.name == "select":
                        opt = tag.find("option", selected=True) or tag.find("option")
                        if opt is not None:
                            cand = str(opt.get("value") or opt.get_text(" ", strip=True) or "").strip()
                    if not cand:
                        continue
                    m2 = re.search(r"(\d{4}[/-]\d{2}[/-]\d{2}|\d{2}[/-]\d{2}[/-]\d{4})", cand)
                    if not m2:
                        continue
                    s2 = str(m2.group(1) or "").strip().replace("-", "/")
                    if re.match(r"^\d{4}/\d{2}/\d{2}$", s2):
                        race_date_page = s2
                        break
                    if re.match(r"^\d{2}/\d{2}/\d{4}$", s2):
                        dd, mm, yyyy = s2.split("/")
                        race_date_page = f"{yyyy}/{mm}/{dd}"
                        break
            except Exception:
                pass
        if not race_date_page:
            qp = self._url_params()
            s3 = str(qp.get("racedate") or "").strip().replace("-", "/")
            if s3:
                try:
                    if re.match(r"^\d{4}/\d{2}/\d{2}$", s3):
                        race_date_page = s3
                    elif re.match(r"^\d{2}/\d{2}/\d{4}$", s3):
                        dd, mm, yyyy = s3.split("/")
                        race_date_page = f"{yyyy}/{mm}/{dd}"
                except Exception:
                    pass

        going = ""
        m = re.search(r"場地狀況\s*:\s*([^\s]+)", text)
        if m:
            going = m.group(1).strip()

        venue = ""
        venue = venue_code("HV" if (("跑馬地" in text) or ("Happy Valley" in text)) else ("ST" if (("沙田" in text) or ("Sha Tin" in text)) else ""))

        race_no_page = None
        m = re.search(r"第\s*(\d{1,2})\s*場", text)
        if m:
            try:
                race_no_page = int(m.group(1))
            except Exception:
                race_no_page = None
        if not race_no_page:
            qp = self._url_params()
            rn = str(qp.get("raceno") or "").strip()
            if rn:
                try:
                    race_no_page = int(rn)
                except Exception:
                    race_no_page = None

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

        sc = surface_code(surface, track_type, course_type)
        course_type = normalize_course_type(course_type, surface_code_=sc)

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
            "race_date_page": race_date_page,
            "venue": venue,
            "race_no_page": race_no_page,
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

        if header_row is None:
            return []

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

        if "horse_no" not in idx:
            return []

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

            rank_s = get_i("rank")
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

        rows = [r for r in rows if r.get("horse_no") and r.get("rank")]
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

    def _parse_event_report(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        self.last_event_report_found = False
        target = None
        for tag in soup.find_all(["h1", "h2", "h3", "h4", "div", "span", "p"]):
            s = tag.get_text(" ", strip=True)
            if not s:
                continue
            if "競賽事件報告" in s:
                t2 = tag.find_next("table")
                if t2 is not None:
                    target = t2
                    self.last_event_report_found = True
                    break

        if target is None:
            for table in soup.find_all("table"):
                first_tr = table.find("tr")
                if not first_tr:
                    continue
                headers = [c.get_text(" ", strip=True) for c in first_tr.find_all(["th", "td"])]
                header_norm = "".join(headers).replace(" ", "")
                if ("馬號" in header_norm) and ("描述" in header_norm or "事件" in header_norm):
                    target = table
                    self.last_event_report_found = True
                    break

        if target is None:
            return []

        out: List[Dict[str, Any]] = []
        for tr in target.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if not cells:
                continue
            cols = [c.get_text(" ", strip=True) for c in cells]
            if len(cols) < 2:
                continue
            c0 = str(cols[0] or "").strip()
            if not re.match(r"^\d+$", c0):
                continue
            try:
                horse_no = int(c0)
            except Exception:
                continue
            if horse_no <= 0:
                continue
            horse_name = ""
            desc = ""
            if len(cols) >= 3:
                horse_name = str(cols[1] or "").strip()
                desc = str(cols[-1] or "").strip()
            else:
                desc = str(cols[1] or "").strip()
            if not desc:
                continue
            out.append({"horse_no": int(horse_no), "horse_name": horse_name, "desc": desc})
        out.sort(key=lambda x: int(x.get("horse_no") or 0))
        return out

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
