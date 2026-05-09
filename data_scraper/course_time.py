from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from sqlalchemy.orm import Session

from database.models import SystemConfig
from scoring_engine.config_value import build_meta, wrap_value


COURSE_TIME_URL = "https://racing.hkjc.com/zh-hk/local/page/racing-course-time"
COURSE_TIME_CFG_KEY = "course_time_reference:v1"


def _utc_iso(ms: Optional[int]) -> Optional[str]:
    if not ms:
        return None
    try:
        dt = datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None


def _val(node: Any) -> str:
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    if isinstance(node, (int, float)):
        return str(node)
    if isinstance(node, dict):
        if "value" in node:
            return _val(node.get("value"))
        if "displayLabel" in node and isinstance(node.get("displayLabel"), dict):
            return _val(node["displayLabel"].get("value"))
        if "targetItem" in node and isinstance(node.get("targetItem"), dict):
            return _val(node.get("targetItem"))
        if "label" in node and isinstance(node.get("label"), dict):
            return _val(node.get("label"))
    return ""


def _parse_time_to_seconds(s: str) -> Optional[float]:
    v = str(s or "").strip().replace(" ", "").replace("．", ".").replace("：", ":")
    if not v or v == "-" or v.upper() == "N/A":
        return None
    if ":" in v:
        parts = v.split(":")
        if len(parts) != 2:
            return None
        try:
            m = int(parts[0])
            sec = float(parts[1])
        except Exception:
            return None
        return (m * 60.0 + sec) if (m >= 0 and sec > 0) else None
    if v.count(".") >= 2:
        p = v.split(".")
        try:
            m = int(p[0])
            s2 = int(p[1])
            frac = int(p[2])
        except Exception:
            return None
        if s2 < 0 or s2 >= 60:
            return None
        return m * 60.0 + s2 + (frac / (100.0 if frac >= 10 else 10.0))
    try:
        sec = float(v)
    except Exception:
        return None
    return sec if sec > 0 else None


def _track_key(display_title: str) -> Optional[Tuple[str, str]]:
    t = str(display_title or "")
    venue = "HV" if ("跑馬地" in t or "HV" in t) else "ST"
    if "全天候" in t or "A/W" in t or "AWT" in t:
        surface = "AW"
    elif "草" in t or "TURF" in t:
        surface = "TURF"
    else:
        surface = "U"
    return venue, surface


def _extract_payload_object(html: str, keyword: str) -> Optional[Dict[str, Any]]:
    h = str(html or "")
    if not h:
        return None
    key = str(keyword or "").strip()
    if not key:
        return None
    i = h.find(key)
    if i < 0:
        return None
    j = h.rfind("self.__next_f.push([1,\"", 0, i)
    if j < 0:
        return None
    q1 = h.find("\"", j)  # first quote after push(
    if q1 < 0:
        return None
    q2 = q1 + 1
    esc = []
    while q2 < len(h):
        ch = h[q2]
        if ch == "\\":
            if q2 + 1 < len(h):
                esc.append(ch)
                esc.append(h[q2 + 1])
                q2 += 2
                continue
        if ch == "\"":
            break
        esc.append(ch)
        q2 += 1
    if q2 >= len(h) or h[q2] != "\"":
        return None
    escaped = "".join(esc)
    try:
        decoded = json.loads("\"" + escaped + "\"")
        if key not in decoded:
            return None
        arr = json.loads(decoded.split(":", 1)[1])
        for x in reversed(arr):
            if isinstance(x, dict):
                return x
    except Exception:
        return None
    return None


def _extract_payload_object_fallback(html: str, keyword: str) -> Optional[Dict[str, Any]]:
    h = str(html or "")
    key = str(keyword or "").strip()
    if not h or not key:
        return None
    i = h.find(key)
    if i < 0:
        return None
    anchors = [m.start() for m in re.finditer(r"self\.__next_f\.push\(\[1,\"", h)]
    if not anchors:
        return None
    anchors = [a for a in anchors if a < i]
    anchors.sort(reverse=True)
    for j in anchors[:20]:
        q1 = h.find("\"", j)
        if q1 < 0:
            continue
        q2 = q1 + 1
        esc = []
        while q2 < len(h):
            ch = h[q2]
            if ch == "\\":
                if q2 + 1 < len(h):
                    esc.append(ch)
                    esc.append(h[q2 + 1])
                    q2 += 2
                    continue
            if ch == "\"":
                break
            esc.append(ch)
            q2 += 1
        if q2 >= len(h) or h[q2] != "\"":
            continue
        escaped = "".join(esc)
        try:
            decoded = json.loads("\"" + escaped + "\"")
            if key not in decoded:
                continue
            arr = json.loads(decoded.split(":", 1)[1])
            for x in reversed(arr):
                if isinstance(x, dict):
                    return x
        except Exception:
            continue
    return None


class CourseTimeScraper:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept-Language": "zh-HK,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://racing.hkjc.com/",
        }

    def scrape(self) -> Dict[str, Any]:
        resp = self.session.get(COURSE_TIME_URL, headers=self.headers, timeout=30)
        html = resp.text or ""

        std_obj = _extract_payload_object(html, "StandardTimes") or _extract_payload_object_fallback(html, "StandardTimes") or {}
        sec_obj = _extract_payload_object(html, "ReferenceSectionalTimes") or _extract_payload_object_fallback(html, "ReferenceSectionalTimes") or {}

        std_data = (std_obj.get("standardTimeData") if isinstance(std_obj, dict) else {}) or {}
        sec_data = (sec_obj.get("sectionalData") if isinstance(sec_obj, dict) else {}) or {}

        standard_update = _utc_iso((std_data.get("updateDate") or {}).get("dateValue") if isinstance(std_data.get("updateDate"), dict) else None)
        sectional_update = _utc_iso((sec_data.get("updateDate") or {}).get("dateValue") if isinstance(sec_data.get("updateDate"), dict) else None)

        out_std: Dict[str, Any] = {}
        out_sec: Dict[str, Any] = {}

        for track_node in (std_data.get("children") or []):
            title = _val((track_node or {}).get("displayTitle"))
            tk = _track_key(title)
            if not tk:
                continue
            venue, surface = tk
            tkey = f"{venue}:{surface}"
            for dist_node in ((track_node or {}).get("children") or []):
                dist = _val((dist_node or {}).get("distance"))
                try:
                    dist_i = int(dist)
                except Exception:
                    continue
                for row in ((dist_node or {}).get("children") or []):
                    cls = _val((row or {}).get("class"))
                    st = _val((row or {}).get("standardTime"))
                    st_sec = _parse_time_to_seconds(st)
                    if not cls or st_sec is None:
                        continue
                    out_std.setdefault(tkey, {}).setdefault(str(dist_i), {})[cls] = {"standard_time_sec": float(st_sec)}

        for track_node in (sec_data.get("children") or []):
            title = _val((track_node or {}).get("displayTitle"))
            tk = _track_key(title)
            if not tk:
                continue
            venue, surface = tk
            tkey = f"{venue}:{surface}"
            for dist_node in ((track_node or {}).get("children") or []):
                dist = _val((dist_node or {}).get("distance"))
                try:
                    dist_i = int(dist)
                except Exception:
                    continue
                for row in ((dist_node or {}).get("children") or []):
                    cls = _val((row or {}).get("class"))
                    st = _val((row or {}).get("standardTime")) or _val((row or {}).get("standardTimes"))
                    st_sec = _parse_time_to_seconds(st)
                    segs: List[float] = []
                    labels: List[str] = []
                    for k2, v2 in (row or {}).items():
                        kk2 = str(k2 or "")
                        if kk2 in {"versions", "class", "standardTime", "standardTimes"}:
                            continue
                        if not isinstance(v2, dict):
                            continue
                        tv = _val(v2)
                        secv = _parse_time_to_seconds(tv)
                        if secv is None:
                            continue
                        labels.append(kk2)
                        segs.append(float(secv))
                    if not cls or st_sec is None or not segs:
                        continue
                    out_sec.setdefault(tkey, {}).setdefault(str(dist_i), {})[cls] = {
                        "standard_time_sec": float(st_sec),
                        "segment_labels": labels,
                        "segment_times_sec": segs,
                    }

        if not out_std and out_sec:
            for tkey, dist_map in out_sec.items():
                if not isinstance(dist_map, dict):
                    continue
                for dist_k, cls_map in dist_map.items():
                    if not isinstance(cls_map, dict):
                        continue
                    for cls_k, row in cls_map.items():
                        if not isinstance(row, dict):
                            continue
                        st_sec = row.get("standard_time_sec")
                        if st_sec is None:
                            continue
                        out_std.setdefault(str(tkey), {}).setdefault(str(dist_k), {})[str(cls_k)] = {"standard_time_sec": float(st_sec)}

        return {
            "ok": True,
            "source_url": COURSE_TIME_URL,
            "standard_update_at": standard_update or sectional_update,
            "sectional_update_at": sectional_update,
            "fetched_at": datetime.utcnow().isoformat(),
            "standard_times": out_std,
            "reference_sectionals": out_sec,
        }

    def update_system_config(self, db_session: Session) -> Dict[str, Any]:
        payload = self.scrape()
        if not isinstance(payload, dict) or payload.get("ok") is not True:
            return {"ok": False}
        cfg = db_session.query(SystemConfig).filter_by(key=COURSE_TIME_CFG_KEY).first()
        if not cfg:
            cfg = SystemConfig(key=COURSE_TIME_CFG_KEY, description="跑道標準時間/參考分段時間（HKJC racing-course-time）")
            db_session.add(cfg)
        payload_clean = json.loads(json.dumps(payload, ensure_ascii=False))
        cfg.value = wrap_value(
            payload_clean,
            build_meta(
                source="COURSE_TIME",
                fetched_at=str(payload_clean.get("fetched_at") or "").strip() or None,
                url=COURSE_TIME_URL,
                schema="course_time_reference:v1",
                extra={
                    "standard_update_at": str(payload_clean.get("standard_update_at") or "").strip() or None,
                    "sectional_update_at": str(payload_clean.get("sectional_update_at") or "").strip() or None,
                },
            ),
        )
        db_session.commit()
        return {"ok": True, "key": COURSE_TIME_CFG_KEY, "standard_tracks": len(payload.get("standard_times") or {}), "sectional_tracks": len(payload.get("reference_sectionals") or {})}
