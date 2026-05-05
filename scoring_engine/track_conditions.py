from __future__ import annotations

import re
from typing import Tuple


def normalize_going(raw: str) -> Tuple[str, str]:
    s_display = str(raw or "").strip()
    s_display = re.sub(r"\s+", "", s_display)
    if not s_display:
        return "", ""

    m = {
        "好地": "G",
        "好快": "GF",
        "好至快": "GF",
        "快地": "F",
        "黏地": "Y",
        "黏至軟": "YS",
        "軟至黏": "YS",
        "軟地": "S",
        "大爛地": "H",
        "濕快": "WET_FAST",
        "濕慢": "WET_SLOW",
    }
    s_key = s_display
    if (s_key not in m) and s_key.endswith("地") and (s_key[:-1] in m):
        s_key = s_key[:-1]
    code = m.get(s_key)
    if code:
        return s_display, code
    return s_display, s_display


GOING_CODE_LABELS = {
    "G": "好地",
    "GF": "好至快",
    "F": "快地",
    "Y": "黏地",
    "YS": "黏至軟",
    "S": "軟地",
    "H": "大爛地",
    "WET_FAST": "濕快",
    "WET_SLOW": "濕慢",
}


def going_code_label(code: str) -> str:
    c = str(code or "").strip()
    if not c:
        return ""
    return str(GOING_CODE_LABELS.get(c) or c)
