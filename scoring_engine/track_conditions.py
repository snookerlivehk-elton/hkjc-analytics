from __future__ import annotations

import re
from typing import Tuple


def normalize_going(raw: str) -> Tuple[str, str]:
    s = str(raw or "").strip()
    s = re.sub(r"\s+", "", s)
    if not s:
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
    code = m.get(s)
    if code:
        return s, code
    return s, s


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
