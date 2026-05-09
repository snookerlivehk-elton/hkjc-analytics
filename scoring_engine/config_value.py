from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Tuple


def unwrap_value(value: Any) -> Tuple[Any, Dict[str, Any]]:
    if isinstance(value, dict) and ("payload" in value) and isinstance(value.get("_meta"), dict):
        return value.get("payload"), dict(value.get("_meta") or {})
    return value, {}


def wrap_value(payload: Any, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    m = dict(meta or {})
    if "saved_at" not in m:
        m["saved_at"] = datetime.utcnow().isoformat()
    return {"payload": payload, "_meta": m}


def build_meta(
    source: str,
    fetched_at: Optional[str] = None,
    url: Optional[str] = None,
    schema: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    m: Dict[str, Any] = {"source": str(source or "").strip() or None}
    if fetched_at:
        m["fetched_at"] = str(fetched_at)
    if url:
        m["url"] = str(url)
    if schema:
        m["schema"] = str(schema)
    if isinstance(extra, dict) and extra:
        for k, v in extra.items():
            if k and (k not in m):
                m[str(k)] = v
    return m
