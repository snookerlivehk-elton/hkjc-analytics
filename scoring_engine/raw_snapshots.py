from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session


def upsert_raw_snapshot(
    session: Session,
    *,
    source: str,
    entity_type: str,
    entity_key: str,
    payload: Any,
    race_id: Optional[int] = None,
    meta: Optional[Dict[str, Any]] = None,
    fetched_at: Optional[datetime] = None,
) -> None:
    from database.models import RawSnapshot

    src = str(source or "").strip()
    et = str(entity_type or "").strip()
    ek = str(entity_key or "").strip()
    if not src or not et or not ek:
        return

    row = session.query(RawSnapshot).filter_by(source=src, entity_type=et, entity_key=ek).first()
    if not row:
        row = RawSnapshot(source=src, entity_type=et, entity_key=ek, payload={})
        session.add(row)

    row.race_id = int(race_id) if race_id is not None else None
    row.payload = payload if payload is not None else {}
    if isinstance(meta, dict):
        row.meta = meta
    if fetched_at is not None:
        row.fetched_at = fetched_at
