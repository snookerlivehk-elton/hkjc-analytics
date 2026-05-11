from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from sqlalchemy.orm import Session

from database.models import SystemConfig


QUEUE_KEY = "job_queue_v1"


def _now() -> str:
    return datetime.utcnow().isoformat()


def _get_cfg(session: Session, key: str) -> Optional[SystemConfig]:
    return session.query(SystemConfig).filter_by(key=str(key)).first()


def _upsert_cfg(session: Session, key: str, value: Any, description: str = "") -> SystemConfig:
    cfg = _get_cfg(session, key)
    if not cfg:
        cfg = SystemConfig(key=str(key), description=str(description or ""))
        session.add(cfg)
    cfg.value = value
    return cfg


def _ensure_queue(session: Session) -> Dict[str, Any]:
    cfg = _get_cfg(session, QUEUE_KEY)
    if not cfg or not isinstance(cfg.value, dict):
        cfg = _upsert_cfg(session, QUEUE_KEY, {"queued": []}, "Job queue")
        session.commit()
    val = cfg.value if isinstance(cfg.value, dict) else {"queued": []}
    if "queued" not in val or not isinstance(val.get("queued"), list):
        val["queued"] = []
        cfg.value = val
        session.commit()
    return val


def enqueue_job(session: Session, job_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    job_id = uuid4().hex
    key = f"job:{job_id}"
    job = {
        "id": job_id,
        "type": str(job_type or "").strip(),
        "payload": dict(payload or {}),
        "status": "queued",
        "created_at": _now(),
        "updated_at": _now(),
        "progress": {"total": 0, "done": 0, "current": ""},
        "result": None,
        "error": None,
    }
    _upsert_cfg(session, key, job, f"Job {job.get('type')}")
    qv = _ensure_queue(session)
    q = list(qv.get("queued") or [])
    q.append(job_id)
    qv["queued"] = q[-200:]
    _upsert_cfg(session, QUEUE_KEY, qv, "Job queue")
    session.commit()
    return job


def get_job(session: Session, job_id: str) -> Optional[Dict[str, Any]]:
    cfg = _get_cfg(session, f"job:{str(job_id).strip()}")
    if not cfg or not isinstance(cfg.value, dict):
        return None
    return dict(cfg.value)


def update_job(session: Session, job_id: str, patch: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    cfg = _get_cfg(session, f"job:{str(job_id).strip()}")
    if not cfg or not isinstance(cfg.value, dict):
        return None
    val = dict(cfg.value)
    for k, v in dict(patch or {}).items():
        val[str(k)] = v
    val["updated_at"] = _now()
    cfg.value = val
    session.commit()
    return val


def append_job_log(session: Session, job_id: str, line: str, max_lines: int = 200) -> None:
    cfg = _get_cfg(session, f"job:{str(job_id).strip()}")
    if not cfg or not isinstance(cfg.value, dict):
        return
    val = dict(cfg.value)
    log = val.get("log")
    if not isinstance(log, list):
        log = []
    s = str(line or "").strip()
    if s:
        log.append(f"{_now()} {s}")
    if int(max_lines or 200) > 0:
        log = log[-int(max_lines):]
    val["log"] = log
    val["updated_at"] = _now()
    cfg.value = val
    session.commit()


def claim_next_job(session: Session) -> Optional[Dict[str, Any]]:
    cfg0 = session.query(SystemConfig).filter_by(key=QUEUE_KEY).first()
    if not cfg0 or not isinstance(cfg0.value, dict):
        _ensure_queue(session)

    cfg = session.query(SystemConfig).filter_by(key=QUEUE_KEY).with_for_update().first()
    if not cfg or not isinstance(cfg.value, dict):
        return None

    q = cfg.value.get("queued")
    if not isinstance(q, list):
        q = []

    if not q:
        rebuild_queue_from_recent_jobs(session, limit=200)
        cfg = session.query(SystemConfig).filter_by(key=QUEUE_KEY).with_for_update().first()
        q = cfg.value.get("queued") if cfg and isinstance(cfg.value, dict) else []
        if not isinstance(q, list):
            q = []

    while q:
        job_id = str(q.pop(0)).strip()
        if not job_id:
            continue
        job = get_job(session, job_id)
        if not isinstance(job, dict):
            continue
        if str(job.get("status") or "") not in {"queued"}:
            continue
        cfg.value = {"queued": q[-200:]}
        session.commit()
        update_job(
            session,
            job_id,
            {"status": "running", "started_at": _now(), "error": None, "result": None},
        )
        return get_job(session, job_id)

    cfg.value = {"queued": q[-200:]}
    session.commit()
    return None


def list_recent_jobs(session: Session, limit: int = 20) -> List[Dict[str, Any]]:
    rows = (
        session.query(SystemConfig)
        .filter(SystemConfig.key.like("job:%"))
        .order_by(SystemConfig.updated_at.desc())
        .limit(int(limit or 20))
        .all()
    )
    out = []
    for r in rows:
        if isinstance(r.value, dict):
            out.append(dict(r.value))
    return out


def peek_queue(session: Session) -> Dict[str, Any]:
    qv = _ensure_queue(session)
    q = qv.get("queued")
    if not isinstance(q, list):
        q = []
    return {"queued": list(q), "len": len(q)}


def rebuild_queue_from_recent_jobs(session: Session, limit: int = 200) -> Dict[str, Any]:
    qv = _ensure_queue(session)
    q = qv.get("queued")
    if not isinstance(q, list):
        q = []
    seen = set(str(x).strip() for x in q if str(x).strip())

    rows = (
        session.query(SystemConfig)
        .filter(SystemConfig.key.like("job:%"))
        .order_by(SystemConfig.updated_at.desc())
        .limit(int(limit or 200))
        .all()
    )
    added = 0
    for r in rows:
        v = r.value if isinstance(r.value, dict) else None
        if not isinstance(v, dict):
            continue
        if str(v.get("status") or "") != "queued":
            continue
        jid = str(v.get("id") or "").strip()
        if not jid or jid in seen:
            continue
        q.append(jid)
        seen.add(jid)
        added += 1

    qv["queued"] = q[-200:]
    _upsert_cfg(session, QUEUE_KEY, qv, "Job queue")
    session.commit()
    return {"added": int(added), "len": len(qv["queued"])}
