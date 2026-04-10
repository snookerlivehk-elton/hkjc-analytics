from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func

from database.connection import init_db, get_session
from database.models import PredictionTop5


def _parse_date(s: str) -> Optional[date]:
    v = str(s or "").strip()
    if not v:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(v, fmt).date()
        except Exception:
            continue
    return None


app = FastAPI(title="HKJC Analytics API", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _startup():
    init_db()


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/health/db")
def health_db() -> Dict[str, Any]:
    session = get_session()
    try:
        race_cnt = session.query(PredictionTop5.race_id).count()
        return {"ok": True, "prediction_top5": int(race_cnt)}
    finally:
        session.close()


@app.get("/api/v1/top5")
def top5(
    date_: Optional[str] = Query(default=None, alias="date"),
    date_from: Optional[str] = Query(default=None, alias="from"),
    date_to: Optional[str] = Query(default=None, alias="to"),
    type_: str = Query(default="all", alias="type", pattern="^(all|factor|preset)$"),
    factor: Optional[str] = Query(default=None),
    email: Optional[str] = Query(default=None),
    preset: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    d = _parse_date(date_) if date_ else None
    d1 = _parse_date(date_from) if date_from else None
    d2 = _parse_date(date_to) if date_to else None
    if d:
        d1, d2 = d, d
    if d1 and d2 and d1 > d2:
        d1, d2 = d2, d1

    session = get_session()
    try:
        q = (
            session.query(
                PredictionTop5.race_date,
                PredictionTop5.race_no,
                PredictionTop5.race_id,
                PredictionTop5.predictor_type,
                PredictionTop5.predictor_key,
                PredictionTop5.member_email,
                PredictionTop5.top5,
                PredictionTop5.meta,
            )
            .order_by(
                func.date(PredictionTop5.race_date).asc(),
                PredictionTop5.race_no.asc(),
                PredictionTop5.predictor_type.asc(),
                PredictionTop5.predictor_key.asc(),
                PredictionTop5.member_email.asc(),
            )
        )

        if type_ != "all":
            q = q.filter(PredictionTop5.predictor_type == type_)
        if d1:
            q = q.filter(func.date(PredictionTop5.race_date) >= d1.isoformat())
        if d2:
            q = q.filter(func.date(PredictionTop5.race_date) <= d2.isoformat())

        if factor:
            q = q.filter(PredictionTop5.predictor_type == "factor").filter(PredictionTop5.predictor_key == str(factor))
        if email:
            q = q.filter(PredictionTop5.predictor_type == "preset").filter(PredictionTop5.member_email == str(email).strip().lower())
        if preset:
            q = q.filter(PredictionTop5.predictor_type == "preset").filter(PredictionTop5.predictor_key == str(preset).strip())

        rows: List[Dict[str, Any]] = []
        for race_date, race_no, race_id, predictor_type, predictor_key, member_email, top5, meta in q.all():
            rd = race_date.date().strftime("%Y/%m/%d") if race_date else None
            m = meta if isinstance(meta, dict) else {}
            rows.append(
                {
                    "race_date": rd,
                    "race_no": int(race_no or 0),
                    "race_id": int(race_id),
                    "predictor_type": str(predictor_type),
                    "predictor_key": str(predictor_key),
                    "predictor_desc": (m.get("desc") if isinstance(m, dict) else None),
                    "member_email": (str(member_email).lower() if member_email else None),
                    "top5": top5 if isinstance(top5, list) else [],
                    "hits": (m.get("hits") if isinstance(m, dict) else None),
                }
            )

        return {"code": 0, "message": "ok", "data": {"rows": rows}, "count": len(rows)}
    finally:
        session.close()


@app.get("/api/hkjc/base")
@app.get("/api/v1/like")
def like(
    date_: str = Query(..., alias="date"),
    type_: str = Query(default="all", alias="type", pattern="^(all|factor|preset)$"),
) -> Dict[str, Any]:
    d = _parse_date(date_)
    if not d:
        return {"code": 1, "message": "date format must be YYYY-MM-DD", "data": None, "count": 0}

    session = get_session()
    try:
        q = (
            session.query(
                PredictionTop5.race_no,
                PredictionTop5.predictor_type,
                PredictionTop5.predictor_key,
                PredictionTop5.member_email,
                PredictionTop5.top5,
                PredictionTop5.meta,
            )
            .filter(func.date(PredictionTop5.race_date) == d.isoformat())
            .order_by(
                PredictionTop5.predictor_type.asc(),
                PredictionTop5.member_email.asc(),
                PredictionTop5.predictor_key.asc(),
                PredictionTop5.race_no.asc(),
            )
        )
        if type_ != "all":
            q = q.filter(PredictionTop5.predictor_type == type_)

        groups: Dict[str, Dict[str, Any]] = {}
        for race_no, predictor_type, predictor_key, member_email, top5, meta in q.all():
            pt = str(predictor_type)
            pk = str(predictor_key)
            me = str(member_email).lower() if member_email else None

            if pt == "preset" and me:
                code = f"{pk}::{me}"
            else:
                code = pk

            m = meta if isinstance(meta, dict) else {}
            name = m.get("desc") if isinstance(m, dict) and m.get("desc") else pk

            g = groups.get(code)
            if g is None:
                g = {"condition_name": name, "condition_code": code, "races": {}}
                groups[code] = g

            g["races"][str(int(race_no or 0))] = top5 if isinstance(top5, list) else []

        items = list(groups.values())
        items.sort(key=lambda x: str(x.get("condition_code") or ""))

        return {"code": 0, "message": "查询成功", "data": {"date": d.isoformat(), "items": items}, "count": len(items)}
    finally:
        session.close()
