import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from sqlalchemy import func

from database.connection import init_db, get_session
from database.models import PredictionTop5


def _parse_ymd(s: str):
    try:
        return datetime.strptime(str(s), "%Y/%m/%d").date()
    except Exception:
        return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=None, help="YYYY/MM/DD")
    p.add_argument("--from", dest="date_from", default=None, help="YYYY/MM/DD")
    p.add_argument("--to", dest="date_to", default=None, help="YYYY/MM/DD")
    p.add_argument("--type", default="all", choices=["all", "factor", "preset"])
    p.add_argument("--out", default=None, help="output json file path (optional)")
    args = p.parse_args()

    init_db()
    session = get_session()
    try:
        q = session.query(PredictionTop5).order_by(PredictionTop5.race_date.asc(), PredictionTop5.race_no.asc(), PredictionTop5.predictor_type.asc(), PredictionTop5.predictor_key.asc())

        if args.type != "all":
            q = q.filter(PredictionTop5.predictor_type == args.type)

        if args.date:
            d = _parse_ymd(args.date)
            if not d:
                raise ValueError("--date must be YYYY/MM/DD")
            q = q.filter(func.date(PredictionTop5.race_date) == d.isoformat())
        else:
            d1 = _parse_ymd(args.date_from) if args.date_from else None
            d2 = _parse_ymd(args.date_to) if args.date_to else None
            if d1 and d2 and d1 > d2:
                d1, d2 = d2, d1
            if d1:
                q = q.filter(func.date(PredictionTop5.race_date) >= d1.isoformat())
            if d2:
                q = q.filter(func.date(PredictionTop5.race_date) <= d2.isoformat())

        out = []
        for r in q.all():
            rd = r.race_date.date().strftime("%Y/%m/%d") if r.race_date else None
            meta = r.meta if isinstance(r.meta, dict) else {}
            out.append(
                {
                    "race_date": rd,
                    "race_no": int(r.race_no or 0),
                    "race_id": int(r.race_id),
                    "predictor_type": str(r.predictor_type),
                    "predictor_key": str(r.predictor_key),
                    "predictor_desc": meta.get("desc") if isinstance(meta, dict) else None,
                    "member_email": (str(r.member_email).lower() if r.member_email else None),
                    "top5": r.top5 if isinstance(r.top5, list) else [],
                }
            )

        payload = json.dumps(out, ensure_ascii=False, indent=2)
        if args.out:
            Path(args.out).write_text(payload, encoding="utf-8")
            print(f"OK: wrote {len(out)} rows to {args.out}")
        else:
            print(payload)
    finally:
        session.close()


if __name__ == "__main__":
    main()

