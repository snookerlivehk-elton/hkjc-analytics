import os
import time
from datetime import datetime, time as dtime, timedelta

from database.connection import init_db, get_session
from database.models import Race, SystemConfig
from scoring_engine.ai_advisor import run_ai_race_summary
from scoring_engine.job_queue import append_job_log, claim_next_job, update_job


def _day_range(date_str: str):
    d0 = datetime.strptime(str(date_str), "%Y/%m/%d").date()
    start = datetime.combine(d0, dtime.min)
    end = start + timedelta(days=1)
    return start, end


def _handle_ai_batch_generate(session, job):
    payload = job.get("payload") if isinstance(job.get("payload"), dict) else {}
    date_str = str(payload.get("date") or "").strip()
    if not date_str:
        raise ValueError("missing date")
    start, end = _day_range(date_str)

    races = (
        session.query(Race)
        .filter(Race.race_date >= start)
        .filter(Race.race_date < end)
        .order_by(Race.race_no.asc())
        .all()
    )
    total = len(races)
    update_job(session, job["id"], {"progress": {"total": total, "done": 0, "current": ""}})
    append_job_log(session, job["id"], f"ai_batch_generate date={date_str} races={total}")

    ok = 0
    skipped = 0
    failed = 0
    for i, r in enumerate(races, 1):
        update_job(session, job["id"], {"progress": {"total": total, "done": i - 1, "current": f"R{int(r.race_no)}"}})
        res = run_ai_race_summary(session, int(r.id))
        if isinstance(res, dict) and res.get("ok") is True:
            ok += 1
        else:
            reason = str(res.get("reason") if isinstance(res, dict) else "").strip()
            if reason in {"no_formguide_data", "missing_api_key"}:
                skipped += 1
            else:
                failed += 1
        append_job_log(
            session,
            job["id"],
            f"R{int(r.race_no)} ok={bool(isinstance(res, dict) and res.get('ok') is True)} reason={str(res.get('reason') if isinstance(res, dict) else '')}",
        )

    update_job(
        session,
        job["id"],
        {
            "status": "done",
            "finished_at": datetime.utcnow().isoformat(),
            "progress": {"total": total, "done": total, "current": ""},
            "result": {"ok": ok, "skipped": skipped, "failed": failed, "total": total},
        },
    )


JOB_HANDLERS = {
    "ai_batch_generate": _handle_ai_batch_generate,
}


def main():
    init_db()
    poll_sec = float(os.environ.get("JOB_POLL_SEC") or 3.0)
    while True:
        session = get_session()
        try:
            job = claim_next_job(session)
            if not job:
                session.close()
                time.sleep(poll_sec)
                continue

            jid = str(job.get("id") or "").strip()
            jtype = str(job.get("type") or "").strip()
            handler = JOB_HANDLERS.get(jtype)
            if not handler:
                update_job(session, jid, {"status": "failed", "error": f"unknown_job_type:{jtype}"})
                session.close()
                continue

            try:
                handler(session, job)
            except Exception as e:
                update_job(session, jid, {"status": "failed", "error": str(e), "finished_at": datetime.utcnow().isoformat()})
            finally:
                session.close()
        except Exception:
            try:
                session.close()
            except Exception:
                pass
            time.sleep(poll_sec)


if __name__ == "__main__":
    main()

