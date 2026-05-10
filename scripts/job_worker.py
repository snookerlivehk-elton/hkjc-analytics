import os
import time
from datetime import datetime, time as dtime, timedelta

from database.connection import init_db, get_session
from database.models import Race, SystemConfig
from scoring_engine.ai_advisor import run_ai_race_summary
from scoring_engine.job_queue import append_job_log, claim_next_job, update_job
from scoring_engine.search_index import index_system_config_doc


def _day_range(date_str: str):
    d0 = datetime.strptime(str(date_str), "%Y/%m/%d").date()
    start = datetime.combine(d0, dtime.min)
    end = start + timedelta(days=1)
    return start, end


def _parse_ymd(s: str):
    try:
        return datetime.strptime(str(s).strip(), "%Y/%m/%d").date()
    except Exception:
        try:
            return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()
        except Exception:
            return None


def _handle_search_backfill(session, job):
    payload = job.get("payload") if isinstance(job.get("payload"), dict) else {}
    d1 = _parse_ymd(payload.get("from"))
    d2 = _parse_ymd(payload.get("to"))
    limit_races = int(payload.get("limit_races") or 200)
    if not d1:
        raise ValueError("missing from")
    if not d2:
        d2 = d1

    start = datetime.combine(d1, dtime.min)
    end = datetime.combine(d2, dtime.min) + timedelta(days=1)
    races = (
        session.query(Race)
        .filter(Race.race_date >= start)
        .filter(Race.race_date < end)
        .order_by(Race.race_date.asc(), Race.race_no.asc())
        .limit(limit_races)
        .all()
    )
    total = len(races)
    update_job(session, job["id"], {"progress": {"total": total, "done": 0, "current": ""}})
    append_job_log(session, job["id"], f"search_backfill from={d1.strftime('%Y/%m/%d')} to={d2.strftime('%Y/%m/%d')} races={total}")

    for i, r in enumerate(races, 1):
        rn = int(getattr(r, "race_no", 0) or 0)
        ds = ""
        try:
            ds = r.race_date.strftime("%Y/%m/%d")
        except Exception:
            ds = ""
        update_job(session, job["id"], {"progress": {"total": total, "done": i - 1, "current": f"{ds} R{rn}"}})
        if ds and rn:
            index_system_config_doc(session, f"ai_race_report:{ds}:{rn}", doc_type="ai_report", title=f"{ds} R{rn} AI report")
            index_system_config_doc(session, f"ai_race_reflection:{ds}:{rn}", doc_type="ai_reflection", title=f"{ds} R{rn} AI reflection")
            scenario_keys = (
                session.query(SystemConfig.key)
                .filter(SystemConfig.key.like(f"ai_race_report_scenario:{ds}:{rn}:%"))
                .order_by(SystemConfig.key.asc())
                .all()
            )
            for (k,) in scenario_keys:
                if not k:
                    continue
                index_system_config_doc(session, str(k), doc_type="ai_report", title=str(k))
        if i % 50 == 0:
            session.commit()
            append_job_log(session, job["id"], f"progress races={i}")

    index_system_config_doc(session, "ai_learned_rules", doc_type="ai_learned_rules", title="ai_learned_rules")
    session.commit()
    update_job(
        session,
        job["id"],
        {
            "status": "done",
            "finished_at": datetime.utcnow().isoformat(),
            "progress": {"total": total, "done": total, "current": ""},
            "result": {"total_races": total},
        },
    )


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
    "search_backfill": _handle_search_backfill,
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
