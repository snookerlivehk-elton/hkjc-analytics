import os
import sys
import subprocess
import time
from datetime import datetime, time as dtime, timedelta
import traceback
from urllib.parse import urlparse

from pathlib import Path

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import init_db, get_session
from database.models import Race, SystemConfig
from scoring_engine.ai_advisor import run_ai_race_summary
from scoring_engine.job_queue import append_job_log, claim_next_job, update_job
from scoring_engine.search_index import index_system_config_doc
from scoring_engine.core import ScoringEngine
from scoring_engine.readiness import get_speedpro_readiness


def _now() -> str:
    return datetime.utcnow().isoformat()


def _log(msg: str) -> None:
    try:
        print(f"{_now()} {str(msg)}", flush=True)
    except Exception:
        pass


def _safe_db_label() -> str:
    raw = str(os.environ.get("DATABASE_URL") or "").strip()
    if not raw:
        return "DATABASE_URL=missing"
    try:
        if raw.startswith("postgres://"):
            raw = raw.replace("postgres://", "postgresql://", 1)
        u = urlparse(raw)
        host = str(u.hostname or "")
        port = f":{int(u.port)}" if u.port else ""
        db = str(u.path or "").lstrip("/")
        scheme = str(u.scheme or "")
        if db:
            return f"{scheme}://{host}{port}/{db}"
        return f"{scheme}://{host}{port}"
    except Exception:
        return "DATABASE_URL=present"


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


def _handle_rescore_race_date(session, job):
    payload = job.get("payload") if isinstance(job.get("payload"), dict) else {}
    date_str = str(payload.get("date") or "").strip()
    if not date_str:
        raise ValueError("missing date")
    start, end = _day_range(date_str)

    races = (
        session.query(Race)
        .filter(Race.race_date >= start)
        .filter(Race.race_date < end)
        .order_by(Race.race_no.asc(), Race.id.asc())
        .all()
    )
    total = len(races)
    update_job(session, job["id"], {"progress": {"total": total, "done": 0, "current": ""}})
    append_job_log(session, job["id"], f"rescore_race_date date={date_str} races={total}")

    ok, failed = _run_rescore_race_date_inner(session, job["id"], date_str, races=races, total=total, update_progress=True)

    update_job(
        session,
        job["id"],
        {
            "status": "done",
            "finished_at": datetime.utcnow().isoformat(),
            "progress": {"total": total, "done": total, "current": ""},
            "result": {"ok": ok, "failed": failed, "total": total},
        },
    )


def _run_script_and_stream_log(session, job_id: str, script_path: str, env: dict) -> None:
    argv = [sys.executable, str(script_path)]
    proc = subprocess.Popen(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        bufsize=1,
    )
    try:
        if proc.stdout:
            for line in iter(proc.stdout.readline, ""):
                s = str(line or "").rstrip()
                if s:
                    append_job_log(session, job_id, s, max_lines=400)
    finally:
        try:
            if proc.stdout:
                proc.stdout.close()
        except Exception:
            pass
    code = proc.wait()
    if int(code or 0) != 0:
        raise RuntimeError(f"script_failed:{script_path}:exit_code={code}")


def _run_rescore_race_date_inner(session, job_id: str, date_str: str, *, races: list, total: int, update_progress: bool) -> tuple:
    engine = ScoringEngine(session)
    ok = 0
    failed = 0
    for i, r in enumerate(races, 1):
        if update_progress:
            update_job(session, job_id, {"progress": {"total": total, "done": i - 1, "current": f"R{int(r.race_no)}"}})
        try:
            engine.score_race(int(r.id))
            ok += 1
        except Exception as e:
            failed += 1
            append_job_log(session, job_id, f"R{int(r.race_no)} failed err={str(e)}", max_lines=400)
        if i % 3 == 0:
            append_job_log(session, job_id, f"progress races={i}", max_lines=400)
    return ok, failed


def _handle_daily_update_pipeline(session, job):
    payload = job.get("payload") if isinstance(job.get("payload"), dict) else {}
    date_str = str(payload.get("date") or "").strip()
    steps = payload.get("steps")
    if not date_str:
        raise ValueError("missing date")
    if not isinstance(steps, list) or not steps:
        steps = ["scrape", "history", "rescore", "snapshot"]

    steps = [str(x).strip().lower() for x in steps if str(x).strip()]
    total = len(steps)
    update_job(session, job["id"], {"progress": {"total": total, "done": 0, "current": ""}})
    append_job_log(session, job["id"], f"daily_update_pipeline date={date_str} steps={','.join(steps)}")

    env0 = os.environ.copy()
    env0["TARGET_DATE"] = date_str

    for i, step in enumerate(steps, 1):
        update_job(session, job["id"], {"progress": {"total": total, "done": i - 1, "current": step}})
        append_job_log(session, job["id"], f"step_start {step}")
        if step == "scrape":
            _run_script_and_stream_log(session, job["id"], "scripts/run_scraper.py", env0)
        elif step == "history":
            env = dict(env0)
            env["BACKFILL_MODE"] = "date"
            _run_script_and_stream_log(session, job["id"], "scripts/fetch_history.py", env)
        elif step == "rescore":
            start, end = _day_range(date_str)
            races = (
                session.query(Race)
                .filter(Race.race_date >= start)
                .filter(Race.race_date < end)
                .order_by(Race.race_no.asc(), Race.id.asc())
                .all()
            )
            append_job_log(session, job["id"], f"rescore_race_date date={date_str} races={len(races)}")
            _run_rescore_race_date_inner(session, job["id"], date_str, races=races, total=len(races), update_progress=False)
        elif step == "snapshot":
            min_cov = payload.get("speedpro_min_coverage")
            try:
                min_cov_v = float(min_cov) if min_cov is not None else 0.85
            except Exception:
                min_cov_v = 0.85
            ready = get_speedpro_readiness(session, date_str=date_str, min_coverage=min_cov_v)
            if not bool(ready.get("ok")):
                append_job_log(
                    session,
                    job["id"],
                    f"step_skipped snapshot reason=speedpro_not_ready min_coverage={ready.get('min_coverage')} races={ready.get('races')}",
                    max_lines=400,
                )
            else:
                _run_script_and_stream_log(session, job["id"], "scripts/generate_predictions.py", env0)
        elif step == "results":
            _run_script_and_stream_log(session, job["id"], "scripts/fetch_race_results.py", env0)
        else:
            raise ValueError(f"unknown_step:{step}")
        append_job_log(session, job["id"], f"step_done {step}")

    update_job(
        session,
        job["id"],
        {
            "status": "done",
            "finished_at": datetime.utcnow().isoformat(),
            "progress": {"total": total, "done": total, "current": ""},
            "result": {"date": date_str, "steps": steps},
        },
    )


def _handle_fetch_race_results(session, job):
    payload = job.get("payload") if isinstance(job.get("payload"), dict) else {}
    date_str = str(payload.get("date") or "").strip()
    if not date_str:
        raise ValueError("missing date")
    update_job(session, job["id"], {"progress": {"total": 1, "done": 0, "current": "fetch_race_results"}})
    append_job_log(session, job["id"], f"fetch_race_results date={date_str}")
    env0 = os.environ.copy()
    env0["TARGET_DATE"] = date_str
    _run_script_and_stream_log(session, job["id"], "scripts/fetch_race_results.py", env0)
    update_job(
        session,
        job["id"],
        {
            "status": "done",
            "finished_at": datetime.utcnow().isoformat(),
            "progress": {"total": 1, "done": 1, "current": ""},
            "result": {"date": date_str},
        },
    )


def _handle_speedpro_fetch(session, job):
    payload = job.get("payload") if isinstance(job.get("payload"), dict) else {}
    date_str = str(payload.get("date") or "").strip()
    if not date_str:
        raise ValueError("missing date")
    race_nos = str(payload.get("race_nos") or "").strip()
    retry_minutes = int(payload.get("retry_minutes") or 120)
    force = bool(payload.get("force") if payload.get("force") is not None else True)

    update_job(session, job["id"], {"progress": {"total": 1, "done": 0, "current": "speedpro_fetch"}})
    append_job_log(session, job["id"], f"speedpro_fetch date={date_str} race_nos={race_nos} retry_minutes={retry_minutes} force={force}")

    env0 = os.environ.copy()
    env0["TARGET_DATE"] = date_str
    if race_nos:
        env0["RACE_NOS"] = race_nos
    env0["SPEEDPRO_RETRY_MINUTES"] = str(int(retry_minutes))
    if force:
        env0["FORCE_SPEEDPRO_FETCH"] = "1"

    _run_script_and_stream_log(session, job["id"], "scripts/cron_speedpro_fetch.py", env0)
    update_job(
        session,
        job["id"],
        {
            "status": "done",
            "finished_at": datetime.utcnow().isoformat(),
            "progress": {"total": 1, "done": 1, "current": ""},
            "result": {"date": date_str},
        },
    )


JOB_HANDLERS = {
    "ai_batch_generate": _handle_ai_batch_generate,
    "search_backfill": _handle_search_backfill,
    "rescore_race_date": _handle_rescore_race_date,
    "daily_update_pipeline": _handle_daily_update_pipeline,
    "fetch_race_results": _handle_fetch_race_results,
    "speedpro_fetch": _handle_speedpro_fetch,
}


def main():
    init_db()
    base_poll_sec = float(os.environ.get("JOB_POLL_SEC") or 3.0)
    max_poll_sec = float(os.environ.get("JOB_POLL_MAX_SEC") or 30.0)
    if base_poll_sec <= 0:
        base_poll_sec = 3.0
    if max_poll_sec < base_poll_sec:
        max_poll_sec = base_poll_sec
    idle_sleep = base_poll_sec
    last_hb_ts = 0.0
    hb_every_sec = float(os.environ.get("JOB_HEARTBEAT_SEC") or 60.0)
    if hb_every_sec <= 0:
        hb_every_sec = 60.0
    _log(f"job_worker_started db={_safe_db_label()} poll={base_poll_sec}s..{max_poll_sec}s heartbeat={hb_every_sec}s")
    while True:
        session = get_session()
        try:
            job = claim_next_job(session)
            if not job:
                now_ts = time.time()
                if now_ts - last_hb_ts >= hb_every_sec:
                    last_hb_ts = now_ts
                    _log(f"idle queue_empty sleep={idle_sleep}s")
                session.close()
                time.sleep(idle_sleep)
                idle_sleep = min(max_poll_sec, max(base_poll_sec, idle_sleep * 1.5))
                continue
            idle_sleep = base_poll_sec

            jid = str(job.get("id") or "").strip()
            jtype = str(job.get("type") or "").strip()
            handler = JOB_HANDLERS.get(jtype)
            if not handler:
                update_job(session, jid, {"status": "failed", "error": f"unknown_job_type:{jtype}"})
                session.close()
                continue

            try:
                _log(f"claimed job_id={jid} type={jtype}")
                handler(session, job)
                _log(f"finished job_id={jid} type={jtype}")
            except Exception as e:
                _log(f"job_failed job_id={jid} type={jtype} err={str(e)}")
                _log(traceback.format_exc())
                update_job(session, jid, {"status": "failed", "error": str(e), "finished_at": datetime.utcnow().isoformat()})
            finally:
                session.close()
        except Exception:
            _log("worker_loop_error")
            _log(traceback.format_exc())
            try:
                session.close()
            except Exception:
                pass
            time.sleep(idle_sleep)
            idle_sleep = min(max_poll_sec, max(base_poll_sec, idle_sleep * 1.5))


if __name__ == "__main__":
    main()
