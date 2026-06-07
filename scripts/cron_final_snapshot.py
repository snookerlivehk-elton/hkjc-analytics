import hashlib
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import init_db, get_session
from database.models import SystemConfig
from scoring_engine.job_queue import enqueue_job
from scoring_engine.readiness import compute_speedpro_day_hash, get_race_day_anchor_dt, get_speedpro_readiness


HK_TZ = ZoneInfo("Asia/Hong_Kong")


def _get_cfg(session, key: str):
    return session.query(SystemConfig).filter_by(key=key).first()


def _get_cfg_value(session, key: str):
    cfg = _get_cfg(session, key)
    return cfg.value if cfg else None


def _upsert_cfg(session, key: str, value, description: str):
    cfg = _get_cfg(session, key)
    if not cfg:
        cfg = SystemConfig(key=key, description=description)
        session.add(cfg)
    cfg.value = value
    session.commit()


def _target_date_str(session) -> str:
    env_date = str(os.environ.get("TARGET_DATE") or "").strip()
    if env_date:
        return env_date
    v = _get_cfg_value(session, "fixture_next_raceday")
    if isinstance(v, str) and v.strip():
        return v.strip()
    return datetime.now(HK_TZ).strftime("%Y/%m/%d")


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def main():
    init_db()
    session = get_session()
    try:
        date_str = _target_date_str(session)
        now_hk = datetime.now(HK_TZ)
        anchor = get_race_day_anchor_dt(session, date_str)
        ignore_window = str(os.environ.get("FINAL_SNAPSHOT_IGNORE_WINDOW") or "").strip().lower() in ("1", "true", "yes")
        before_h = str(os.environ.get("FINAL_SNAPSHOT_WINDOW_BEFORE_HOURS") or "").strip()
        after_h = str(os.environ.get("FINAL_SNAPSHOT_WINDOW_AFTER_HOURS") or "").strip()
        try:
            before_h_v = float(before_h) if before_h else 10.0
        except Exception:
            before_h_v = 10.0
        try:
            after_h_v = float(after_h) if after_h else 0.5
        except Exception:
            after_h_v = 0.5
        before_h_v = max(0.5, min(48.0, float(before_h_v)))
        after_h_v = max(0.0, min(12.0, float(after_h_v)))

        w_start = anchor - timedelta(hours=before_h_v)
        w_end = anchor - timedelta(hours=after_h_v)

        if (not ignore_window) and (not (w_start <= now_hk <= w_end)):
            print(
                f"outside window date={date_str} now={now_hk.isoformat()} "
                f"window={w_start.isoformat()}..{w_end.isoformat()} "
                f"before_hours={before_h_v} after_hours={after_h_v}"
            )
            return

        min_cov = str(os.environ.get("SPEEDPRO_MIN_COVERAGE") or "").strip()
        try:
            min_cov_v = float(min_cov) if min_cov else 0.85
        except Exception:
            min_cov_v = 0.85

        ready = get_speedpro_readiness(session, date_str=date_str, min_coverage=min_cov_v)
        if not bool(ready.get("ok")):
            print(f"skip final_snapshot date={date_str} reason=speedpro_not_ready min_coverage={ready.get('min_coverage')} races={ready.get('races')}")
            return

        sp_hash = compute_speedpro_day_hash(session, date_str=date_str)
        state_key = f"final_snapshot_state:{date_str}"
        state = _get_cfg_value(session, state_key)
        if isinstance(state, dict):
            prev = str(state.get("speedpro_hash") or "").strip()
            if prev and prev == sp_hash:
                print(f"already_done date={date_str} speedpro_hash={sp_hash}")
                return

        payload = {"date": date_str, "steps": ["rescore", "snapshot"], "speedpro_min_coverage": min_cov_v}
        job = enqueue_job(session, "daily_update_pipeline", payload)
        jid = str(job.get("id") or "")

        _upsert_cfg(
            session,
            state_key,
            {"done_at": now_hk.isoformat(), "speedpro_hash": sp_hash, "job_id": jid, "window": f"{w_start.isoformat()}..{w_end.isoformat()}"},
            f"最終快照狀態（賽日 {date_str}）",
        )
        print(f"enqueued final_snapshot date={date_str} job_id={jid} speedpro_hash={sp_hash}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
