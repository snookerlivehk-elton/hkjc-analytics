import hashlib
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple
from zoneinfo import ZoneInfo

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import init_db, get_session
from database.models import Race, RaceEntry, SystemConfig
from data_scraper.speedpro_energy import SpeedProEnergyScraper
from data_scraper.speedpro_formguide import SpeedProFormGuideScraper


HK_TZ = ZoneInfo("Asia/Hong_Kong")
LOCK_KEY = "job_lock:speedpro_fetch"
LOCK_TTL_MIN = 15


def _get_cfg(session, key: str) -> Optional[SystemConfig]:
    return session.query(SystemConfig).filter_by(key=key).first()


def _upsert_cfg(session, key: str, value, desc: str):
    cfg = _get_cfg(session, key)
    if not cfg:
        cfg = SystemConfig(key=key, description=desc, value=value)
        session.add(cfg)
    else:
        cfg.value = value
        if desc and not cfg.description:
            cfg.description = desc
    session.commit()


def _delete_cfg(session, key: str):
    cfg = _get_cfg(session, key)
    if cfg:
        session.delete(cfg)
        session.commit()


def _acquire_lock(session) -> bool:
    now = datetime.now(HK_TZ)
    cfg = _get_cfg(session, LOCK_KEY)
    if cfg and isinstance(cfg.value, str):
        try:
            ts = datetime.fromisoformat(cfg.value)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=HK_TZ)
            if now - ts < timedelta(minutes=LOCK_TTL_MIN):
                return False
        except Exception:
            pass
    _upsert_cfg(session, LOCK_KEY, now.isoformat(), "Cron lock: speedpro fetch (HK)")
    return True


def _release_lock(session):
    _delete_cfg(session, LOCK_KEY)


def _sha256_json(v) -> str:
    payload = json.dumps(v, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _parse_date(s: str) -> Optional[datetime]:
    v = str(s or "").strip()
    if not v:
        return None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(v, fmt)
        except Exception:
            continue
    return None


def _target_racedate_str(session) -> str:
    env_date = str(os.environ.get("TARGET_DATE") or "").strip()
    if env_date:
        return env_date
    cfg = _get_cfg(session, "fixture_next_raceday")
    if cfg and isinstance(cfg.value, str) and cfg.value.strip():
        return cfg.value.strip()
    return datetime.now(HK_TZ).strftime("%Y/%m/%d")


def _retry_minutes(attempt_count: int) -> int:
    v = str(os.environ.get("SPEEDPRO_RETRY_MINUTES") or "").strip()
    if v.isdigit():
        return max(1, int(v))
    return 120


def _window(session, racedate_str: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    try:
        d = datetime.strptime(racedate_str, "%Y/%m/%d").date()
    except Exception:
        return None, None
    start = datetime.combine(d - timedelta(days=1), datetime.strptime("12:00", "%H:%M").time()).replace(tzinfo=HK_TZ)
    end = datetime.combine(d, datetime.max.time()).replace(tzinfo=HK_TZ)
    return start, end


def _is_done_payload(data_map: Dict) -> Tuple[bool, str]:
    if not isinstance(data_map, dict) or not data_map:
        return False, "empty"
    rows = list(data_map.values())
    if not rows:
        return False, "empty"
    total = len(rows)
    both = 0
    has_energy = 0
    has_status = 0
    for r in rows:
        if not isinstance(r, dict):
            continue
        ea = r.get("energy_assess")
        sr = r.get("status_rating")
        if ea is not None:
            has_energy += 1
        if sr is not None:
            has_status += 1
        if ea is not None and sr is not None:
            both += 1
    if total < 6:
        return False, f"too_few_rows:{total}"
    if has_energy == 0 or has_status == 0:
        return False, "missing_required_fields"
    if both / float(total) < 0.6:
        return False, f"low_coverage:{both}/{total}"
    return True, "ok"


def _race_nos(session, racedate_str: str):
    try:
        d = datetime.strptime(racedate_str, "%Y/%m/%d").date()
    except Exception:
        return list(range(1, 10))
    ids = (
        session.query(Race.race_no)
        .filter(Race.race_date >= datetime.combine(d, datetime.min.time()))
        .filter(Race.race_date < datetime.combine(d, datetime.min.time()) + timedelta(days=1))
        .order_by(Race.race_no.asc())
        .all()
    )
    out = []
    for (rn,) in ids:
        try:
            out.append(int(rn or 0))
        except Exception:
            continue
    out = [x for x in out if x > 0]
    return out if out else list(range(1, 10))


def _expected_horse_count(session, racedate_str: str, race_no: int) -> Optional[int]:
    try:
        d = datetime.strptime(racedate_str, "%Y/%m/%d").date()
    except Exception:
        return None
    race = (
        session.query(Race.id)
        .filter(Race.race_no == int(race_no))
        .filter(Race.race_date >= datetime.combine(d, datetime.min.time()))
        .filter(Race.race_date < datetime.combine(d, datetime.min.time()) + timedelta(days=1))
        .first()
    )
    if not race:
        return None
    cnt = session.query(RaceEntry.id).filter(RaceEntry.race_id == int(race[0])).count()
    return int(cnt) if cnt else None


def main():
    init_db()
    session = get_session()
    try:
        if not _acquire_lock(session):
            print("locked, skip")
            return

        now_hk = datetime.now(HK_TZ)
        force = str(os.environ.get("FORCE_SPEEDPRO_FETCH") or "").strip().lower() in ("1", "true", "yes")
        racedate_str = _target_racedate_str(session)
        window_start, window_end = _window(session, racedate_str)
        if (not force) and window_start and now_hk < window_start:
            print(f"too early racedate={racedate_str} window_start={window_start.isoformat()}")
            return
        if (not force) and window_end and now_hk > window_end:
            race_nos = _race_nos(session, racedate_str)
            for rn in race_nos:
                retry_key = f"speedpro_retry:{racedate_str}:{int(rn)}"
                retry_cfg = _get_cfg(session, retry_key)
                st = retry_cfg.value if retry_cfg and isinstance(retry_cfg.value, dict) else {}
                if st.get("done") is True:
                    continue
                st2 = {
                    "done": True,
                    "attempt_count": int(st.get("attempt_count") or 0),
                    "last_attempt_at": st.get("last_attempt_at"),
                    "next_retry_at": None,
                    "last_error": "expired",
                }
                _upsert_cfg(session, retry_key, st2, f"SpeedPRO 重試狀態（racedate={racedate_str} R{int(rn)}）")
            print(f"expired racedate={racedate_str} window_end={window_end.isoformat()}")
            return
        race_nos = _race_nos(session, racedate_str)
        only_nos = str(os.environ.get("RACE_NOS") or "").strip()
        only_no = str(os.environ.get("RACE_NO") or "").strip()
        wanted = set()
        if only_nos:
            for part in only_nos.split(","):
                part = part.strip()
                if part.isdigit():
                    wanted.add(int(part))
        elif only_no.isdigit():
            wanted.add(int(only_no))
        if wanted:
            race_nos = [rn for rn in race_nos if int(rn) in wanted]

        scraper = SpeedProEnergyScraper()
        fg_scraper = SpeedProFormGuideScraper()
        any_work = False

        for rn in race_nos:
            snap_key = f"speedpro_energy:{racedate_str}:{int(rn)}"
            retry_key = f"speedpro_retry:{racedate_str}:{int(rn)}"
            fg_snap_key = f"speedpro_formguide:{racedate_str}:{int(rn)}"

            snap_cfg = _get_cfg(session, snap_key)
            energy_ok = False
            if snap_cfg and isinstance(snap_cfg.value, dict):
                ok, _ = _is_done_payload(snap_cfg.value)
                if ok:
                    energy_ok = True
                    
            if energy_ok:
                fg_cfg = _get_cfg(session, fg_snap_key)
                if not fg_cfg or not isinstance(fg_cfg.value, dict) or not fg_cfg.value:
                    try:
                        fg_data = fg_scraper.scrape(int(rn))
                        if fg_data:
                            normalized_fg = {str(int(k)): v for k, v in fg_data.items() if str(k).isdigit() and isinstance(v, dict)}
                            _upsert_cfg(session, fg_snap_key, normalized_fg, f"SpeedPRO 賽績指引（racedate={racedate_str} R{int(rn)}）")
                            print(f"ok {fg_snap_key} rows={len(normalized_fg)}")
                    except Exception as e:
                        print(f"error fetching formguide {fg_snap_key}: {e}")
                continue

            retry_cfg = _get_cfg(session, retry_key)
            retry_state = retry_cfg.value if retry_cfg and isinstance(retry_cfg.value, dict) else {}
            done = bool(retry_state.get("done") is True)
            if done:
                continue

            next_retry_at = retry_state.get("next_retry_at")
            if isinstance(next_retry_at, str) and next_retry_at.strip():
                try:
                    nr = datetime.fromisoformat(next_retry_at)
                    if nr.tzinfo is None:
                        nr = nr.replace(tzinfo=HK_TZ)
                    if now_hk < nr:
                        continue
                except Exception:
                    pass

            attempt = int(retry_state.get("attempt_count") or 0) + 1
            any_work = True

            try:
                data_map = scraper.scrape(int(rn))
            except Exception as e:
                data_map = {}
                err = f"fetch_error:{e}"
            else:
                err = ""

            exp_cnt = _expected_horse_count(session, racedate_str, int(rn))
            if exp_cnt and isinstance(data_map, dict) and data_map:
                if len(data_map) < max(6, int(exp_cnt * 0.6)):
                    err = f"insufficient_rows:{len(data_map)}/{exp_cnt}"

            ok, reason = _is_done_payload(data_map) if not err else (False, err)

            if ok:
                normalized = {str(int(k)): v for k, v in data_map.items() if str(k).isdigit() and isinstance(v, dict)}
                _upsert_cfg(session, snap_key, normalized, f"SpeedPRO 能量分（racedate={racedate_str} R{int(rn)}）")

                info_key = f"speedpro_energy_info:{racedate_str}:{int(rn)}"
                info = {
                    "racedate": racedate_str,
                    "race_no": int(rn),
                    "captured_at": now_hk.isoformat(),
                    "raw_hash": _sha256_json(normalized),
                    "rows": len(normalized),
                }
                _upsert_cfg(session, info_key, info, f"SpeedPRO 能量分抓取資訊（racedate={racedate_str} R{int(rn)}）")

                state = {
                    "done": True,
                    "attempt_count": attempt,
                    "last_attempt_at": now_hk.isoformat(),
                    "next_retry_at": None,
                    "last_error": None,
                }
                _upsert_cfg(session, retry_key, state, f"SpeedPRO 重試狀態（racedate={racedate_str} R{int(rn)}）")
                print(f"ok {snap_key} rows={len(normalized)}")

                # Also fetch formguide since energy is done
                try:
                    fg_data = fg_scraper.scrape(int(rn))
                    if fg_data:
                        normalized_fg = {str(int(k)): v for k, v in fg_data.items() if str(k).isdigit() and isinstance(v, dict)}
                        _upsert_cfg(session, fg_snap_key, normalized_fg, f"SpeedPRO 賽績指引（racedate={racedate_str} R{int(rn)}）")
                        print(f"ok {fg_snap_key} rows={len(normalized_fg)}")
                except Exception as e:
                    print(f"error fetching formguide {fg_snap_key}: {e}")

                continue

            minutes = _retry_minutes(attempt)
            next_at = (now_hk + timedelta(minutes=minutes)).isoformat()
            state = {
                "done": False,
                "attempt_count": attempt,
                "last_attempt_at": now_hk.isoformat(),
                "next_retry_at": next_at,
                "last_error": str(reason),
            }
            _upsert_cfg(session, retry_key, state, f"SpeedPRO 重試狀態（racedate={racedate_str} R{int(rn)}）")
            print(f"retry {racedate_str} R{int(rn)} in {minutes}m reason={reason}")

        if not any_work:
            print(f"no work racedate={racedate_str}")
    finally:
        try:
            _release_lock(session)
        except Exception:
            pass
        session.close()


if __name__ == "__main__":
    main()
