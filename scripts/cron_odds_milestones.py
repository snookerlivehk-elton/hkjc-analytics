import os
import sys
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import init_db, get_session
from database.models import OddsHistory, Race, RaceEntry, RacePoolSnapshot, SystemConfig
from data_scraper.odds import OddsScraper
from scoring_engine.normalization import venue_code
from scoring_engine.raw_snapshots import upsert_raw_snapshot


HK_TZ = ZoneInfo("Asia/Hong_Kong")


def _day_range(date_str: str) -> Tuple[datetime, datetime]:
    d0 = datetime.strptime(str(date_str), "%Y/%m/%d").date()
    start = datetime.combine(d0, dtime.min)
    end = start + timedelta(days=1)
    return start, end


def _target_racedate_str(session) -> str:
    env_date = str(os.environ.get("TARGET_DATE") or "").strip()
    if env_date:
        return env_date
    cfg = session.query(SystemConfig).filter_by(key="fixture_next_raceday").first()
    v = cfg.value if cfg else None
    if isinstance(v, str) and v.strip():
        return v.strip()
    return datetime.now(HK_TZ).strftime("%Y/%m/%d")


def _get_cfg(session, key: str) -> Optional[SystemConfig]:
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


def _parse_hhmm_dt(date_str: str, s: str) -> Optional[datetime]:
    t = str(s or "").strip()
    if not t or ":" not in t:
        return None
    try:
        hh, mm = t.split(":")
        hh_i = int(hh)
        mm_i = int(mm)
        if hh_i < 0 or hh_i > 23 or mm_i < 0 or mm_i > 59:
            return None
        d0 = datetime.strptime(str(date_str), "%Y/%m/%d").date()
        return datetime.combine(d0, dtime(hh_i, mm_i)).replace(tzinfo=HK_TZ)
    except Exception:
        return None


def _normalize_odds_rows(rows: List[Dict]) -> Dict[int, Dict[str, float]]:
    out: Dict[int, Dict[str, float]] = {}
    for r in rows or []:
        try:
            hn = int(r.get("horse_no") or 0)
        except Exception:
            continue
        if hn <= 0:
            continue
        wo = r.get("win_odds")
        po = r.get("place_odds")
        try:
            wo_f = float(wo) if wo is not None else None
        except Exception:
            wo_f = None
        try:
            po_f = float(po) if po is not None else None
        except Exception:
            po_f = None
        if not (wo_f and wo_f > 0):
            continue
        if not (po_f and po_f > 0):
            continue
        out[int(hn)] = {"win_odds": wo_f, "place_odds": po_f}
    return out


def _pool_amount(pools: Dict, label: str) -> Optional[int]:
    if not isinstance(pools, dict):
        return None
    if label in pools:
        try:
            return int(pools[label])
        except Exception:
            return None
    for k, v in pools.items():
        if str(k).strip() == label:
            try:
                return int(v)
            except Exception:
                return None
    return None


def _milestones() -> List[int]:
    s = str(os.environ.get("ODDS_MILESTONES") or "").strip()
    if not s:
        return [30, 15, 10, 5]
    out: List[int] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            v = int(part)
        except Exception:
            continue
        if v > 0:
            out.append(v)
    return out or [30, 15, 10, 5]


def main():
    init_db()
    session = get_session()
    try:
        now_hk = datetime.now(HK_TZ)
        date_str = _target_racedate_str(session)
        start, end = _day_range(date_str)

        watch_before_s = str(os.environ.get("ODDS_WATCH_BEFORE_MINUTES") or "").strip()
        watch_after_s = str(os.environ.get("ODDS_WATCH_AFTER_MINUTES") or "").strip()
        try:
            watch_before_min = int(watch_before_s) if watch_before_s else 90
        except Exception:
            watch_before_min = 90
        try:
            watch_after_min = int(watch_after_s) if watch_after_s else 10
        except Exception:
            watch_after_min = 10

        tol_min_s = str(os.environ.get("ODDS_MILESTONE_TOL_MINUTES") or "").strip()
        rearm_s = str(os.environ.get("ODDS_MILESTONE_REARM_MINUTES") or "").strip()
        try:
            tol_min = int(tol_min_s) if tol_min_s else 2
        except Exception:
            tol_min = 2
        try:
            rearm_min = int(rearm_s) if rearm_s else 3
        except Exception:
            rearm_min = 3

        races = (
            session.query(Race.id, Race.race_no, Race.venue, Race.post_time_hk)
            .filter(Race.race_date >= start)
            .filter(Race.race_date < end)
            .order_by(Race.race_no.asc(), Race.id.asc())
            .all()
        )
        if not races:
            print(f"no races date={date_str}")
            return

        scraper = OddsScraper()
        ms = _milestones()

        ok_events = 0
        total_events = 0

        for rid, rn, v, post_time_hk_db in races:
            try:
                rno = int(rn or 0)
            except Exception:
                continue
            if rno <= 0:
                continue

            venue = venue_code(str(v or "").strip())
            if venue not in {"HV", "ST"}:
                venue = "HV"

            entry_rows = session.query(RaceEntry.id, RaceEntry.horse_no).filter(RaceEntry.race_id == int(rid)).all()
            entry_by_hn: Dict[int, int] = {}
            for eid, hn in entry_rows:
                try:
                    hni = int(hn or 0)
                    eidi = int(eid or 0)
                except Exception:
                    continue
                if hni > 0 and eidi > 0:
                    entry_by_hn[hni] = eidi
            if not entry_by_hn:
                continue

            post_time_hk_use = str(post_time_hk_db or "").strip()
            post_dt = _parse_hhmm_dt(date_str, post_time_hk_use)
            if post_dt is None:
                continue
            delta_min = int(round((post_dt - now_hk).total_seconds() / 60.0))

            last_seen_key = f"odds_milestone_last_seen:{date_str}:{rno}"
            prev_delta: Optional[int] = None
            try:
                last_v = _get_cfg_value(session, last_seen_key)
                if isinstance(last_v, dict) and ("delta_min" in last_v):
                    prev_delta = int(last_v.get("delta_min"))
            except Exception:
                prev_delta = None

            if delta_min > int(watch_before_min):
                if prev_delta is None:
                    continue
            if delta_min < -int(watch_after_min):
                continue

            should_fetch = False
            if prev_delta is not None:
                for m in ms:
                    if (prev_delta > int(m)) and (delta_min <= int(m)):
                        should_fetch = True
                        break
            else:
                for m in ms:
                    if abs(delta_min - int(m)) <= int(tol_min):
                        should_fetch = True
                        break

            if not should_fetch:
                try:
                    _upsert_cfg(
                        session,
                        last_seen_key,
                        {"delta_min": int(delta_min), "seen_at_hk": now_hk.isoformat(), "post_time_hk": post_time_hk_use},
                        f"臨場賠率快照監測（最後一次 delta_min）（{date_str} R{rno}）",
                    )
                except Exception:
                    pass
                continue

            snap = scraper.get_wp_snapshot(race_no=rno, race_date=date_str, venue=venue)
            odds_map = _normalize_odds_rows(list(snap.get("odds") or []))
            pools = dict(snap.get("pools") or {})
            update_time_hk = snap.get("update_time_hk")

            post_time_hk_snap = str(snap.get("post_time_hk") or "").strip()
            if post_time_hk_snap and (post_time_hk_snap != post_time_hk_use):
                post_time_hk_use = post_time_hk_snap
                post_dt2 = _parse_hhmm_dt(date_str, post_time_hk_use)
                if post_dt2 is not None:
                    delta_min = int(round((post_dt2 - now_hk).total_seconds() / 60.0))
                try:
                    rr = session.query(Race).filter(Race.id == int(rid)).first()
                    if rr and str(rr.post_time_hk or "").strip() != post_time_hk_snap:
                        rr.post_time_hk = post_time_hk_snap
                        session.commit()
                except Exception:
                    pass

            for m in ms:
                total_events += 1
                odds_type = f"PRE_{int(m)}M"
                done_key = f"odds_milestone_done:{date_str}:{rno}:{int(m)}"

                done_v = _get_cfg_value(session, done_key)
                done = bool(done_v is True)
                if done and (delta_min > int(m) + int(rearm_min)):
                    _upsert_cfg(session, done_key, False, f"臨場賠率快照（-{m} 分）重置（{date_str} R{rno}）")
                    done = False

                if done:
                    ok_events += 1
                    continue

                trigger = False
                if (prev_delta is not None) and (prev_delta > int(m)) and (delta_min <= int(m)):
                    trigger = True
                elif prev_delta is None and abs(delta_min - int(m)) <= int(tol_min):
                    trigger = True
                if not trigger:
                    continue

                valid_cnt = 0
                to_add: List[OddsHistory] = []
                for hn, odds in odds_map.items():
                    eid = entry_by_hn.get(int(hn))
                    if not eid:
                        continue
                    to_add.append(
                        OddsHistory(
                            entry_id=int(eid),
                            odds_type=odds_type,
                            win_odds=float(odds["win_odds"]),
                            place_odds=float(odds["place_odds"]),
                            captured_at=datetime.utcnow(),
                        )
                    )
                    valid_cnt += 1

                if valid_cnt < max(6, int(len(entry_by_hn) * 0.6)):
                    print(f"skip {date_str} R{rno} {odds_type} reason=no_odds_yet valid={valid_cnt}/{len(entry_by_hn)} delta_min={delta_min}")
                    continue

                for row in to_add:
                    session.add(row)

                try:
                    day = start.date()
                    win_pool = _pool_amount(pools, "獨贏")
                    pla_pool = _pool_amount(pools, "位置")
                    ps = (
                        session.query(RacePoolSnapshot)
                        .filter_by(race_id=int(rid), snapshot_type=odds_type, source="BET_WP")
                        .first()
                    )
                    if not ps:
                        ps = RacePoolSnapshot(
                            race_id=int(rid),
                            race_date_day=day,
                            venue=str(venue),
                            race_no=int(rno),
                            snapshot_type=odds_type,
                            source="BET_WP",
                        )
                        session.add(ps)
                    ps.update_time_hk = str(update_time_hk or "").strip() or None
                    ps.pools = pools
                    ps.win_pool = int(win_pool) if win_pool is not None else None
                    ps.place_pool = int(pla_pool) if pla_pool is not None else None
                except Exception:
                    pass

                try:
                    upsert_raw_snapshot(
                        session,
                        source="BET_WP",
                        entity_type="race",
                        entity_key=f"{date_str}:{venue}:{int(rno)}:{odds_type}",
                        payload=snap,
                        race_id=int(rid),
                        meta={"odds_type": odds_type, "delta_min": delta_min, "post_time_hk": post_time_hk_use},
                        fetched_at=datetime.utcnow(),
                    )
                except Exception:
                    pass

                session.commit()
                _upsert_cfg(session, done_key, True, f"臨場賠率快照（-{m} 分）完成（{date_str} R{rno}）")
                info_key = f"odds_milestone_info:{date_str}:{rno}:{int(m)}"
                _upsert_cfg(
                    session,
                    info_key,
                    {
                        "date": date_str,
                        "race_no": rno,
                        "venue": venue,
                        "odds_type": odds_type,
                        "captured_at_hk": now_hk.isoformat(),
                        "post_time_hk": post_time_hk_use,
                        "delta_min": delta_min,
                        "valid_rows": valid_cnt,
                    },
                    f"臨場賠率快照（-{m} 分）資訊（{date_str} R{rno}）",
                )
                ok_events += 1
                print(f"ok {date_str} R{rno} {odds_type} delta_min={delta_min} rows={valid_cnt}")

            try:
                _upsert_cfg(
                    session,
                    last_seen_key,
                    {"delta_min": int(delta_min), "seen_at_hk": now_hk.isoformat(), "post_time_hk": post_time_hk_use},
                    f"臨場賠率快照監測（最後一次 delta_min）（{date_str} R{rno}）",
                )
            except Exception:
                pass

        print(f"done date={date_str} ok_events={ok_events}/{total_events}")
    finally:
        session.close()


if __name__ == '__main__':
    main()
