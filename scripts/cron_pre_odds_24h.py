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
from database.models import OddsHistory, Race, RaceDayWeather, RaceEntry, RacePoolSnapshot, SystemConfig
from data_scraper.odds import OddsScraper
from scoring_engine.normalization import venue_code
from scoring_engine.raw_snapshots import upsert_raw_snapshot


HK_TZ = ZoneInfo("Asia/Hong_Kong")


def _day_range(date_str: str) -> Tuple[datetime, datetime]:
    d0 = datetime.strptime(str(date_str), "%Y/%m/%d").date()
    start = datetime.combine(d0, dtime.min)
    end = start + timedelta(days=1)
    return start, end


def _parse_hhmm(s: str) -> Optional[dtime]:
    t = str(s or "").strip()
    if not t or ":" not in t:
        return None
    parts = t.split(":")
    if len(parts) != 2:
        return None
    try:
        hh = int(parts[0])
        mm = int(parts[1])
    except Exception:
        return None
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        return None
    return dtime(hh, mm)


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


def _target_racedate_str(session) -> str:
    env_date = str(os.environ.get("TARGET_DATE") or "").strip()
    if env_date:
        return env_date
    cfg = _get_cfg(session, "fixture_next_raceday")
    if cfg and isinstance(cfg.value, str) and cfg.value.strip():
        return cfg.value.strip()
    return datetime.now(HK_TZ).strftime("%Y/%m/%d")


def _trigger_time_for_raceday(session, date_str: str) -> Tuple[Optional[datetime], Optional[datetime], str]:
    start, end = _day_range(date_str)
    rows = (
        session.query(Race.race_no, Race.post_time_hk)
        .filter(Race.race_date >= start)
        .filter(Race.race_date < end)
        .order_by(Race.race_no.asc(), Race.id.asc())
        .all()
    )
    times = []
    for rn, pt in rows:
        tt = _parse_hhmm(str(pt or ""))
        if tt:
            times.append((int(rn or 0), tt))
    if not times:
        return None, None, "no_post_time_hk"
    times.sort(key=lambda x: (x[1].hour, x[1].minute, x[0]))
    first_rn, first_t = times[0]

    d0 = datetime.strptime(str(date_str), "%Y/%m/%d").date()
    first_post = datetime.combine(d0, first_t).replace(tzinfo=HK_TZ)
    trigger = first_post - timedelta(hours=24)
    return first_post, trigger, f"R{int(first_rn)}"


def _is_within_trigger_window(now_hk: datetime, trigger_dt: datetime) -> bool:
    if str(os.environ.get("IGNORE_WINDOW") or "").strip().lower() in ("1", "true", "yes"):
        return True
    win_s = str(os.environ.get("PRE_ODDS_24H_WINDOW_MINUTES") or "").strip()
    try:
        win_m = int(win_s) if win_s else 20
    except Exception:
        win_m = 20
    win_m = max(1, int(win_m))
    return (now_hk >= trigger_dt) and (now_hk <= (trigger_dt + timedelta(minutes=int(win_m))))


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


def main():
    init_db()
    session = get_session()
    try:
        now_hk = datetime.now(HK_TZ)
        date_str = _target_racedate_str(session)

        first_post, trigger_dt, anchor = _trigger_time_for_raceday(session, date_str)
        if not first_post or not trigger_dt:
            print(f"skip date={date_str} reason=no_post_time_hk")
            return
        if not _is_within_trigger_window(now_hk, trigger_dt):
            print(
                f"not_due now_hk={now_hk.isoformat()} trigger={trigger_dt.isoformat()} first_post={first_post.isoformat()} anchor={anchor}"
            )
            return

        start, end = _day_range(date_str)
        races = (
            session.query(Race.id, Race.race_no, Race.venue)
            .filter(Race.race_date >= start)
            .filter(Race.race_date < end)
            .order_by(Race.race_no.asc(), Race.id.asc())
            .all()
        )
        if not races:
            print(f"no races date={date_str}")
            return

        enable_weather = str(os.environ.get("ENABLE_WINDTRACKER_IN_PRE_ODDS") or "1").strip().lower() in ("1", "true", "yes")
        weather_done: set[tuple[str, str, str]] = set()

        def _bucket_5m_key(dt_hk: datetime) -> str:
            try:
                m0 = (int(dt_hk.minute) // 5) * 5
            except Exception:
                m0 = 0
            dt2 = dt_hk.replace(minute=int(m0), second=0, microsecond=0)
            return dt2.strftime("%Y%m%d%H%M")

        def _maybe_update_weather(venue: str):
            if not enable_weather:
                return
            bkey = _bucket_5m_key(now_hk)
            sig = (str(date_str), str(venue), str(bkey))
            if sig in weather_done:
                return

            try:
                race_day = datetime.strptime(str(date_str), "%Y/%m/%d").date()
            except Exception:
                return
            try:
                row_w = session.query(RaceDayWeather).filter_by(race_date_day=race_day, venue=str(venue)).first()
                if row_w and getattr(row_w, "updated_at", None):
                    try:
                        age_sec = (datetime.utcnow() - row_w.updated_at).total_seconds()
                        if age_sec >= 0 and age_sec < 290:
                            weather_done.add(sig)
                            return
                    except Exception:
                        pass
            except Exception:
                pass

            try:
                from scripts.fetch_windtracker import main as _fetch_windtracker_main

                os.environ["TARGET_DATE"] = str(date_str)
                os.environ["TARGET_VENUE"] = str(venue)
                _fetch_windtracker_main()
                weather_done.add(sig)
            except Exception:
                return

        try:
            venues = []
            for _rid, _rn, _v in races:
                vv = venue_code(str(_v or "").strip())
                if vv not in {"HV", "ST"}:
                    vv = "HV"
                venues.append(vv)
            venues = list(dict.fromkeys(venues))
            for vv in venues:
                _maybe_update_weather(vv)
        except Exception:
            pass

        scraper = OddsScraper()
        any_ok = False
        ok_races = 0
        total_races = 0

        for rid, rn, v in races:
            try:
                rno = int(rn or 0)
            except Exception:
                rno = 0
            if rno <= 0:
                continue
            total_races += 1

            done_key = f"pre_odds_24h_done:{date_str}:{rno}"
            if bool(_get_cfg_value(session, done_key) is True):
                ok_races += 1
                continue

            entry_rows = (
                session.query(RaceEntry.id, RaceEntry.horse_no)
                .filter(RaceEntry.race_id == int(rid))
                .all()
            )
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
                print(f"skip {date_str} R{rno} reason=no_entries")
                continue

            exists = (
                session.query(OddsHistory.id)
                .filter(OddsHistory.entry_id.in_(list(entry_by_hn.values())))
                .filter(OddsHistory.odds_type == "PRE_24H")
                .limit(1)
                .all()
            )
            if exists:
                _upsert_cfg(session, done_key, True, f"賽前賠率快照（24H）完成（{date_str} R{rno}）")
                ok_races += 1
                continue

            venue = venue_code(str(v or "").strip())
            if venue not in {"HV", "ST"}:
                venue = "HV"

            snap = scraper.get_wp_snapshot(race_no=rno, race_date=date_str, venue=venue)
            odds_map = _normalize_odds_rows(list(snap.get("odds") or []))
            pools = dict(snap.get("pools") or {})
            update_time_hk = snap.get("update_time_hk")

            valid_cnt = 0
            to_add: List[OddsHistory] = []
            for hn, odds in odds_map.items():
                eid = entry_by_hn.get(int(hn))
                if not eid:
                    continue
                to_add.append(
                    OddsHistory(
                        entry_id=int(eid),
                        odds_type="PRE_24H",
                        win_odds=float(odds["win_odds"]),
                        place_odds=float(odds["place_odds"]),
                        captured_at=datetime.utcnow(),
                    )
                )
                valid_cnt += 1

            if valid_cnt < max(6, int(len(entry_by_hn) * 0.6)):
                print(f"retry {date_str} R{rno} venue={venue} reason=no_odds_yet valid={valid_cnt}/{len(entry_by_hn)}")
                continue

            for row in to_add:
                session.add(row)

            try:
                day = start.date()
                win_pool = _pool_amount(pools, "獨贏")
                pla_pool = _pool_amount(pools, "位置")
                ps = (
                    session.query(RacePoolSnapshot)
                    .filter_by(race_id=int(rid), snapshot_type="PRE_24H", source="BET_WP")
                    .first()
                )
                if not ps:
                    ps = RacePoolSnapshot(
                        race_id=int(rid),
                        race_date_day=day,
                        venue=str(venue),
                        race_no=int(rno),
                        snapshot_type="PRE_24H",
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
                    entity_key=f"{date_str}:{venue}:{int(rno)}:PRE_24H",
                    payload=snap,
                    race_id=int(rid),
                    meta={"odds_type": "PRE_24H"},
                    fetched_at=datetime.utcnow(),
                )
            except Exception:
                pass

            session.commit()
            any_ok = True
            ok_races += 1
            _upsert_cfg(session, done_key, True, f"賽前賠率快照（24H）完成（{date_str} R{rno}）")

            info_key = f"pre_odds_24h_info:{date_str}:{rno}"
            _upsert_cfg(
                session,
                info_key,
                {"date": date_str, "race_no": rno, "venue": venue, "captured_at_hk": now_hk.isoformat(), "valid_rows": valid_cnt},
                f"賽前賠率快照（24H）資訊（{date_str} R{rno}）",
            )
            print(f"ok {date_str} R{rno} venue={venue} rows={valid_cnt}")

        if any_ok:
            day_key = f"pre_odds_24h_done:{date_str}"
            _upsert_cfg(
                session,
                day_key,
                True if ok_races >= total_races else {"ok_races": ok_races, "total_races": total_races, "captured_at_hk": now_hk.isoformat()},
                f"賽前賠率快照（24H）狀態（{date_str}）",
            )
        print(f"done date={date_str} ok_races={ok_races}/{total_races}")
    finally:
        session.close()


if __name__ == "__main__":
    main()

