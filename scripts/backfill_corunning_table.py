import argparse
import sys
from pathlib import Path
from datetime import datetime, time as dtime, timedelta

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import init_db, get_session
from database.models import Race, SystemConfig, RaceCoRunning
from scoring_engine.config_value import unwrap_value


def _day_range(d):
    start = datetime.combine(d, dtime.min)
    end = start + timedelta(days=1)
    return start, end


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=None, help="YYYY/MM/DD")
    args = p.parse_args()

    init_db()
    session = get_session()
    try:
        q = session.query(SystemConfig).filter(SystemConfig.key.like("race_corunning:%"))
        if args.date:
            ds = str(args.date).strip()
            q = q.filter(SystemConfig.key.like(f"race_corunning:{ds}:%"))
        cfgs = q.order_by(SystemConfig.key.asc()).all()
        moved = 0
        skipped = 0
        for cfg in cfgs:
            k = str(cfg.key or "")
            parts = k.split(":")
            if len(parts) < 3:
                skipped += 1
                continue
            ds = parts[1]
            try:
                rn = int(parts[2])
            except Exception:
                skipped += 1
                continue
            payload, meta = unwrap_value(cfg.value)
            payload = payload if isinstance(payload, dict) else {}
            items = payload.get("items")
            if not isinstance(items, dict) or not items:
                skipped += 1
                continue
            try:
                d0 = datetime.strptime(ds, "%Y/%m/%d").date()
            except Exception:
                skipped += 1
                continue
            start_dt, end_dt = _day_range(d0)
            race = (
                session.query(Race)
                .filter(Race.race_date >= start_dt, Race.race_date < end_dt)
                .filter(Race.race_no == int(rn))
                .first()
            )
            if not race:
                skipped += 1
                continue
            row = session.query(RaceCoRunning).filter_by(race_id=int(race.id)).first()
            if not row:
                row = RaceCoRunning(
                    race_id=int(race.id),
                    race_date=race.race_date,
                    race_no=int(race.race_no or 0),
                    source=str((meta or {}).get("source") or "HKJC"),
                    items={},
                )
                session.add(row)
            row.items = items
            row.meta = {"migrated_from": k, "schema": str((meta or {}).get("schema") or "race_corunning:v1")}
            try:
                fa = str((meta or {}).get("fetched_at") or "").strip()
                if fa:
                    row.fetched_at = datetime.fromisoformat(fa.replace("Z", ""))
            except Exception:
                pass
            moved += 1
        session.commit()
        print(f"完成：moved={moved} skipped={skipped} total={len(cfgs)}")
    finally:
        session.close()


if __name__ == "__main__":
    main()

