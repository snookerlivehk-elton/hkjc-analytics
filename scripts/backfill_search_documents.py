import argparse
import sys
from pathlib import Path
from datetime import datetime, time as dtime, timedelta

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import init_db, get_session
from database.models import Race, SystemConfig
from scoring_engine.normalization import bucket_parts
from scoring_engine.search_index import index_race_entry_bundle, index_system_config_doc


def _parse_ymd(s: str):
    try:
        return datetime.strptime(str(s).strip(), "%Y/%m/%d").date()
    except Exception:
        try:
            return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()
        except Exception:
            return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="d1", default=None, help="YYYY/MM/DD")
    p.add_argument("--to", dest="d2", default=None, help="YYYY/MM/DD")
    p.add_argument("--limit-races", dest="limit_races", default="5000")
    args = p.parse_args()

    init_db()
    session = get_session()
    try:
        q = session.query(Race).order_by(Race.race_date.asc(), Race.race_no.asc(), Race.id.asc())
        d1 = _parse_ymd(args.d1) if args.d1 else None
        d2 = _parse_ymd(args.d2) if args.d2 else None
        if d1:
            q = q.filter(Race.race_date >= datetime.combine(d1, dtime.min))
        if d2:
            q = q.filter(Race.race_date < datetime.combine(d2, dtime.min) + timedelta(days=1))
        limit_races = int(args.limit_races or 5000)
        races = q.limit(limit_races).all()

        done = 0
        for r in races:
            rid = int(getattr(r, "id") or 0)
            if not rid:
                continue
            index_race_entry_bundle(session, rid)

            ds = ""
            try:
                ds = r.race_date.strftime("%Y/%m/%d")
            except Exception:
                ds = ""
            rn = int(getattr(r, "race_no", 0) or 0)
            if ds and rn:
                index_system_config_doc(session, f"race_runpos:{ds}:{rn}", doc_type="runpos", title=f"{ds} R{rn} runpos")
                index_system_config_doc(session, f"ai_race_report:{ds}:{rn}", doc_type="ai_report", title=f"{ds} R{rn} AI report")
                index_system_config_doc(session, f"ai_race_reflection:{ds}:{rn}", doc_type="ai_reflection", title=f"{ds} R{rn} AI reflection")

            parts = bucket_parts(session, r)
            if parts:
                v, g, c, d = parts
                index_system_config_doc(session, f"trkprof:{v}:{g}:{c}:{d}", doc_type="trkprof", title=f"trkprof:{v}:{g}:{c}:{d}")

            done += 1
            if done % 50 == 0:
                session.commit()
                print(f"progress races={done}")

        index_system_config_doc(session, "trkprof_index", doc_type="trkprof_index", title="trkprof_index")
        index_system_config_doc(session, "ai_learned_rules", doc_type="ai_learned_rules", title="ai_learned_rules")
        scenario_keys = (
            session.query(SystemConfig.key)
            .filter(SystemConfig.key.like("ai_race_report_scenario:%"))
            .order_by(SystemConfig.key.asc())
            .all()
        )
        for (k,) in scenario_keys:
            if not k:
                continue
            index_system_config_doc(session, str(k), doc_type="ai_report", title=str(k))
        session.commit()
        print(f"done races={done}")
    finally:
        session.close()


if __name__ == "__main__":
    main()
