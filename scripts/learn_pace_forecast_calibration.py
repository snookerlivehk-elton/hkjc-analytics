import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from scoring_engine.pace_forecast_calibration import learn_pace_forecast_calibration, save_pace_forecast_calibration


def _parse_ymd(s: str) -> date | None:
    t = str(s or "").strip().replace("-", "/")
    if not t:
        return None
    try:
        return datetime.strptime(t, "%Y/%m/%d").date()
    except Exception:
        return None


def main():
    init_db()
    session = get_session()
    try:
        end_d = _parse_ymd(str(os.environ.get("END_DATE") or "")) or datetime.utcnow().date()
        start_d = _parse_ymd(str(os.environ.get("START_DATE") or "")) or (end_d - timedelta(days=180))
        if start_d > end_d:
            start_d, end_d = end_d, start_d

        min_s = str(os.environ.get("MIN_SAMPLES") or "").strip()
        try:
            min_samples = int(min_s) if min_s else 80
        except Exception:
            min_samples = 80

        a_s = str(os.environ.get("ALPHA") or "").strip()
        try:
            alpha = float(a_s) if a_s else 0.25
        except Exception:
            alpha = 0.25

        payload = learn_pace_forecast_calibration(
            session,
            start_date=start_d,
            end_date=end_d,
            min_samples=int(min_samples),
            alpha=float(alpha),
        )
        save_pace_forecast_calibration(session, payload)

        g = payload.get("global") if isinstance(payload, dict) else None
        g_thr = g.get("thresholds") if isinstance(g, dict) else None
        groups = payload.get("groups") if isinstance(payload, dict) else None
        groups_n = len(groups) if isinstance(groups, dict) else 0

        print(f"range={start_d.isoformat()}..{end_d.isoformat()}")
        print(f"n_pairs={int(payload.get('n_pairs') or 0)} alpha={float(payload.get('alpha') or 0.0)} min_samples={int(payload.get('min_samples') or 0)}")
        print("global_thresholds", g_thr)
        print("groups", groups_n)
    finally:
        session.close()


if __name__ == "__main__":
    main()

