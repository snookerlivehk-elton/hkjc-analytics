import sys
from pathlib import Path
from datetime import timedelta

import pandas as pd
import streamlit as st

root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.models import EntryFact, Trainer
from web_ui.auth import require_superadmin
from web_ui.nav import render_admin_nav


@st.cache_data(ttl=60)
def _date_bounds():
    s = get_session()
    try:
        dmin = s.query(EntryFact.race_date_day).order_by(EntryFact.race_date_day.asc()).limit(1).all()
        dmax = s.query(EntryFact.race_date_day).order_by(EntryFact.race_date_day.desc()).limit(1).all()
        d1 = dmin[0][0] if dmin and dmin[0] else None
        d2 = dmax[0][0] if dmax and dmax[0] else None
        return d1, d2
    finally:
        s.close()


def main():
    st.set_page_config(page_title="同場練馬師多出馬（SP）", layout="wide")
    require_superadmin("同場練馬師多出馬（SP）")
    render_admin_nav(active="trainer_multi")
    st.title("👥 同場練馬師出馬 > 1（SP）")
    st.caption("來源：entry_facts（事實表）。以賽後 SP 賠率作主統計來源。")

    init_db()

    dmin, dmax = _date_bounds()
    if not dmin or not dmax:
        st.info("尚未有 entry_facts。請先抓賽果（或手動回填 build_entry_facts）。")
        return

    default_end = dmax
    default_start = max(dmin, dmax - timedelta(days=180))

    c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
    with c1:
        d1 = st.date_input("開始日期", value=default_start, min_value=dmin, max_value=dmax)
    with c2:
        d2 = st.date_input("結束日期", value=default_end, min_value=dmin, max_value=dmax)
    with c3:
        mode = st.selectbox("統計模式", options=["只計多出馬", "多出馬 vs 單出馬"], index=0)
    with c4:
        group_bucket = st.checkbox("按 SP 賠率分桶", value=False)

    if isinstance(d1, list):
        d1 = d1[0] if d1 else default_start
    if isinstance(d2, list):
        d2 = d2[0] if d2 else default_end
    if d1 > d2:
        st.warning("開始日期不能晚於結束日期。")
        return

    session = get_session()
    try:
        from sqlalchemy import func, case

        base = session.query(EntryFact).filter(EntryFact.race_date_day >= d1).filter(EntryFact.race_date_day <= d2)

        rows_t = (
            session.query(Trainer.id, Trainer.name_ch)
            .join(EntryFact, EntryFact.trainer_id == Trainer.id)
            .filter(EntryFact.race_date_day >= d1)
            .filter(EntryFact.race_date_day <= d2)
            .distinct()
            .order_by(Trainer.name_ch.asc())
            .all()
        )
        trainer_opts = [("ALL", "ALL")] + [(int(i), str(n or "").strip() or f"T{i}") for i, n in rows_t]

        f1, f2, f3, f4, f5, f6, f7, f8 = st.columns([1.4, 1.2, 1.2, 1.2, 1.2, 1.2, 1.2, 1.2])
        with f1:
            trainer_sel = st.selectbox("練馬師", options=trainer_opts, format_func=lambda x: x[1], index=0)
        with f2:
            venue = st.selectbox("場地", options=["ALL"] + [r[0] for r in session.query(EntryFact.venue).distinct().order_by(EntryFact.venue.asc()).all() if r and r[0]], index=0)
        with f3:
            surface = st.selectbox("場地類型", options=["ALL"] + [r[0] for r in session.query(EntryFact.surface).distinct().order_by(EntryFact.surface.asc()).all() if r and r[0]], index=0)
        with f4:
            course_type = st.selectbox("賽道", options=["ALL"] + [r[0] for r in session.query(EntryFact.course_type).distinct().order_by(EntryFact.course_type.asc()).all() if r and r[0]], index=0)
        with f5:
            going_code = st.selectbox("場地狀況", options=["ALL"] + [r[0] for r in session.query(EntryFact.going_code).distinct().order_by(EntryFact.going_code.asc()).all() if r and r[0]], index=0)
        with f6:
            race_class = st.selectbox("班次", options=["ALL"] + [r[0] for r in session.query(EntryFact.race_class).distinct().order_by(EntryFact.race_class.asc()).all() if r and r[0]], index=0)
        with f7:
            runstyle_opts = ["ALL", "LEADER", "PROMINENT", "MIDFIELD", "BACKMARKER", "UNKNOWN"]
            runstyle_label = {"ALL": "ALL", "LEADER": "領放", "PROMINENT": "跟前", "MIDFIELD": "中置", "BACKMARKER": "後上", "UNKNOWN": "UNKNOWN"}
            runstyle = st.selectbox("跑法", options=runstyle_opts, format_func=lambda x: runstyle_label.get(str(x), str(x)), index=0)
        with f8:
            pace_opts = ["ALL", "FAST", "NORMAL", "SLOW", "UNKNOWN"]
            pace_label = {"ALL": "ALL", "FAST": "偏快", "NORMAL": "正常", "SLOW": "偏慢", "UNKNOWN": "UNKNOWN"}
            pace_bucket = st.selectbox("步速", options=pace_opts, format_func=lambda x: pace_label.get(str(x), str(x)), index=0)

        q = base
        if trainer_sel[0] != "ALL":
            q = q.filter(EntryFact.trainer_id == int(trainer_sel[0]))
        if venue != "ALL":
            q = q.filter(EntryFact.venue == str(venue))
        if surface != "ALL":
            q = q.filter(EntryFact.surface == str(surface))
        if course_type != "ALL":
            q = q.filter(EntryFact.course_type == str(course_type))
        if going_code != "ALL":
            q = q.filter(EntryFact.going_code == str(going_code))
        if race_class != "ALL":
            q = q.filter(EntryFact.race_class == str(race_class))
        if runstyle != "ALL":
            q = q.filter(EntryFact.runstyle_bucket == str(runstyle))
        if pace_bucket != "ALL":
            q = q.filter(EntryFact.pace_bucket == str(pace_bucket))

        cnt_sub = (
            session.query(
                EntryFact.race_id.label("race_id"),
                EntryFact.trainer_id.label("trainer_id"),
                func.count(EntryFact.id).label("runner_cnt"),
            )
            .filter(EntryFact.race_date_day >= d1)
            .filter(EntryFact.race_date_day <= d2)
            .group_by(EntryFact.race_id, EntryFact.trainer_id)
            .subquery()
        )

        q2 = q.join(cnt_sub, (EntryFact.race_id == cnt_sub.c.race_id) & (EntryFact.trainer_id == cnt_sub.c.trainer_id))
        is_multi = cnt_sub.c.runner_cnt >= 2

        dims = [EntryFact.trainer_id]
        cols = ["trainer_id"]
        if mode == "多出馬 vs 單出馬":
            dims.append(case((is_multi, "multi"), else_="single"))
            cols.append("multi_flag")
        if group_bucket:
            dims.append(EntryFact.odds_bucket_sp)
            cols.append("odds_bucket_sp")

        if mode == "只計多出馬":
            q2 = q2.filter(is_multi)

        rows = (
            q2.with_entities(
                *dims,
                func.count(EntryFact.id).label("samples"),
                func.sum(case((EntryFact.is_win == True, 1), else_=0)).label("win_cnt"),
                func.sum(case((EntryFact.is_place == True, 1), else_=0)).label("place_cnt"),
            )
            .group_by(*dims)
            .order_by(*dims)
            .all()
        )

        if not rows:
            st.info("此條件下未有任何樣本。")
            return

        df = pd.DataFrame(rows, columns=cols + ["samples", "win_cnt", "place_cnt"])
        name_map = {int(i): str(n or "").strip() or f"T{i}" for i, n in rows_t}
        df["trainer_name"] = df["trainer_id"].map(name_map).fillna(df["trainer_id"].astype(str))
        df["win_rate"] = (df["win_cnt"] / df["samples"]).fillna(0.0)
        df["place_rate"] = (df["place_cnt"] / df["samples"]).fillna(0.0)

        show_cols = ["trainer_name"] + [c for c in cols if c != "trainer_id"] + ["samples", "win_cnt", "place_cnt", "win_rate", "place_rate"]
        show = df[show_cols].copy()
        show["win_rate"] = (show["win_rate"] * 100.0).round(2)
        show["place_rate"] = (show["place_rate"] * 100.0).round(2)
        show = show.rename(columns={"win_rate": "win_rate(%)", "place_rate": "place_rate(%)"})
        st.dataframe(show, use_container_width=True, hide_index=True)
        st.download_button(
            "下載 CSV",
            data=show.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"trainer_multi_sp_{d1.isoformat()}_{d2.isoformat()}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    finally:
        session.close()


if __name__ == "__main__":
    main()
