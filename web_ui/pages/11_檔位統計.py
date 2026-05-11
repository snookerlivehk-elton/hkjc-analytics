import sys
from pathlib import Path
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.models import DrawStatsDaily
from web_ui.auth import require_superadmin
from web_ui.nav import render_admin_nav


@st.cache_data(ttl=60)
def _date_bounds():
    s = get_session()
    try:
        dmin = s.query(DrawStatsDaily.race_date_day).order_by(DrawStatsDaily.race_date_day.asc()).limit(1).all()
        dmax = s.query(DrawStatsDaily.race_date_day).order_by(DrawStatsDaily.race_date_day.desc()).limit(1).all()
        d1 = dmin[0][0] if dmin and dmin[0] else None
        d2 = dmax[0][0] if dmax and dmax[0] else None
        return d1, d2
    finally:
        s.close()


def main():
    st.set_page_config(page_title="檔位統計（快照）", layout="wide")
    require_superadmin("檔位統計（快照）")
    render_admin_nav(active="draw_stats")
    st.title("🎯 檔位勝出/入圍統計（快照｜SP）")
    st.caption("來源：draw_stats_daily（每日聚合快照）。以賽後 SP 賠率作主統計來源。")

    init_db()

    dmin, dmax = _date_bounds()
    if not dmin or not dmax:
        st.info("尚未有 draw_stats_daily 快照。請先抓賽果（或手動回填 build_draw_stats_daily）。")
        return

    default_end = dmax
    default_start = max(dmin, dmax - timedelta(days=180))

    c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
    with c1:
        d1 = st.date_input("開始日期", value=default_start, min_value=dmin, max_value=dmax)
    with c2:
        d2 = st.date_input("結束日期", value=default_end, min_value=dmin, max_value=dmax)
    with c3:
        group_bucket = st.checkbox("按賠率分桶顯示", value=True)
    with c4:
        place_k = st.selectbox("入圍口徑", options=["Top3"], index=0)

    if isinstance(d1, list):
        d1 = d1[0] if d1 else default_start
    if isinstance(d2, list):
        d2 = d2[0] if d2 else default_end
    if d1 > d2:
        st.warning("開始日期不能晚於結束日期。")
        return

    session = get_session()
    try:
        from sqlalchemy import func

        q = session.query(DrawStatsDaily)
        q = q.filter(DrawStatsDaily.race_date_day >= d1)
        q = q.filter(DrawStatsDaily.race_date_day <= d2)

        venue_opts = [r[0] for r in session.query(DrawStatsDaily.venue).distinct().order_by(DrawStatsDaily.venue.asc()).all() if r and r[0]]
        surface_opts = [r[0] for r in session.query(DrawStatsDaily.surface).distinct().order_by(DrawStatsDaily.surface.asc()).all() if r and r[0]]
        course_opts = [r[0] for r in session.query(DrawStatsDaily.course_type).distinct().order_by(DrawStatsDaily.course_type.asc()).all() if r and r[0]]
        going_opts = [r[0] for r in session.query(DrawStatsDaily.going_code).distinct().order_by(DrawStatsDaily.going_code.asc()).all() if r and r[0]]
        class_opts = [r[0] for r in session.query(DrawStatsDaily.race_class).distinct().order_by(DrawStatsDaily.race_class.asc()).all() if r and r[0]]
        dist_opts = [r[0] for r in session.query(DrawStatsDaily.distance).distinct().order_by(DrawStatsDaily.distance.asc()).all() if r and r[0]]
        bucket_opts = [r[0] for r in session.query(DrawStatsDaily.odds_bucket_sp).distinct().order_by(DrawStatsDaily.odds_bucket_sp.asc()).all() if r and r[0]]

        f1, f2, f3, f4, f5, f6, f7 = st.columns([1.3, 1.3, 1.3, 1.3, 1.3, 1.3, 1.3])
        with f1:
            venue = st.selectbox("場地", options=["ALL"] + venue_opts, index=0)
        with f2:
            surface = st.selectbox("場地類型", options=["ALL"] + surface_opts, index=0)
        with f3:
            course_type = st.selectbox("賽道", options=["ALL"] + course_opts, index=0)
        with f4:
            going_code = st.selectbox("場地狀況", options=["ALL"] + going_opts, index=0)
        with f5:
            race_class = st.selectbox("班次", options=["ALL"] + class_opts, index=0)
        with f6:
            distance = st.selectbox("途程", options=["ALL"] + [int(x) for x in dist_opts if x], index=0)
        with f7:
            odds_bucket = st.selectbox("SP 賠率分桶", options=["ALL"] + bucket_opts, index=0)

        if venue != "ALL":
            q = q.filter(DrawStatsDaily.venue == str(venue))
        if surface != "ALL":
            q = q.filter(DrawStatsDaily.surface == str(surface))
        if course_type != "ALL":
            q = q.filter(DrawStatsDaily.course_type == str(course_type))
        if going_code != "ALL":
            q = q.filter(DrawStatsDaily.going_code == str(going_code))
        if race_class != "ALL":
            q = q.filter(DrawStatsDaily.race_class == str(race_class))
        if distance != "ALL":
            q = q.filter(DrawStatsDaily.distance == int(distance))
        if odds_bucket != "ALL":
            q = q.filter(DrawStatsDaily.odds_bucket_sp == str(odds_bucket))

        if group_bucket:
            rows = (
                q.with_entities(
                    DrawStatsDaily.draw,
                    DrawStatsDaily.odds_bucket_sp,
                    func.sum(DrawStatsDaily.samples).label("samples"),
                    func.sum(DrawStatsDaily.win_cnt).label("win_cnt"),
                    func.sum(DrawStatsDaily.place_cnt).label("place_cnt"),
                )
                .group_by(DrawStatsDaily.draw, DrawStatsDaily.odds_bucket_sp)
                .order_by(DrawStatsDaily.draw.asc(), DrawStatsDaily.odds_bucket_sp.asc())
                .all()
            )
            df = pd.DataFrame(rows, columns=["draw", "odds_bucket_sp", "samples", "win_cnt", "place_cnt"])
        else:
            rows = (
                q.with_entities(
                    DrawStatsDaily.draw,
                    func.sum(DrawStatsDaily.samples).label("samples"),
                    func.sum(DrawStatsDaily.win_cnt).label("win_cnt"),
                    func.sum(DrawStatsDaily.place_cnt).label("place_cnt"),
                )
                .group_by(DrawStatsDaily.draw)
                .order_by(DrawStatsDaily.draw.asc())
                .all()
            )
            df = pd.DataFrame(rows, columns=["draw", "samples", "win_cnt", "place_cnt"])

        if df.empty:
            st.info("此條件下未有任何樣本。")
            return

        df["win_rate"] = (df["win_cnt"] / df["samples"]).fillna(0.0)
        df["place_rate"] = (df["place_cnt"] / df["samples"]).fillna(0.0)

        show = df.copy()
        show["win_rate"] = (show["win_rate"] * 100.0).round(2)
        show["place_rate"] = (show["place_rate"] * 100.0).round(2)
        show = show.rename(columns={"win_rate": "win_rate(%)", "place_rate": "place_rate(%)"})

        st.dataframe(show, use_container_width=True, hide_index=True)
        st.download_button(
            "下載 CSV",
            data=show.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"draw_stats_sp_{d1.isoformat()}_{d2.isoformat()}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    finally:
        session.close()


if __name__ == "__main__":
    main()

