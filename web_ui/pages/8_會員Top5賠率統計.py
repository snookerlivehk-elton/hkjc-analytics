from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from database.connection import get_session, init_db
from database.models import SystemConfig
from scoring_engine.top5_odds_stats import ODDS_BUCKETS, compute_top5_odds_stats
from web_ui.auth import require_superadmin
from web_ui.nav import render_admin_nav


def _list_members(session):
    rows = session.query(SystemConfig.key).filter(SystemConfig.key.like("member_weight_presets:%")).order_by(SystemConfig.key.asc()).all()
    out = []
    for (k,) in rows:
        s = str(k or "")
        if ":" not in s:
            continue
        out.append(s.split(":", 1)[1].strip().lower())
    out2 = []
    seen = set()
    for e in out:
        if not e or e in seen:
            continue
        seen.add(e)
        out2.append(e)
    return out2


@st.cache_data(ttl=120)
def _cached_stats(d1: date, d2: date, member_email: str, inc_preset: bool, inc_factor: bool, inc_ai: bool, odds_source: str):
    session = get_session()
    try:
        me = str(member_email or "").strip().lower() or None
        df = compute_top5_odds_stats(
            session,
            d1=d1,
            d2=d2,
            member_email=me,
            include_presets=bool(inc_preset),
            include_factors=bool(inc_factor),
            include_ai=bool(inc_ai),
            place_k=3,
            top_k=5,
            odds_source=str(odds_source or "result_win_odds"),
        )
        return df
    finally:
        session.close()


def main():
    st.set_page_config(page_title="會員 Top5 賠率統計", layout="wide")
    require_superadmin("會員 Top5 賠率統計")
    render_admin_nav(active="member_top5_odds")
    st.title("📌 會員 Top5 賠率統計")
    init_db()

    c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
    today = datetime.utcnow().date()
    with c1:
        d2 = st.date_input("到 (to)", value=today, key="mstats_d2")
    with c2:
        d1 = st.date_input("從 (from)", value=today - timedelta(days=90), key="mstats_d1")
    with c3:
        odds_source = st.selectbox("odds 來源", options=["result_win_odds", "latest_history"], index=0)
    with c4:
        limit_hint = st.checkbox("只看 Top1", value=False)

    session = get_session()
    try:
        members = _list_members(session)
    finally:
        session.close()

    c5, c6, c7, c8 = st.columns([2, 2, 2, 2])
    with c5:
        member_sel = st.selectbox("會員", options=["全部"] + members, index=0)
    with c6:
        inc_preset = st.checkbox("儲存組合(preset)", value=True)
    with c7:
        inc_factor = st.checkbox("獨立條件(factor)", value=True)
    with c8:
        inc_ai = st.checkbox("AI 推介(top5_horse_nos)", value=True)

    df = _cached_stats(d1, d2, "" if member_sel == "全部" else member_sel, inc_preset, inc_factor, inc_ai, odds_source)
    if df.empty:
        st.warning("未有可統計資料（可能未生成 Top5 快照 / 未有賽果 / 或日期範圍內無資料）。")
        return

    if limit_hint:
        df = df[df["position"] == 1]

    st.caption("odds buckets：" + " / ".join([b.label for b in ODDS_BUCKETS if b.key != "UNKNOWN"] + ["未知"]))
    st.dataframe(df, use_container_width=True, hide_index=True)

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("下載 CSV", data=csv, file_name=f"member_top5_odds_{d1.isoformat()}_{d2.isoformat()}.csv", mime="text/csv")


if __name__ == "__main__":
    main()

