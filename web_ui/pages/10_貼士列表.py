import sys
from pathlib import Path
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.models import Race
from scoring_engine.top5_tip_config import load_tip_config
from scoring_engine.top5_tips import generate_top5_tips_for_race
from web_ui.auth import require_superadmin
from web_ui.nav import render_admin_nav


@st.cache_data(ttl=60)
def _race_dates(limit_days: int = 365):
    s = get_session()
    try:
        need = int(limit_days or 365)
        take_rows = max(200, need * 20)
        rows = s.query(Race.race_date).order_by(Race.race_date.desc()).limit(int(take_rows)).all()
        out = []
        seen = set()
        for (dt,) in rows:
            if not dt:
                continue
            dd = dt.date()
            if dd in seen:
                continue
            seen.add(dd)
            out.append(dd)
            if len(out) >= need:
                break
        return out
    finally:
        s.close()


def main():
    st.set_page_config(page_title="貼士列表", layout="wide")
    require_superadmin("貼士列表")
    render_admin_nav(active="tip_list")
    st.title("💡 貼士列表（後台）")
    init_db()

    dates = _race_dates(365)
    if not dates:
        st.info("未有任何賽事資料。")
        return

    c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
    with c1:
        d_sel = st.selectbox("賽日", options=dates, index=0, format_func=lambda x: x.isoformat())
    with c2:
        race_no = st.number_input("場次 (R#)", min_value=1, max_value=14, value=1)
    with c3:
        ignore_enabled = st.checkbox("忽略 enabled（預覽）", value=True)
    with c4:
        max_show = st.selectbox("最多顯示貼士", options=[20, 50, 100, 200], index=1)

    session = get_session()
    try:
        start = datetime.combine(d_sel, datetime.min.time())
        end = start + timedelta(days=1)
        race = (
            session.query(Race)
            .filter(Race.race_date >= start)
            .filter(Race.race_date < end)
            .filter(Race.race_no == int(race_no))
            .first()
        )
        if not race:
            st.warning("該賽日未找到此場次。")
            return

        cfg = load_tip_config(session)
        if ignore_enabled:
            cfg = dict(cfg)
            cfg["enabled"] = True

        with st.expander("使用中的貼士設定", expanded=False):
            st.json(cfg)

        tips = generate_top5_tips_for_race(session, race_id=int(race.id), member_email=None, preset_name=None, override_config=cfg)
        if not tips:
            st.info("未有貼士達標（或缺少賠率資料 / 統計樣本不足）。")
            return

        df = pd.DataFrame(tips)
        if not df.empty:
            df = df.head(int(max_show or 50))
            cols = [
                "hit_label",
                "hit_rate",
                "hit_threshold",
                "appear",
                "predictor_type_label",
                "predictor_key_label",
                "member_email",
                "position",
                "odds_bucket_label",
                "win_odds",
                "horse_no",
                "horse_name",
                "jockey",
                "trainer",
            ]
            cols2 = [c for c in cols if c in df.columns]
            st.dataframe(df[cols2], use_container_width=True, hide_index=True)

            st.download_button(
                "下載 CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name=f"tips_{d_sel.isoformat()}_R{int(race_no)}.csv",
                mime="text/csv",
            )

        st.divider()
        st.subheader("貼士詳情")
        for t in tips[: int(max_show or 50)]:
            head = (
                f"{t.get('race_date')} {t.get('venue')} R{t.get('race_no')} | "
                f"TOP{t.get('position')} | {t.get('odds_bucket_label')} | "
                f"{t.get('predictor_type_label')}：{t.get('predictor_key_label')} | "
                f"{t.get('hit_label')}達標"
            )
            with st.expander(head, expanded=False):
                st.write(
                    {
                        "member_email": t.get("member_email"),
                        "馬號": int(t.get("horse_no") or 0),
                        "馬名": str(t.get("horse_name") or ""),
                        "騎師": str(t.get("jockey") or ""),
                        "練馬師": str(t.get("trainer") or ""),
                        "賠率": t.get("win_odds"),
                        "賠率來源": str(t.get("odds_source_label") or ""),
                        "樣本": int(t.get("appear") or 0),
                        "入圍率": (round(float(t.get("place_rate") or 0.0) * 100.0, 1) if t.get("place_rate") is not None else None),
                        "勝出率": (round(float(t.get("win_rate") or 0.0) * 100.0, 1) if t.get("win_rate") is not None else None),
                        "達標項": str(t.get("hit_label") or ""),
                        "命中率": round(float(t.get("hit_rate") or 0.0) * 100.0, 1),
                        "門檻": round(float(t.get("hit_threshold") or 0.0) * 100.0, 1),
                    }
                )
    finally:
        session.close()


if __name__ == "__main__":
    main()
