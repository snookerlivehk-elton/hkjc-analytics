import sys
from pathlib import Path

import streamlit as st

# 加入專案路徑
root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from scoring_engine.top5_odds_stats import ODDS_BUCKETS
from scoring_engine.top5_tip_config import default_tip_config, load_tip_config, save_tip_config
from web_ui.auth import require_superadmin
from web_ui.nav import render_admin_nav


def main():
    st.set_page_config(page_title="貼士設定", layout="wide")
    require_superadmin("貼士設定")
    render_admin_nav(active="tip_config")
    st.title("💡 貼士設定")
    init_db()

    session = get_session()
    try:
        cfg = load_tip_config(session)
    finally:
        session.close()

    st.caption("貼士觸發：每個『推介來源×順序TOPn×賠率區』，只要樣本數及命中率（入圍/勝出）達標就可生成多條推介。")

    with st.form("tip_cfg_form"):
        enabled = st.checkbox("啟用貼士", value=bool(cfg.get("enabled")))
        stats_days = st.number_input("統計窗口（最近 N 日）", min_value=1, max_value=2000, value=int(cfg.get("stats_days") or 180))
        min_samples = st.number_input("最少樣本數", min_value=1, max_value=100000, value=int(cfg.get("min_samples") or 10))
        min_place_rate = st.number_input("入圍命中率門檻（Top3）", min_value=0.0, max_value=1.0, value=float(cfg.get("min_place_rate") or 0.4), step=0.01, format="%.2f")
        min_win_rate = st.number_input("勝出命中率門檻", min_value=0.0, max_value=1.0, value=float(cfg.get("min_win_rate") or 0.2), step=0.01, format="%.2f")
        max_tips = st.number_input("每場最多貼士數", min_value=1, max_value=200, value=int(cfg.get("max_tips") or 20))

        positions = st.multiselect("順序（TOPn）", options=[1, 2, 3, 4, 5], default=[int(x) for x in (cfg.get("positions") or [1, 2, 3, 4, 5])])
        bucket_opts = [b.key for b in ODDS_BUCKETS]
        bucket_default = [str(x) for x in (cfg.get("odds_buckets") or bucket_opts)]
        odds_buckets = st.multiselect(
            "賠率區域",
            options=bucket_opts,
            default=[x for x in bucket_default if x in bucket_opts],
            format_func=lambda x: next((b.label for b in ODDS_BUCKETS if b.key == x), str(x)),
        )
        predictor_types = st.multiselect("推介來源", options=["preset", "factor", "ai"], default=[str(x) for x in (cfg.get("predictor_types") or ["preset", "factor", "ai"])])
        odds_source_opts = ["pre_race_latest", "result_win_odds", "latest_history", "history:PRE_24H", "history:PRE_0100", "history:PRE_5M"]
        odds_source_cur = str(cfg.get("odds_source") or "pre_race_latest").strip()
        odds_source = st.selectbox("賠率來源", options=odds_source_opts, index=(odds_source_opts.index(odds_source_cur) if odds_source_cur in odds_source_opts else 0))

        submitted = st.form_submit_button("保存設定", type="primary")
        if submitted:
            new_cfg = {
                "enabled": bool(enabled),
                "stats_days": int(stats_days or 180),
                "min_samples": int(min_samples or 10),
                "min_place_rate": float(min_place_rate or 0.0),
                "min_win_rate": float(min_win_rate or 0.0),
                "positions": [int(x) for x in (positions or [])],
                "odds_buckets": [str(x) for x in (odds_buckets or [])],
                "predictor_types": [str(x) for x in (predictor_types or [])],
                "odds_source": str(odds_source or "pre_race_latest"),
                "max_tips": int(max_tips or 20),
            }
            s2 = get_session()
            try:
                save_tip_config(s2, new_cfg)
            finally:
                s2.close()
            st.success("已保存")

    st.divider()
    st.subheader("目前設定")
    st.json(cfg)


if __name__ == "__main__":
    main()
