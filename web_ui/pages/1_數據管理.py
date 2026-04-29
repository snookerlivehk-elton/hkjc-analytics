import streamlit as st
import pandas as pd
import os
import subprocess
import sys
from pathlib import Path

# 加入專案路徑
root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import get_session, init_db
from scoring_engine.core import ScoringEngine
from web_ui.nav import render_admin_nav

st.set_page_config(page_title="數據管理 - HKJC Analytics", page_icon="🛠️", layout="wide")

# 全站列表文字靠左
st.markdown(
    """
    <style>
    div[data-testid="stDataFrame"] div[role="gridcell"],
    div[data-testid="stDataFrame"] div[role="columnheader"] {
      text-align: left !important;
      justify-content: flex-start !important;
    }
    div[data-testid="stDataFrame"] table td,
    div[data-testid="stDataFrame"] table th,
    div[data-testid="stTable"] table td,
    div[data-testid="stTable"] table th {
      text-align: left !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# 初始化資料庫 (確保在雲端環境表結構存在)
init_db()

if not st.session_state.get("is_superadmin", False):
    st.title("🛠️ 數據管理後台")
    st.markdown("🔐 需要 Superadmin 登入後才能操作。")
    super_pw = os.environ.get("SUPERADMIN_PASSWORD", "")
    if not super_pw:
        st.error("❌ 未設定 SUPERADMIN_PASSWORD 環境變數，無法登入後台。")
        st.stop()

    with st.form("superadmin_login_form"):
        pw = st.text_input("Superadmin 密碼", value="", type="password")
        submitted = st.form_submit_button("登入", type="primary")
        if submitted:
            if str(pw) == super_pw:
                st.session_state["is_superadmin"] = True
                st.rerun()
            else:
                st.error("❌ 密碼錯誤")
    st.stop()

st.title("🛠️ 數據管理後台")
render_admin_nav()

def trigger_scraper(target_date: str = None):
    """實時日誌串流輸出"""
    st.markdown("### 🚀 爬蟲執行進度")
    log_placeholder = st.empty() 
    full_log = ""
    try:
        env = os.environ.copy()
        if target_date:
            env["TARGET_DATE"] = target_date
            
        process = subprocess.Popen(
            ["python3", "scripts/run_scraper.py"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=env, bufsize=1
        )
        for line in iter(process.stdout.readline, ""):
            full_log += line
            log_placeholder.code(full_log)
        process.stdout.close()
        return process.wait() == 0
    except Exception as e:
        st.error(f"❌ 系統錯誤: {e}")
        return False

def trigger_history_backfill(target_date: str = None, mode: str = None):
    """歷史數據回填進度"""
    st.markdown("### 📚 歷史數據回填進度")
    log_placeholder = st.empty()
    full_log = ""
    try:
        env = os.environ.copy()
        if target_date:
            env["TARGET_DATE"] = target_date
        if mode:
            env["BACKFILL_MODE"] = mode
        process = subprocess.Popen(
            ["python3", "scripts/fetch_history.py"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=env, bufsize=1
        )
        for line in iter(process.stdout.readline, ""):
            full_log += line
            log_placeholder.code(full_log)
        process.stdout.close()
        return process.wait() == 0
    except Exception as e:
        st.error(f"❌ 系統錯誤: {e}")
        return False

def trigger_race_results_fetch(target_date: str = None):
    st.markdown("### 🏁 賽果與派彩抓取進度")
    log_placeholder = st.empty()
    full_log = ""
    try:
        env = os.environ.copy()
        if target_date:
            env["TARGET_DATE"] = target_date
        process = subprocess.Popen(
            ["python3", "scripts/fetch_race_results.py"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=env, bufsize=1
        )
        for line in iter(process.stdout.readline, ""):
            full_log += line
            log_placeholder.code(full_log)
        process.stdout.close()
        return process.wait() == 0
    except Exception as e:
        st.error(f"❌ 系統錯誤: {e}")
        return False

def trigger_fixture_fetch():
    st.markdown("### 📅 賽期表更新進度")
    log_placeholder = st.empty()
    full_log = ""
    try:
        env = os.environ.copy()
        process = subprocess.Popen(
            ["python3", "scripts/fetch_fixture.py"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=env, bufsize=1
        )
        for line in iter(process.stdout.readline, ""):
            full_log += line
            log_placeholder.code(full_log)
        process.stdout.close()
        return process.wait() == 0
    except Exception as e:
        st.error(f"❌ 系統錯誤: {e}")
        return False

def trigger_predictions_snapshot(target_date: str):
    st.markdown("### 🧾 Top5 預測快照生成進度")
    log_placeholder = st.empty()
    full_log = ""
    try:
        env = os.environ.copy()
        if target_date:
            env["TARGET_DATE"] = target_date
        process = subprocess.Popen(
            ["python3", "scripts/generate_predictions.py"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=env, bufsize=1
        )
        for line in iter(process.stdout.readline, ""):
            full_log += line
            log_placeholder.code(full_log)
        process.stdout.close()
        return process.wait() == 0
    except Exception as e:
        st.error(f"❌ 系統錯誤: {e}")
        return False


def trigger_speedpro_fetch(target_date: str, race_nos: str = "", retry_minutes: int = 120, force: bool = True):
    st.markdown("### ⚡ SpeedPRO 能量分抓取進度")
    log_placeholder = st.empty()
    full_log = ""
    try:
        env = os.environ.copy()
        if target_date:
            env["TARGET_DATE"] = target_date
        if race_nos:
            env["RACE_NOS"] = race_nos
        env["SPEEDPRO_RETRY_MINUTES"] = str(int(retry_minutes or 120))
        if force:
            env["FORCE_SPEEDPRO_FETCH"] = "1"
        process = subprocess.Popen(
            ["python3", "scripts/cron_speedpro_fetch.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            bufsize=1,
        )
        for line in iter(process.stdout.readline, ""):
            full_log += line
            log_placeholder.code(full_log)
        process.stdout.close()
        return process.wait() == 0
    except Exception as e:
        st.error(f"❌ 系統錯誤: {e}")
        return False

def cleanup_removed_factor_data(session):
    try:
        from database.models import ScoringFactor, ScoringWeight, SystemConfig
        deleted_sf = session.query(ScoringFactor).filter(ScoringFactor.factor_name == "trainer_horse_bond").delete()
        deleted_sw = session.query(ScoringWeight).filter(ScoringWeight.factor_name == "trainer_horse_bond").delete()
        deleted_cfg = session.query(SystemConfig).filter(SystemConfig.key == "trainer_horse_bond_config").delete()
        session.commit()
        return deleted_sf, deleted_sw, deleted_cfg
    except Exception as e:
        session.rollback()
        st.error(f"❌ 清理失敗: {e}")
        return 0, 0, 0

st.title("🛠️ 數據管理後台")
st.markdown("在此頁面執行數據更新、回填與清理操作。")

from web_ui.nav import render_admin_nav


with st.expander("🗺️ 優化排程（Roadmap）", expanded=False):
    st.markdown(
        "\n".join(
            [
                "### 第一階段（已完成）",
                "- ✅ 因子啟用校驗（避免誤開未實作/不穩定因子）",
                "- ✅ 賽日日期一致性（HK）與 HorseHistory 去重修正",
                "- ✅ raw_value 落庫（支援追因）",
                "- ✅ baseline 診斷腳本（命中率/分桶/覆蓋率）",
                "",
                "### 第二階段（進行中）",
                "- ✅ 單場診斷：反向統計（BottomN 淘汰）＋誤推/漏網主要因子貢獻",
                "- ⏳ 範圍統計：按賽日範圍聚合 BottomN 淘汰準確率／錯殺率（分場地/距離/班次）",
                "- ⏳ 原因剖析：缺資料標籤／低覆蓋警示（更精準指出不足）",
                "",
                "### 第三階段（待開始）",
                "- ⏳ 因子治理：factor catalog（描述/依賴/方向/狀態/版本）",
                "- ⏳ 管理介面：模型診斷頁（Overall/分桶/因子健康度/場次 drilldown）",
                "",
                "### 第四階段（可選）",
                "- ⏳ 自動調權重：walk-forward 回測＋權重版本化回寫",
            ]
        )
    )

tab_ops, tab_members, tab_hits = st.tabs(["🛠️ 系統操作", "👥 會員組合", "📈 命中統計"])

with tab_ops:
    st.subheader("👥 會員白名單")
    session_cfg = get_session()
    try:
        from database.models import SystemConfig

        cfg = session_cfg.query(SystemConfig).filter_by(key="member_whitelist_emails").first()
        emails = []
        if cfg and isinstance(cfg.value, list):
            emails = [str(x).strip().lower() for x in cfg.value if str(x).strip()]
        emails = list(dict.fromkeys(emails))
        default_text = "\n".join(emails)

        with st.form("member_whitelist_form"):
            text = st.text_area("允許登入的 Email（每行一個）", value=default_text, height=160, placeholder="name@example.com")
            submitted = st.form_submit_button("💾 儲存白名單", type="primary")
            if submitted:
                new_list = []
                for line in str(text or "").splitlines():
                    e = line.strip().lower()
                    if e:
                        new_list.append(e)
                new_list = list(dict.fromkeys(new_list))
                if not cfg:
                    cfg = SystemConfig(key="member_whitelist_emails", description="會員登入白名單 (email)")
                    session_cfg.add(cfg)
                cfg.value = new_list
                session_cfg.commit()
                st.success(f"已儲存 {len(new_list)} 個 Email。")
                st.rerun()
    except Exception as e:
        session_cfg.rollback()
        st.error(f"❌ 白名單讀寫失敗: {e}")
    finally:
        session_cfg.close()

    st.subheader("📊 數據取得狀態")
    session_status = get_session()
    try:
        from database.models import Race, Horse, Jockey, Trainer, RaceEntry, HorseHistory, ScoringFactor, RaceResult, RaceDividend, OddsHistory, ScoringWeight, SystemConfig, PredictionTop5

        status = {
            "賽事": session_status.query(Race).count(),
            "排位": session_status.query(RaceEntry).count(),
            "賽果": session_status.query(RaceResult).count(),
            "派彩": session_status.query(RaceDividend).count(),
            "計分": session_status.query(ScoringFactor).count(),
            "Top5快照": session_status.query(PredictionTop5).count(),
            "馬匹": session_status.query(Horse).count(),
            "往績": session_status.query(HorseHistory).count(),
            "騎師": session_status.query(Jockey).count(),
            "練馬師": session_status.query(Trainer).count(),
            "賠率": session_status.query(OddsHistory).count(),
            "權重": session_status.query(ScoringWeight).count(),
            "系統設定": session_status.query(SystemConfig).count(),
        }

        c1, c2, c3, c4, c5, c6 = st.columns(6)
        items = list(status.items())
        cols = [c1, c2, c3, c4, c5, c6]
        for i, (k, v) in enumerate(items):
            cols[i % 6].metric(k, v)
    except Exception as e:
        st.error(f"❌ 讀取資料庫狀態失敗: {e}")
    finally:
        session_status.close()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📅 賽期表")
        st.caption("用途：更新「本月＋下月」有賽事的日期清單，供系統決定下一賽日與排程目標。若已設定 fixture cron（每日 HK 06:00），通常不需手動按。")
        if st.button("📅 更新賽期表 (本月+下月)", use_container_width=True):
            if trigger_fixture_fetch():
                st.success("✅ 賽期表已更新！")

        st.subheader("📡 抓取排位表與即時數據")
        
        from datetime import datetime, timedelta
        default_date = datetime.now()
        selected_date = st.date_input("選擇要抓取的賽事日期", value=default_date)
        st.session_state["admin_selected_date"] = selected_date

        session_meta = get_session()
        try:
            from database.models import SystemConfig

            fx_updated = session_meta.query(SystemConfig).filter_by(key="fixture_dates_updated_at").first()
            fx_next = session_meta.query(SystemConfig).filter_by(key="fixture_next_raceday").first()
            rr_last = session_meta.query(SystemConfig).filter(SystemConfig.key.like("auto_results_fetched:%")).order_by(SystemConfig.key.desc()).first()
            fx_updated_s = str(fx_updated.value) if fx_updated else ""
            fx_next_s = str(fx_next.value) if fx_next else ""
            rr_last_s = rr_last.key.split(":", 1)[1] if rr_last and ":" in rr_last.key else ""
            meta_lines = []
            if fx_next_s:
                meta_lines.append(f"下一賽日：{fx_next_s}")
            if fx_updated_s:
                meta_lines.append(f"賽期表最後更新：{fx_updated_s}")
            if rr_last_s:
                meta_lines.append(f"賽果 cron 最後自動抓取：{rr_last_s}")
            if meta_lines:
                st.caption("｜".join(meta_lines))
        finally:
            session_meta.close()

        st.subheader("🧩 因子資料不足策略")
        st.caption("用於識別因子資料是否齊全：可只提示，或在資料覆蓋不足時自動忽略該因子（本場有效權重設為 0）。")
        session_q = get_session()
        try:
            from database.models import SystemConfig, ScoringWeight
            from scoring_engine.constants import DISABLED_FACTORS

            cfg = session_q.query(SystemConfig).filter_by(key="factor_quality_policy").first()
            val = cfg.value if cfg and isinstance(cfg.value, dict) else {}
            default_p = val.get("default") if isinstance(val.get("default"), dict) else {}
            overrides = val.get("overrides") if isinstance(val.get("overrides"), dict) else {}

            def_action = str(default_p.get("action") or "warn").strip().lower()
            def_min_cov = default_p.get("min_coverage")
            try:
                def_min_cov = float(def_min_cov if def_min_cov is not None else 0.7)
            except Exception:
                def_min_cov = 0.7
            if def_min_cov > 1.0:
                def_min_cov = def_min_cov / 100.0
            if def_min_cov < 0.0:
                def_min_cov = 0.0
            if def_min_cov > 1.0:
                def_min_cov = 1.0

            weights = (
                session_q.query(ScoringWeight.factor_name, ScoringWeight.description)
                .filter(ScoringWeight.is_active == True)
                .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
                .order_by(ScoringWeight.factor_name.asc())
                .all()
            )
            factor_rows = []
            for fn, desc in weights:
                code = str(fn or "").strip()
                if not code:
                    continue
                ov = overrides.get(code) if isinstance(overrides.get(code), dict) else {}
                act = str((ov.get("action") if isinstance(ov, dict) else None) or "default").strip().lower()
                mc = ov.get("min_coverage") if isinstance(ov, dict) else None
                try:
                    mc = float(mc) if mc is not None else None
                except Exception:
                    mc = None
                if mc is not None and mc > 1.0:
                    mc = mc / 100.0
                if mc is not None and mc < 0.0:
                    mc = 0.0
                if mc is not None and mc > 1.0:
                    mc = 1.0
                factor_rows.append(
                    {
                        "因子代號": code,
                        "因子名稱": str(desc or code),
                        "模式": act,
                        "門檻(%)": round((mc * 100.0), 0) if mc is not None else None,
                    }
                )

            with st.form("factor_quality_policy_form"):
                c1, c2 = st.columns(2)
                with c1:
                    action_label = "只提示" if def_action != "ignore" else "自動忽略"
                    new_action_label = st.selectbox("預設策略", ["只提示", "自動忽略"], index=0 if action_label == "只提示" else 1)
                with c2:
                    new_min_pct = st.slider("預設門檻(覆蓋率%)", min_value=0, max_value=100, value=int(round(def_min_cov * 100.0)))

                st.markdown("**因子個別設定（可留空＝跟預設）**")
                df_edit = pd.DataFrame(factor_rows)
                edited = st.data_editor(
                    df_edit,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "模式": st.column_config.SelectboxColumn("模式", options=["default", "warn", "ignore"], help="default=跟預設；warn=只提示；ignore=自動忽略"),
                        "門檻(%)": st.column_config.NumberColumn("門檻(%)", min_value=0, max_value=100, step=1, help="留空＝跟預設"),
                    },
                    disabled=["因子代號", "因子名稱"],
                )
                submitted = st.form_submit_button("💾 儲存策略", type="primary")
                if submitted:
                    new_default_action = "ignore" if new_action_label == "自動忽略" else "warn"
                    new_policy = {"default": {"action": new_default_action, "min_coverage": float(new_min_pct) / 100.0}, "overrides": {}}
                    if isinstance(edited, pd.DataFrame):
                        for _, r in edited.iterrows():
                            code = str(r.get("因子代號") or "").strip()
                            if not code:
                                continue
                            mode = str(r.get("模式") or "").strip().lower()
                            mc = r.get("門檻(%)")
                            mc_v = None
                            try:
                                mc_v = float(mc) / 100.0 if mc is not None and str(mc) != "nan" else None
                            except Exception:
                                mc_v = None
                            if mode in ("warn", "ignore") or mc_v is not None:
                                ov = {}
                                if mode in ("warn", "ignore"):
                                    ov["action"] = mode
                                if mc_v is not None:
                                    ov["min_coverage"] = mc_v
                                new_policy["overrides"][code] = ov

                    if not cfg:
                        cfg = SystemConfig(key="factor_quality_policy", description="因子資料不足策略")
                        session_q.add(cfg)
                    cfg.value = new_policy
                    session_q.commit()
                    st.success("✅ 已儲存。新策略會於下一次重新計分後生效。")
                    st.rerun()
        except Exception as e:
            session_q.rollback()
            st.error(f"❌ 策略讀寫失敗: {e}")
        finally:
            session_q.close()
        
        st.subheader("⚡ 一鍵完整更新（建議）")
        st.caption("會依序完成：抓排位 → 回填該日涉及馬匹往績 → 重算該日所有場次 → 生成 Top5 快照（factor + preset）。每一步會等待上一個完成。")
        if st.button("⚡ 一鍵：抓排位 → 回填馬匹往績 → 重算當日 → 生成Top5快照", use_container_width=True):
            target_date_str = selected_date.strftime("%Y/%m/%d")
            ok1 = trigger_scraper(target_date=target_date_str)
            if not ok1:
                st.error("❌ 抓取排位/即時數據失敗，已中止後續流程。")
            else:
                st.success(f"✅ {target_date_str} 排位/即時數據更新完成。")

                ok2 = trigger_history_backfill(target_date=target_date_str, mode="date")
                if not ok2:
                    st.error("❌ 回填馬匹往績失敗，已中止後續流程。")
                else:
                    st.success(f"✅ {target_date_str} 馬匹往績回填完成。")

                    session_rescore = get_session()
                    try:
                        from database.models import Race
                        from sqlalchemy import func
                        races_to_score = (
                            session_rescore.query(Race)
                            .filter(func.date(Race.race_date) == selected_date)
                            .order_by(Race.race_no.asc(), Race.id.asc())
                            .all()
                        )
                        if not races_to_score:
                            st.warning("⚠️ 找不到該日賽事資料（請先確認排位已成功入庫）。")
                        else:
                            engine = ScoringEngine(session_rescore)
                            progress_bar = st.progress(0)
                            status_text = st.empty()
                            for i, race in enumerate(races_to_score):
                                status_text.text(f"正在重算：第 {race.race_no} 場...")
                                engine.score_race(race.id)
                                progress_bar.progress((i + 1) / len(races_to_score))
                            st.success(f"✅ 已完成 {target_date_str} {len(races_to_score)} 場賽事重新計分。")
                    except Exception as e:
                        st.error(f"❌ 重算當日賽事失敗: {e}")
                    finally:
                        session_rescore.close()

                    ok4 = trigger_predictions_snapshot(target_date_str)
                    if ok4:
                        st.success(f"✅ 已生成 {target_date_str} Top5 預測快照（包含 factor + preset）。")
                    else:
                        st.error("❌ 生成 Top5 預測快照失敗。")

        st.caption("只做「抓排位/即時數據 + 計分（不包含回填往績/重算）」；如要產生更完整的條件結果與 Top5 快照，建議使用上方「一鍵完整更新」。")
        if st.button("🔄 開始抓取該日賽事", use_container_width=True):
            target_date_str = selected_date.strftime("%Y/%m/%d")
            if trigger_scraper(target_date=target_date_str):
                st.success(f"✅ {target_date_str} 數據更新成功！")

        st.subheader("🧾 預測快照 (Top5)")
        st.caption("只生成 Top5 快照（落庫 PredictionTop5）。需要先完成該日計分/重算，否則快照會反映不完整數據。")
        if st.button("🧾 生成當日 Top5 預測快照", use_container_width=True):
            target_date_str = selected_date.strftime("%Y/%m/%d")
            if trigger_predictions_snapshot(target_date_str):
                st.success(f"✅ 已生成 {target_date_str} Top5 預測快照！")

        st.subheader("🏁 抓取賽果與派彩")
        st.caption("抓取賽果/派彩入庫後，會自動結算：會員組合命中率 + Top5 快照命中（回寫 hits/actual_top5）。若已設定賽果 cron（每日 HK 23:55）通常不需手動按。")
        if st.button("🏁 抓取該日賽果與派彩", use_container_width=True):
            target_date_str = selected_date.strftime("%Y/%m/%d")
            if trigger_race_results_fetch(target_date=target_date_str):
                st.success(f"✅ 已完成 {target_date_str} 賽果與派彩同步！")

        st.subheader("⚡ SpeedPRO 能量分（手動備用）")
        st.caption("用途：當 cron 未成功抓到 SpeedPRO（速勢能量評估/狀態評級）時可手動觸發一次。建議先選日期，再選場次。")
        target_date_str = selected_date.strftime("%Y/%m/%d")
        sp_cols = st.columns(2)
        with sp_cols[0]:
            race_opts = [str(i) for i in range(1, 10)]
            selected_races = st.multiselect("選擇場次（留空＝全部）", options=race_opts, default=[])
        with sp_cols[1]:
            retry_minutes = st.selectbox("失敗後重試間距（分鐘）", options=[30, 60, 120], index=2)

        session_sp = get_session()
        try:
            from database.models import SystemConfig

            rows = []
            for rn in range(1, 10):
                retry_key = f"speedpro_retry:{target_date_str}:{rn}"
                info_key = f"speedpro_energy_info:{target_date_str}:{rn}"
                r_cfg = session_sp.query(SystemConfig).filter_by(key=retry_key).first()
                i_cfg = session_sp.query(SystemConfig).filter_by(key=info_key).first()
                r_val = r_cfg.value if r_cfg and isinstance(r_cfg.value, dict) else {}
                i_val = i_cfg.value if i_cfg and isinstance(i_cfg.value, dict) else {}
                if not r_val and not i_val:
                    continue
                rows.append(
                    {
                        "race_no": rn,
                        "done": bool(r_val.get("done") is True),
                        "attempts": int(r_val.get("attempt_count") or 0),
                        "last_attempt_at": r_val.get("last_attempt_at"),
                        "next_retry_at": r_val.get("next_retry_at"),
                        "last_error": r_val.get("last_error"),
                        "rows": i_val.get("rows"),
                        "captured_at": i_val.get("captured_at"),
                    }
                )
            if rows:
                st.dataframe(pd.DataFrame(rows).sort_values(["race_no"]), use_container_width=True, hide_index=True)
        finally:
            session_sp.close()

        race_nos_str = ",".join([str(int(x)) for x in selected_races if str(x).isdigit()])
        if st.button("⚡ 立即抓取 SpeedPRO", use_container_width=True):
            ok = trigger_speedpro_fetch(target_date=target_date_str, race_nos=race_nos_str, retry_minutes=int(retry_minutes), force=True)
            if ok:
                st.success("✅ 已觸發 SpeedPRO 抓取（詳情見上方日誌/狀態表）。")
            else:
                st.error("❌ SpeedPRO 抓取失敗，請查看日誌。")

        st.subheader("📚 歷史回填")
        st.caption("回填馬匹往績（HorseHistory），供部分條件計分使用。更新排位後、重算前先回填，結果較完整。")
        col_h1, col_h2 = st.columns(2)
        with col_h1:
            if st.button("📚 回填所選日期馬匹往績", use_container_width=True):
                target_date_str = selected_date.strftime("%Y/%m/%d")
                if trigger_history_backfill(target_date=target_date_str, mode="date"):
                    st.success(f"✅ 已完成 {target_date_str} 所需馬匹之歷史往績回填！")
        with col_h2:
            with st.expander("完整回填 (較慢)"):
                if st.button("📚 回填所有馬匹往績", use_container_width=True):
                    if trigger_history_backfill(mode="all"):
                        st.success("✅ 已完成所有馬匹之歷史往績回填！")

    with col2:
        st.subheader("🚀 批量計分操作")
        st.caption("只做「重算所選日期」所有場次（不包含回填/快照）。適合你已完成回填但想再重算一次。")
        if st.button("🚀 重算所選日期所有賽事", use_container_width=True):
            session = get_session()
            try:
                from database.models import Race
                from sqlalchemy import func

                sd = st.session_state.get("admin_selected_date")
                races_to_score = []
                if sd:
                    races_to_score = (
                        session.query(Race)
                        .filter(func.date(Race.race_date) == sd)
                        .order_by(Race.race_no.asc(), Race.id.asc())
                        .all()
                    )

                if races_to_score:
                    engine = ScoringEngine(session)
                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    for i, race in enumerate(races_to_score):
                        status_text.text(f"正在計算第 {race.race_no} 場賽事分數...")
                        engine.score_race(race.id)
                        progress_bar.progress((i + 1) / len(races_to_score))

                    sd_str = sd.strftime("%Y/%m/%d") if hasattr(sd, "strftime") else str(sd)
                    st.success(f"✅ 已成功為 {sd_str} 的 {len(races_to_score)} 場賽事完成重新計分！")
                else:
                    st.warning("⚠️ 找不到所選日期的賽事資料。")
            except Exception as e:
                st.error(f"❌ 批量計分失敗: {e}")
            finally:
                session.close()

        st.subheader("🧹 系統清理")
        st.caption("僅清理已移除因子的舊資料（不影響賽事/馬匹/往績）。一般情況毋須操作。")
        with st.expander("清理已移除因子舊記錄", expanded=False):
            st.markdown("此操作只會刪除已移除因子在資料庫中的舊計分結果與設定，不會影響賽事、馬匹、往績等核心數據。")
            confirm = st.checkbox("我明白此操作會刪除舊因子資料", value=False)
            if st.button("🧹 清理 trainer_horse_bond 舊記錄", use_container_width=True, disabled=not confirm):
                session = get_session()
                deleted_sf, deleted_sw, deleted_cfg = cleanup_removed_factor_data(session)
                session.close()
                st.success(f"✅ 已刪除舊記錄：ScoringFactor {deleted_sf} 筆、ScoringWeight {deleted_sw} 筆、SystemConfig {deleted_cfg} 筆")

        st.subheader("🔌 系統測試與升級")
        st.caption("用於排查連線/結構問題。一般日常不用操作。")
        if st.button("🔌 測試資料庫連線", use_container_width=True):
            session = get_session()
            try:
                from database.models import ScoringWeight
                count = session.query(ScoringWeight).count()
                st.success(f"✅ 連線正常 (權重紀錄: {count})")
            except Exception as e:
                st.error(f"❌ 連線失敗: {e}")
            session.close()
            
        if st.button("🆙 執行資料庫欄位升級 (新增原始數據欄位)", use_container_width=True):
            try:
                env = os.environ.copy()
                process = subprocess.Popen(
                    [sys.executable, "scripts/upgrade_db.py"],
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, env=env, bufsize=1
                )
                out, _ = process.communicate()
                if process.returncode == 0:
                    st.success(f"✅ 升級腳本執行完成！\n\n```\n{out}\n```")
                else:
                    st.error(f"❌ 執行失敗: {out}")
            except Exception as e:
                st.error(f"❌ 系統錯誤: {e}")

with tab_members:
    st.subheader("👥 全部會員「儲存組合」列表")
    from database.models import SystemConfig, ScoringWeight
    from scoring_engine.constants import DISABLED_FACTORS
    from scoring_engine.member_stats import load_member_preset_stats

    session_all = get_session()
    try:
        weights = (
            session_all.query(ScoringWeight)
            .filter(ScoringWeight.is_active == True)
            .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
            .all()
        )
        factor_desc = {w.factor_name: w.description for w in weights}

        cfgs = (
            session_all.query(SystemConfig)
            .filter(SystemConfig.key.like("member_weight_presets:%"))
            .order_by(SystemConfig.key.asc())
            .all()
        )

        rows = []
        for cfg in cfgs:
            if not isinstance(cfg.value, list):
                continue
            email = cfg.key.split(":", 1)[1] if ":" in cfg.key else cfg.key
            stats_map = load_member_preset_stats(session_all, email)
            for p in cfg.value[:3]:
                if not isinstance(p, dict):
                    continue
                name = str(p.get("name") or "").strip()
                weights_map = p.get("weights") if isinstance(p.get("weights"), dict) else {}
                stt = stats_map.get(name, {}) if isinstance(stats_map, dict) else {}
                races_n = int(stt.get("races") or 0)
                win_n = int(stt.get("win") or 0)
                p_n = int(stt.get("p") or 0)
                q1_n = int(stt.get("q1") or 0)
                pq_n = int(stt.get("pq") or 0)
                t3e_n = int(stt.get("t3e") or 0)
                t3_n = int(stt.get("t3") or 0)
                f4_n = int(stt.get("f4") or 0)
                f4q_n = int(stt.get("f4q") or 0)
                b5w_n = int(stt.get("b5w") or 0)
                b5p_n = int(stt.get("b5p") or 0)
                rows.append(
                    {
                        "Email": email,
                        "組合": name,
                        "更新時間": str(p.get("updated_at") or ""),
                        "樣本(場)": races_n,
                        "WIN%": round((win_n / races_n * 100.0), 1) if races_n else 0.0,
                        "P%": round((p_n / races_n * 100.0), 1) if races_n else 0.0,
                        "Q1%": round((q1_n / races_n * 100.0), 1) if races_n else 0.0,
                        "PQ%": round((pq_n / races_n * 100.0), 1) if races_n else 0.0,
                        "T3E%": round((t3e_n / races_n * 100.0), 1) if races_n else 0.0,
                        "T3%": round((t3_n / races_n * 100.0), 1) if races_n else 0.0,
                        "F4%": round((f4_n / races_n * 100.0), 1) if races_n else 0.0,
                        "F4Q%": round((f4q_n / races_n * 100.0), 1) if races_n else 0.0,
                        "B5W%": round((b5w_n / races_n * 100.0), 1) if races_n else 0.0,
                        "B5P%": round((b5p_n / races_n * 100.0), 1) if races_n else 0.0,
                        "_weights": weights_map,
                    }
                )

        if not rows:
            st.info("目前沒有任何會員儲存組合。")
        else:
            df_overview = []
            for r in rows:
                rr = dict(r)
                rr.pop("_weights", None)
                df_overview.append(rr)
            st.dataframe(pd.DataFrame(df_overview), use_container_width=True, hide_index=True)

            st.markdown("---")
            st.markdown("### 🔎 組合權重參數")
            for r in rows:
                email = r["Email"]
                name = r["組合"]
                weights_map = r.get("_weights") or {}
                with st.expander(f"{email} / {name}", expanded=False):
                    total_w = sum(float(v) for v in weights_map.values()) if weights_map else 0.0
                    items = []
                    for k, v in weights_map.items():
                        if k in factor_desc:
                            share = (float(v) / total_w * 100.0) if total_w > 0 else 0.0
                            items.append({"條件": factor_desc[k], "代號": k, "權重": round(float(v), 2), "佔比%": round(share, 1)})
                    items = sorted(items, key=lambda x: x["佔比%"], reverse=True)
                    if items:
                        st.dataframe(pd.DataFrame(items), use_container_width=True, hide_index=True)
                    else:
                        st.info("此組合沒有可用的權重資料。")
    finally:
        session_all.close()

with tab_hits:
    sub_factor, sub_preset = st.tabs(["📈 獨立條件", "👥 會員儲存組合"])

    with sub_factor:
        st.subheader("📈 獨立條件命中率統計")
        from datetime import date, timedelta
        from sqlalchemy import func
        from database.models import PredictionTop5, RaceResult, RaceEntry, ScoringWeight
        from scoring_engine.constants import DISABLED_FACTORS

        session_hit = get_session()
        try:
            factors = (
                session_hit.query(ScoringWeight.factor_name, ScoringWeight.description)
                .filter(ScoringWeight.is_active == True)
                .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
                .order_by(ScoringWeight.factor_name.asc())
                .all()
            )
            factor_desc = {str(fn): str(desc or fn) for fn, desc in factors}
            factor_names = list(factor_desc.keys())

            drows = (
                session_hit.query(func.date(PredictionTop5.race_date))
                .filter(PredictionTop5.predictor_type == "factor")
                .distinct()
                .order_by(func.date(PredictionTop5.race_date).desc())
                .limit(90)
                .all()
            )
            available_dates = [r[0] for r in drows if r and r[0]]
            if not available_dates:
                st.info("目前未有任何獨立條件 Top5 快照。請先抓取排位並生成預測快照。")
            else:
                end_default = available_dates[0]
                start_default = max(end_default - timedelta(days=30), min(available_dates))
                d1, d2 = st.date_input("統計日期範圍", value=(start_default, end_default))
                if isinstance(d1, date) and isinstance(d2, date) and d1 > d2:
                    d1, d2 = d2, d1

                preds = (
                    session_hit.query(
                        PredictionTop5.race_id,
                        PredictionTop5.predictor_key,
                        PredictionTop5.top5,
                        PredictionTop5.meta,
                    )
                    .filter(PredictionTop5.predictor_type == "factor")
                    .filter(PredictionTop5.predictor_key.in_(factor_names))
                    .filter(func.date(PredictionTop5.race_date) >= d1.isoformat())
                    .filter(func.date(PredictionTop5.race_date) <= d2.isoformat())
                    .all()
                )
                if not preds:
                    st.info("選定範圍內沒有任何獨立條件 Top5 快照。")
                else:
                    from scoring_engine.member_stats import _calc_hits

                    def actual_top5(race_id: int):
                        rows = (
                            session_hit.query(RaceEntry.horse_no, RaceResult.rank)
                            .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
                            .filter(RaceEntry.race_id == race_id)
                            .filter(RaceResult.rank != None)
                            .order_by(RaceResult.rank.asc())
                            .limit(5)
                            .all()
                        )
                        return [int(r[0]) for r in rows]

                    agg = {
                        fn: {"races": 0, "win": 0, "p": 0, "q1": 0, "pq": 0, "t3e": 0, "t3": 0, "f4": 0, "f4q": 0, "b5w": 0, "b5p": 0}
                        for fn in factor_names
                    }
                    cache_act = {}

                    for race_id, factor_name, top5, meta in preds:
                        if not isinstance(top5, list) or len(top5) < 5:
                            continue

                        hits = None
                        if isinstance(meta, dict):
                            h = meta.get("hits")
                            if isinstance(h, dict):
                                hits = {str(k).lower(): int(v) for k, v in h.items()}

                        if hits is None:
                            act = cache_act.get(int(race_id))
                            if act is None:
                                act = actual_top5(int(race_id))
                                cache_act[int(race_id)] = act
                            if len(act) < 5:
                                continue
                            hits = _calc_hits([int(x) for x in top5], act)

                        if not hits:
                            continue

                        a = agg.get(str(factor_name))
                        if not a:
                            continue
                        a["races"] += 1
                        for k, v in hits.items():
                            kk = str(k).lower()
                            if kk in a:
                                a[kk] += int(v)

                    rows = []
                    for fn in factor_names:
                        a = agg[fn]
                        n = int(a["races"] or 0)
                        rows.append(
                            {
                                "條件": factor_desc.get(fn, fn),
                                "代號": fn,
                                "樣本(場)": n,
                                "WIN%": round((a["win"] / n * 100.0), 1) if n else 0.0,
                                "P%": round((a["p"] / n * 100.0), 1) if n else 0.0,
                                "Q1%": round((a["q1"] / n * 100.0), 1) if n else 0.0,
                                "PQ%": round((a["pq"] / n * 100.0), 1) if n else 0.0,
                                "T3E%": round((a["t3e"] / n * 100.0), 1) if n else 0.0,
                                "T3%": round((a["t3"] / n * 100.0), 1) if n else 0.0,
                                "F4%": round((a["f4"] / n * 100.0), 1) if n else 0.0,
                                "F4Q%": round((a["f4q"] / n * 100.0), 1) if n else 0.0,
                                "B5W%": round((a["b5w"] / n * 100.0), 1) if n else 0.0,
                                "B5P%": round((a["b5p"] / n * 100.0), 1) if n else 0.0,
                            }
                        )
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                    with st.expander("🧩 因子缺資料統計（所選日期範圍）", expanded=False):
                        st.caption("用途：檢查各因子在所選範圍內「無數據/空白」比例，幫你判斷要補數據、降低權重或暫時忽略。")
                        from database.models import Race, ScoringFactor
                        from sqlalchemy import case

                        q = (
                            session_hit.query(
                                ScoringFactor.factor_name.label("factor"),
                                func.count(ScoringFactor.id).label("rows"),
                                func.sum(
                                    case(
                                        (
                                            (ScoringFactor.raw_data_display == None)
                                            | (ScoringFactor.raw_data_display == "")
                                            | (ScoringFactor.raw_data_display == "無數據"),
                                            1,
                                        ),
                                        else_=0,
                                    )
                                ).label("missing_display"),
                                func.sum(case((ScoringFactor.raw_value == None, 1), else_=0)).label("missing_raw"),
                            )
                            .join(RaceEntry, RaceEntry.id == ScoringFactor.entry_id)
                            .join(Race, Race.id == RaceEntry.race_id)
                            .filter(ScoringFactor.factor_name.in_(factor_names))
                            .filter(func.date(Race.race_date) >= d1.isoformat())
                            .filter(func.date(Race.race_date) <= d2.isoformat())
                            .group_by(ScoringFactor.factor_name)
                            .all()
                        )
                        rowsq = []
                        for factor, rows_n, miss_d, miss_r in q:
                            total = int(rows_n or 0)
                            md = int(miss_d or 0)
                            mr = int(miss_r or 0)
                            cov = (1.0 - (md / total)) if total else 0.0
                            rowsq.append(
                                {
                                    "條件": factor_desc.get(str(factor), str(factor)),
                                    "代號": str(factor),
                                    "樣本(匹)": total,
                                    "缺失顯示(匹)": md,
                                    "缺失顯示(%)": round((md / total * 100.0), 1) if total else 0.0,
                                    "缺失原始(匹)": mr,
                                    "缺失原始(%)": round((mr / total * 100.0), 1) if total else 0.0,
                                    "覆蓋率(%)": round(cov * 100.0, 1),
                                }
                            )
                        if not rowsq:
                            st.info("選定範圍內未找到因子計分資料。")
                        else:
                            st.dataframe(
                                pd.DataFrame(rowsq).sort_values(["缺失顯示(%)", "缺失原始(%)"], ascending=[False, False]),
                                use_container_width=True,
                                hide_index=True,
                            )
                            st.markdown("---")
                            st.markdown("**缺失原因分類（按場次 factor_quality 匯總）**")
                            st.caption("只統計已重新計分過、且已寫入 factor_quality 的場次；舊場次如未重算可能無法顯示原因分類。")
                            from database.models import SystemConfig, HorseHistory
                            from scoring_engine.core import ScoringEngine as _SE

                            if factor_names:
                                selected_factor = st.selectbox(
                                    "選擇因子",
                                    options=factor_names,
                                    format_func=lambda x: f"{factor_desc.get(x, x)} ({x})",
                                    key="missing_reason_factor",
                                )
                                race_ids = [
                                    int(r[0])
                                    for r in (
                                        session_hit.query(Race.id)
                                        .filter(func.date(Race.race_date) >= d1.isoformat())
                                        .filter(func.date(Race.race_date) <= d2.isoformat())
                                        .all()
                                    )
                                    if r and r[0]
                                ]
                                keys = [f"factor_quality:{rid}" for rid in race_ids]
                                cfgs = []
                                if keys:
                                    cfgs = session_hit.query(SystemConfig.key, SystemConfig.value).filter(SystemConfig.key.in_(keys)).all()
                                agg_reason = {}
                                total_missing = 0
                                cfg_key_set = set()
                                race_ids_with_reason = set()
                                for k, v in cfgs:
                                    ks = None
                                    try:
                                        ks = str(k)
                                        cfg_key_set.add(ks)
                                    except Exception:
                                        ks = None
                                    rid = None
                                    try:
                                        if ks and ks.startswith("factor_quality:"):
                                            rid = int(ks.split(":", 1)[1])
                                    except Exception:
                                        rid = None
                                    if not isinstance(v, dict):
                                        continue
                                    fs = v.get("factors") if isinstance(v.get("factors"), dict) else {}
                                    fv = fs.get(selected_factor) if isinstance(fs, dict) else None
                                    if not isinstance(fv, dict):
                                        continue
                                    if rid is not None:
                                        race_ids_with_reason.add(int(rid))
                                    reasons = fv.get("reasons") if isinstance(fv.get("reasons"), dict) else {}
                                    for rk, rv in reasons.items():
                                        n = int(rv or 0)
                                        agg_reason[str(rk)] = int(agg_reason.get(str(rk)) or 0) + n
                                        total_missing += n

                                missing_race_ids = [rid for rid in race_ids if int(rid) not in race_ids_with_reason]
                                if missing_race_ids:
                                    engine = _SE(session_hit)
                                    miss_rows = (
                                        session_hit.query(
                                            ScoringFactor.raw_data_display,
                                            RaceEntry.draw,
                                            RaceEntry.rating,
                                            RaceEntry.actual_weight,
                                            RaceEntry.horse_id,
                                            Race.race_date,
                                            Race.race_no,
                                        )
                                        .join(RaceEntry, RaceEntry.id == ScoringFactor.entry_id)
                                        .join(Race, Race.id == RaceEntry.race_id)
                                        .filter(RaceEntry.race_id.in_(missing_race_ids))
                                        .filter(ScoringFactor.factor_name == selected_factor)
                                        .all()
                                    )

                                    horse_ids = []
                                    race_keys = []
                                    for disp, draw, rating, wt, hid, rd, rno in miss_rows:
                                        dd = str(disp or "").strip()
                                        if dd not in {"", "無數據"}:
                                            continue
                                        try:
                                            if hid is not None:
                                                horse_ids.append(int(hid))
                                        except Exception:
                                            pass
                                        try:
                                            if rd is not None and hasattr(rd, "date") and int(rno or 0) > 0:
                                                date_str = rd.date().strftime("%Y/%m/%d")
                                                race_keys.append((date_str, int(rno or 0)))
                                        except Exception:
                                            pass

                                    horse_ids = sorted(set([x for x in horse_ids if x > 0]))
                                    horse_has_history = {hid: False for hid in horse_ids}
                                    if horse_ids:
                                        rows_h = (
                                            session_hit.query(HorseHistory.horse_id, func.count(HorseHistory.id))
                                            .filter(HorseHistory.horse_id.in_(horse_ids))
                                            .group_by(HorseHistory.horse_id)
                                            .all()
                                        )
                                        for hid, cnt in rows_h:
                                            try:
                                                horse_has_history[int(hid)] = int(cnt or 0) > 0
                                            except Exception:
                                                continue

                                    race_keys = sorted(set([rk for rk in race_keys if rk and rk[0] and rk[1]]))
                                    sp_key_list = []
                                    for ds, rno in race_keys:
                                        sp_key_list.append(f"speedpro_energy:{ds}:{rno}")
                                        sp_key_list.append(f"speedpro_retry:{ds}:{rno}")
                                    sp_cfg = {}
                                    if sp_key_list:
                                        sp_rows = session_hit.query(SystemConfig.key, SystemConfig.value).filter(SystemConfig.key.in_(sp_key_list)).all()
                                        for kk, vv in sp_rows:
                                            try:
                                                sp_cfg[str(kk)] = vv
                                            except Exception:
                                                continue

                                    speedpro_state_by_race = {}
                                    for ds, rno in race_keys:
                                        sp = sp_cfg.get(f"speedpro_energy:{ds}:{rno}")
                                        rr = sp_cfg.get(f"speedpro_retry:{ds}:{rno}")
                                        rv = rr if isinstance(rr, dict) else {}
                                        speedpro_state_by_race[(ds, rno)] = {
                                            "has_data": bool(isinstance(sp, dict) and sp),
                                            "had_retry": bool(isinstance(rv, dict) and rv),
                                            "last_error": (rv.get("last_error") if isinstance(rv, dict) else None),
                                        }

                                    for disp, draw, rating, wt, hid, rd, rno in miss_rows:
                                        dd = str(disp or "").strip()
                                        if dd not in {"", "無數據"}:
                                            continue
                                        date_str = None
                                        try:
                                            if rd is not None and hasattr(rd, "date"):
                                                date_str = rd.date().strftime("%Y/%m/%d")
                                        except Exception:
                                            date_str = None
                                        sp_state = speedpro_state_by_race.get((date_str, int(rno or 0))) if date_str else None
                                        if not isinstance(sp_state, dict):
                                            sp_state = {"has_data": False, "had_retry": False, "last_error": None}
                                        row = {"draw": draw, "rating": rating, "weight": wt, "horse_id": hid}
                                        r = engine._missing_reason(
                                            factor_name=selected_factor,
                                            display=dd,
                                            row=row,
                                            speedpro_state=sp_state,
                                            horse_has_history=horse_has_history,
                                        )
                                        agg_reason[str(r)] = int(agg_reason.get(str(r)) or 0) + 1
                                        total_missing += 1

                                if not agg_reason:
                                    st.info("所選範圍內暫無缺失原因分類資料（可先對該範圍場次重新計分）。")
                                else:
                                    rr = []
                                    for rk, n in sorted(agg_reason.items(), key=lambda x: (-(int(x[1] or 0)), str(x[0]))):
                                        rr.append(
                                            {
                                                "原因": rk,
                                                "缺失(匹)": int(n or 0),
                                                "佔缺失(%)": round((int(n or 0) / total_missing * 100.0), 1) if total_missing else 0.0,
                                            }
                                        )
                                    st.dataframe(pd.DataFrame(rr), use_container_width=True, hide_index=True)

                    with st.expander("🤖 權重建議（Top5 模型）", expanded=False):
                        st.caption("用所選日期範圍的歷史賽果（Top5=正例）自動估計各因子重要性，輸出建議權重。建議只作參考，套用後需重算場次才會影響排名。")
                        from scoring_engine.weight_tuning import tune_weights_topk
                        from database.models import Race
                        import json

                        w_rows = (
                            session_hit.query(ScoringWeight.factor_name, ScoringWeight.weight)
                            .filter(ScoringWeight.is_active == True)
                            .filter(ScoringWeight.factor_name.in_(factor_names))
                            .all()
                        )
                        current_w = {str(fn): float(w or 0.0) for fn, w in w_rows if fn}

                        c1, c2, c3 = st.columns([2, 2, 3])
                        max_w = float(c1.selectbox("建議權重上限", [2.0, 3.0, 4.0, 5.0], index=1, key="tune_max_w"))
                        top_k = int(c2.selectbox("TopK 定義", [5], index=0, key="tune_topk"))
                        run = c3.button("生成建議", use_container_width=True, key="tune_run_btn")

                        if run:
                            res = tune_weights_topk(
                                session_hit,
                                d1=d1,
                                d2=d2,
                                top_k=top_k,
                                factor_names=factor_names,
                                max_suggest_weight=max_w,
                            )
                            st.session_state["tune_top5_result"] = res

                        res = st.session_state.get("tune_top5_result")
                        if isinstance(res, dict) and res.get("ok") is True:
                            m1, m2, m3, m4 = st.columns(4)
                            m1.metric("樣本(匹)", int(res.get("rows") or 0))
                            m2.metric("Top5 比例", f"{float(res.get('pos_rate') or 0.0):.1%}" if res.get("pos_rate") is not None else "-")
                            m3.metric("AUC", f"{float(res.get('auc') or 0.0):.3f}" if res.get("auc") is not None else "-")
                            m4.metric("LogLoss", f"{float(res.get('log_loss') or 0.0):.3f}" if res.get("log_loss") is not None else "-")

                            sugg = res.get("suggested_weights") if isinstance(res.get("suggested_weights"), dict) else {}
                            cs = res.get("coef_score") if isinstance(res.get("coef_score"), dict) else {}
                            cm = res.get("coef_missing") if isinstance(res.get("coef_missing"), dict) else {}

                            out_rows = []
                            for fn in factor_names:
                                out_rows.append(
                                    {
                                        "條件": factor_desc.get(fn, fn),
                                        "代號": fn,
                                        "目前權重": round(float(current_w.get(fn) or 0.0), 3),
                                        "建議權重": round(float(sugg.get(fn) or 0.0), 3),
                                        "係數(分數)": round(float(cs.get(fn) or 0.0), 4),
                                        "係數(缺失)": round(float(cm.get(fn) or 0.0), 4),
                                    }
                                )
                            df_out = pd.DataFrame(out_rows).sort_values(["建議權重", "目前權重"], ascending=[False, False])
                            st.dataframe(df_out, use_container_width=True, hide_index=True)

                            payload = {
                                "top_k": int(res.get("top_k") or 0),
                                "date_range": {"from": d1.isoformat(), "to": d2.isoformat()},
                                "metrics": {"rows": res.get("rows"), "pos_rate": res.get("pos_rate"), "auc": res.get("auc"), "log_loss": res.get("log_loss")},
                                "suggested_weights": {str(k): float(v) for k, v in (sugg or {}).items()},
                            }
                            st.download_button(
                                "下載建議權重 JSON",
                                data=json.dumps(payload, ensure_ascii=False, indent=2),
                                file_name=f"tuned_weights_top{int(top_k)}_{d1.isoformat()}_{d2.isoformat()}.json",
                                mime="application/json",
                                use_container_width=True,
                                key="tune_download_btn",
                            )

                            st.markdown("---")
                            st.caption("套用會直接更新 ScoringWeight（全局權重）。套用後可選擇立即重算該日期範圍內的所有場次。")
                            apply_rescore = st.checkbox("同時重算所選日期範圍", value=False, key="tune_apply_rescore")
                            confirm = st.text_input("輸入 APPLY 以套用", value="", key="tune_apply_confirm")
                            if st.button("套用到全局權重", use_container_width=True, key="tune_apply_btn"):
                                if str(confirm or "").strip().upper() != "APPLY":
                                    st.warning("請先輸入 APPLY 再套用。")
                                else:
                                    for fn, w in (sugg or {}).items():
                                        try:
                                            ww = session_hit.query(ScoringWeight).filter_by(factor_name=str(fn)).first()
                                            if ww:
                                                ww.weight = float(w or 0.0)
                                        except Exception:
                                            continue
                                    session_hit.commit()

                                    if apply_rescore:
                                        races2 = (
                                            session_hit.query(Race)
                                            .filter(func.date(Race.race_date) >= d1.isoformat())
                                            .filter(func.date(Race.race_date) <= d2.isoformat())
                                            .order_by(Race.race_date.asc(), Race.race_no.asc(), Race.id.asc())
                                            .all()
                                        )
                                        engine = ScoringEngine(session_hit)
                                        for r in races2:
                                            rid2 = int(getattr(r, "id") or 0)
                                            if rid2:
                                                engine.score_race(rid2)
                                    st.success("✅ 已套用建議權重。")
                        elif isinstance(res, dict) and res.get("ok") is False and res.get("reason"):
                            st.info("選定範圍內未找到足夠的已結算賽果 + 計分資料，無法生成建議。")
        finally:
            session_hit.close()

    with sub_preset:
        st.subheader("👥 會員儲存組合命中率統計")
        from datetime import date, timedelta
        from sqlalchemy import func
        from database.models import PredictionTop5

        session_p = get_session()
        try:
            drows = (
                session_p.query(func.date(PredictionTop5.race_date))
                .filter(PredictionTop5.predictor_type == "preset")
                .distinct()
                .order_by(func.date(PredictionTop5.race_date).desc())
                .limit(90)
                .all()
            )
            available_dates = [r[0] for r in drows if r and r[0]]
            if not available_dates:
                st.info("目前未有任何會員組合 Top5 快照。請先抓取排位並生成預測快照。")
            else:
                end_default = available_dates[0]
                start_default = max(end_default - timedelta(days=30), min(available_dates))
                d1, d2 = st.date_input("統計日期範圍", value=(start_default, end_default), key="preset_hit_range")
                if isinstance(d1, date) and isinstance(d2, date) and d1 > d2:
                    d1, d2 = d2, d1

                preds = (
                    session_p.query(
                        PredictionTop5.member_email,
                        PredictionTop5.predictor_key,
                        PredictionTop5.meta,
                    )
                    .filter(PredictionTop5.predictor_type == "preset")
                    .filter(func.date(PredictionTop5.race_date) >= d1.isoformat())
                    .filter(func.date(PredictionTop5.race_date) <= d2.isoformat())
                    .all()
                )
                if not preds:
                    st.info("選定範圍內沒有任何會員組合 Top5 快照。")
                else:
                    agg = {}
                    for email, preset_name, meta in preds:
                        email_k = str(email or "").strip().lower()
                        preset_k = str(preset_name or "").strip()
                        if not email_k or not preset_k:
                            continue
                        h = None
                        if isinstance(meta, dict):
                            h = meta.get("hits")
                        if not isinstance(h, dict):
                            continue
                        key = (email_k, preset_k)
                        a = agg.get(key)
                        if a is None:
                            a = {"races": 0, "win": 0, "p": 0, "q1": 0, "pq": 0, "t3e": 0, "t3": 0, "f4": 0, "f4q": 0, "b5w": 0, "b5p": 0}
                            agg[key] = a
                        a["races"] += 1
                        for mk, mv in h.items():
                            kk = str(mk).lower()
                            if kk in a:
                                a[kk] += int(mv or 0)

                    rows = []
                    for (email_k, preset_k), a in agg.items():
                        n = int(a["races"] or 0)
                        rows.append(
                            {
                                "Email": email_k,
                                "組合": preset_k,
                                "樣本(場)": n,
                                "WIN%": round((a["win"] / n * 100.0), 1) if n else 0.0,
                                "P%": round((a["p"] / n * 100.0), 1) if n else 0.0,
                                "Q1%": round((a["q1"] / n * 100.0), 1) if n else 0.0,
                                "PQ%": round((a["pq"] / n * 100.0), 1) if n else 0.0,
                                "T3E%": round((a["t3e"] / n * 100.0), 1) if n else 0.0,
                                "T3%": round((a["t3"] / n * 100.0), 1) if n else 0.0,
                                "F4%": round((a["f4"] / n * 100.0), 1) if n else 0.0,
                                "F4Q%": round((a["f4q"] / n * 100.0), 1) if n else 0.0,
                                "B5W%": round((a["b5w"] / n * 100.0), 1) if n else 0.0,
                                "B5P%": round((a["b5p"] / n * 100.0), 1) if n else 0.0,
                            }
                        )
                    if not rows:
                        st.info("目前未有任何已結算（已抓賽果）的會員組合命中資料。")
                    else:
                        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        finally:
            session_p.close()
