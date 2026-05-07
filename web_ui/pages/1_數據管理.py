import streamlit as st
import pandas as pd
import os
import subprocess
import sys
from pathlib import Path

# 加入專案路徑
root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from scoring_engine.core import ScoringEngine
from scoring_engine.member_stats import HIT_METRICS, METRIC_LABELS
from web_ui.nav import render_admin_nav
from web_ui.utils import _confirm_run

st.set_page_config(page_title="數據管理 - HKJC Analytics", page_icon="⚙️", layout="wide")

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
st.markdown("在此頁面執行數據更新、回填與清理操作。")
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
            [sys.executable, "scripts/run_scraper.py"],
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
            [sys.executable, "scripts/fetch_history.py"],
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
            [sys.executable, "scripts/fetch_race_results.py"],
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
            [sys.executable, "scripts/fetch_fixture.py"],
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
            [sys.executable, "scripts/generate_predictions.py"],
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
            [sys.executable, "scripts/cron_speedpro_fetch.py"],
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

    st.subheader("📉 會員反向統計總表（回填/重建）")
    st.caption("用途：補回歷史淘汰準確率/錯殺率統計，並覆寫保存到 SystemConfig（member_weight_preset_elim_stats:<email>）。")
    session_elim = get_session()
    try:
        from database.models import SystemConfig
        from scoring_engine.member_stats import rebuild_member_preset_elim_stats
        from datetime import datetime, date, timedelta

        cfg = session_elim.query(SystemConfig).filter_by(key="member_whitelist_emails").first()
        emails = []
        if cfg and isinstance(cfg.value, list):
            emails = [str(x).strip().lower() for x in cfg.value if str(x).strip()]
        emails = list(dict.fromkeys(emails))
        if not emails:
            st.info("未設定會員白名單，無法回填。")
        else:
            end_default = date.today()
            start_default = end_default - timedelta(days=30)
            d1, d2 = st.date_input("回填日期範圍", value=(start_default, end_default), key="admin_elim_rebuild_range")
            if isinstance(d1, date) and isinstance(d2, date) and d1 > d2:
                d1, d2 = d2, d1

            cols = st.columns([2, 3])
            ok = _confirm_run(cols[0], "admin_elim_rebuild", label="輸入 RUN 以回填/重建")
            if cols[1].button("📉 回填會員反向統計（覆寫）", use_container_width=True, disabled=not ok):
                progress = st.progress(0)
                done = 0
                for i, em in enumerate(emails):
                    cfg2 = session_elim.query(SystemConfig).filter_by(key=f"member_weight_presets:{str(em)}").first()
                    presets = cfg2.value if cfg2 and isinstance(cfg2.value, list) else []
                    rebuild_member_preset_elim_stats(
                        session=session_elim,
                        email=str(em),
                        presets=presets,
                        d1=datetime.combine(d1, datetime.min.time()),
                        d2=datetime.combine(d2, datetime.min.time()),
                    )
                    done += 1
                    progress.progress((i + 1) / len(emails))
                st.success(f"✅ 已回填 {done} 位會員。")
                st.rerun()
    finally:
        session_elim.close()

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
        c_confirm, c_btn = st.columns([2, 3])
        ok = _confirm_run(c_confirm, "fixture", label="輸入 RUN 以更新賽期表")
        if c_btn.button("📅 更新賽期表 (本月+下月)", use_container_width=True, disabled=not ok):
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

        st.subheader("⚖️ 全局權重設定（ScoringWeight）")
        st.caption("此處係「全局」權重（會影響後台按總分排序、以及以全局權重生成的 Top5/淘汰診斷）。用戶端會員組合係另一套 preset 權重。")
        session_w = get_session()
        try:
            from database.models import ScoringWeight
            from scoring_engine.constants import DISABLED_FACTORS

            rows = (
                session_w.query(ScoringWeight)
                .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
                .order_by(ScoringWeight.factor_name.asc())
                .all()
            )
            items = []
            for w in rows:
                fn = str(getattr(w, "factor_name", "") or "").strip()
                if not fn:
                    continue
                items.append(
                    {
                        "因子代號": fn,
                        "因子名稱": str(getattr(w, "description", "") or fn),
                        "權重": (float(w.weight) if getattr(w, "weight", None) is not None else None),
                        "啟用": bool(getattr(w, "is_active", False)),
                    }
                )

            if not items:
                st.info("目前未找到任何全局權重設定。")
            else:
                dfw = pd.DataFrame(items)
                edited_w = st.data_editor(
                    dfw,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "權重": st.column_config.NumberColumn("權重", step=0.1, help="留空會視作 0；建議一般保持 >0"),
                        "啟用": st.column_config.CheckboxColumn("啟用"),
                    },
                    disabled=["因子代號", "因子名稱"],
                    key="global_weight_editor",
                )
                c_save, c_hint = st.columns([2, 3])
                save_w = c_save.button("💾 儲存全局權重", use_container_width=True, key="save_global_weights")
                c_hint.caption("儲存後需重算相關場次，才會寫回 RaceEntry.total_score。")

                if save_w and isinstance(edited_w, pd.DataFrame):
                    w_by_name = {str(x.factor_name): x for x in rows if getattr(x, "factor_name", None)}
                    for _, r in edited_w.iterrows():
                        code = str(r.get("因子代號") or "").strip()
                        if not code or code not in w_by_name:
                            continue
                        obj = w_by_name[code]
                        v = r.get("權重")
                        try:
                            obj.weight = float(v) if v is not None and str(v) != "nan" else 0.0
                        except Exception:
                            obj.weight = 0.0
                        obj.is_active = bool(r.get("啟用") is True)
                    session_w.commit()
                    st.success("✅ 已儲存全局權重。")
                    st.rerun()
        except Exception as e:
            session_w.rollback()
            st.error(f"❌ 全局權重讀寫失敗: {e}")
        finally:
            session_w.close()

        st.subheader("🎯 勝率校準（Temperature）")
        st.caption("用途：把「總分→預估勝率」的 softmax 溫度做校準，讓勝率分佈更貼近歷史賽果（只影響顯示/勝率欄位，不改排名）。")
        session_cal = get_session()
        try:
            from scoring_engine.calibration import fit_winprob_temperature, load_winprob_temperature, save_winprob_temperature
            from database.models import Race, RaceEntry, RaceResult
            from scoring_engine.core import ScoringEngine
            from sqlalchemy import func
            from datetime import date, timedelta

            current_t = load_winprob_temperature(session_cal)
            if current_t:
                st.info(f"目前 temperature：{float(current_t):.3f}")
            else:
                st.info("目前未設定 temperature（預設 1.0）。")

            drows = (
                session_cal.query(func.date(Race.race_date))
                .join(RaceEntry, RaceEntry.race_id == Race.id)
                .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
                .filter(RaceResult.rank != None)
                .distinct()
                .order_by(func.date(Race.race_date).desc())
                .limit(365)
                .all()
            )
            available_dates = [r[0] for r in drows if r and r[0]]
            if not available_dates:
                st.info("目前未有任何已結算賽果可供校準。")
            else:
                end_default = available_dates[0]
                start_default = max(end_default - timedelta(days=60), min(available_dates))
                d1, d2 = st.date_input("校準日期範圍", value=(start_default, end_default), key="calib_dates")
                if isinstance(d1, date) and isinstance(d2, date) and d1 > d2:
                    d1, d2 = d2, d1

                c1, c2 = st.columns([2, 3])
                do_rescore = c1.checkbox("同時重算所選範圍", value=False, key="calib_rescore")
                ok = _confirm_run(c1, "calib_train", label="輸入 RUN 以訓練/保存")
                run = c2.button("訓練並保存 temperature", use_container_width=True, key="calib_train_btn", disabled=not ok)

                if run:
                    res = fit_winprob_temperature(session_cal, d1=d1, d2=d2)
                    if res.get("ok") is True:
                        save_winprob_temperature(session_cal, res)
                        st.success(f"✅ 已保存 temperature={float(res.get('temperature') or 1.0):.3f}（races={int(res.get('races') or 0)} nll={float(res.get('nll') or 0.0):.4f}）")
                        if do_rescore:
                            races2 = (
                                session_cal.query(Race)
                                .filter(func.date(Race.race_date) >= d1.isoformat())
                                .filter(func.date(Race.race_date) <= d2.isoformat())
                                .order_by(Race.race_date.asc(), Race.race_no.asc(), Race.id.asc())
                                .all()
                            )
                            engine = ScoringEngine(session_cal)
                            for r in races2:
                                rid2 = int(getattr(r, "id") or 0)
                                if rid2:
                                    engine.score_race(rid2)
                            st.success("✅ 已重算所選範圍場次。")
                        st.rerun()
                    else:
                        st.error("❌ 訓練失敗：所選範圍內沒有足夠的已結算賽果/計分資料。")
        finally:
            session_cal.close()
        
        st.subheader("⚡ 一鍵完整更新（建議）")
        st.caption("會依序完成：抓排位 → 回填該日涉及馬匹往績 → 重算該日所有場次 → 生成 Top5 快照（factor + preset）。每一步會等待上一個完成。")
        c_confirm, c_btn = st.columns([2, 3])
        ok = _confirm_run(c_confirm, "oneclick_update", label="輸入 RUN 以執行一鍵完整更新")
        if c_btn.button("⚡ 一鍵：抓排位 → 回填馬匹往績 → 重算當日 → 生成Top5快照", use_container_width=True, disabled=not ok):
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
        c_confirm, c_btn = st.columns([2, 3])
        ok = _confirm_run(c_confirm, "scrape_day", label="輸入 RUN 以開始抓取")
        if c_btn.button("🔄 開始抓取該日賽事", use_container_width=True, disabled=not ok):
            target_date_str = selected_date.strftime("%Y/%m/%d")
            if trigger_scraper(target_date=target_date_str):
                st.success(f"✅ {target_date_str} 數據更新成功！")

        st.subheader("🧾 預測快照 (Top5)")
        st.caption("只生成 Top5 快照（落庫 PredictionTop5）。需要先完成該日計分/重算，否則快照會反映不完整數據。")
        c_confirm, c_btn = st.columns([2, 3])
        ok = _confirm_run(c_confirm, "snapshot_day", label="輸入 RUN 以生成快照")
        if c_btn.button("🧾 生成當日 Top5 預測快照", use_container_width=True, disabled=not ok):
            target_date_str = selected_date.strftime("%Y/%m/%d")
            if trigger_predictions_snapshot(target_date_str):
                st.success(f"✅ 已生成 {target_date_str} Top5 預測快照！")

        st.subheader("🏁 抓取賽果與派彩")
        st.caption("抓取賽果/派彩入庫後，會自動結算：會員組合命中率 + Top5 快照命中（回寫 hits/actual_top5）。若已設定賽果 cron（每日 HK 23:55）通常不需手動按。")
        c_confirm, c_btn = st.columns([2, 3])
        ok = _confirm_run(c_confirm, "fetch_results", label="輸入 RUN 以抓取賽果")
        if c_btn.button("🏁 抓取該日賽果與派彩", use_container_width=True, disabled=not ok):
            target_date_str = selected_date.strftime("%Y/%m/%d")
            if trigger_race_results_fetch(target_date=target_date_str):
                st.success(f"✅ 已完成 {target_date_str} 賽果與派彩同步！")
                session_upd = get_session()
                try:
                    from database.models import SystemConfig
                    from scoring_engine.member_stats import update_member_preset_elim_stats_incremental

                    cfg = session_upd.query(SystemConfig).filter_by(key="member_whitelist_emails").first()
                    emails = []
                    if cfg and isinstance(cfg.value, list):
                        emails = [str(x).strip().lower() for x in cfg.value if str(x).strip()]
                    emails = list(dict.fromkeys(emails))
                    for em in emails:
                        cfg2 = session_upd.query(SystemConfig).filter_by(key=f"member_weight_presets:{str(em)}").first()
                        presets = cfg2.value if cfg2 and isinstance(cfg2.value, list) else []
                        update_member_preset_elim_stats_incremental(session_upd, str(em), presets, per_preset_max_new_races=200)
                finally:
                    session_upd.close()

        st.subheader("🌦️ 回填場地狀況（賽後）")
        st.caption("用途：把已入庫的「賽果與派彩」中 meta.going/meta.track 回填到 RaceTrackCondition（可作篩選條件），不需重新爬網。")
        c_confirm, c_btn = st.columns([2, 3])
        ok = _confirm_run(c_confirm, "backfill_going", label="輸入 RUN 以回填")
        if c_btn.button("🌦️ 回填該日場地狀況", use_container_width=True, disabled=not ok):
            target_date_str = selected_date.strftime("%Y/%m/%d")
            session_bf = get_session()
            try:
                from datetime import datetime
                from database.models import Race, RaceDividend, RaceTrackCondition
                from scoring_engine.track_conditions import normalize_going
                from sqlalchemy import func

                races = (
                    session_bf.query(Race.id)
                    .filter(func.date(Race.race_date) == datetime.strptime(target_date_str, "%Y/%m/%d").date().isoformat())
                    .all()
                )
                race_ids = [int(r[0]) for r in races if r and int(r[0] or 0) > 0]
                if not race_ids:
                    st.info("該日沒有任何賽事資料。")
                else:
                    divs = session_bf.query(RaceDividend.race_id, RaceDividend.meta).filter(RaceDividend.race_id.in_(race_ids)).all()
                    updated = 0
                    for rid, meta in divs:
                        if not isinstance(meta, dict):
                            continue
                        going_raw, going_code = normalize_going(str(meta.get("going") or ""))
                        track_raw = str(meta.get("track") or "").strip()
                        if not (going_raw or track_raw):
                            continue
                        tc = session_bf.query(RaceTrackCondition).filter_by(race_id=int(rid)).first()
                        if not tc:
                            tc = RaceTrackCondition(race_id=int(rid), source="HKJC_LOCALRESULTS")
                            session_bf.add(tc)
                        tc.going_raw = going_raw or tc.going_raw
                        tc.going_code = (going_code or going_raw) or tc.going_code
                        tc.track_raw = track_raw or tc.track_raw
                        tc.updated_at = datetime.now()
                        updated += 1
                    session_bf.commit()
                    st.success(f"✅ 已回填 {updated} 場（{target_date_str}）")
            except Exception as e:
                st.error(f"❌ 回填失敗：{e}")
            finally:
                session_bf.close()

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
        c_confirm, c_btn = st.columns([2, 3])
        ok = _confirm_run(c_confirm, "speedpro_fetch", label="輸入 RUN 以抓取 SpeedPRO")
        if c_btn.button("⚡ 立即抓取 SpeedPRO", use_container_width=True, disabled=not ok):
            ok = trigger_speedpro_fetch(target_date=target_date_str, race_nos=race_nos_str, retry_minutes=int(retry_minutes), force=True)
            if ok:
                st.success("✅ 已觸發 SpeedPRO 抓取（詳情見上方日誌/狀態表）。")
            else:
                st.error("❌ SpeedPRO 抓取失敗，請查看日誌。")

        st.subheader("📚 歷史回填")
        st.caption("回填馬匹往績（HorseHistory），供部分條件計分使用。更新排位後、重算前先回填，結果較完整。")
        col_h1, col_h2 = st.columns(2)
        with col_h1:
            ok = _confirm_run(col_h1, "backfill_date", label="輸入 RUN 以回填（所選日期）")
            if st.button("📚 回填所選日期馬匹往績", use_container_width=True, disabled=not ok):
                target_date_str = selected_date.strftime("%Y/%m/%d")
                if trigger_history_backfill(target_date=target_date_str, mode="date"):
                    st.success(f"✅ 已完成 {target_date_str} 所需馬匹之歷史往績回填！")
        with col_h2:
            with st.expander("完整回填 (較慢)"):
                ok = _confirm_run(st, "backfill_all", label="輸入 RUN 以回填（全部）")
                if st.button("📚 回填所有馬匹往績", use_container_width=True, disabled=not ok):
                    if trigger_history_backfill(mode="all"):
                        st.success("✅ 已完成所有馬匹之歷史往績回填！")

    with col2:
        st.subheader("🚀 批量計分操作")
        st.caption("只做「重算所選日期」所有場次（不包含回填/快照）。適合你已完成回填但想再重算一次。")
        c_confirm, c_btn = st.columns([2, 3])
        ok = _confirm_run(c_confirm, "rescore_date", label="輸入 RUN 以重算所選日期")
        if c_btn.button("🚀 重算所選日期所有賽事", use_container_width=True, disabled=not ok):
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
            
        c_confirm, c_btn = st.columns([2, 3])
        ok = _confirm_run(c_confirm, "db_upgrade", label="輸入 RUN 以執行升級")
        if c_btn.button("🆙 執行資料庫欄位升級 (新增原始數據欄位)", use_container_width=True, disabled=not ok):
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
            for p in cfg.value[:20]:
                if not isinstance(p, dict):
                    continue
                name = str(p.get("name") or "").strip()
                weights_map = p.get("weights") if isinstance(p.get("weights"), dict) else {}
                stt = stats_map.get(name, {}) if isinstance(stats_map, dict) else {}
                races_n = int(stt.get("races") or 0)
                row = {
                    "Email": email,
                    "組合": name,
                    "更新時間": str(p.get("updated_at") or ""),
                    "樣本(場)": races_n,
                    "_weights": weights_map,
                }
                for k in HIT_METRICS:
                    col = f"{METRIC_LABELS.get(k, k)}%"
                    v = int(stt.get(k) or 0)
                    row[col] = round((v / races_n * 100.0), 1) if races_n else 0.0
                rows.append(row)

        if not rows:
            st.info("目前沒有任何會員儲存組合。")
        else:
            df_overview = []
            for r in rows:
                rr = dict(r)
                rr.pop("_weights", None)
                df_overview.append(rr)
            st.dataframe(
                pd.DataFrame(df_overview), 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "條件": st.column_config.TextColumn(width="medium"),
                    "描述": st.column_config.TextColumn(width="large"),
                    "代號": st.column_config.TextColumn(width="medium"),
                }
            )

            st.markdown("---")
            st.markdown("### 🔎 組合權重參數")
            
            # Group rows by email
            from collections import defaultdict
            grouped_by_email = defaultdict(list)
            for r in rows:
                grouped_by_email[r["Email"]].append(r)
                
            for email, member_rows in grouped_by_email.items():
                with st.expander(f"👤 {email} ({len(member_rows)} 個組合)", expanded=False):
                    for r in member_rows:
                        name = r["組合"]
                        weights_map = r.get("_weights") or {}
                        st.markdown(f"**🔹 {name}**")
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
                        st.markdown("<br>", unsafe_allow_html=True)
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
            end_default = available_dates[0] if available_dates else date.today()
            start_default = (
                max(end_default - timedelta(days=30), min(available_dates)) if available_dates else (end_default - timedelta(days=30))
            )
            d1, d2 = st.date_input("統計日期範圍", value=(start_default, end_default), key="hit_factor_range_admin")
            if isinstance(d1, date) and isinstance(d2, date) and d1 > d2:
                d1, d2 = d2, d1

            if not available_dates:
                st.info("目前未有任何獨立條件 Top5 快照。你仍可先設定 AI；要生成建議需先抓排位並生成預測快照，並且有已結算賽果。")

            preds = []
            if available_dates and factor_names:
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

            if preds:
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

                agg = {fn: {"races": 0, **{k: 0 for k in HIT_METRICS}} for fn in factor_names}
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
                    row = {"條件": factor_desc.get(fn, fn), "代號": fn, "樣本(場)": n}
                    for k in HIT_METRICS:
                        row[f"{METRIC_LABELS.get(k, k)}%"] = round((int(a.get(k) or 0) / n * 100.0), 1) if n else 0.0
                    rows.append(row)
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True, column_config={"條件": st.column_config.TextColumn(width="medium"), "代號": st.column_config.TextColumn(width="medium"), "組合": st.column_config.TextColumn(width="medium")})

                with st.expander("🧩 因子缺資料統計（所選日期範圍）", expanded=False):
                    st.caption("用途：檢查各因子在所選範圍內「無數據/空白」比例，幫你判斷要補數據、降低權重或暫時忽略。")
                    from database.models import Race, ScoringFactor
                    from sqlalchemy import case

                    with st.container():

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

            elif available_dates:
                st.info("選定範圍內沒有任何獨立條件 Top5 快照。")

            with st.expander("🤖 權重建議（Top3 重心模型）", expanded=False):
                if not factor_names:
                    st.info("目前沒有可用的獨立條件因子。")
                else:
                    st.caption("用所選日期範圍的歷史賽果自動估計各因子重要性，目標聚焦 Top2 勝出率＋PQ(3)（後台只作分析與下載）。")
                    st.markdown(
                        """
**方法說明（自動估計因子重要性）**
- **資料來源**：使用所選日期範圍內、已結算賽果的場次；每匹馬取資料庫 `ScoringFactor` 的各因子分數與 `raw_data_display`。
- **目標定義**：同一份資料會學兩個目標：`勝出(名次=1)` 與 `入圍Top3(名次≤3)`（更貼近 PQ(3)），再按目標權重加總。
- **特徵**：每個因子會產生 2 個特徵：
  - `分數`：該因子在該場的相對分數（0–10）。
  - `缺失`：若 `raw_data_display` 為空白/無數據 → 1，否則 0。
- **缺失處理**：若某因子分數缺失，分數以 5.0（中間值）補上；同時 `缺失=1` 讓模型學到「缺資料時應該如何調整」。
- **模型**：Logistic Regression（二分類），並用 `class_weight=balanced` 減少正負例比例不均造成的偏差。
- **建議權重**：把兩個模型的正向係數按目標權重加總，再按「最大值」比例縮放到你選的「建議權重上限」。
- **指標**：回算同一批資料的 Top2 勝出率與 PQ(3)（in-sample）作方向參考；建議以不同日期範圍反覆驗證。
                        """.strip()
                    )
                    from scoring_engine.weight_tuning import tune_weights_top3_focus
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
                    w2_w = float(c2.selectbox("目標權重：Top2 勝出率", [0.5, 0.7, 0.9], index=1, key="tune_w2_w"))
                    run = c3.button("生成建議", use_container_width=True, key="tune_run_btn")
                    t2_w = float(st.selectbox("目標權重：PQ(3)", [0.1, 0.3, 0.5], index=1, key="tune_t2_w"))

                    if run:
                        res = tune_weights_top3_focus(
                            session_hit,
                            d1=d1,
                            d2=d2,
                            factor_names=factor_names,
                            max_suggest_weight=max_w,
                            objective={"w2_weight": float(w2_w), "pq3_weight": float(t2_w)},
                        )
                        st.session_state["tune_top5_result"] = res

                    res = st.session_state.get("tune_top5_result")
                    if isinstance(res, dict) and res.get("ok") is True:
                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("樣本(匹)", int(res.get("rows") or 0))
                        m2.metric("樣本(場)", int(res.get("races") or 0))
                        ins = res.get("in_sample") if isinstance(res.get("in_sample"), dict) else {}
                        m3.metric("Top2 勝出率", f"{float(ins.get('w2_rate') or 0.0):.1f}%")
                        m4.metric("PQ(3)", f"{float(ins.get('pq3_rate') or ins.get('top3_2in_rate') or 0.0):.1f}%")

                        sugg = res.get("suggested_weights") if isinstance(res.get("suggested_weights"), dict) else {}
                        cs = res.get("coef_win_score") if isinstance(res.get("coef_win_score"), dict) else {}
                        cm = res.get("coef_win_missing") if isinstance(res.get("coef_win_missing"), dict) else {}

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
                        st.dataframe(
                            df_out, 
                            use_container_width=True, 
                            hide_index=True,
                            column_config={
                                "條件": st.column_config.TextColumn(width="medium"),
                                "代號": st.column_config.TextColumn(width="medium")
                            }
                        )

                        payload = {
                            "date_range": {"from": d1.isoformat(), "to": d2.isoformat()},
                            "objective": res.get("objective"),
                            "metrics": {"rows": res.get("rows"), "races": res.get("races"), "in_sample": res.get("in_sample")},
                            "suggested_weights": {str(k): float(v) for k, v in (sugg or {}).items()},
                        }
                        st.download_button(
                            "下載建議權重 JSON",
                            data=json.dumps(payload, ensure_ascii=False, indent=2),
                            file_name=f"tuned_weights_top3focus_{d1.isoformat()}_{d2.isoformat()}.json",
                            mime="application/json",
                            use_container_width=True,
                            key="tune_download_btn",
                        )
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
                            a = {"races": 0, **{k: 0 for k in HIT_METRICS}}
                            agg[key] = a
                        a["races"] += 1
                        for mk, mv in h.items():
                            kk = str(mk).lower()
                            if kk in a:
                                a[kk] += int(mv or 0)

                    rows = []
                    for (email_k, preset_k), a in agg.items():
                        n = int(a["races"] or 0)
                        row = {"Email": email_k, "組合": preset_k, "樣本(場)": n}
                        for k in HIT_METRICS:
                            row[f"{METRIC_LABELS.get(k, k)}%"] = round((int(a.get(k) or 0) / n * 100.0), 1) if n else 0.0
                        rows.append(row)
                    if not rows:
                        st.info("目前未有任何已結算（已抓賽果）的會員組合命中資料。")
                    else:
                        st.dataframe(
                            pd.DataFrame(rows), 
                            use_container_width=True, 
                            hide_index=True, 
                            column_config={
                                "Email": st.column_config.TextColumn(width="medium"), 
                                "組合": st.column_config.TextColumn(width="medium")
                            }
                        )
        finally:
            session_p.close()
