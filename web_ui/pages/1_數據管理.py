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

st.set_page_config(page_title="數據管理 - HKJC Analytics", layout="wide")

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

st.markdown("[➡️ 前往獨立條件分析](/%E7%8D%A8%E7%AB%8B%E6%A2%9D%E4%BB%B6%E5%88%86%E6%9E%90)")

tab_ops, tab_members = st.tabs(["🛠️ 系統操作", "👥 會員組合"])

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
        from database.models import Race, Horse, Jockey, Trainer, RaceEntry, HorseHistory, ScoringFactor, RaceResult, RaceDividend, OddsHistory, ScoringWeight, SystemConfig

        status = {
            "賽事": session_status.query(Race).count(),
            "排位": session_status.query(RaceEntry).count(),
            "賽果": session_status.query(RaceResult).count(),
            "派彩": session_status.query(RaceDividend).count(),
            "計分": session_status.query(ScoringFactor).count(),
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
        st.subheader("📡 抓取排位表與即時數據")
        
        from datetime import datetime, timedelta
        default_date = datetime.now()
        selected_date = st.date_input("選擇要抓取的賽事日期", value=default_date)
        
        if st.button("🔄 開始抓取該日賽事", use_container_width=True):
            target_date_str = selected_date.strftime("%Y/%m/%d")
            if trigger_scraper(target_date=target_date_str):
                st.success(f"✅ {target_date_str} 數據更新成功！")

        st.subheader("🏁 抓取賽果與派彩")
        if st.button("🏁 抓取該日賽果與派彩", use_container_width=True):
            target_date_str = selected_date.strftime("%Y/%m/%d")
            if trigger_race_results_fetch(target_date=target_date_str):
                st.success(f"✅ 已完成 {target_date_str} 賽果與派彩同步！")

        st.subheader("📚 歷史回填")
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
        if st.button("🚀 一鍵為當日所有賽事重新計分", use_container_width=True):
            session = get_session()
            try:
                from database.models import Race
                from datetime import datetime
                races = session.query(Race).order_by(Race.race_date.desc()).all()
                
                if races:
                    latest_date_val = races[0].race_date
                    latest_date_only = latest_date_val.date() if hasattr(latest_date_val, 'date') else latest_date_val
                    races_to_score = [r for r in races if (r.race_date.date() if hasattr(r.race_date, 'date') else r.race_date) == latest_date_only]
                    
                    engine = ScoringEngine(session)
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    for i, race in enumerate(races_to_score):
                        status_text.text(f"正在計算第 {race.race_no} 場賽事分數...")
                        engine.score_race(race.id)
                        progress_bar.progress((i + 1) / len(races_to_score))
                        
                    st.success(f"✅ 已成功為 {latest_date_only} 的 {len(races_to_score)} 場賽事完成重新計分！")
                else:
                    st.warning("⚠️ 找不到任何賽事資料。")
            except Exception as e:
                st.error(f"❌ 批量計分失敗: {e}")
            finally:
                session.close()

        st.subheader("🧹 系統清理")
        with st.expander("清理已移除因子舊記錄", expanded=False):
            st.markdown("此操作只會刪除已移除因子在資料庫中的舊計分結果與設定，不會影響賽事、馬匹、往績等核心數據。")
            confirm = st.checkbox("我明白此操作會刪除舊因子資料", value=False)
            if st.button("🧹 清理 trainer_horse_bond 舊記錄", use_container_width=True, disabled=not confirm):
                session = get_session()
                deleted_sf, deleted_sw, deleted_cfg = cleanup_removed_factor_data(session)
                session.close()
                st.success(f"✅ 已刪除舊記錄：ScoringFactor {deleted_sf} 筆、ScoringWeight {deleted_sw} 筆、SystemConfig {deleted_cfg} 筆")

        st.subheader("🔌 系統測試與升級")
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
                qin_n = int(stt.get("qin") or 0)
                tri_n = int(stt.get("tri") or 0)
                q4_n = int(stt.get("q4") or 0)
                rows.append(
                    {
                        "Email": email,
                        "組合": name,
                        "更新時間": str(p.get("updated_at") or ""),
                        "樣本(場)": races_n,
                        "獨贏%": round((win_n / races_n * 100.0), 1) if races_n else 0.0,
                        "正Q%": round((qin_n / races_n * 100.0), 1) if races_n else 0.0,
                        "三重彩%": round((tri_n / races_n * 100.0), 1) if races_n else 0.0,
                        "四重彩%": round((q4_n / races_n * 100.0), 1) if races_n else 0.0,
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
