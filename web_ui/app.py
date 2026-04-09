import streamlit as st
import pandas as pd
import numpy as np
import sys
from pathlib import Path
from sqlalchemy.orm import Session

# 加入專案路徑
root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import get_session, init_db
from database.models import Race, RaceEntry, ScoringFactor, ScoringWeight, Horse, SystemConfig
from scoring_engine.core import ScoringEngine
from scoring_engine.constants import DISABLED_FACTORS
from scoring_engine.utils import estimate_win_probability
from utils.logger import logger
import asyncio
import subprocess
from datetime import datetime

# 設定頁面配置
st.set_page_config(page_title="HKJC 每場賽事獨立計分排名系統", layout="wide")

# 初始化資料庫 (確保在雲端環境表結構存在)
init_db()

def get_db():
    return get_session()

import os
import subprocess

# 終極修復：指定 Playwright 瀏覽器安裝路徑 (Railway 必備)
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "/app/playwright_browsers"

def trigger_scraper(target_date: str = None):
    """使用 Popen 實現實時日誌串流輸出 (穩定版)"""
    st.markdown("### 🚀 爬蟲執行進度")
    log_placeholder = st.empty() 
    full_log = ""
    
    try:
        env = os.environ.copy()
        if target_date:
            env["TARGET_DATE"] = target_date
            
        # 直接執行，不再檢查 Playwright
        process = subprocess.Popen(
            ["python3", "scripts/run_scraper.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            bufsize=1
        )

        # 持續讀取輸出直到進程結束
        for line in iter(process.stdout.readline, ""):
            full_log += line
            # 即時更新 UI 上的代碼框
            log_placeholder.code(full_log)
            
        process.stdout.close()
        return_code = process.wait()

        if return_code == 0:
            st.success("✅ 數據更新成功！正在刷新頁面...")
            return True
        else:
            st.error(f"❌ 執行結束，但代碼顯示異常 (Exit Code: {return_code})")
            return False
            
    except Exception as e:
        st.error(f"❌ 系統錯誤: {e}")
        return False

def trigger_history_backfill():
    """執行歷史往績回填任務"""
    st.markdown("### 📚 歷史數據回填進度")
    log_placeholder = st.empty()
    full_log = ""
    
    try:
        env = os.environ.copy()
        process = subprocess.Popen(
            ["python3", "scripts/fetch_history.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            bufsize=1
        )

        for line in iter(process.stdout.readline, ""):
            full_log += line
            log_placeholder.code(full_log)
            
        process.stdout.close()
        return_code = process.wait()

        if return_code == 0:
            st.success("✅ 歷史數據回填完成！")
            return True
        else:
            st.error(f"❌ 執行結束，Exit Code: {return_code}")
            return False
    except Exception as e:
        st.error(f"❌ 系統錯誤: {e}")
        return False

def test_db_connection(session):
    """測試資料庫寫入功能"""
    try:
        from database.models import ScoringWeight
        count = session.query(ScoringWeight).count()
        st.sidebar.success(f"✅ 資料庫連線正常 (權重表紀錄: {count})")
    except Exception as e:
        st.sidebar.error(f"❌ 資料庫連線失敗: {e}")

def create_dummy_data(session):
    """生成一筆測試用的賽事數據 (先清理舊的避免重複)"""
    try:
        from scripts.test_phase3 import setup_dummy_race
        from database.models import Race, RaceEntry, ScoringFactor, RaceResult
        
        # 找到測試賽事
        test_races = session.query(Race).filter(Race.race_id.like("TEST-%")).all()
        for race in test_races:
            # 1. 找到所有關聯的 Entry IDs
            entry_ids = [e.id for e in race.entries]
            if entry_ids:
                # 2. 由下而上刪除所有關聯數據
                session.query(ScoringFactor).filter(ScoringFactor.entry_id.in_(entry_ids)).delete(synchronize_session=False)
                session.query(RaceResult).filter(RaceResult.entry_id.in_(entry_ids)).delete(synchronize_session=False)
                session.query(RaceEntry).filter(RaceEntry.race_id == race.id).delete(synchronize_session=False)
            # 3. 刪除賽事本身
            session.delete(race)
        
        session.commit()
        
        race_id = setup_dummy_race()
        engine = ScoringEngine(session)
        engine.score_race(race_id)
        st.sidebar.success("✅ 測試數據已重置並生成！")
        return True
    except Exception as e:
        session.rollback()
        st.sidebar.error(f"❌ 生成失敗: {e}")
        return False

def load_races(session: Session):
    """載入所有可選賽事 (日期由新到舊排序)"""
    return session.query(Race).order_by(Race.race_date.desc(), Race.race_no.asc()).all()

def get_db_status(session: Session):
    """獲取資料庫各表統計數量"""
    from database.models import Race, Horse, Jockey, Trainer, RaceEntry, ScoringFactor
    return {
        "賽事 (Races)": session.query(Race).count(),
        "馬匹 (Horses)": session.query(Horse).count(),
        "騎師 (Jockeys)": session.query(Jockey).count(),
        "練馬師 (Trainers)": session.query(Trainer).count(),
        "排位紀錄 (Entries)": session.query(RaceEntry).count(),
        "計分結果 (Scores)": session.query(ScoringFactor).count()
    }

def _get_member_presets(session: Session, email: str):
    e = str(email or "").strip().lower()
    if not e:
        return []
    key = f"member_weight_presets:{e}"
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if cfg and isinstance(cfg.value, list):
        out = []
        for item in cfg.value:
            if isinstance(item, dict) and item.get("name") and isinstance(item.get("weights"), dict):
                out.append(item)
        return out[:3]
    return []

def _save_member_presets(session: Session, email: str, presets: list):
    e = str(email or "").strip().lower()
    if not e:
        return
    key = f"member_weight_presets:{e}"
    cfg = session.query(SystemConfig).filter_by(key=key).first()
    if not cfg:
        cfg = SystemConfig(key=key, description="會員權重配置組合")
        session.add(cfg)
    cfg.value = presets[:3]
    session.commit()

def _predict_top4_for_race(session: Session, race_id: int, weight_map: dict):
    entries = session.query(RaceEntry).filter_by(race_id=race_id).all()
    if not entries:
        return []
    factor_names = list(weight_map.keys())
    scores = []
    for entry in entries:
        factor_scores = (
            session.query(ScoringFactor)
            .filter_by(entry_id=entry.id)
            .filter(ScoringFactor.factor_name.in_(factor_names))
            .all()
        )
        total = 0.0
        for f in factor_scores:
            total += float(f.score or 0.0) * float(weight_map.get(f.factor_name, 0.0))
        scores.append((entry.horse_no, total))
    scores.sort(key=lambda x: x[1], reverse=True)
    return [h for h, _ in scores[:4]]

def _hit_rate_stats(session: Session, weight_map: dict, max_races: int = 200):
    from database.models import RaceResult

    race_ids = (
        session.query(RaceEntry.race_id)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceResult.rank != None)
        .distinct()
        .all()
    )
    race_ids = [r[0] for r in race_ids]
    if not race_ids:
        return {"races": 0, "win": 0, "qin": 0, "tri": 0, "q4": 0}

    races = (
        session.query(Race)
        .filter(Race.id.in_(race_ids))
        .order_by(Race.race_date.desc(), Race.race_no.desc())
        .limit(max_races)
        .all()
    )

    win = qin = tri = q4 = 0
    used = 0
    for race in races:
        rows = (
            session.query(RaceEntry.horse_no, RaceResult.rank)
            .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
            .filter(RaceEntry.race_id == race.id)
            .filter(RaceResult.rank != None)
            .all()
        )
        rows = sorted(rows, key=lambda x: x[1])
        if len(rows) < 4:
            continue
        act = [rows[i][0] for i in range(4)]
        pred = _predict_top4_for_race(session, race.id, weight_map)
        if len(pred) < 4:
            continue
        used += 1
        if pred[0] == act[0]:
            win += 1
        if pred[:2] == act[:2]:
            qin += 1
        if pred[:3] == act[:3]:
            tri += 1
        if pred[:4] == act[:4]:
            q4 += 1

    return {"races": used, "win": win, "qin": qin, "tri": tri, "q4": q4}

def load_scoring_data(session: Session, race_id: int, weight_map: dict):
    entries = session.query(RaceEntry).filter_by(race_id=race_id).all()
    if not entries:
        return pd.DataFrame()

    factor_names = list(weight_map.keys())
    data = []
    for entry in entries:
        row = {
            "馬號": entry.horse_no,
            "馬名": entry.horse.name_ch if entry.horse else "未知",
            "馬匹編號": entry.horse.code if entry.horse else "",
            "排名": 0,
            "騎師": entry.jockey.name_ch if entry.jockey else "",
            "練馬師": entry.trainer.name_ch if entry.trainer else "",
            "檔位": entry.draw,
            "負磅": entry.actual_weight,
            "評分": entry.rating,
        }

        factor_scores = (
            session.query(ScoringFactor)
            .filter_by(entry_id=entry.id)
            .filter(ScoringFactor.factor_name.in_(factor_names))
            .all()
        )
        factor_map = {f.factor_name: float(f.score or 0.0) for f in factor_scores}
        total = 0.0
        for k, w in weight_map.items():
            total += float(factor_map.get(k, 0.0)) * float(w)
        row["總分"] = total
        for k, v in factor_map.items():
            row[k] = round(v, 1)
        data.append(row)

    df = pd.DataFrame(data)
    df = df.sort_values("總分", ascending=False).reset_index(drop=True)
    df["排名"] = range(1, len(df) + 1)
    df["預估勝率"] = (estimate_win_probability(df["總分"]) * 100).round(1).astype(str) + "%"
    return df

def main():
    st.title("🏇 HKJC 每場賽事獨立計分排名系統")
    st.markdown("---")

    session = get_db()

    if not st.session_state.get("is_superadmin", False) and not st.session_state.get("member_email"):
        wl = []
        cfg = session.query(SystemConfig).filter_by(key="member_whitelist_emails").first()
        if cfg and isinstance(cfg.value, list):
            wl = [str(x).strip().lower() for x in cfg.value if str(x).strip()]
        wl = list(dict.fromkeys(wl))

        st.subheader("🔐 會員登入")
        with st.form("member_login_form"):
            email = st.text_input("Email", value="", placeholder="name@example.com")
            submitted = st.form_submit_button("登入", type="primary")
            if submitted:
                e = str(email or "").strip().lower()
                if e and e in wl:
                    st.session_state["member_email"] = e
                    st.rerun()
                else:
                    st.error("❌ 未授權：請先在後台白名單加入此 Email。")
        st.stop()

    # Sidebar: 賽事選擇
    st.sidebar.header("🔍 賽事選擇")

    races = load_races(session)
    if not races:
        st.sidebar.warning("資料庫中尚無賽事數據，請先執行抓取與計分。")
        return

    # 提取所有可用的日期 (去重複並降序排列)
    # 將 datetime object 轉換為 date 來進行去重，避免因為時間部分不同而導致重複日期
    available_dates = sorted(list(set(r.race_date.date() if hasattr(r.race_date, 'date') else r.race_date for r in races)), reverse=True)
    
    # 將 datetime.date 陣列轉換回 datetime，以相容後面的比較
    from datetime import datetime
    available_datetimes = [datetime.combine(d, datetime.min.time()) for d in available_dates]
    
    # 1. 選擇日期 (日曆選擇器 Date Input)
    st.sidebar.markdown("📅 **選擇賽事日期**")
    selected_date_input = st.sidebar.date_input(
        "請選擇日期",
        value=available_dates[0] if available_dates else None,
        min_value=available_dates[-1] if available_dates else None,
        max_value=available_dates[0] if available_dates else None
    )
    
    # 檢查選擇的日期是否有賽事資料
    if selected_date_input not in available_dates:
        st.sidebar.error("❌ 該日期沒有賽事資料，請選擇日曆上有顏色的日期。")
        # 如果使用者選錯，自動退回最新有資料的一天
        selected_date_input = available_dates[0]
        
    selected_date_str = selected_date_input.strftime('%Y-%m-%d')
    selected_datetime = datetime.combine(selected_date_input, datetime.min.time())
    
    # 過濾出該日期的所有場次
    # 比較時只比對 date 部分
    races_on_date = [r for r in races if (r.race_date.date() if hasattr(r.race_date, 'date') else r.race_date) == selected_date_input]
    
    st.sidebar.markdown("🏁 **選擇場次**")

    if not races_on_date:
        st.sidebar.warning("該日期沒有場次資料。")
        return

    race_no_options = [r.race_no for r in races_on_date]
    race_no_to_id = {r.race_no: r.id for r in races_on_date}

    if "selected_race_no" not in st.session_state or st.session_state.selected_race_no not in race_no_options:
        st.session_state.selected_race_no = race_no_options[0]

    selected_race_no = st.sidebar.selectbox(
        "場次",
        race_no_options,
        index=race_no_options.index(st.session_state.selected_race_no),
        label_visibility="collapsed",
    )
    st.session_state.selected_race_no = selected_race_no
    selected_race_id = race_no_to_id[selected_race_no]

    # Sidebar: 權重動態調整 (可折疊)
    with st.sidebar.expander("⚙️ 權重配置 (動態調整)"):
        weights = (
            session.query(ScoringWeight)
            .filter(ScoringWeight.is_active == True)
            .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
            .all()
        )
        base_weight_map = {w.factor_name: float(w.weight) for w in weights}
        if "active_weight_map" not in st.session_state:
            st.session_state["active_weight_map"] = dict(base_weight_map)

        member_email = st.session_state.get("member_email")
        presets = _get_member_presets(session, member_email) if member_email else []
        preset_names = ["（手動調整）"] + [p["name"] for p in presets]
        if "selected_preset_name" not in st.session_state:
            st.session_state["selected_preset_name"] = preset_names[0]

        selected_preset_name = st.selectbox(
            "已儲存組合",
            preset_names,
            index=preset_names.index(st.session_state["selected_preset_name"]) if st.session_state["selected_preset_name"] in preset_names else 0,
        )
        if selected_preset_name != st.session_state["selected_preset_name"]:
            st.session_state["selected_preset_name"] = selected_preset_name
            if selected_preset_name != "（手動調整）":
                p = next((x for x in presets if x["name"] == selected_preset_name), None)
                if p:
                    new_map = dict(base_weight_map)
                    for k, v in p.get("weights", {}).items():
                        if k in new_map:
                            try:
                                new_map[k] = float(v)
                            except Exception:
                                pass
                    st.session_state["active_weight_map"] = new_map
                    for k, v in new_map.items():
                        st.session_state[f"weight_{k}"] = float(v)
            st.rerun()

        updated_weights = {}
        for w in weights:
            key = f"weight_{w.factor_name}"
            default_val = st.session_state.get(key, float(st.session_state["active_weight_map"].get(w.factor_name, w.weight)))
            updated_weights[w.factor_name] = st.slider(
                f"{w.description}",
                0.0,
                5.0,
                float(default_val),
                0.1,
                key=key,
            )

        st.session_state["active_weight_map"] = dict(updated_weights)

        if member_email:
            st.markdown("**儲存/編輯組合（每位會員最多 3 個）**")
            with st.form("preset_save_form"):
                name = st.text_input("組合名稱", value="", placeholder="例如：穩健型 / 追熱型")
                action = st.selectbox("操作", ["另存新組合", "更新目前組合", "刪除目前組合"])
                submitted = st.form_submit_button("執行", type="primary")
                if submitted:
                    n = str(name or "").strip()
                    now = datetime.now().isoformat()
                    if action == "另存新組合":
                        if not n:
                            st.error("❌ 請輸入組合名稱")
                        elif any(p["name"] == n for p in presets):
                            st.error("❌ 組合名稱已存在")
                        elif len(presets) >= 3:
                            st.error("❌ 已達上限（最多 3 個組合）")
                        else:
                            presets.append({"name": n, "weights": dict(updated_weights), "updated_at": now})
                            _save_member_presets(session, member_email, presets)
                            st.session_state["selected_preset_name"] = n
                            st.rerun()
                    elif action == "更新目前組合":
                        if st.session_state["selected_preset_name"] == "（手動調整）":
                            st.error("❌ 請先選擇要更新的已儲存組合")
                        else:
                            target = st.session_state["selected_preset_name"]
                            for p in presets:
                                if p["name"] == target:
                                    p["weights"] = dict(updated_weights)
                                    p["updated_at"] = now
                            _save_member_presets(session, member_email, presets)
                            st.success("✅ 已更新")
                            st.rerun()
                    else:
                        if st.session_state["selected_preset_name"] == "（手動調整）":
                            st.error("❌ 請先選擇要刪除的已儲存組合")
                        else:
                            target = st.session_state["selected_preset_name"]
                            presets = [p for p in presets if p["name"] != target]
                            _save_member_presets(session, member_email, presets)
                            st.session_state["selected_preset_name"] = "（手動調整）"
                            st.rerun()

    # 主面板：賽事資訊
    race = session.query(Race).get(selected_race_id)
    st.subheader(f"📊 賽事詳情: {selected_date_str} | 第 {race.race_no} 場")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("跑道資訊", race.track_type if race.track_type else race.venue)
    col2.metric("班次", race.race_class or "N/A")
    col3.metric("路程", f"{race.distance}m" if race.distance else "N/A")
    col4.metric("場地狀況", race.going or "未知")

    # 數據加載與顯示
    weight_map = st.session_state.get("active_weight_map", {})
    df = load_scoring_data(session, selected_race_id, weight_map)
    if df.empty:
        st.info("本場賽事尚未進行計分運算，請先於「數據管理後台」執行抓取與計分。")
        
    if not df.empty:
        member_email = st.session_state.get("member_email")
        if member_email:
            presets = _get_member_presets(session, member_email)
            if presets:
                st.markdown("### 📌 已儲存權重配置組合")
                rows = []
                for p in presets:
                    stats = _hit_rate_stats(session, p.get("weights", {}), max_races=200)
                    races_n = stats["races"]
                    rows.append(
                        {
                            "組合": p["name"],
                            "樣本(場)": races_n,
                            "獨贏命中%": round((stats["win"] / races_n * 100.0), 1) if races_n else 0.0,
                            "正Q命中%": round((stats["qin"] / races_n * 100.0), 1) if races_n else 0.0,
                            "三重彩命中%": round((stats["tri"] / races_n * 100.0), 1) if races_n else 0.0,
                            "四重彩命中%": round((stats["q4"] / races_n * 100.0), 1) if races_n else 0.0,
                        }
                    )
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                with st.expander("🔖 本場各組合 Top4 預測", expanded=False):
                    pr = []
                    for p in presets:
                        top4 = _predict_top4_for_race(session, selected_race_id, p.get("weights", {}))
                        pr.append(
                            {
                                "組合": p["name"],
                                "Top1": top4[0] if len(top4) > 0 else "",
                                "Top2": top4[1] if len(top4) > 1 else "",
                                "Top3": top4[2] if len(top4) > 2 else "",
                                "Top4": top4[3] if len(top4) > 3 else "",
                            }
                        )
                    st.dataframe(pd.DataFrame(pr), use_container_width=True, hide_index=True)

        with st.expander("ℹ️ 專業排名表計算邏輯", expanded=False):
            st.markdown("""
            - 每個計分條件會先在同一場內獨立標準化成 0–10 分（分數越高越有利）。
            - 總分 = Σ（條件分數 × 權重）。
            - 預估勝率：以總分做 softmax 正規化，只作相對參考。
            """)

        with st.expander("📚 各條件功能說明", expanded=False):
            weights_list = (
                session.query(ScoringWeight)
                .filter(ScoringWeight.is_active == True)
                .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
                .all()
            )
            items = [{"條件": w.description, "代號": w.factor_name} for w in weights_list]
            st.dataframe(pd.DataFrame(items), use_container_width=True, hide_index=True)

        # 專業排名表格
        st.markdown("### 🏆 專業排名表")
        
        # 定義顯示列與格式化
        display_cols = ["排名", "馬號", "馬名", "總分", "預估勝率", "建議"]
        
        # 根據總分與勝率給出建議
        def get_recommendation(row):
            if row["排名"] == 1: return "🔥 首選 (Top Pick)"
            if row["排名"] == 2: return "🥈 次選 (Second)"
            if row["排名"] == 3: return "🥉 穩健 (Solid)"
            if float(row["預估勝率"].strip('%')) > 15: return "💰 價值 (Value)"
            return "-"
        
        df["建議"] = df.apply(get_recommendation, axis=1)
        
        # 顏色標記與樣式
        def style_ranking(row):
            if row["排名"] == 1: return ['background-color: #ffeb3b'] * len(row)
            return [''] * len(row)

        st.dataframe(
            df[display_cols + ["騎師", "練馬師", "檔位", "負磅", "評分"]],
            use_container_width=True,
            hide_index=True
        )

        # 詳細得分雷達圖或條形圖
        st.markdown("---")
        st.markdown("### 🔍 深度因子分析 (Top 3 馬匹)")
        
        # 獲取所有因子列
        factor_cols = [c for c in df.columns if c in updated_weights.keys()]
        top_3_df = df.head(3)
        
        import plotly.graph_objects as go
        fig = go.Figure()
        
        for _, row in top_3_df.iterrows():
            fig.add_trace(go.Scatterpolar(
                r=[row[c] for c in factor_cols],
                theta=[session.query(ScoringWeight).filter_by(factor_name=c).first().description for c in factor_cols],
                fill='toself',
                name=f"({row['馬號']}) {row['馬名']}"
            ))

        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 10])),
            showlegend=True,
            title="前三名馬匹戰力雷達圖 (各維度 0-10 分)"
        )
        st.plotly_chart(fig, use_container_width=True)

    session.close()

if __name__ == "__main__":
    main()
