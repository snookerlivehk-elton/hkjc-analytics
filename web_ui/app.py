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
from database.models import Race, RaceEntry, ScoringFactor, ScoringWeight, Horse, SystemConfig, RaceResult, RaceDividend
from scoring_engine.core import ScoringEngine
from scoring_engine.constants import DISABLED_FACTORS
from scoring_engine.utils import estimate_win_probability
from scoring_engine.member_stats import update_member_preset_stats_incremental, load_member_preset_stats, delete_member_preset_stats, STATS_START_DATE, STATS_WINDOW_DAYS
from web_ui.ui_table import render_dividends
from utils.logger import logger
import asyncio
import subprocess
from datetime import datetime

# 設定頁面配置
st.set_page_config(page_title="HKJC 每場賽事獨立計分排名系統", page_icon="🏇", layout="wide")

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

def _predict_topk_for_race(session: Session, race_id: int, weight_map: dict, k: int):
    weights = {k: float(v) for k, v in (weight_map or {}).items()}
    if not weights or k <= 0:
        return []

    entries = session.query(RaceEntry.id, RaceEntry.horse_no).filter_by(race_id=race_id).all()
    if not entries:
        return []
    entry_ids = [e[0] for e in entries]
    entry_id_to_no = {e[0]: int(e[1]) for e in entries}

    factors = (
        session.query(ScoringFactor.entry_id, ScoringFactor.factor_name, ScoringFactor.score)
        .filter(ScoringFactor.entry_id.in_(entry_ids))
        .filter(ScoringFactor.factor_name.in_(list(weights.keys())))
        .all()
    )

    totals = {eid: 0.0 for eid in entry_ids}
    for entry_id, factor_name, score in factors:
        totals[int(entry_id)] += float(score or 0.0) * float(weights.get(factor_name, 0.0))

    ranked = sorted(totals.items(), key=lambda x: x[1], reverse=True)
    return [entry_id_to_no[eid] for eid, _ in ranked[:k] if eid in entry_id_to_no]


def _predict_top4_for_race(session: Session, race_id: int, weight_map: dict):
    return _predict_topk_for_race(session, race_id, weight_map, 4)

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

    cols = st.sidebar.columns(min(6, max(1, len(race_no_options))))
    for i, rn in enumerate(race_no_options):
        col = cols[i % len(cols)]
        label = f"{rn}"
        if col.button(label, key=f"race_btn_{selected_date_str}_{rn}", use_container_width=True):
            st.session_state.selected_race_no = rn
            st.rerun()

    selected_race_id = race_no_to_id[st.session_state.selected_race_no]

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
                    pending = {
                        "action": action,
                        "name": n,
                        "selected": st.session_state.get("selected_preset_name"),
                        "weights": dict(updated_weights),
                        "ts": now,
                    }
                    st.session_state["pending_preset_op"] = pending
                    st.rerun()

            pending = st.session_state.get("pending_preset_op")
            if isinstance(pending, dict) and pending.get("action"):
                act = pending.get("action")
                n = str(pending.get("name") or "").strip()
                selected = str(pending.get("selected") or "")
                wmap = pending.get("weights") if isinstance(pending.get("weights"), dict) else {}

                st.markdown("---")
                st.markdown("**二次確認**")

                if act in ("另存新組合", "更新目前組合"):
                    total_w = sum(float(v) for v in wmap.values()) if wmap else 0.0
                    weights_lookup = {w.factor_name: w.description for w in weights}
                    rows = []
                    for k, v in wmap.items():
                        if k in weights_lookup:
                            share = (float(v) / total_w * 100.0) if total_w > 0 else 0.0
                            rows.append({"條件": weights_lookup[k], "權重": round(float(v), 2), "佔比%": round(share, 1)})
                    rows = sorted(rows, key=lambda x: x["佔比%"], reverse=True)
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                c1, c2 = st.columns(2)
                confirm = c1.button("確認儲存", type="primary", use_container_width=True)
                cancel = c2.button("取消", use_container_width=True)

                if cancel:
                    st.session_state.pop("pending_preset_op", None)
                    st.rerun()

                if confirm:
                    if act == "另存新組合":
                        if not n:
                            st.error("❌ 請輸入組合名稱")
                        elif any(p["name"] == n for p in presets):
                            st.error("❌ 組合名稱已存在")
                        elif len(presets) >= 3:
                            st.error("❌ 已達上限（最多 3 個組合）")
                        else:
                            presets.append({"name": n, "weights": dict(wmap), "updated_at": pending.get("ts")})
                            _save_member_presets(session, member_email, presets)
                            st.session_state["selected_preset_name"] = n
                            st.session_state.pop("pending_preset_op", None)
                            st.rerun()
                    elif act == "更新目前組合":
                        if selected == "（手動調整）":
                            st.error("❌ 請先選擇要更新的已儲存組合")
                        else:
                            for p in presets:
                                if p["name"] == selected:
                                    p["weights"] = dict(wmap)
                                    p["updated_at"] = pending.get("ts")
                            _save_member_presets(session, member_email, presets)
                            st.session_state.pop("pending_preset_op", None)
                            st.success("✅ 已更新")
                            st.rerun()
                    else:
                        if selected == "（手動調整）":
                            st.error("❌ 請先選擇要刪除的已儲存組合")
                        else:
                            presets2 = [p for p in presets if p["name"] != selected]
                            _save_member_presets(session, member_email, presets2)
                            delete_member_preset_stats(session, member_email, selected)
                            st.session_state["selected_preset_name"] = "（手動調整）"
                            st.session_state.pop("pending_preset_op", None)
                            st.rerun()

    # 主面板：賽事資訊
    race = session.query(Race).get(selected_race_id)
    st.subheader(f"📊 賽事詳情: {selected_date_str} | 第 {race.race_no} 場")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("跑道資訊", race.track_type if race.track_type else race.venue)
    col2.metric("班次", race.race_class or "N/A")
    col3.metric("路程", f"{race.distance}m" if race.distance else "N/A")
    going = str(race.going or "").strip()
    if going in ("草地", "泥地", "全天候", "TURF", "AW", "A/W", "ALL WEATHER", ""):
        going_display = "N/A"
    else:
        going_display = going
    col4.metric("場地狀況", going_display)

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
                stats_map = update_member_preset_stats_incremental(session, member_email, presets, per_preset_max_new_races=30)
                with st.expander("📌 已儲存權重配置組合", expanded=False):
                    rows = []
                    for p in presets:
                        stt = stats_map.get(p["name"], {}) if isinstance(stats_map, dict) else {}
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
                                "組合": p["name"],
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
                            }
                        )
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
                    with st.expander("📌 命中率統計口徑", expanded=False):
                        st.markdown(f"""
                        - 統計起始：{STATS_START_DATE.date().isoformat()}（之前忽略）
                        - 統計窗口：最近 {STATS_WINDOW_DAYS} 天（若起始日更近，則以起始日為準）
                        - 命中定義：以模型 Top5/Top4/Top3/Top2 預測與賽果名次比較：
                          - WIN：預測首2位包含冠軍
                          - P：預測首3位包含三甲中任意一隻
                          - Q1：預測首2位包含冠軍 且 預測首3位包含亞軍
                          - PQ：預測首3位命中三甲其中兩隻或以上
                          - T3E：預測首2位包含冠軍 且 預測首4位包含亞軍+季軍
                          - T3：預測首4位包含三甲全部馬匹
                          - F4：預測首2位包含冠軍 且 預測首5位包含2-4名
                          - F4Q：預測首5位包含四甲全部馬匹
                          - B5W：預測首5位包含冠軍
                          - B5P：預測首5位包含三甲中任意一隻
                        """)

                    st.markdown("### 🧾 分享字段（會員組合 Top5）")
                    from sqlalchemy import func
                    from database.models import PredictionTop5
                    import json

                    drows = (
                        session.query(func.date(PredictionTop5.race_date))
                        .filter(PredictionTop5.predictor_type == "preset")
                        .filter(PredictionTop5.member_email == str(member_email).strip().lower())
                        .distinct()
                        .order_by(func.date(PredictionTop5.race_date).desc())
                        .limit(180)
                        .all()
                    )
                    available_dates = [r[0] for r in drows if r and r[0]]
                    preset_names = [str(p.get("name", "")).strip() for p in (presets or []) if str(p.get("name", "")).strip()]

                    if not available_dates:
                        st.info("目前未有任何會員組合 Top5 快照可供分享。")
                    elif not preset_names:
                        st.info("未找到任何已儲存權重組合。")
                    else:
                        c1, c2, c3 = st.columns([2, 4, 2])
                        share_date = c1.selectbox(
                            "賽日",
                            available_dates,
                            index=0,
                            format_func=lambda x: x.isoformat() if hasattr(x, "isoformat") else str(x),
                            key="member_preset_share_date",
                        )
                        share_preset = c2.selectbox(
                            "組合名稱",
                            preset_names,
                            index=0,
                            key="member_preset_share_name",
                        )

                        if c3.button("生成分享字段", use_container_width=True, key="member_preset_share_btn"):
                            rows2 = (
                                session.query(PredictionTop5.race_no, PredictionTop5.top5)
                                .filter(PredictionTop5.predictor_type == "preset")
                                .filter(PredictionTop5.member_email == str(member_email).strip().lower())
                                .filter(PredictionTop5.predictor_key == str(share_preset))
                                .filter(func.date(PredictionTop5.race_date) == share_date.isoformat())
                                .order_by(PredictionTop5.race_no.asc())
                                .all()
                            )
                            races = []
                            for rn, top5 in rows2:
                                races.append(
                                    {
                                        "race_no": int(rn or 0),
                                        "top5": [int(x) for x in (top5 or []) if str(x).strip().isdigit()],
                                    }
                                )
                            races.sort(key=lambda x: x["race_no"])

                            if not races:
                                st.info("該賽日未找到此會員組合的 Top5 快照。")
                            else:
                                preset_weights = None
                                for p in presets:
                                    if str(p.get("name", "")).strip() == str(share_preset):
                                        preset_weights = p.get("weights")
                                        break

                                payload = {
                                    "race_date": share_date.isoformat(),
                                    "member_email": str(member_email).strip().lower(),
                                    "preset_name": str(share_preset),
                                    "preset_weights": preset_weights,
                                    "races": races,
                                }
                                txt_lines = [
                                    f"會員：{str(member_email).strip().lower()}",
                                    f"組合：{share_preset}",
                                    f"賽日：{share_date.isoformat()}",
                                ]
                                for r in races:
                                    top5_s = ",".join(str(x) for x in (r.get("top5") or [])[:5])
                                    txt_lines.append(f"第{int(r.get('race_no') or 0)}場：{top5_s}")
                                txt = "\n".join(txt_lines) + "\n"

                                st.code(txt, language="text")
                                st.download_button(
                                    "下載 TXT",
                                    data=txt.encode("utf-8"),
                                    file_name=f"preset_top5_{str(member_email).strip().lower()}_{share_preset}_{share_date.isoformat()}.txt",
                                    mime="text/plain",
                                    use_container_width=False,
                                    key="member_preset_share_txt",
                                )
                                st.download_button(
                                    "下載 JSON",
                                    data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
                                    file_name=f"preset_top5_{str(member_email).strip().lower()}_{share_preset}_{share_date.isoformat()}.json",
                                    mime="application/json",
                                    use_container_width=False,
                                    key="member_preset_share_json",
                                )

                with st.expander("🔖 本場各組合 Top5 預測", expanded=False):
                    pr = []
                    active_name = st.session_state.get("selected_preset_name", "（手動調整）")
                    active_weights = st.session_state.get("active_weight_map", {})
                    active_top5 = _predict_topk_for_race(session, selected_race_id, active_weights, 5)
                    pr.append(
                        {
                            "組合": f"目前頁面：{active_name}",
                            "Top1": active_top5[0] if len(active_top5) > 0 else "",
                            "Top2": active_top5[1] if len(active_top5) > 1 else "",
                            "Top3": active_top5[2] if len(active_top5) > 2 else "",
                            "Top4": active_top5[3] if len(active_top5) > 3 else "",
                            "Top5": active_top5[4] if len(active_top5) > 4 else "",
                        }
                    )
                    for p in presets:
                        top5 = _predict_topk_for_race(session, selected_race_id, p.get("weights", {}), 5)
                        pr.append(
                            {
                                "組合": p["name"],
                                "Top1": top5[0] if len(top5) > 0 else "",
                                "Top2": top5[1] if len(top5) > 1 else "",
                                "Top3": top5[2] if len(top5) > 2 else "",
                                "Top4": top5[3] if len(top5) > 3 else "",
                                "Top5": top5[4] if len(top5) > 4 else "",
                            }
                        )
                    st.dataframe(pd.DataFrame(pr), use_container_width=True, hide_index=True)

        if member_email:
            with st.expander("📈 各獨立條件命中統計", expanded=False):
                from datetime import date, timedelta
                from sqlalchemy import func
                from database.models import PredictionTop5
                from scoring_engine.member_stats import _calc_hits

                factors = (
                    session.query(ScoringWeight.factor_name, ScoringWeight.description)
                    .filter(ScoringWeight.is_active == True)
                    .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
                    .order_by(ScoringWeight.factor_name.asc())
                    .all()
                )
                factor_desc = {str(fn): str(desc or fn) for fn, desc in factors}
                factor_names = list(factor_desc.keys())

                drows = (
                    session.query(func.date(PredictionTop5.race_date))
                    .filter(PredictionTop5.predictor_type == "factor")
                    .distinct()
                    .order_by(func.date(PredictionTop5.race_date).desc())
                    .limit(90)
                    .all()
                )
                available_dates = [r[0] for r in drows if r and r[0]]
                if not available_dates:
                    st.info("目前未有任何獨立條件 Top5 快照。")
                else:
                    end_default = available_dates[0]
                    start_default = max(end_default - timedelta(days=30), min(available_dates))
                    range_key = "member_factor_hit_range"
                    if range_key not in st.session_state:
                        st.session_state[range_key] = (start_default, end_default)

                    b1, b2, b3, b4 = st.columns(4)
                    if b1.button("前30日", use_container_width=True):
                        st.session_state[range_key] = (max(end_default - timedelta(days=30), min(available_dates)), end_default)
                        st.rerun()
                    if b2.button("前60日", use_container_width=True):
                        st.session_state[range_key] = (max(end_default - timedelta(days=60), min(available_dates)), end_default)
                        st.rerun()
                    if b3.button("前180日", use_container_width=True):
                        st.session_state[range_key] = (max(end_default - timedelta(days=180), min(available_dates)), end_default)
                        st.rerun()
                    if b4.button("最長日子", use_container_width=True):
                        st.session_state[range_key] = (min(available_dates), end_default)
                        st.rerun()

                    d1, d2 = st.date_input("統計日期範圍", value=st.session_state[range_key], key=range_key)
                    if isinstance(d1, date) and isinstance(d2, date) and d1 > d2:
                        d1, d2 = d2, d1

                    preds = (
                        session.query(
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
                        def actual_top5(race_id: int):
                            rows = (
                                session.query(RaceEntry.horse_no, RaceResult.rank)
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

        active_name = st.session_state.get("selected_preset_name", "（手動調整）")
        with st.expander(f"🏆 專業排名表（目前權重：{active_name}）", expanded=True):
            display_cols = ["排名", "馬號", "馬名", "總分", "預估勝率", "建議"]

            def get_recommendation(row):
                if row["排名"] == 1:
                    return "🔥 首選 (Top Pick)"
                if row["排名"] == 2:
                    return "🥈 次選 (Second)"
                if row["排名"] == 3:
                    return "🥉 穩健 (Solid)"
                if float(row["預估勝率"].strip("%")) > 15:
                    return "💰 價值 (Value)"
                return "-"

            df["建議"] = df.apply(get_recommendation, axis=1)

            res_rows = (
                session.query(RaceEntry.horse_no, RaceResult.rank)
                .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
                .filter(RaceEntry.race_id == selected_race_id)
                .filter(RaceResult.rank != None)
                .all()
            )
            rank_map = {int(h): int(r) for h, r in res_rows if h is not None and r is not None}
            df_display = df[display_cols + ["騎師", "練馬師", "檔位", "負磅", "評分"]].copy()
            df_display.insert(0, "賽果", df_display["馬號"].apply(lambda x: rank_map.get(int(x), "")))
            st.dataframe(df_display, use_container_width=True, hide_index=True)

            with st.expander("ℹ️ 專業排名表計算邏輯", expanded=False):
                st.markdown("""
                - 每個計分條件會先在同一場內獨立標準化成 0–10 分（分數越高越有利）。
                - 總分 = Σ（條件分數 × 權重）。
                - 預估勝率：以總分做 softmax 正規化，只作相對參考。
                """)

            with st.expander("🧠 演算法說明", expanded=False):
                st.markdown("""
                - 系統會在每場把多個「獨立條件」轉成分數，並按權重合成總分排序。
                - 獨立條件（factor）：單一條件各自產生 Top5，用於做「條件本身」準確度統計。
                - 會員組合（preset）：多條件按會員儲存權重加權後產生 Top5，用於做「組合表現」統計。
                - Top5 會在排位爬取後生成快照；賽果入庫後會結算命中（WIN/P/Q1/PQ/T3E/T3/F4/F4Q/B5W/B5P）。
                """)

                with st.expander("📚 各條件計算邏輯", expanded=False):
                    weights_list = (
                        session.query(ScoringWeight)
                        .filter(ScoringWeight.is_active == True)
                        .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
                        .all()
                    )
                    logic = {
                        "jockey_trainer_bond": "計算騎師×練馬師的歷史合作勝/上名率（全庫＋本駒），按可調權重合併，得到原始分後同場標準化。",
                        "horse_time_perf": "以同路程歷史最佳完成時間作速度指標（track_type→草/泥→同程 fallback）；時間越短越好，加入樣本/可信度降權後同場標準化。",
                        "venue_dist_specialty": "以同跑道資訊＋同路程的勝/上名率計分，可選半衰期時間衰減，並加入樣本可信度降權後同場標準化。",
                        "draw_stats": "用當日官方檔位統計（勝率/上名率）計算相對強度後同場標準化。",
                        "weight_rating_perf": "以同程勝仗可贏評分差＋同程上名率（可衰減）合成 raw，再同場標準化。",
                        "class_performance": "現階段以降班訊號為主（例如 3→4/4→5），再同場標準化。",
                        "recent_form": "取最近 6 仗有效名次，按時間權重加權平均後轉為 raw，再同場標準化。",
                        "debut_long_rest": "本場若屬長休復出（可調門檻），回看歷史長休復出賽的勝/入位並疊加加分，再同場標準化。",
                    }
                    for w in weights_list:
                        st.markdown(f"**{w.description}**")
                        st.markdown(f"- {logic.get(w.factor_name, '（待補充）')}")

        div = session.query(RaceDividend).filter_by(race_id=selected_race_id).first()
        has_div = bool(div and isinstance(div.dividends, list) and div.dividends)
        if rank_map or has_div:
            with st.expander("🏁 賽果與派彩", expanded=False):
                if rank_map:
                    top4 = sorted(rank_map.items(), key=lambda kv: kv[1])[:4]
                    top4_str = " / ".join([f"{rk}名: {hn}" for hn, rk in top4])
                    st.markdown(f"**賽果 Top4**：{top4_str}")

                if has_div:
                    meta = div.meta if isinstance(div.meta, dict) else {}
                    going = str(meta.get("going") or "").strip()
                    track = str(meta.get("track") or "").strip()
                    race_time = str(meta.get("race_time") or "").strip()
                    sectional = meta.get("sectional_times") if isinstance(meta.get("sectional_times"), list) else []
                    sectional_str = " / ".join([f"{x:.2f}" for x in sectional if isinstance(x, (int, float))]) if sectional else ""

                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("場地狀況", going or "未知")
                    m2.metric("賽道", track or "未知")
                    m3.metric("全場時間", race_time or "未知")
                    m4.metric("分段時間", sectional_str or "未知")

                    render_dividends(div.dividends, key=f"div_{selected_race_id}")
                else:
                    st.info("本場尚未有派彩資料。")
    session.close()


if __name__ == "__main__":
    main()
