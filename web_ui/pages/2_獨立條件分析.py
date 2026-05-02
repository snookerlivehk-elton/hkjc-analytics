import streamlit as st
import pandas as pd
import sys
from pathlib import Path
from sqlalchemy.orm import Session

# 加入專案路徑
root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import get_session, init_db
from database.models import Race, RaceEntry, ScoringFactor, ScoringWeight
from scoring_engine.constants import DISABLED_FACTORS
from web_ui.auth import require_superadmin
from web_ui.nav import render_admin_nav

st.set_page_config(page_title="獨立條件分析 - HKJC Analytics", page_icon="📊", layout="wide")

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

require_superadmin("📊 獨立條件分析")

render_admin_nav()

def load_races(session: Session):
    return session.query(Race).order_by(Race.race_date.desc(), Race.race_no.asc()).all()

def load_factor_data(session: Session, race_id: int):
    entries = session.query(RaceEntry).filter_by(race_id=race_id).all()
    if not entries:
        return None
        
    weights = (
        session.query(ScoringWeight)
        .filter(ScoringWeight.is_active == True)
        .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
        .all()
    )
    factor_desc_map = {w.factor_name: w.description for w in weights}
    factor_weight_map = {w.factor_name: float(w.weight) if w.weight is not None else 0.0 for w in weights}
    
    data = []
    for entry in entries:
        total_calc = 0.0
        row = {
            "馬號": entry.horse_no,
            "馬名": entry.horse.name_ch if entry.horse else "未知",
            "檔位": entry.draw,
            "負磅": entry.actual_weight,
            "評分": entry.rating,
            "總分(落庫)": round(float(entry.total_score), 2) if entry.total_score is not None else None,
        }
        
        factors = (
            session.query(ScoringFactor)
            .filter_by(entry_id=entry.id)
            .filter(ScoringFactor.factor_name.in_(list(factor_desc_map.keys())))
            .all()
        )
        for f in factors:
            # 儲存分數，使用中文描述作為欄位名
            desc = factor_desc_map.get(f.factor_name, f.factor_name)
            row[desc] = round(f.score, 2)
            row[f"{desc}_raw"] = f.raw_data_display if f.raw_data_display else "無數據"
            total_calc += float(f.score or 0.0) * float(factor_weight_map.get(f.factor_name, 0.0))
            
        row["總分(全局權重)"] = round(float(total_calc), 2)
        data.append(row)
        
    return pd.DataFrame(data), list(factor_desc_map.values())

st.title("📊 獨立條件分析")
st.markdown("在此頁面檢視每場賽事各計分條件的獨立運算結果與排名。")

session = get_session()

# Sidebar: 賽事選擇
st.sidebar.header("🔍 賽事選擇")
races = load_races(session)

if not races:
    st.warning("資料庫中尚無賽事數據，請先執行抓取。")
else:
    # 提取所有可用的日期 (去重複並降序排列)
    # 將 datetime object 轉換為 date 來進行去重，避免因為時間部分不同而導致重複日期
    available_dates = sorted(list(set(r.race_date.date() if hasattr(r.race_date, 'date') else r.race_date for r in races)), reverse=True)
    
    from datetime import datetime
    
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
        selected_date_input = available_dates[0]
        
    selected_date_str = selected_date_input.strftime('%Y-%m-%d')
    
    # 過濾出該日期的所有場次
    races_on_date = [r for r in races if (r.race_date.date() if hasattr(r.race_date, 'date') else r.race_date) == selected_date_input]
    
    # 2. 選擇場次 (按鈕陣列)
    st.sidebar.markdown("🏁 **選擇場次**")
    
    # 初始化 session_state 以記憶當前選中的場次 ID
    if 'factor_selected_race_id' not in st.session_state or st.session_state.factor_selected_race_id not in [r.id for r in races_on_date]:
        st.session_state.factor_selected_race_id = races_on_date[0].id
        
    # 使用 columns 建立場次按鈕網格 (每行 5 個按鈕)
    cols_per_row = 5
    for i in range(0, len(races_on_date), cols_per_row):
        cols = st.sidebar.columns(cols_per_row)
        for j in range(cols_per_row):
            if i + j < len(races_on_date):
                r = races_on_date[i + j]
                btn_label = str(r.race_no)
                # 當前選中的場次使用 primary 顏色
                btn_type = "primary" if st.session_state.factor_selected_race_id == r.id else "secondary"
                
                if cols[j].button(btn_label, key=f"factor_race_btn_{r.id}", type=btn_type, use_container_width=True):
                    st.session_state.factor_selected_race_id = r.id
                    st.rerun()

    selected_race_id = st.session_state.factor_selected_race_id

    result = load_factor_data(session, selected_race_id)
    
    if not result or result[0].empty:
        st.warning("⚠️ 本場尚未計分，各條件分數均無法計算。")
        st.info("👉 請到左側導航「🔧 數據管理」頁，選擇同日期並點擊 **⚡ 一鍵完整更新**（抓排位→回填→計分→生成Top5）後再回本頁。")
    else:
        df, factor_columns = result
        
        # 創建 Tabs 顯示不同的視圖
        tab1, tab2, tab3 = st.tabs(["🗂️ 獨立條件分頁檢視", "📋 全局數據總覽", "🤖 AI 賽事前瞻 (FormGuide)"])
        
        with tab1:
            st.markdown("### 各條件獨立排名")
            st.markdown("選擇不同的計分條件，查看該條件下各匹馬的得分與排名（分數由高至低排列，最高 10 分）。")
            
            # 過濾出 DataFrame 中實際存在的因子欄位
            available_factors = [col for col in factor_columns if col in df.columns]
            
            if available_factors:
                st.markdown("#### 🔍 請選擇要檢視的計分條件：")
                
                # 初始化 session_state 以記憶當前選中的因子
                if 'selected_factor' not in st.session_state:
                    st.session_state.selected_factor = available_factors[0]
                if st.session_state.selected_factor not in available_factors:
                    st.session_state.selected_factor = available_factors[0]
                
                # 使用 columns 建立按鈕網格 (每行 4 個按鈕)
                cols_per_row = 4
                for i in range(0, len(available_factors), cols_per_row):
                    cols = st.columns(cols_per_row)
                    for j in range(cols_per_row):
                        if i + j < len(available_factors):
                            factor = available_factors[i + j]
                            # 如果是當前選中的因子，使用 primary 顏色標示
                            button_type = "primary" if st.session_state.selected_factor == factor else "secondary"
                            if cols[j].button(factor, key=f"btn_{factor}", type=button_type, use_container_width=True):
                                st.session_state.selected_factor = factor
                                st.rerun()

                selected_factor = st.session_state.selected_factor
                st.markdown("---")
                st.markdown(f"#### 📌 目前檢視：{selected_factor}")

                if selected_factor in ("騎師＋練馬師合作 (同路程/場地)", "騎師＋練馬師合作 (不論馬匹)", "騎師＋練馬師合作 (綜合)"):
                    from database.models import SystemConfig, RaceEntry, ScoringFactor
                    
                    sample = (
                        session.query(ScoringFactor.raw_data_display)
                        .join(RaceEntry, RaceEntry.id == ScoringFactor.entry_id)
                        .filter(
                            RaceEntry.race_id == selected_race_id,
                            ScoringFactor.factor_name == "jockey_trainer_bond",
                        )
                        .first()
                    )
                    
                    needs_rescore = False
                    if not sample or not sample[0]:
                        needs_rescore = True
                    elif " | " not in sample[0] and ("全庫不足" not in sample[0] and "全(" not in sample[0]):
                        needs_rescore = True
                        
                    auto_key = f"auto_rescore_jt_bond_combined_{selected_race_id}"
                    if needs_rescore and not st.session_state.get(auto_key, False):
                        st.session_state[auto_key] = True
                        from scoring_engine.core import ScoringEngine
                        engine = ScoringEngine(session)
                        engine.score_race(selected_race_id)
                        st.rerun()

                if selected_factor == "檔位偏差 (官方 Draw Statistics)":
                    from database.models import SystemConfig

                    race = session.get(Race, selected_race_id)
                    race_date_str = ""
                    if race and hasattr(race.race_date, "strftime"):
                        race_date_str = race.race_date.strftime("%Y/%m/%d")
                    elif race:
                        race_date_str = str(race.race_date)[:10].replace("-", "/")

                    config_key = f"draw_stats_{race_date_str}" if race_date_str else ""
                    config = session.query(SystemConfig).filter_by(key=config_key).first() if config_key else None
                    has_official = bool(
                        race
                        and config
                        and isinstance(config.value, dict)
                        and (str(race.race_no) in config.value or race.race_no in config.value)
                    )

                    sample = (
                        session.query(ScoringFactor.raw_data_display)
                        .join(RaceEntry, RaceEntry.id == ScoringFactor.entry_id)
                        .filter(
                            RaceEntry.race_id == selected_race_id,
                            ScoringFactor.factor_name == "draw_stats",
                        )
                        .first()
                    )

                    needs_rescore = False
                    if has_official:
                        if not sample or not sample[0]:
                            needs_rescore = True
                        elif ("未載入官方統計" in sample[0]) or ("無統計數據" in sample[0]):
                            needs_rescore = True

                    auto_key = f"auto_rescore_draw_stats_{selected_race_id}_{config_key}"
                    if needs_rescore and not st.session_state.get(auto_key, False):
                        st.session_state[auto_key] = True
                        from scoring_engine.core import ScoringEngine
                        engine = ScoringEngine(session)
                        engine.score_race(selected_race_id)
                        st.rerun()

                if selected_factor == "負磅／評分表現":
                    from database.models import SystemConfig, RaceEntry, ScoringFactor

                    cfg = {"window_days": 365, "half_life_days": 180, "min_samples": 5, "place_weight": 0.2}
                    config = session.query(SystemConfig).filter_by(key="weight_rating_perf_config").first()
                    if config and isinstance(config.value, dict):
                        v = config.value
                        if "window_days" in v:
                            cfg["window_days"] = int(v["window_days"])
                        if "half_life_days" in v:
                            cfg["half_life_days"] = int(v["half_life_days"])
                        if "min_samples" in v:
                            cfg["min_samples"] = int(v["min_samples"])
                        if "place_weight" in v:
                            cfg["place_weight"] = float(v["place_weight"])

                    expected = f"PW{cfg['place_weight']:.2f}"
                    sample = (
                        session.query(ScoringFactor.raw_data_display)
                        .join(RaceEntry, RaceEntry.id == ScoringFactor.entry_id)
                        .filter(
                            RaceEntry.race_id == selected_race_id,
                            ScoringFactor.factor_name == "weight_rating_perf",
                        )
                        .first()
                    )

                    needs_rescore = False
                    if not sample or not sample[0]:
                        needs_rescore = True
                    elif expected not in sample[0]:
                        needs_rescore = True

                    auto_key = f"auto_rescore_weight_rating_perf_{selected_race_id}_{cfg['window_days']}_{cfg['half_life_days']}_{cfg['min_samples']}_{cfg['place_weight']:.2f}"
                    if needs_rescore and not st.session_state.get(auto_key, False):
                        st.session_state[auto_key] = True
                        from scoring_engine.core import ScoringEngine
                        engine = ScoringEngine(session)
                        engine.score_race(selected_race_id)
                        st.rerun()

                if selected_factor == "場地＋路程專長":
                    from database.models import SystemConfig, RaceEntry, ScoringFactor

                    cfg = {"window_days": 720, "half_life_days": 365, "min_samples": 3, "confidence_runs": 8, "win_w": 0.6, "place_w": 0.4}
                    config = session.query(SystemConfig).filter_by(key="venue_dist_specialty_config").first()
                    if config and isinstance(config.value, dict):
                        v = config.value
                        if "window_days" in v:
                            cfg["window_days"] = int(v["window_days"])
                        if "half_life_days" in v:
                            cfg["half_life_days"] = int(v["half_life_days"])
                        if "min_samples" in v:
                            cfg["min_samples"] = int(v["min_samples"])
                        if "confidence_runs" in v:
                            cfg["confidence_runs"] = int(v["confidence_runs"])
                        if "win_w" in v:
                            cfg["win_w"] = float(v["win_w"])
                        if "place_w" in v:
                            cfg["place_w"] = float(v["place_w"])

                    expected = f"W{cfg['window_days']}d | HL{cfg['half_life_days']}d | N{cfg['min_samples']} | C{cfg['confidence_runs']} | WW{cfg['win_w']:.2f} | PW{cfg['place_w']:.2f}"
                    sample = (
                        session.query(ScoringFactor.raw_data_display)
                        .join(RaceEntry, RaceEntry.id == ScoringFactor.entry_id)
                        .filter(
                            RaceEntry.race_id == selected_race_id,
                            ScoringFactor.factor_name == "venue_dist_specialty",
                        )
                        .first()
                    )

                    needs_rescore = False
                    if not sample or not sample[0]:
                        needs_rescore = True
                    elif expected not in sample[0]:
                        needs_rescore = True

                    auto_key = f"auto_rescore_venue_dist_specialty_{selected_race_id}_{expected}"
                    if needs_rescore and not st.session_state.get(auto_key, False):
                        st.session_state[auto_key] = True
                        from scoring_engine.core import ScoringEngine
                        engine = ScoringEngine(session)
                        engine.score_race(selected_race_id)
                        st.rerun()

                if selected_factor == "初出／長休後表現":
                    from database.models import SystemConfig, RaceEntry, ScoringFactor

                    cfg = {
                        "rest_days": 90,
                        "rest_tau": 60.0,
                        "prior_strength": 6.0,
                        "prior_top4": 0.28,
                        "conf_k": 3.0,
                        "sample_max": 12,
                        "dnf_rank": 14,
                    }
                    config = session.query(SystemConfig).filter_by(key="debut_long_rest_config").first()
                    if config and isinstance(config.value, dict):
                        v = config.value
                        if "rest_days" in v:
                            cfg["rest_days"] = int(v["rest_days"])
                        if "rest_tau" in v:
                            cfg["rest_tau"] = float(v["rest_tau"])
                        if "prior_strength" in v:
                            cfg["prior_strength"] = float(v["prior_strength"])
                        if "prior_top4" in v:
                            cfg["prior_top4"] = float(v["prior_top4"])
                        if "conf_k" in v:
                            cfg["conf_k"] = float(v["conf_k"])
                        if "sample_max" in v:
                            cfg["sample_max"] = int(v["sample_max"])
                        if "dnf_rank" in v:
                            cfg["dnf_rank"] = int(v["dnf_rank"])

                    expected = (
                        f"R{int(cfg['rest_days'])}d|T{float(cfg['rest_tau']):.0f}d|"
                        f"PS{float(cfg['prior_strength']):.1f}|P{float(cfg['prior_top4']):.2f}|"
                        f"K{float(cfg['conf_k']):.1f}|M{int(cfg['sample_max'])}|D{int(cfg['dnf_rank'])}"
                    )
                    sample = (
                        session.query(ScoringFactor.raw_data_display)
                        .join(RaceEntry, RaceEntry.id == ScoringFactor.entry_id)
                        .filter(
                            RaceEntry.race_id == selected_race_id,
                            ScoringFactor.factor_name == "debut_long_rest",
                        )
                        .first()
                    )

                    needs_rescore = False
                    if not sample or not sample[0]:
                        needs_rescore = True
                    elif expected not in sample[0]:
                        needs_rescore = True

                    auto_key = f"auto_rescore_debut_long_rest_{selected_race_id}_{expected}"
                    if needs_rescore and not st.session_state.get(auto_key, False):
                        st.session_state[auto_key] = True
                        from scoring_engine.core import ScoringEngine
                        engine = ScoringEngine(session)
                        engine.score_race(selected_race_id)
                        st.rerun()

                if selected_factor == "班次表現":
                    from database.models import SystemConfig, RaceEntry, ScoringFactor

                    cfg = {"lookback_races": 8, "half_life_days": 45, "max_gap_days": 120, "allowed_pairs": [[3, 4], [4, 5]]}
                    config = session.query(SystemConfig).filter_by(key="class_drop_signal_config").first()
                    if config and isinstance(config.value, dict):
                        v = config.value
                        if "lookback_races" in v:
                            cfg["lookback_races"] = int(v["lookback_races"])
                        if "half_life_days" in v:
                            cfg["half_life_days"] = int(v["half_life_days"])
                        if "max_gap_days" in v:
                            cfg["max_gap_days"] = int(v["max_gap_days"])
                        if "allowed_pairs" in v:
                            cfg["allowed_pairs"] = v["allowed_pairs"]

                    pairs = cfg.get("allowed_pairs")
                    ap = []
                    if isinstance(pairs, list):
                        for item in pairs:
                            if isinstance(item, (list, tuple)) and len(item) == 2:
                                ap.append(f"{int(item[0])}->{int(item[1])}")
                            elif isinstance(item, str) and "->" in item:
                                ap.append(item)
                    if not ap:
                        ap = ["3->4", "4->5"]
                    expected = f"LB{int(cfg['lookback_races'])}|HL{int(cfg['half_life_days'])}|MG{int(cfg['max_gap_days'])}|AP{','.join(sorted(ap))}"
                    sample = (
                        session.query(ScoringFactor.raw_data_display)
                        .join(RaceEntry, RaceEntry.id == ScoringFactor.entry_id)
                        .filter(
                            RaceEntry.race_id == selected_race_id,
                            ScoringFactor.factor_name == "class_performance",
                        )
                        .first()
                    )

                    needs_rescore = False
                    if not sample or not sample[0]:
                        needs_rescore = True
                    elif expected not in sample[0]:
                        needs_rescore = True

                    auto_key = f"auto_rescore_class_performance_{selected_race_id}_{expected}"
                    if needs_rescore and not st.session_state.get(auto_key, False):
                        st.session_state[auto_key] = True
                        from scoring_engine.core import ScoringEngine
                        engine = ScoringEngine(session)
                        engine.score_race(selected_race_id)
                        st.rerun()

                if selected_factor == "馬匹分段時間＋完成時間 (同路程歷史)":
                    from database.models import SystemConfig, RaceEntry, ScoringFactor

                    cfg = {"min_samples": 3, "confidence_runs": 8, "fallback_strategy": "A_B_C"}
                    config = session.query(SystemConfig).filter_by(key="horse_time_perf_config").first()
                    if config and isinstance(config.value, dict):
                        v = config.value
                        if "min_samples" in v:
                            cfg["min_samples"] = int(v["min_samples"])
                        if "confidence_runs" in v:
                            cfg["confidence_runs"] = int(v["confidence_runs"])
                        if "fallback_strategy" in v:
                            cfg["fallback_strategy"] = str(v["fallback_strategy"])

                    expected = f"N{cfg['min_samples']}|C{cfg['confidence_runs']}|{cfg['fallback_strategy']}"
                    sample = (
                        session.query(ScoringFactor.raw_data_display)
                        .join(RaceEntry, RaceEntry.id == ScoringFactor.entry_id)
                        .filter(
                            RaceEntry.race_id == selected_race_id,
                            ScoringFactor.factor_name == "horse_time_perf",
                        )
                        .first()
                    )

                    needs_rescore = False
                    if not sample or not sample[0]:
                        needs_rescore = True
                    elif expected not in sample[0]:
                        needs_rescore = True

                    auto_key = f"auto_rescore_horse_time_perf_{selected_race_id}_{expected}"
                    if needs_rescore and not st.session_state.get(auto_key, False):
                        st.session_state[auto_key] = True
                        from scoring_engine.core import ScoringEngine
                        engine = ScoringEngine(session)
                        engine.score_race(selected_race_id)
                        st.rerun()

                # 提取基本資訊與該因子的分數
                race = session.get(Race, selected_race_id)
                track_display = race.track_type if race.track_type else race.venue
                st.markdown(f"#### 📊 {selected_date_str} | 第 {race.race_no} 場 | {track_display}")
                view_cols = ["馬號", "馬名", "檔位", "負磅", "評分", f"{selected_factor}_raw", selected_factor]
                factor_df = df[view_cols].copy()
                
                # 重新命名欄位讓 UI 更清晰
                factor_df = factor_df.rename(columns={
                    f"{selected_factor}_raw": "原始數據 (分析基礎)",
                    selected_factor: "系統標準化得分 (0-10分)"
                })
                
                # 根據該因子分數進行降序排序
                factor_df = factor_df.sort_values(by="系統標準化得分 (0-10分)", ascending=False).reset_index(drop=True)
                
                # 加上名次標籤
                factor_df = factor_df.reset_index(drop=True)
                factor_df.insert(0, "該項排名", range(1, len(factor_df) + 1))
                st.dataframe(
                    factor_df, 
                    use_container_width=True, 
                    hide_index=True,
                    column_config={
                        "原始數據 (分析基礎)": st.column_config.TextColumn(width="large"),
                        "馬名": st.column_config.TextColumn(width="medium"),
                    }
                )
                
                # 針對特定因子顯示詳細說明與參數調整
                if selected_factor == "近期狀態 (Last 6 Runs)":
                    st.markdown("---")
                    st.markdown("### 💡 演算法說明：近期狀態 (Last 6 Runs)")
                    st.markdown("""
                    這個條件用於評估馬匹最近 6 場的**「Top4 取向近期狀態」**，核心不是直接平均名次，而是把每一仗的名次轉換成「更貼近 Top4 命中」的分數，再做近期加權與保守化。
                    
                    **計分重點：**
                    1. **名次→Top4 分數**：每一仗名次會轉成 0–1 的「Top4 相似度」（名次越前分越高）。退出/未完成等會視作差名次處理，避免被忽略而偏樂觀。
                    2. **近期權重**：仍採用最近 6 仗權重（第 1 仗最重），並可選擇加入「按距離今天幾日」的時間衰減，使更近期的賽事影響更大。
                    3. **樣本不足保守**：近仗不足時，分數會向中性值收斂，避免 1–2 場爆分。
                    4. **長休中性化**：距離上一仗太久時，分數會自動向中性靠攏（狀態不明）。
                    5. **趨勢加成**：若最近 3 仗比之前 3 仗明顯進步，會有小幅加分（可調整或設為 0 關閉）。
                    
                    最後，引擎會把本場所有馬匹的 raw 分數做相對百分位標準化成 0–10 分。
                    """)
                    
                    with st.expander("⚙️ 調整加權參數 (調整後將即時儲存並重算)"):
                        from database.models import SystemConfig
                        
                        config = session.query(SystemConfig).filter_by(key="recent_form_weights").first()
                        if config and isinstance(config.value, list) and len(config.value) == 6:
                            current_weights = config.value
                        else:
                            current_weights = [6, 5, 4, 3, 2, 1]
                            
                        st.markdown("設定過去 6 場比賽的權重 (第 1 場代表最近一場)：")
                        
                        with st.form("recent_form_weights_form"):
                            col_w1, col_w2, col_w3, col_w4, col_w5, col_w6 = st.columns(6)
                            w1 = col_w1.number_input("第 1 場 (最近)", value=current_weights[0], min_value=0, max_value=20)
                            w2 = col_w2.number_input("第 2 場", value=current_weights[1], min_value=0, max_value=20)
                            w3 = col_w3.number_input("第 3 場", value=current_weights[2], min_value=0, max_value=20)
                            w4 = col_w4.number_input("第 4 場", value=current_weights[3], min_value=0, max_value=20)
                            w5 = col_w5.number_input("第 5 場", value=current_weights[4], min_value=0, max_value=20)
                            w6 = col_w6.number_input("第 6 場 (最遠)", value=current_weights[5], min_value=0, max_value=20)
                            
                            submitted = st.form_submit_button("💾 儲存參數並為本場重新計分", type="primary")
                            if submitted:
                                new_weights = [w1, w2, w3, w4, w5, w6]
                                if not config:
                                    config = SystemConfig(key="recent_form_weights", description="近期狀態 (Last 6) 權重陣列")
                                    session.add(config)
                                config.value = new_weights
                                session.commit()
                                
                                # 觸發重新計分
                                from scoring_engine.core import ScoringEngine
                                engine = ScoringEngine(session)
                                engine.score_race(selected_race_id)
                                
                                st.success(f"參數已儲存為 {new_weights}，並已重新計算分數！")
                                st.rerun()
                    
                    with st.expander("⚙️ 調整進階參數（Top4 取向＋保守化；調整後將即時儲存並重算）", expanded=False):
                        from database.models import SystemConfig
                        
                        cfg = {
                            "mid_rank": 4.5,
                            "rank_slope": 1.6,
                            "dnf_rank": 14,
                            "rank_cap": 20,
                            "use_day_decay": True,
                            "day_tau": 120.0,
                            "conf_k": 2.0,
                            "gap_days_neutral": 60.0,
                            "gap_tau": 60.0,
                            "trend_w": 0.08,
                            "trend_tau": 3.0,
                        }
                        
                        config = session.query(SystemConfig).filter_by(key="recent_form_config").first()
                        if config and isinstance(config.value, dict):
                            for k in cfg.keys():
                                if k in config.value:
                                    try:
                                        cfg[k] = type(cfg[k])(config.value[k])
                                    except Exception:
                                        pass
                        
                        with st.form("recent_form_config_form"):
                            st.caption("用途：更貼近 Top4 命中；並針對樣本不足、長休、退出等情況作保守化。")
                            
                            c1, c2, c3, c4 = st.columns(4)
                            mid_rank = c1.number_input("Top4 分數中點名次", value=float(cfg["mid_rank"]), min_value=1.0, max_value=12.0, step=0.1)
                            rank_slope = c2.number_input("名次曲線斜率(越小越極端)", value=float(cfg["rank_slope"]), min_value=0.5, max_value=5.0, step=0.1)
                            dnf_rank = c3.number_input("退出/未完成視作名次", value=int(cfg["dnf_rank"]), min_value=6, max_value=40, step=1)
                            rank_cap = c4.number_input("名次上限裁剪", value=int(cfg["rank_cap"]), min_value=6, max_value=60, step=1)
                            
                            c5, c6, c7, c8 = st.columns(4)
                            use_day_decay = c5.checkbox("啟用按日時間衰減", value=bool(cfg["use_day_decay"]))
                            day_tau = c6.number_input("按日衰減 τ（天；越大越不衰減）", value=float(cfg["day_tau"]), min_value=0.0, max_value=365.0, step=5.0)
                            conf_k = c7.number_input("樣本不足保守係數(越大越保守)", value=float(cfg["conf_k"]), min_value=0.0, max_value=20.0, step=0.5)
                            gap_days_neutral = c8.number_input("長休中性化門檻(天)", value=float(cfg["gap_days_neutral"]), min_value=0.0, max_value=365.0, step=5.0)
                            
                            c9, c10, c11, c12 = st.columns(4)
                            gap_tau = c9.number_input("長休中性化衰減 τ（天）", value=float(cfg["gap_tau"]), min_value=0.0, max_value=365.0, step=5.0)
                            trend_w = c10.number_input("趨勢加成權重(設0關閉)", value=float(cfg["trend_w"]), min_value=0.0, max_value=0.3, step=0.01)
                            trend_tau = c11.number_input("趨勢飽和 τ（名次差）", value=float(cfg["trend_tau"]), min_value=0.5, max_value=20.0, step=0.5)
                            _ = c12.empty()
                            
                            submitted = st.form_submit_button("💾 儲存進階參數並為本場重新計分", type="primary")
                            if submitted:
                                new_cfg = {
                                    "mid_rank": float(mid_rank),
                                    "rank_slope": float(rank_slope),
                                    "dnf_rank": int(dnf_rank),
                                    "rank_cap": int(rank_cap),
                                    "use_day_decay": bool(use_day_decay),
                                    "day_tau": float(day_tau),
                                    "conf_k": float(conf_k),
                                    "gap_days_neutral": float(gap_days_neutral),
                                    "gap_tau": float(gap_tau),
                                    "trend_w": float(trend_w),
                                    "trend_tau": float(trend_tau),
                                }
                                if not config:
                                    config = SystemConfig(key="recent_form_config", description="近期狀態(Last 6)：Top4 取向＋保守化參數")
                                    session.add(config)
                                config.value = new_cfg
                                session.commit()
                                
                                from scoring_engine.core import ScoringEngine
                                engine = ScoringEngine(session)
                                engine.score_race(selected_race_id)
                                
                                st.success("進階參數已儲存並重新計分！")
                                st.rerun()
                
                if selected_factor in ("騎師＋練馬師合作 (同路程/場地)", "騎師＋練馬師合作 (不論馬匹)", "騎師＋練馬師合作 (綜合)"):
                    st.markdown("---")
                    st.markdown(f"### 💡 演算法說明：騎師＋練馬師合作 (綜合)")
                    st.markdown('''
                    這個條件用於綜合衡量「騎師＋練馬師」的合作表現，分為「全庫歷史合作」與「本駒合作」兩個維度，並針對樣本不足做保守化處理：
                    
                    - **全庫合作**：不論馬匹，該騎練組合的歷史勝率與入位率（前 3）。
                    - **本駒合作**：專指該騎練組合在「本場這匹馬」身上的歷史勝率與入位率（前 3）。
                    - **先驗平滑（避免少樣本硬 0 或爆分）**：勝率/入位率會加入先驗（先驗勝率/入位率 + 先驗強度），樣本越少越接近先驗。
                    - **信心折扣（偏保守）**：以「有效樣本量」計算折扣，樣本越少折扣越大，避免 1–2 筆造成誤導。
                    - **動態本駒權重**：本駒樣本不足時，本駒佔比會自動降低；樣本足夠才逐步回到設定的本駒佔比。
                    - **最終計分**：全庫與本駒分數（含平滑/折扣）合併後，再於同場內相對百分位標準化成 0–10 分。
                    ''')
                    
                    with st.expander("⚙️ 調整全庫/本駒合作樣本範圍與權重比例 (調整後將即時儲存並重算)", expanded=True):
                        from database.models import SystemConfig
                        
                        cfg = {
                            "global_window": 0, "global_win_w": 0.7, "global_place_w": 0.3,
                            "horse_window": 0, "horse_win_w": 0.7, "horse_place_w": 0.3,
                            "global_weight": 0.5, "horse_weight": 0.5,
                            "prior_strength_global": 12.0,
                            "prior_strength_horse": 8.0,
                            "prior_win_rate": 0.08,
                            "prior_place_rate": 0.28,
                            "confidence_runs_global": 12.0,
                            "confidence_runs_horse": 8.0,
                            "horse_weight_full_runs": 8.0,
                        }
                        
                        config = session.query(SystemConfig).filter_by(key="jt_bond_combined_config").first()
                        if config and isinstance(config.value, dict):
                            for k in cfg.keys():
                                if k in config.value:
                                    cfg[k] = type(cfg[k])(config.value[k])
                        else:
                            # 嘗試讀取舊設定
                            old_cfg = session.query(SystemConfig).filter_by(key="jt_bond_config").first()
                            if old_cfg and isinstance(old_cfg.value, dict):
                                cfg["global_window"] = int(old_cfg.value.get("window", 0))
                                cfg["global_win_w"] = float(old_cfg.value.get("win", 0.7))
                                cfg["global_place_w"] = float(old_cfg.value.get("place", 0.3))
                                
                        window_options = {
                            "近 5 次": 5, "近 10 次": 10, "近 15 次": 15,
                            "近 20 次": 20, "近 25 次": 25, "最大 (全部)": 0
                        }
                        gl_label = next((k for k, v in window_options.items() if v == cfg["global_window"]), "最大 (全部)")
                        hl_label = next((k for k, v in window_options.items() if v == cfg["horse_window"]), "最大 (全部)")
                        
                        with st.form("jt_bond_combined_form"):
                            st.markdown("##### 1️⃣ 全庫合作 (不論馬匹) 設定")
                            c1, c2, c3 = st.columns(3)
                            g_win = c1.selectbox("全庫樣本範圍", list(window_options.keys()), index=list(window_options.keys()).index(gl_label))
                            g_ww = c2.number_input("全庫勝率權重", value=cfg["global_win_w"], min_value=0.0, max_value=1.0, step=0.05)
                            g_pw = c3.number_input("全庫入圍權重", value=cfg["global_place_w"], min_value=0.0, max_value=1.0, step=0.05)
                            
                            st.markdown("##### 2️⃣ 本駒合作 設定")
                            c4, c5, c6 = st.columns(3)
                            h_win = c4.selectbox("本駒樣本範圍", list(window_options.keys()), index=list(window_options.keys()).index(hl_label))
                            h_ww = c5.number_input("本駒勝率權重", value=cfg["horse_win_w"], min_value=0.0, max_value=1.0, step=0.05)
                            h_pw = c6.number_input("本駒入圍權重", value=cfg["horse_place_w"], min_value=0.0, max_value=1.0, step=0.05)
                            
                            st.markdown("##### 3️⃣ 綜合比例設定")
                            c7, c8 = st.columns(2)
                            gw = c7.number_input("全庫得分佔比", value=cfg["global_weight"], min_value=0.0, max_value=1.0, step=0.05)
                            hw = c8.number_input("本駒得分佔比", value=cfg["horse_weight"], min_value=0.0, max_value=1.0, step=0.05)

                            st.markdown("##### 4️⃣ 樣本不足（保守）設定：平滑 + 信心折扣 + 動態本駒權重")
                            st.caption("用途：樣本不足時避免硬 0 分或爆分；以先驗平滑並按樣本量自動保守，目標更貼近 Top4 命中。")
                            c9, c10, c11, c12 = st.columns(4)
                            prior_win = c9.number_input("先驗勝率", value=float(cfg.get("prior_win_rate") or 0.08), min_value=0.0, max_value=1.0, step=0.01)
                            prior_place = c10.number_input("先驗入位率(1-3)", value=float(cfg.get("prior_place_rate") or 0.28), min_value=0.0, max_value=1.0, step=0.01)
                            ps_g = c11.number_input("全庫先驗強度(等價場數)", value=float(cfg.get("prior_strength_global") or 12.0), min_value=0.0, max_value=200.0, step=1.0)
                            ps_h = c12.number_input("本駒先驗強度(等價場數)", value=float(cfg.get("prior_strength_horse") or 8.0), min_value=0.0, max_value=200.0, step=1.0)

                            c13, c14, c15 = st.columns(3)
                            cr_g = c13.number_input("全庫信心折扣(越大越保守)", value=float(cfg.get("confidence_runs_global") or 12.0), min_value=0.0, max_value=200.0, step=1.0)
                            cr_h = c14.number_input("本駒信心折扣(越大越保守)", value=float(cfg.get("confidence_runs_horse") or 8.0), min_value=0.0, max_value=200.0, step=1.0)
                            hw_full = c15.number_input("本駒權重滿檔所需樣本(場)", value=float(cfg.get("horse_weight_full_runs") or 8.0), min_value=1.0, max_value=100.0, step=1.0)
                            
                            submitted = st.form_submit_button("💾 儲存合併參數並為本場重新計分", type="primary")
                            if submitted:
                                new_cfg = {
                                    "global_window": window_options[g_win], "global_win_w": g_ww, "global_place_w": g_pw,
                                    "horse_window": window_options[h_win], "horse_win_w": h_ww, "horse_place_w": h_pw,
                                    "global_weight": gw, "horse_weight": hw,
                                    "prior_strength_global": float(ps_g),
                                    "prior_strength_horse": float(ps_h),
                                    "prior_win_rate": float(prior_win),
                                    "prior_place_rate": float(prior_place),
                                    "confidence_runs_global": float(cr_g),
                                    "confidence_runs_horse": float(cr_h),
                                    "horse_weight_full_runs": float(hw_full),
                                }
                                if not config:
                                    config = SystemConfig(key="jt_bond_combined_config", description="騎師＋練馬師合作(綜合)：全庫與本駒參數")
                                    session.add(config)
                                config.value = new_cfg
                                session.commit()
                                
                                from scoring_engine.core import ScoringEngine
                                engine = ScoringEngine(session)
                                engine.score_race(selected_race_id)
                                st.success("參數已儲存！已合併「近X次」與「不論馬匹」的邏輯並重新計分！")
                                st.rerun()

                elif selected_factor == "場地＋路程專長":
                    st.markdown("---")
                    st.markdown("### 💡 演算法說明：場地＋路程專長")
                    st.markdown("""
                    這個條件用於衡量馬匹是否在「同跑道類型（草地/泥地）＋同路程」具備明顯專長。
                    
                    - **匹配層級 (fallback)**：優先使用更精準的歷史條件；若樣本不足則逐級放寬，以避免大量馬匹因樣本不足而變成 0 分。
                      - **A（最精準）**：同跑道資訊（track_type/跑道）＋同路程
                      - **B（中等）**：同草/泥（surface，泥地含全天候）＋同路程
                      - **C（最寬鬆）**：只用同路程
                    - **時間衰減（可選）**：以半衰期對較舊的賽績降權（近期更重要）。
                    - **先驗平滑（偏保守）**：勝率/上名率會加入先驗（先驗勝率/上名率 + 先驗強度），樣本越少越接近先驗，避免小樣本爆分或硬 0。
                    - **可信度（偏保守）**：以平滑後的信心折扣對原始分降權，樣本越少折扣越大。
                    - 可設定時間窗（近 X 日）、樣本下限、先驗與權重比例
                    - 最後在同一場內進行百分位標準化成 0–10 分
                    """)
                    with st.expander("⚙️ 調整時間窗/樣本下限/可信度與勝率權重 (調整後將即時儲存並重算)", expanded=False):
                        from database.models import SystemConfig

                        cfg = {
                            "window_days": 720,
                            "half_life_days": 365,
                            "min_samples": 3,
                            "confidence_runs": 12.0,
                            "prior_strength": 12.0,
                            "prior_win_rate": 0.08,
                            "prior_place_rate": 0.28,
                            "win_w": 0.6,
                            "place_w": 0.4,
                            "fallback_strategy": "A_B_C",
                        }
                        config = session.query(SystemConfig).filter_by(key="venue_dist_specialty_config").first()
                        if config and isinstance(config.value, dict):
                            v = config.value
                            if "window_days" in v:
                                cfg["window_days"] = int(v["window_days"])
                            if "half_life_days" in v:
                                cfg["half_life_days"] = int(v["half_life_days"])
                            if "min_samples" in v:
                                cfg["min_samples"] = int(v["min_samples"])
                            if "confidence_runs" in v:
                                cfg["confidence_runs"] = float(v["confidence_runs"])
                            if "prior_strength" in v:
                                cfg["prior_strength"] = float(v["prior_strength"])
                            if "prior_win_rate" in v:
                                cfg["prior_win_rate"] = float(v["prior_win_rate"])
                            if "prior_place_rate" in v:
                                cfg["prior_place_rate"] = float(v["prior_place_rate"])
                            if "win_w" in v:
                                cfg["win_w"] = float(v["win_w"])
                            if "place_w" in v:
                                cfg["place_w"] = float(v["place_w"])
                            if "fallback_strategy" in v:
                                cfg["fallback_strategy"] = str(v["fallback_strategy"])

                        window_options = {"近 180 日": 180, "近 365 日": 365, "近 720 日": 720, "全部": 0}
                        hl_options = {"半衰期 180 日": 180, "半衰期 365 日": 365, "半衰期 720 日": 720, "不衰減": 0}
                        cur_window_label = next((k for k, v in window_options.items() if v == cfg["window_days"]), "近 720 日")
                        cur_hl_label = next((k for k, v in hl_options.items() if v == cfg["half_life_days"]), "半衰期 365 日")
                        fs_map = {"A→B→C": "A_B_C", "B→C": "B_C", "只用 C": "C"}
                        cur_fs_label = next((k for k, v in fs_map.items() if v == cfg["fallback_strategy"]), "A→B→C")

                        with st.form("venue_dist_specialty_config_form"):
                            c1, c2, c3, c4, c5, c6 = st.columns(6)
                            window_label = c1.selectbox("時間窗", list(window_options.keys()), index=list(window_options.keys()).index(cur_window_label))
                            hl_label = c2.selectbox("時間衰減", list(hl_options.keys()), index=list(hl_options.keys()).index(cur_hl_label))
                            min_samples = c3.number_input("樣本下限 N", value=int(cfg["min_samples"]), min_value=0, max_value=30, step=1)
                            confidence_runs = c4.number_input("信心折扣(越大越保守)", value=float(cfg["confidence_runs"]), min_value=0.0, max_value=200.0, step=1.0)
                            win_w = c5.number_input("勝率權重", value=float(cfg["win_w"]), min_value=0.0, max_value=1.0, step=0.05)
                            place_w = c6.number_input("上名率權重", value=float(cfg["place_w"]), min_value=0.0, max_value=1.0, step=0.05)

                            c7, c8, c9, c10 = st.columns(4)
                            prior_strength = c7.number_input("先驗強度(等價場數)", value=float(cfg["prior_strength"]), min_value=0.0, max_value=200.0, step=1.0)
                            prior_win_rate = c8.number_input("先驗勝率", value=float(cfg["prior_win_rate"]), min_value=0.0, max_value=1.0, step=0.01)
                            prior_place_rate = c9.number_input("先驗上名率", value=float(cfg["prior_place_rate"]), min_value=0.0, max_value=1.0, step=0.01)
                            fs_label = c10.selectbox("Fallback 規則", list(fs_map.keys()), index=list(fs_map.keys()).index(cur_fs_label))

                            submitted = st.form_submit_button("💾 儲存參數並為本場重新計分", type="primary")
                            if submitted:
                                new_cfg = {
                                    "window_days": int(window_options[window_label]),
                                    "half_life_days": int(hl_options[hl_label]),
                                    "min_samples": int(min_samples),
                                    "confidence_runs": float(confidence_runs),
                                    "prior_strength": float(prior_strength),
                                    "prior_win_rate": float(prior_win_rate),
                                    "prior_place_rate": float(prior_place_rate),
                                    "win_w": float(win_w),
                                    "place_w": float(place_w),
                                    "fallback_strategy": str(fs_map[fs_label]),
                                }
                                if not config:
                                    config = SystemConfig(key="venue_dist_specialty_config", description="場地＋路程專長：時間窗/樣本/可信度/勝率權重")
                                    session.add(config)
                                config.value = new_cfg
                                session.commit()

                                from scoring_engine.core import ScoringEngine
                                engine = ScoringEngine(session)
                                engine.score_race(selected_race_id)
                                st.success(f"參數已儲存：{new_cfg}，並已重新計分。")
                                st.rerun()

                elif selected_factor == "初出／長休後表現":
                    st.markdown("---")
                    st.markdown("### 💡 演算法說明：初出／長休後表現")
                    st.markdown("""
                    這個條件用於評估馬匹在「**初出／長休復出**」場景下的 Top4 命中傾向，重點是**保守、可泛化**：
                    
                    - **先判斷是否長休**：本場距離上仗日數 ≥ 門檻，才視作長休復出；否則回傳中性（不加不減），避免大部分馬全變 0 分造成噪音。
                    - **馬匹自身復出樣本**：回看該馬歷史上每次「gap ≥ 門檻」後的第一仗，把成績轉成 **Top4 命中(0/1)**，計算平滑後的 Top4 率。
                    - **樣本不足保守**：復出樣本少時，分數向中性收斂，避免 1–2 次復出就爆分或誤導。
                    - **長休越久越不確定**：距離上仗越久，會把分數再往中性拉回（可調 τ），避免超長休被過度解讀。
                    - **退出/未完成**：不會被忽略，會視作差表現樣本（避免偏樂觀）。
                    
                    最後，引擎會把本場所有馬匹的 raw 分數做相對百分位標準化成 0–10 分。
                    """)
                    with st.expander("⚙️ 調整參數（調整後將即時儲存並重算）", expanded=False):
                        from database.models import SystemConfig

                        cfg = {
                            "rest_days": 90,
                            "rest_tau": 60.0,
                            "prior_strength": 6.0,
                            "prior_top4": 0.28,
                            "conf_k": 3.0,
                            "sample_max": 12,
                            "dnf_rank": 14,
                        }
                        config = session.query(SystemConfig).filter_by(key="debut_long_rest_config").first()
                        if config and isinstance(config.value, dict):
                            v = config.value
                            if "rest_days" in v:
                                cfg["rest_days"] = int(v["rest_days"])
                            if "rest_tau" in v:
                                cfg["rest_tau"] = float(v["rest_tau"])
                            if "prior_strength" in v:
                                cfg["prior_strength"] = float(v["prior_strength"])
                            if "prior_top4" in v:
                                cfg["prior_top4"] = float(v["prior_top4"])
                            if "conf_k" in v:
                                cfg["conf_k"] = float(v["conf_k"])
                            if "sample_max" in v:
                                cfg["sample_max"] = int(v["sample_max"])
                            if "dnf_rank" in v:
                                cfg["dnf_rank"] = int(v["dnf_rank"])

                        with st.form("debut_long_rest_config_form"):
                            c1, c2, c3, c4 = st.columns(4)
                            rest_days = c1.number_input("長休門檻 (日)", value=int(cfg["rest_days"]), min_value=0, max_value=365, step=5)
                            rest_tau = c2.number_input("長休不確定 τ(日)", value=float(cfg["rest_tau"]), min_value=0.0, max_value=365.0, step=5.0)
                            sample_max = c3.number_input("最多回看復出樣本", value=int(cfg["sample_max"]), min_value=1, max_value=60, step=1)
                            dnf_rank = c4.number_input("退出/未完成視作名次", value=int(cfg["dnf_rank"]), min_value=6, max_value=60, step=1)

                            c5, c6, c7 = st.columns(3)
                            prior_top4 = c5.number_input("先驗Top4率", value=float(cfg["prior_top4"]), min_value=0.0, max_value=1.0, step=0.01)
                            prior_strength = c6.number_input("先驗強度(等價樣本)", value=float(cfg["prior_strength"]), min_value=0.0, max_value=200.0, step=1.0)
                            conf_k = c7.number_input("樣本不足保守係數(越大越保守)", value=float(cfg["conf_k"]), min_value=0.0, max_value=50.0, step=0.5)

                            submitted = st.form_submit_button("💾 儲存參數並為本場重新計分", type="primary")
                            if submitted:
                                new_cfg = {
                                    "rest_days": int(rest_days),
                                    "rest_tau": float(rest_tau),
                                    "prior_strength": float(prior_strength),
                                    "prior_top4": float(prior_top4),
                                    "conf_k": float(conf_k),
                                    "sample_max": int(sample_max),
                                    "dnf_rank": int(dnf_rank),
                                }
                                if not config:
                                    config = SystemConfig(key="debut_long_rest_config", description="初出／長休後表現：Top4取向＋保守化參數")
                                    session.add(config)
                                config.value = new_cfg
                                session.commit()

                                from scoring_engine.core import ScoringEngine
                                engine = ScoringEngine(session)
                                engine.score_race(selected_race_id)
                                st.success(f"參數已儲存：{new_cfg}，並已重新計分。")
                                st.rerun()

                elif selected_factor == "班次表現":
                    st.markdown("---")
                    st.markdown("### 💡 演算法說明：班次表現（降班訊號）")
                    st.markdown("""
                    這個條件專門用於捕捉「3→4」與「4→5」的降班訊號（依你提供的歷史觀察：這兩個降班類型的入位/勝出機率較高；其他班次降班不適用，會直接忽略）。

                    - **班次解析**：優先辨識「第 1–5 班 / Class 1–5」；亦能辨識「一/二/三級賽、G1/2/3、Group 1/2/3」作為級際賽（grade）。
                    - **降班強度**：
                      - 只計算 **3→4** 與 **4→5**；其他變化一律視為「無降班」。
                    - **時效性**：降班距離上一次可解析班次的賽事越久，訊號越弱（半衰期衰減）；超過最大間隔則不計。
                    - **輸出**：raw 分數先於同場做百分位標準化成 0–10 分。
                    """)
                    with st.expander("⚙️ 調整 lookback/半衰期/最大間隔／啟用的降班類型（調整後將即時儲存並重算）", expanded=False):
                        from database.models import SystemConfig

                        cfg = {"lookback_races": 8, "half_life_days": 45, "max_gap_days": 120, "allowed_pairs": [[3, 4], [4, 5]]}
                        config = session.query(SystemConfig).filter_by(key="class_drop_signal_config").first()
                        if config and isinstance(config.value, dict):
                            v = config.value
                            if "lookback_races" in v:
                                cfg["lookback_races"] = int(v["lookback_races"])
                            if "half_life_days" in v:
                                cfg["half_life_days"] = int(v["half_life_days"])
                            if "max_gap_days" in v:
                                cfg["max_gap_days"] = int(v["max_gap_days"])
                            if "allowed_pairs" in v:
                                cfg["allowed_pairs"] = v["allowed_pairs"]

                        with st.form("class_drop_signal_config_form"):
                            c1, c2, c3, c4, c5 = st.columns(5)
                            lookback = c1.number_input("回看最近幾仗", value=int(cfg["lookback_races"]), min_value=1, max_value=30, step=1)
                            half_life = c2.number_input("半衰期(日)", value=int(cfg["half_life_days"]), min_value=7, max_value=365, step=1)
                            max_gap = c3.number_input("最大間隔(日)", value=int(cfg["max_gap_days"]), min_value=14, max_value=730, step=1)
                            cur_pairs = cfg.get("allowed_pairs")
                            enabled = set()
                            if isinstance(cur_pairs, list):
                                for item in cur_pairs:
                                    if isinstance(item, (list, tuple)) and len(item) == 2:
                                        try:
                                            enabled.add((int(item[0]), int(item[1])))
                                        except Exception:
                                            continue
                            enable_34 = c4.checkbox("啟用 3→4", value=((3, 4) in enabled))
                            enable_45 = c5.checkbox("啟用 4→5", value=((4, 5) in enabled))

                            submitted = st.form_submit_button("💾 儲存參數並為本場重新計分", type="primary")
                            if submitted:
                                pairs = []
                                if enable_34:
                                    pairs.append([3, 4])
                                if enable_45:
                                    pairs.append([4, 5])
                                if not pairs:
                                    pairs = [[3, 4], [4, 5]]
                                new_cfg = {
                                    "lookback_races": int(lookback),
                                    "half_life_days": int(half_life),
                                    "max_gap_days": int(max_gap),
                                    "allowed_pairs": pairs,
                                }
                                if not config:
                                    config = SystemConfig(key="class_drop_signal_config", description="班次表現：降班訊號參數")
                                    session.add(config)
                                config.value = new_cfg
                                session.commit()

                                from scoring_engine.core import ScoringEngine
                                engine = ScoringEngine(session)
                                engine.score_race(selected_race_id)
                                st.success(f"參數已儲存：{new_cfg}，並已重新計分。")
                                st.rerun()

                elif selected_factor == "馬匹分段時間＋完成時間 (同路程歷史)":
                    st.markdown("---")
                    st.markdown("### 💡 演算法說明：馬匹完成時間 (同路程歷史)")
                    st.markdown("""
                    這個條件用於衡量馬匹在「同路程」下的歷史速度能力（先以完成時間為主）。
                    
                    - **匹配層級 (fallback)**：為避免樣本不足，會由精準到寬鬆逐級尋找歷史紀錄，找到第一個「樣本數 ≥ N」的層級就使用；若所有層級都不足或無法解析完成時間，則以中性處理。
                      - **A（最精準）**：同路程 + 同跑道資訊（track_type / 跑道，例如「跑馬地草地 C+3」）
                      - **B（中等）**：同路程 + 同草/泥（surface，例如草地/泥地(全天候)）
                      - **C（最寬鬆）**：只用同路程（不分跑道/草泥）
                    - **Fallback 規則選項**：
                      - **A→B→C**：先嘗試 A，樣本不足再降到 B，再不足才用 C（推薦，最符合「同條件」概念）
                      - **B→C**：略過 A，直接用草/泥同程，再不足用純同程（樣本較多，但較不精準）
                      - **只用 C**：只用純同程（最容易有數據，但條件最粗）
                    - **代表值（偏保守）**：不取最佳時間（min），改用同條件完成時間的分位數（預設 P20）作代表值，降低偶發超快一場造成的誤導。
                    - **差距轉換（gap_pct）**：以相對差距衡量速度差：gap_pct = (t - t_min) / t_min，並用 exp(-gap_pct / pct_tau) 轉成 base 分數，避免不同路程用固定秒差造成失真。
                    - **可信度（偏保守）**：以平滑後的信心折扣 conf = (n + prior_strength) / (n + prior_strength + confidence_runs) 對 base 降權；樣本越少越保守。
                    - **最後調整**：同場再做百分位標準化成 0–10 分。
                    """)
                    with st.expander("⚙️ 調整樣本下限/可信度與 fallback 規則 (調整後將即時儲存並重算)", expanded=False):
                        from database.models import SystemConfig

                        cfg = {
                            "min_samples": 3,
                            "confidence_runs": 12.0,
                            "prior_strength": 12.0,
                            "fallback_strategy": "A_B_C",
                            "use_quantile": 0.2,
                            "pct_tau": 0.012,
                        }
                        config = session.query(SystemConfig).filter_by(key="horse_time_perf_config").first()
                        if config and isinstance(config.value, dict):
                            v = config.value
                            if "min_samples" in v:
                                cfg["min_samples"] = int(v["min_samples"])
                            if "confidence_runs" in v:
                                cfg["confidence_runs"] = float(v["confidence_runs"])
                            if "prior_strength" in v:
                                cfg["prior_strength"] = float(v["prior_strength"])
                            if "fallback_strategy" in v:
                                cfg["fallback_strategy"] = str(v["fallback_strategy"])
                            if "use_quantile" in v:
                                cfg["use_quantile"] = float(v["use_quantile"])
                            if "pct_tau" in v:
                                cfg["pct_tau"] = float(v["pct_tau"])

                        fs_map = {
                            "A→B→C": "A_B_C",
                            "B→C": "B_C",
                            "只用 C": "C",
                        }
                        cur_fs_label = next((k for k, v in fs_map.items() if v == cfg["fallback_strategy"]), "A→B→C")

                        with st.form("horse_time_perf_config_form"):
                            st.caption("本因子會以同程完成時間的 P20（較保守）作代表值；並以 gap_pct（相對差距）轉換成分數，避免不同路程用固定秒差造成失真。")
                            c1, c2, c3, c4 = st.columns(4)
                            min_samples = c1.number_input("樣本下限 N", value=int(cfg["min_samples"]), min_value=0, max_value=30, step=1)
                            use_q = c2.number_input("代表值分位數", value=float(cfg["use_quantile"]), min_value=0.0, max_value=1.0, step=0.05)
                            pct_tau = c3.number_input("gap_pct 衰減尺度", value=float(cfg["pct_tau"]), min_value=0.001, max_value=0.050, step=0.001, format="%.3f")
                            fs_label = c4.selectbox("Fallback 規則", list(fs_map.keys()), index=list(fs_map.keys()).index(cur_fs_label))

                            c5, c6 = st.columns(2)
                            prior_strength = c5.number_input("先驗強度(等價場數，越大越保守)", value=float(cfg["prior_strength"]), min_value=0.0, max_value=200.0, step=1.0)
                            confidence_runs = c6.number_input("信心折扣(越大越保守)", value=float(cfg["confidence_runs"]), min_value=0.0, max_value=200.0, step=1.0)

                            submitted = st.form_submit_button("💾 儲存參數並為本場重新計分", type="primary")
                            if submitted:
                                new_cfg = {
                                    "min_samples": int(min_samples),
                                    "confidence_runs": float(confidence_runs),
                                    "prior_strength": float(prior_strength),
                                    "fallback_strategy": str(fs_map[fs_label]),
                                    "use_quantile": float(use_q),
                                    "pct_tau": float(pct_tau),
                                }
                                if not config:
                                    config = SystemConfig(key="horse_time_perf_config", description="馬匹完成時間(同路程)：樣本/可信度/fallback")
                                    session.add(config)
                                config.value = new_cfg
                                session.commit()

                                from scoring_engine.core import ScoringEngine
                                engine = ScoringEngine(session)
                                engine.score_race(selected_race_id)
                                st.success(f"參數已儲存：{new_cfg}，並已重新計分。")
                                st.rerun()

                elif selected_factor == "負磅／評分表現":
                    st.markdown("---")
                    st.markdown("### 💡 演算法說明：負磅／評分表現")
                    st.markdown("""
                    這個條件用於衡量馬匹在「同路程」下，是否出現對 Top4 更有利的「讓磅／評分形勢」。
                    
                    - **同程強勢評分差（核心）**：找出同程曾經勝出（若無勝仗則用曾入 TopK）時的「最高評分」，若目前評分低於該值代表處於較有利的讓賽形勢。
                    - **同程讓磅差（輔助）**：找出同程最近一次入 TopK 的負磅，若本場負磅較低代表讓磅更有利。
                    - **同程 TopK 率（穩定性）**：同程近 X 日的 TopK 率（可時間衰減），作為穩定性補強；樣本不足時會自動降低影響力。
                    - **場內形勢（缺歷史時仍可用）**：用本場「評分在場內的相對位置」與「同評分下是否較輕磅（場內線性校正後的磅差）」作為保守基準，避免缺歷史直接變 0。
                    - **最後調整**：同場再做百分位標準化成 0–10 分。
                    """)
                    with st.expander("⚙️ 調整時間窗/半衰期/樣本下限/入圍權重 (調整後將即時儲存並重算)", expanded=False):
                        from database.models import SystemConfig

                        cfg = {
                            "window_days": 365,
                            "half_life_days": 180,
                            "min_samples": 5,
                            "place_weight": 0.25,
                            "target_k": 4,
                            "field_weight": 0.25,
                        }
                        config = session.query(SystemConfig).filter_by(key="weight_rating_perf_config").first()
                        if config and isinstance(config.value, dict):
                            v = config.value
                            if "window_days" in v:
                                cfg["window_days"] = int(v["window_days"])
                            if "half_life_days" in v:
                                cfg["half_life_days"] = int(v["half_life_days"])
                            if "min_samples" in v:
                                cfg["min_samples"] = int(v["min_samples"])
                            if "place_weight" in v:
                                cfg["place_weight"] = float(v["place_weight"])
                            if "target_k" in v:
                                cfg["target_k"] = int(v["target_k"])
                            if "field_weight" in v:
                                cfg["field_weight"] = float(v["field_weight"])

                        window_options = {"近 180 日": 180, "近 365 日": 365, "近 730 日": 730, "全部": 0}
                        hl_options = {"半衰期 90 日": 90, "半衰期 180 日": 180, "半衰期 365 日": 365, "不衰減": 0}
                        n_options = {"3": 3, "5": 5, "8": 8, "10": 10}
                        k_options = {"Top3": 3, "Top4": 4, "Top5": 5}

                        cur_window_label = next((k for k, v in window_options.items() if v == cfg["window_days"]), "近 365 日")
                        cur_hl_label = next((k for k, v in hl_options.items() if v == cfg["half_life_days"]), "半衰期 180 日")
                        cur_n_label = next((k for k, v in n_options.items() if v == cfg["min_samples"]), "5")
                        cur_k_label = next((k for k, v in k_options.items() if v == cfg.get("target_k", 4)), "Top4")

                        with st.form("weight_rating_perf_config_form"):
                            c1, c2, c3, c4, c5, c6 = st.columns(6)
                            window_label = c1.selectbox("時間窗", list(window_options.keys()), index=list(window_options.keys()).index(cur_window_label))
                            hl_label = c2.selectbox("時間衰減", list(hl_options.keys()), index=list(hl_options.keys()).index(cur_hl_label))
                            n_label = c3.selectbox("同程樣本下限 N", list(n_options.keys()), index=list(n_options.keys()).index(cur_n_label))
                            k_label = c4.selectbox("TopK 目標", list(k_options.keys()), index=list(k_options.keys()).index(cur_k_label))
                            place_weight = c5.number_input("同程TopK率權重", value=float(cfg["place_weight"]), min_value=0.0, max_value=1.0, step=0.05)
                            field_weight = c6.number_input("場內形勢權重", value=float(cfg.get("field_weight", 0.25)), min_value=0.0, max_value=1.0, step=0.05)

                            submitted = st.form_submit_button("💾 儲存參數並為本場重新計分", type="primary")
                            if submitted:
                                new_cfg = {
                                    "window_days": int(window_options[window_label]),
                                    "half_life_days": int(hl_options[hl_label]),
                                    "min_samples": int(n_options[n_label]),
                                    "place_weight": float(place_weight),
                                    "target_k": int(k_options[k_label]),
                                    "field_weight": float(field_weight),
                                }
                                if not config:
                                    config = SystemConfig(key="weight_rating_perf_config", description="負磅／評分表現：時間窗/半衰期/N/入圍權重")
                                    session.add(config)
                                config.value = new_cfg
                                session.commit()

                                from scoring_engine.core import ScoringEngine
                                engine = ScoringEngine(session)
                                engine.score_race(selected_race_id)
                                st.success(f"參數已儲存：{new_cfg}，並已重新計分。")
                                st.rerun()

                elif selected_factor == "HKJC SpeedPRO 能量分":
                    st.markdown("---")
                    st.markdown("### 💡 演算法說明：HKJC SpeedPRO 能量分")
                    st.markdown("""
                    這個條件用於根據 HKJC「速勢能量表」評估馬匹在本場的能量匹配與狀態。
                    
                    - **資料來源**：HKJC SpeedPRO 速勢能量頁 `https://racing.hkjc.com/zh-hk/local/info/speedpro/speedguide?raceno=X`（頁面顯示用）；系統抓取其背後的官方 JSON：`https://consvc.hkjc.com/-/media/Sites/JCRW/SpeedPro/current/sg_race_X`。
                    - **發佈時間注意**：其中「速勢能量評估」與「狀態評級」較晚發佈，太早抓取容易出現缺欄位或覆蓋不足；系統會在未到可用時間或覆蓋不足時回傳 0 分並顯示原因（避免產生錯誤結果）。
                    - **擷取欄位**：
                      - 能量所需
                      - 狀態評級
                      - 速勢能量評估
                      - 速勢能量評估差值 = 速勢能量評估 - 能量所需
                    - **排序規則**：依「第1優先 → 第2優先 → 第3優先」進行多重排序（同值再以馬號作 tie-break）。
                      - 能量所需：越低越好
                      - 狀態評級：越高越好
                      - 速勢能量評估：越高越好
                      - 差值：越高越好（正數代表評估高於所需，越有利）
                    - **最後調整**：排序結果會轉成原始分，再於同場內標準化成 0–10 分。
                    """)

                    with st.expander("⚙️ 參數調整：3 項排序優先（調整後將即時儲存並重算）", expanded=True):
                        from database.models import SystemConfig

                        options = {
                            "能量所需": "energy_required",
                            "狀態評級": "status_rating",
                            "速勢能量評估": "energy_assess",
                            "速勢能量評估差值": "energy_diff",
                            "（不使用）": "",
                        }
                        reverse_options = {v: k for k, v in options.items()}

                        config = session.query(SystemConfig).filter_by(key="speedpro_energy_sort_priority").first()
                        cur = config.value if config and isinstance(config.value, list) else None
                        if not isinstance(cur, list) or not cur:
                            cur = ["energy_required", "status_rating", "energy_assess"]
                        cur = [str(x) for x in cur][:3]
                        while len(cur) < 3:
                            cur.append("")

                        def _idx(v):
                            lab = reverse_options.get(str(v), "（不使用）")
                            return list(options.keys()).index(lab)

                        with st.form("speedpro_energy_sort_priority_form"):
                            c1, c2, c3 = st.columns(3)
                            p1 = c1.selectbox("第1優先", list(options.keys())[:-1], index=min(_idx(cur[0]), len(list(options.keys())[:-1]) - 1))
                            p2 = c2.selectbox("第2優先", list(options.keys()), index=_idx(cur[1]))
                            p3 = c3.selectbox("第3優先", list(options.keys()), index=_idx(cur[2]))

                            submitted = st.form_submit_button("💾 儲存參數並為本場重新計分", type="primary")
                            if submitted:
                                vals = [options.get(p1, "energy_required"), options.get(p2, ""), options.get(p3, "")]
                                vals = [v for v in vals if v]
                                if len(vals) != len(set(vals)):
                                    st.error("❌ 排序優先不可重複，請重新選擇。")
                                else:
                                    if not config:
                                        config = SystemConfig(key="speedpro_energy_sort_priority", description="SpeedPRO 能量分：排序優先 (最多 3 項)")
                                        session.add(config)
                                    config.value = vals
                                    session.commit()

                                    from scoring_engine.core import ScoringEngine
                                    engine = ScoringEngine(session)
                                    engine.score_race(selected_race_id)
                                    st.success(f"參數已儲存：{vals}，並已重新計分。")
                                    st.rerun()

                elif selected_factor == "檔位偏差 (官方 Draw Statistics)":
                    st.markdown("---")
                    st.markdown("### 💡 演算法說明：檔位偏差 (官方 Draw Statistics)")
                    st.markdown("""
                    這個條件用於評估馬匹排在該檔位是否具有統計上的優勢。
                    
                    - **數據來源**：系統於抓取當日排位時，會同步從馬會官方抓取當日各場次（同場地、同路程、同跑道）的檔位歷史統計數據。
                    - **計分公式（偏保守、抗樣本不足）**：
                      1. 先把該檔位的勝率/上名率（或 Top4% 如官方有提供）做先驗平滑：樣本越少越接近先驗。
                      2. 再以本場各檔位的「平滑後最高值」作相對基準，得到相對分（勝率權重＋上名/Top4 權重）。
                      3. 最後套用信心折扣（樣本越少折扣越大），避免小樣本爆分。
                      4. 若該檔位缺少統計資料，則視為同場中性（避免被硬性打成 0）。
                    - **最後調整**：將上述綜合分數在同場賽事中進行百分位標準化，得出 0–10 分。
                    """)

                    from database.models import SystemConfig, Race
                    import pandas as pd

                    race = session.get(Race, selected_race_id)
                    if race and hasattr(race.race_date, "strftime"):
                        race_date_str = race.race_date.strftime("%Y/%m/%d")
                    elif race:
                        race_date_str = str(race.race_date)[:10].replace("-", "/")
                    else:
                        race_date_str = ""

                    config_key = f"draw_stats_{race_date_str}" if race_date_str else ""
                    config = session.query(SystemConfig).filter_by(key=config_key).first() if config_key else None
                    raw = config.value if (config and isinstance(config.value, dict)) else None

                    st.markdown("#### 📌 來源數據檢查")
                    if race_date_str and race:
                        st.markdown(f"- HKJC 檔位統計頁： https://racing.hkjc.com/zh-hk/local/information/draw#race{race.race_no}")
                        st.markdown(f"- 本地暫存 Key：{config_key}")

                    if raw:
                        race_key_str = str(race.race_no) if race else ""
                        stats_list = raw.get(race_key_str) or raw.get(race.race_no) or []
                        if isinstance(stats_list, list) and stats_list:
                            stats_df = pd.DataFrame(stats_list)
                            if "draw" in stats_df.columns:
                                stats_df = stats_df.sort_values(by="draw")

                            show_cols = [c for c in ["draw", "total_runs", "win", "win_rate", "place_rate", "top4_rate"] if c in stats_df.columns]
                            st.dataframe(stats_df[show_cols], use_container_width=True, hide_index=True)

                            chart_cols = [c for c in ["win_rate", "place_rate", "top4_rate"] if c in stats_df.columns]
                            chart_df = stats_df.set_index("draw")[chart_cols] if ("draw" in stats_df.columns and chart_cols) else None
                            if chart_df is not None:
                                st.bar_chart(chart_df, use_container_width=True)
                        else:
                            st.warning("找不到本場次的官方檔位統計（可能尚未爬取或 Key 不匹配）。")
                    else:
                        st.warning("尚未載入當日官方檔位統計，請先執行賽日資料抓取後再檢查。")

                    st.markdown("---")
                    st.markdown("#### ⚙️ 因子參數（偏保守，調整後將即時儲存並重算本場）")
                    cfg = {
                        "win_w": 0.4,
                        "place_w": 0.6,
                        "confidence_runs": 50.0,
                        "prior_strength": 50.0,
                        "prior_win_rate": 8.0,
                        "prior_place_rate": 28.0,
                        "use_top4_if_available": True,
                    }
                    config2 = session.query(SystemConfig).filter_by(key="draw_stats_factor_config").first()
                    if config2 and isinstance(config2.value, dict):
                        v2 = config2.value
                        if "win_w" in v2:
                            cfg["win_w"] = float(v2["win_w"])
                        if "place_w" in v2:
                            cfg["place_w"] = float(v2["place_w"])
                        if "confidence_runs" in v2:
                            cfg["confidence_runs"] = float(v2["confidence_runs"])
                        if "prior_strength" in v2:
                            cfg["prior_strength"] = float(v2["prior_strength"])
                        if "prior_win_rate" in v2:
                            cfg["prior_win_rate"] = float(v2["prior_win_rate"])
                        if "prior_place_rate" in v2:
                            cfg["prior_place_rate"] = float(v2["prior_place_rate"])
                        if "use_top4_if_available" in v2:
                            cfg["use_top4_if_available"] = bool(v2["use_top4_if_available"])

                    with st.form("draw_stats_factor_config_form"):
                        c1, c2, c3, c4 = st.columns(4)
                        win_w = c1.number_input("勝率權重", value=float(cfg["win_w"]), min_value=0.0, max_value=1.0, step=0.05)
                        place_w = c2.number_input("上名/Top4 權重", value=float(cfg["place_w"]), min_value=0.0, max_value=1.0, step=0.05)
                        prior_strength = c3.number_input("先驗強度(等價樣本)", value=float(cfg["prior_strength"]), min_value=0.0, max_value=500.0, step=1.0)
                        confidence_runs = c4.number_input("信心折扣(越大越保守)", value=float(cfg["confidence_runs"]), min_value=0.0, max_value=500.0, step=1.0)

                        c5, c6, c7 = st.columns(3)
                        prior_win_rate = c5.number_input("先驗勝率(%)", value=float(cfg["prior_win_rate"]), min_value=0.0, max_value=100.0, step=0.5)
                        prior_place_rate = c6.number_input("先驗上名/Top4(%)", value=float(cfg["prior_place_rate"]), min_value=0.0, max_value=100.0, step=0.5)
                        use_top4_if_available = c7.checkbox("若有 Top4% 則優先使用", value=bool(cfg["use_top4_if_available"]))

                        submitted = st.form_submit_button("💾 儲存參數並為本場重新計分", type="primary")
                        if submitted:
                            new_cfg = {
                                "win_w": float(win_w),
                                "place_w": float(place_w),
                                "confidence_runs": float(confidence_runs),
                                "prior_strength": float(prior_strength),
                                "prior_win_rate": float(prior_win_rate),
                                "prior_place_rate": float(prior_place_rate),
                                "use_top4_if_available": bool(use_top4_if_available),
                            }
                            if not config2:
                                config2 = SystemConfig(key="draw_stats_factor_config", description="檔位偏差：先驗/信心/權重")
                                session.add(config2)
                            config2.value = new_cfg
                            session.commit()

                            from scoring_engine.core import ScoringEngine
                            engine = ScoringEngine(session)
                            engine.score_race(selected_race_id)
                            st.success(f"參數已儲存：{new_cfg}，並已重新計分。")
                            st.rerun()
                    
            else:
                st.warning("未找到計分條件數據。")
                
        with tab2:
            st.markdown("### 全局因子得分總表")
            st.markdown(f"包含所有馬匹在 {len(available_factors)} 個條件下的原始計算得分（0-10分）。")
            
            # 總表按馬號排序
            full_df = df.sort_values(by="馬號").reset_index(drop=True)
            full_df.insert(0, "序", range(1, len(full_df) + 1))
            preferred_cols = [
                "序",
                "馬號",
                "馬名",
                "檔位",
                "負磅",
                "評分",
                "總分(落庫)",
                "總分(全局權重)",
            ]
            head_cols = [c for c in preferred_cols if c in full_df.columns]
            tail_cols = [c for c in full_df.columns if c not in head_cols]
            full_df = full_df[head_cols + tail_cols]
            st.dataframe(full_df, use_container_width=True, hide_index=True)

        with tab3:
            st.markdown("### 🤖 AI 賽事前瞻 (FormGuide)")
            st.markdown("基於馬會官方 SpeedPRO 賽績指引的文字評述、步速、意外與走位等質化數據，交由 AI 模型綜合分析，找出可原諒的落敗黑馬，並預測今場形勢。")
            
            # Check if FormGuide data exists
            race = session.get(Race, selected_race_id)
            race_date_str = race.race_date.strftime("%Y/%m/%d") if hasattr(race.race_date, "strftime") else str(race.race_date)[:10].replace("-", "/")
            fg_key = f"speedpro_formguide:{race_date_str}:{race.race_no}"
            from database.models import SystemConfig
            fg_cfg = session.query(SystemConfig).filter_by(key=fg_key).first()
            
            if not fg_cfg or not fg_cfg.value:
                st.warning(f"⚠️ 尚未抓取到本場（{race_date_str} R{race.race_no}）的 FormGuide 賽績指引數據。")
                st.info("💡 請前往「數據管理」頁面，執行「一鍵完整更新」來抓取最新數據。")
            else:
                from scoring_engine.ai_advisor import load_ai_api_key
                api_key_info = load_ai_api_key(session)
                api_key = api_key_info.get("env") or api_key_info.get("stored")
                
                if not api_key:
                    st.error("❌ 尚未設定 AI API Key，無法生成報告。")
                    st.info("💡 請前往「系統維護 -> AI 參數設定」設定 API Key。")
                else:
                    if st.button("✨ 立即生成 / 重新生成 AI 賽事總結", type="primary", use_container_width=True):
                        with st.spinner("🤖 AI 正在閱讀各駒近仗走勢與評述，請稍候（約需 10-20 秒）..."):
                            from scoring_engine.ai_advisor import run_ai_race_summary
                            res = run_ai_race_summary(session, selected_race_id)
                            if res.get("ok"):
                                st.session_state[f"ai_summary_{selected_race_id}"] = res.get("summary")
                            else:
                                st.error(f"生成失敗: {res.get('reason')} - {res.get('error')}")
                    
                    if f"ai_summary_{selected_race_id}" in st.session_state:
                        st.markdown("---")
                        st.markdown("#### 📜 AI 賽事分析報告")
                        st.info(st.session_state[f"ai_summary_{selected_race_id}"])

session.close()
