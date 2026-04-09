import streamlit as st
import pandas as pd
import sys
from pathlib import Path
from sqlalchemy.orm import Session

# 加入專案路徑
root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import get_session
from database.models import Race, RaceEntry, ScoringFactor, ScoringWeight

st.set_page_config(page_title="獨立條件分析 - HKJC Analytics", layout="wide")

def load_races(session: Session):
    return session.query(Race).order_by(Race.race_date.desc(), Race.race_no.asc()).all()

def load_factor_data(session: Session, race_id: int):
    entries = session.query(RaceEntry).filter_by(race_id=race_id).all()
    if not entries:
        return None
        
    weights = session.query(ScoringWeight).filter_by(is_active=True).all()
    factor_desc_map = {w.factor_name: w.description for w in weights}
    
    data = []
    for entry in entries:
        row = {
            "馬號": entry.horse_no,
            "馬名": entry.horse.name_ch if entry.horse else "未知",
            "檔位": entry.draw,
            "負磅": entry.actual_weight,
            "評分": entry.rating,
            "總分": round(entry.total_score, 2) if entry.total_score else 0,
        }
        
        factors = session.query(ScoringFactor).filter_by(entry_id=entry.id).all()
        for f in factors:
            # 儲存分數，使用中文描述作為欄位名
            desc = factor_desc_map.get(f.factor_name, f.factor_name)
            row[desc] = round(f.score, 2)
            row[f"{desc}_raw"] = f.raw_data_display if f.raw_data_display else "無數據"
            
        data.append(row)
        
    return pd.DataFrame(data), list(factor_desc_map.values())

st.title("📊 獨立條件分析")
st.markdown("在此頁面檢視每場賽事 17 個計分條件的獨立運算結果與排名。")

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
                
                if cols[j].button(btn_label, key=f"factor_race_btn_{r.id}", type=btn_type, width="stretch"):
                    st.session_state.factor_selected_race_id = r.id
                    st.rerun()

    selected_race_id = st.session_state.factor_selected_race_id

    result = load_factor_data(session, selected_race_id)
    
    if not result or result[0].empty:
        st.info("本場賽事尚未進行計分運算，請先回主頁面點擊「立即執行計分」。")
    else:
        df, factor_columns = result
        
        # 創建 Tabs 顯示不同的視圖
        tab1, tab2 = st.tabs(["🗂️ 獨立條件分頁檢視", "📋 全局數據總覽"])
        
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
                            if cols[j].button(factor, key=f"btn_{factor}", type=button_type, width="stretch"):
                                st.session_state.selected_factor = factor
                                st.rerun()

                selected_factor = st.session_state.selected_factor
                st.markdown("---")
                st.markdown(f"#### 📌 目前檢視：{selected_factor}")
                
                if selected_factor in ("騎練與本駒合作 (近X次)", "騎師＋馬匹組合"):
                    from database.models import SystemConfig, RaceEntry, ScoringFactor

                    cfg = session.query(SystemConfig).filter_by(key="jt_horse_bond_config").first()
                    win_w, place_w, window = 0.7, 0.3, 0
                    if cfg and isinstance(cfg.value, dict):
                        win_w = float(cfg.value.get("win", win_w))
                        place_w = float(cfg.value.get("place", place_w))
                        window = int(cfg.value.get("window", window))

                    if win_w < 0:
                        win_w = 0.0
                    if place_w < 0:
                        place_w = 0.0
                    total_w = win_w + place_w
                    if total_w <= 0:
                        win_w, place_w, total_w = 0.7, 0.3, 1.0
                    win_w /= total_w
                    place_w /= total_w
                    if window < 0:
                        window = 0

                    expected_window_label = f"近{window}" if window > 0 else "最大"
                    expected_weight_label = f"權重 {win_w:.2f}/{place_w:.2f}"

                    sample = (
                        session.query(ScoringFactor.raw_data_display)
                        .join(RaceEntry, RaceEntry.id == ScoringFactor.entry_id)
                        .filter(
                            RaceEntry.race_id == selected_race_id,
                            ScoringFactor.factor_name == "jockey_horse_bond",
                        )
                        .first()
                    )

                    needs_rescore = (
                        (not sample)
                        or (not sample[0])
                        or (expected_window_label not in sample[0])
                        or (expected_weight_label not in sample[0])
                    )
                    auto_key = f"auto_rescore_jockey_horse_bond_{selected_race_id}_{expected_window_label}_{expected_weight_label}"
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
                factor_df.index = factor_df.index + 1
                factor_df.index.name = "該項排名"
                
                # 設定樣式：高亮第一名
                def highlight_first(s):
                    return ['background-color: rgba(40, 167, 69, 0.2)'] * len(s) if s.name == 1 else [''] * len(s)
                
                st.dataframe(
                    factor_df.style.apply(highlight_first, axis=1),
                    width="stretch"
                )
                
                # 針對特定因子顯示詳細說明與參數調整
                if selected_factor == "近期狀態 (Last 6 Runs)":
                    st.markdown("---")
                    st.markdown("### 💡 演算法說明：近期狀態 (Last 6 Runs)")
                    st.markdown("""
                    這個條件用於評估馬匹最近 6 場比賽的**「時間衰減加權平均名次」**。
                    
                    **為什麼要用加權平均？**
                    因為賽馬最重要的就是「當下狀態」。一匹最近一場跑第 1 名的馬，其狀態絕對比半年前跑第 1 名的馬更好。
                    因此，我們賦予**越近期的比賽越高的權重**。
                    
                    **計分公式：**
                    1. 系統會抓取馬匹最近 6 場有效名次 (忽略退出等異常紀錄)。
                    2. 將每場名次乘上對應的權重 (預設最近一場為 6，最遠一場為 1)。
                    3. 算出加權平均名次。加權平均名次越小（越接近 1），代表狀態越好。
                    4. 引擎會將這場比賽所有馬匹的加權平均名次進行百分位標準化，最優秀者得 10 分。
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
                
                if selected_factor == "騎師＋練馬師合作 (同路程/場地)":
                    st.markdown("---")
                    st.markdown("### 💡 演算法說明：騎師＋練馬師合作 (同路程/場地)")
                    st.markdown("""
                    這個條件用於衡量「本場騎師＋練馬師」在歷史往績資料中的合作表現（目前以合作樣本中的勝出率與前 3 名入圍率計算）。
                    
                    - 勝出率 = 冠軍次數 / 合作總次數
                    - 入圍率(前3) = 前3名次數 / 合作總次數
                    - 原始分 = 勝出率 × 勝率權重 + 入圍率 × 入圍權重
                    - 最後會在同一場內做相對百分位標準化成 0–10 分
                    """)
                    
                    with st.expander("⚙️ 調整勝率/入圍率權重 (調整後將即時儲存並重算)"):
                        from database.models import SystemConfig
                        
                        config = session.query(SystemConfig).filter_by(key="jt_bond_weights").first()
                        if config and isinstance(config.value, dict):
                            current_win_w = float(config.value.get("win", 0.7))
                            current_place_w = float(config.value.get("place", 0.3))
                        elif config and isinstance(config.value, list) and len(config.value) == 2:
                            current_win_w = float(config.value[0])
                            current_place_w = float(config.value[1])
                        else:
                            current_win_w, current_place_w = 0.7, 0.3
                        
                        st.markdown("可把其中一項設為 0 以停用該項（例如只用勝率或只用入圍率）。系統會自動把兩者正規化成總和 = 1。")
                        
                        with st.form("jt_bond_weights_form"):
                            col_a, col_b = st.columns(2)
                            win_w = col_a.number_input("勝率權重", value=current_win_w, min_value=0.0, max_value=1.0, step=0.05)
                            place_w = col_b.number_input("入圍率(前3)權重", value=current_place_w, min_value=0.0, max_value=1.0, step=0.05)
                            
                            submitted = st.form_submit_button("💾 儲存參數並為本場重新計分", type="primary")
                            if submitted:
                                if not config:
                                    config = SystemConfig(key="jt_bond_weights", description="騎師＋練馬師合作 (J/T Bond) 勝率與入圍率權重")
                                    session.add(config)
                                config.value = {"win": float(win_w), "place": float(place_w)}
                                session.commit()
                                
                                from scoring_engine.core import ScoringEngine
                                engine = ScoringEngine(session)
                                engine.score_race(selected_race_id)
                                
                                st.success(f"權重已儲存為 勝率={win_w}、入圍率={place_w}，並已重新計算分數！")
                                st.rerun()

                if selected_factor in ("騎練與本駒合作 (近X次)", "騎師＋馬匹組合"):
                    st.markdown("---")
                    st.markdown("### 💡 演算法說明：騎練與本駒合作 (近X次)")
                    st.markdown("""
                    這個條件用於衡量「本場騎師＋練馬師」在**同一匹馬**身上的合作表現。
                    
                    - 只統計本駒歷史往績中，與本場相同騎師＋練馬師的合作紀錄
                    - 可設定只取「近 5 次 / 近 10 次 / 最大(全部)」合作樣本
                    - 原始分 = 勝出率 × 勝率權重 + 入圍率(前3) × 入圍權重
                    - 最後會在同一場內做相對百分位標準化成 0–10 分
                    """)
                    
                    with st.expander("⚙️ 調整近X次樣本 + 勝率/入圍率權重 (調整後將即時儲存並重算)"):
                        from database.models import SystemConfig
                        
                        config = session.query(SystemConfig).filter_by(key="jt_horse_bond_config").first()
                        if config and isinstance(config.value, dict):
                            current_win_w = float(config.value.get("win", 0.7))
                            current_place_w = float(config.value.get("place", 0.3))
                            current_window = int(config.value.get("window", 0))
                        else:
                            current_win_w, current_place_w, current_window = 0.7, 0.3, 0
                        
                        window_options = {
                            "近 5 次": 5,
                            "近 10 次": 10,
                            "最大 (全部)": 0
                        }
                        current_label = next((k for k, v in window_options.items() if v == current_window), "最大 (全部)")
                        
                        with st.form("jt_horse_bond_config_form"):
                            col_a, col_b, col_c = st.columns(3)
                            window_label = col_a.selectbox("合作樣本範圍", list(window_options.keys()), index=list(window_options.keys()).index(current_label))
                            win_w = col_b.number_input("勝率權重", value=current_win_w, min_value=0.0, max_value=1.0, step=0.05)
                            place_w = col_c.number_input("入圍率(前3)權重", value=current_place_w, min_value=0.0, max_value=1.0, step=0.05)
                            
                            submitted = st.form_submit_button("💾 儲存參數並為本場重新計分", type="primary")
                            if submitted:
                                new_cfg = {"window": int(window_options[window_label]), "win": float(win_w), "place": float(place_w)}
                                if not config:
                                    config = SystemConfig(key="jt_horse_bond_config", description="騎練與本駒合作：近X次樣本 + 勝率/入圍率權重")
                                    session.add(config)
                                config.value = new_cfg
                                session.commit()
                                
                                from scoring_engine.core import ScoringEngine
                                engine = ScoringEngine(session)
                                engine.score_race(selected_race_id)
                                
                                st.success(f"參數已儲存為 {new_cfg}，並已重新計算分數！")
                                st.rerun()

            else:
                st.warning("未找到計分條件數據。")
                
        with tab2:
            st.markdown("### 全局因子得分總表")
            st.markdown("包含所有馬匹在 17 個條件下的原始計算得分（0-10分）。")
            
            # 總表按馬號排序
            full_df = df.sort_values(by="馬號").reset_index(drop=True)
            full_df.index = full_df.index + 1
            st.dataframe(full_df, width="stretch")

session.close()
