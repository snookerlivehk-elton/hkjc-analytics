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
                
                if cols[j].button(btn_label, key=f"factor_race_btn_{r.id}", type=btn_type, use_container_width=True):
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
                
                # 提取基本資訊與該因子的分數
                race = session.query(Race).get(selected_race_id)
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
                    use_container_width=True
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

            else:
                st.warning("未找到計分條件數據。")
                
        with tab2:
            st.markdown("### 全局因子得分總表")
            st.markdown("包含所有馬匹在 17 個條件下的原始計算得分（0-10分）。")
            
            # 總表按馬號排序
            full_df = df.sort_values(by="馬號").reset_index(drop=True)
            full_df.index = full_df.index + 1
            st.dataframe(full_df, use_container_width=True)

session.close()
