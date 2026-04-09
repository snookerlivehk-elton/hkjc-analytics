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
    race_options = {f"{r.race_date.strftime('%Y-%m-%d')} | 第 {r.race_no} 場 | {r.venue}": r.id for r in races}
    selected_race_label = st.sidebar.selectbox("選擇賽事", list(race_options.keys()))
    selected_race_id = race_options[selected_race_label]

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
