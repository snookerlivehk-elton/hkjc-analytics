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
from database.models import Race, RaceEntry, ScoringFactor, ScoringWeight, Horse
from scoring_engine.core import ScoringEngine
from utils.logger import logger

# 設定頁面配置
st.set_page_config(page_title="HKJC 每場賽事獨立計分排名系統", layout="wide")

# 初始化資料庫 (確保在雲端環境表結構存在)
init_db()

def get_db():
    return get_session()

def load_races(session: Session):
    """載入所有可選賽事"""
    return session.query(Race).order_by(Race.race_date.desc(), Race.race_no.asc()).all()

def load_scoring_data(session: Session, race_id: int):
    """載入特定賽事的計分結果數據"""
    entries = session.query(RaceEntry).filter_by(race_id=race_id).all()
    data = []
    for entry in entries:
        row = {
            "馬號": entry.horse_no,
            "馬名": entry.horse.name_ch if entry.horse else "未知",
            "馬匹編號": entry.horse.code if entry.horse else "",
            "總分": round(entry.total_score, 2) if entry.total_score else 0,
            "預估勝率": f"{round(entry.win_probability * 100, 1)}%" if entry.win_probability else "0%",
            "排名": entry.rank_in_race,
            "騎師": entry.jockey.name_ch if entry.jockey else "",
            "練馬師": entry.trainer.name_ch if entry.trainer else "",
            "檔位": entry.draw,
            "負磅": entry.actual_weight,
            "評分": entry.rating
        }
        # 載入個別因子分數
        factors = session.query(ScoringFactor).filter_by(entry_id=entry.id).all()
        for f in factors:
            row[f.factor_name] = round(f.score, 1)
        data.append(row)
    
    df = pd.DataFrame(data)
    if not df.empty:
        df = df.sort_values("排名")
    return df

def main():
    st.title("🏇 HKJC 每場賽事獨立計分排名系統")
    st.markdown("---")

    session = get_db()
    
    # Sidebar: 賽事選擇
    st.sidebar.header("🔍 賽事選擇")
    races = load_races(session)
    if not races:
        st.sidebar.warning("資料庫中尚無賽事數據，請先執行抓取與計分。")
        return

    race_options = {f"{r.race_date.strftime('%Y-%m-%d')} | 第 {r.race_no} 場 | {r.venue}": r.id for r in races}
    selected_race_label = st.sidebar.selectbox("選擇賽事日期與場次", list(race_options.keys()))
    selected_race_id = race_options[selected_race_label]

    # Sidebar: 權重動態調整 (可折疊)
    with st.sidebar.expander("⚙️ 權重配置 (動態調整)"):
        weights = session.query(ScoringWeight).filter_by(is_active=True).all()
        updated_weights = {}
        for w in weights:
            updated_weights[w.factor_name] = st.slider(f"{w.description}", 0.0, 5.0, float(w.weight), 0.1)
        
        if st.button("重新計算排名"):
            # 更新權重並重新計算
            for w in weights:
                w.weight = updated_weights[w.factor_name]
            session.commit()
            
            engine = ScoringEngine(session)
            engine.score_race(selected_race_id)
            st.success("排名已根據新權重重新計算！")
            st.rerun()

    # 主面板：賽事資訊
    race = session.query(Race).get(selected_race_id)
    st.subheader(f"📊 賽事詳情: {selected_race_label}")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("場地", race.venue)
    col2.metric("班次", race.race_class or "N/A")
    col3.metric("路程", f"{race.distance}m")
    col4.metric("場地狀況", race.going or "未知")

    # 數據加載與顯示
    df = load_scoring_data(session, selected_race_id)
    if df.empty:
        st.info("本場賽事尚未進行計分運算，點擊下方按鈕開始。")
        if st.button("立即執行計分"):
            engine = ScoringEngine(session)
            engine.score_race(selected_race_id)
            st.rerun()
    else:
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
