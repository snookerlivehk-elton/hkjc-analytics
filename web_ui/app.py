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
import asyncio
import subprocess

# 設定頁面配置
st.set_page_config(page_title="HKJC 每場賽事獨立計分排名系統", layout="wide")

# 初始化資料庫 (確保在雲端環境表結構存在)
init_db()

def get_db():
    return get_session()

import os

def trigger_scraper():
    """使用獨立進程執行抓取任務，並傳遞環境變數"""
    with st.spinner("🚀 正在啟動雲端爬蟲，請勿關閉網頁..."):
        try:
            # 獲取目前環境變數 (包含 DATABASE_URL)
            env = os.environ.copy()
            # 改用 python3 以符合 Linux 伺服器標準
            result = subprocess.run(
                ["python3", "scripts/run_scraper.py"],
                capture_output=True,
                text=True,
                timeout=300,
                env=env
            )
            
            # 顯示抓取日誌供除錯
            if result.stdout:
                with st.expander("查看抓取日誌"):
                    st.code(result.stdout)
            
            if result.returncode == 0:
                st.success("✅ 執行完成！請檢查下方數據狀態。")
                return True
            else:
                st.error(f"❌ 抓取失敗！詳細錯誤：\n{result.stderr}")
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
        test_race = session.query(Race).filter_by(race_id="TEST-RACE-1").first()
        if test_race:
            # 1. 找到所有關聯的 Entry IDs
            entry_ids = [e.id for e in test_race.entries]
            if entry_ids:
                # 2. 由下而上刪除所有關聯數據
                session.query(ScoringFactor).filter(ScoringFactor.entry_id.in_(entry_ids)).delete(synchronize_session=False)
                session.query(RaceResult).filter(RaceResult.entry_id.in_(entry_ids)).delete(synchronize_session=False)
                session.query(RaceEntry).filter(RaceEntry.race_id == test_race.id).delete(synchronize_session=False)
            # 3. 刪除賽事本身
            session.delete(test_race)
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

def clear_database(session):
    """清空資料庫中所有賽事相關數據"""
    try:
        from database.models import Race, Horse, Jockey, Trainer, RaceEntry, ScoringFactor, RaceResult
        session.query(ScoringFactor).delete()
        session.query(RaceResult).delete()
        session.query(RaceEntry).delete()
        session.query(Race).delete()
        session.query(Horse).delete()
        session.query(Jockey).delete()
        session.query(Trainer).delete()
        session.commit()
        st.sidebar.success("✅ 資料庫已清空！")
        return True
    except Exception as e:
        session.rollback()
        st.sidebar.error(f"❌ 清空失敗: {e}")
        return False

def load_races(session: Session):
    """載入所有可選賽事"""
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
    st.sidebar.header("🔍 賽事管理")
    
    # 新增：抓取/更新按鈕
    if st.sidebar.button("🔄 更新當日賽事數據"):
        if trigger_scraper():
            st.rerun()
    
    if st.sidebar.button("🔌 測試資料庫連線"):
        test_db_connection(session)
    
    if st.sidebar.button("📝 生成一筆測試數據"):
        if create_dummy_data(session):
            st.rerun()
    
    if st.sidebar.button("🗑️ 清空資料庫數據"):
        if clear_database(session):
            st.rerun()
    
    st.sidebar.markdown("---")
    
    # 顯示資料庫狀態
    st.sidebar.subheader("📊 數據取得狀態")
    status = get_db_status(session)
    for label, count in status.items():
        color = "green" if count > 0 else "red"
        st.sidebar.markdown(f"{label}: :{color}[{count}]")
    
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
