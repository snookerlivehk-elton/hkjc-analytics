import streamlit as st
import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime
import json
import sys
from pathlib import Path

# 加入專案路徑
root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import get_session
from database.models import SystemConfig, Race

st.set_page_config(page_title="AI 報告總覽 - HKJC Analytics", page_icon="🤖", layout="wide")

st.markdown("## 🤖 AI 賽事報告總覽")
st.markdown("在此檢視所有已生成的 AI 賽前分析報告歷史紀錄。")

session = get_session()
try:
    # 撈取所有報告
    reports = session.query(SystemConfig).filter(SystemConfig.key.like("ai_race_report:%")).all()
    
    if not reports:
        st.info("目前尚未有任何 AI 分析報告紀錄。")
    else:
        # 解析數據
        data = []
        for r in reports:
            parts = r.key.split(":")
            if len(parts) >= 3:
                date_str = parts[1]
                race_no = parts[2]
                
                # Extract creation time if available
                created_at = ""
                if isinstance(r.value, dict) and "created_at" in r.value:
                    try:
                        dt = datetime.fromisoformat(r.value["created_at"])
                        created_at = dt.strftime("%Y/%m/%d %H:%M:%S")
                    except:
                        pass
                        
                data.append({
                    "Date": date_str,
                    "RaceNo": int(race_no),
                    "Key": r.key,
                    "Created": created_at,
                    "Value": r.value
                })
                
        if data:
            df = pd.DataFrame(data).sort_values(by=["Date", "RaceNo"], ascending=[False, True])
            
            # Sidebar filters
            st.sidebar.markdown("### 🔍 篩選報告")
            dates = ["全部"] + list(df["Date"].unique())
            selected_date = st.sidebar.selectbox("選擇賽事日期", dates)
            
            if selected_date != "全部":
                df = df[df["Date"] == selected_date]
                
            races = ["全部"] + sorted(list(df["RaceNo"].unique()))
            selected_race = st.sidebar.selectbox("選擇場次", races)
            
            if selected_race != "全部":
                df = df[df["RaceNo"] == selected_race]
                
            st.markdown(f"共找到 **{len(df)}** 份報告。")
            
            # Display reports
            for idx, row in df.iterrows():
                with st.expander(f"📅 {row['Date']} 第 {row['RaceNo']} 場 (建立於: {row['Created']})", expanded=False):
                    val = row["Value"]
                    if isinstance(val, dict) and "report" in val:
                        st.markdown(val["report"])
                    else:
                        st.info("報告內容格式無法解析。")
                        
finally:
    session.close()
