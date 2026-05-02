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

st.markdown("## 🤖 AI 賽事報告總覽與進化引擎")
st.markdown("在此檢視所有已生成的 AI 賽前分析報告歷史紀錄，以及透過賽果反思提煉的「黃金法則」。")

tab1, tab2 = st.tabs(["📜 賽前分析報告", "🧠 賽後反思與進化法則"])

session = get_session()
try:
    with tab1:
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
                selected_date = st.sidebar.selectbox("選擇賽事日期", dates, key="tab1_date")
                
                if selected_date != "全部":
                    df = df[df["Date"] == selected_date]
                    
                races = ["全部"] + sorted(list(df["RaceNo"].unique()))
                selected_race = st.sidebar.selectbox("選擇場次", races, key="tab1_race")
                
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

    with tab2:
        st.markdown("### 💡 系統學習到的黃金法則")
        from scoring_engine.ai_reflection import get_learned_rules, generate_race_reflection
        
        learned_rules = get_learned_rules(session)
        if learned_rules:
            for i, r in enumerate(learned_rules, 1):
                st.info(f"**法則 {i}:** {r}")
        else:
            st.warning("目前尚未學習到任何法則。請先執行賽後反思。")
            
        st.markdown("---")
        st.markdown("### 🔄 執行賽後反思")
        st.write("請選擇已經有賽果（且已有賽前 AI 報告）的賽事，讓 AI 對比預測與實際結果，提煉新法則。")
        
        # Get races that have both a report and results
        races = session.query(Race).order_by(Race.race_date.desc(), Race.race_no).limit(100).all()
        race_opts = {}
        for r in races:
            date_str = r.race_date.strftime("%Y/%m/%d")
            report_key = f"ai_race_report:{date_str}:{r.race_no}"
            report_cfg = session.query(SystemConfig).filter_by(key=report_key).first()
            if report_cfg:
                # Check if reflection exists
                reflection_key = f"ai_race_reflection:{date_str}:{r.race_no}"
                ref_cfg = session.query(SystemConfig).filter_by(key=reflection_key).first()
                status = "✅ 已反思" if ref_cfg else "⏳ 待反思"
                race_opts[r.id] = f"{date_str} 第 {r.race_no} 場 [{status}]"
                
        if not race_opts:
            st.info("找不到有 AI 報告的近期賽事。")
        else:
            sel_race_id = st.selectbox("選擇賽事", options=list(race_opts.keys()), format_func=lambda x: race_opts[x])
            
            if st.button("🧠 立即執行 AI 賽後檢討與反思", type="primary"):
                with st.spinner("AI 正在深度檢討預測落差並提煉法則 (約需 20-30 秒)..."):
                    res = generate_race_reflection(session, sel_race_id)
                    if res.get("ok"):
                        if res.get("reason") == "already_reflected":
                            st.success("✅ 此場賽事之前已經反思過。")
                            st.markdown("#### 檢討內容")
                            st.write(res.get("reflection"))
                        else:
                            st.success("🎉 反思完成！已提煉新法則並加入系統知識庫。")
                            st.markdown("#### 檢討內容")
                            st.write(res.get("reflection"))
                            st.markdown("#### 新增法則")
                            for r in res.get("learned_rules", []):
                                st.success(f"- {r}")
                    else:
                        err_reason = res.get("reason")
                        if err_reason == "no_results":
                            st.error("❌ 找不到此場賽事的真實賽果（Top 4），無法進行反思。")
                        elif err_reason == "no_pre_race_report":
                            st.error("❌ 找不到此場賽事的賽前 AI 報告。")
                        else:
                            st.error(f"❌ 反思失敗: {err_reason} ({res.get('error')})")

                        
finally:
    session.close()
