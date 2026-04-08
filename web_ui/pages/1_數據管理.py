import streamlit as st
import os
import subprocess
import sys
from pathlib import Path

# 加入專案路徑
root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import get_session, init_db
from scoring_engine.core import ScoringEngine

st.set_page_config(page_title="數據管理 - HKJC Analytics", layout="wide")

def trigger_scraper():
    """實時日誌串流輸出"""
    st.markdown("### 🚀 爬蟲執行進度")
    log_placeholder = st.empty() 
    full_log = ""
    try:
        env = os.environ.copy()
        process = subprocess.Popen(
            ["python3", "scripts/run_scraper.py"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=env, bufsize=1
        )
        for line in iter(process.stdout.readline, ""):
            full_log += line
            log_placeholder.code(full_log)
        process.stdout.close()
        return process.wait() == 0
    except Exception as e:
        st.error(f"❌ 系統錯誤: {e}")
        return False

def trigger_history_backfill():
    """歷史數據回填進度"""
    st.markdown("### 📚 歷史數據回填進度")
    log_placeholder = st.empty()
    full_log = ""
    try:
        env = os.environ.copy()
        process = subprocess.Popen(
            ["python3", "scripts/fetch_history.py"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=env, bufsize=1
        )
        for line in iter(process.stdout.readline, ""):
            full_log += line
            log_placeholder.code(full_log)
        process.stdout.close()
        return process.wait() == 0
    except Exception as e:
        st.error(f"❌ 系統錯誤: {e}")
        return False

def clear_database(session):
    """清空資料庫"""
    try:
        from database.models import Race, Horse, Jockey, Trainer, RaceEntry, ScoringFactor, RaceResult, HorseHistory
        session.query(ScoringFactor).delete()
        session.query(RaceResult).delete()
        session.query(RaceEntry).delete()
        session.query(HorseHistory).delete()
        session.query(Race).delete()
        session.query(Horse).delete()
        session.query(Jockey).delete()
        session.query(Trainer).delete()
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        st.error(f"❌ 清空失敗: {e}")
        return False

st.title("🛠️ 數據管理後台")
st.markdown("在此頁面執行數據更新、回填與清理操作。")

col1, col2 = st.columns(2)

with col1:
    st.subheader("📡 即時抓取")
    if st.button("🔄 更新當日賽事數據", use_container_width=True):
        if trigger_scraper():
            st.success("✅ 當日數據更新成功！")

    st.subheader("📚 歷史回填")
    if st.button("📚 回填馬匹歷史往績", use_container_width=True):
        if trigger_history_backfill():
            st.success("✅ 歷史往績回填完成！")

with col2:
    st.subheader("🧹 系統清理")
    if st.button("🗑️ 清空資料庫所有數據", use_container_width=True):
        session = get_session()
        if clear_database(session):
            st.success("✅ 資料庫已完全清空！")
        session.close()

    st.subheader("🔌 系統測試與升級")
    if st.button("🔌 測試資料庫連線", use_container_width=True):
        session = get_session()
        try:
            from database.models import ScoringWeight
            count = session.query(ScoringWeight).count()
            st.success(f"✅ 連線正常 (權重紀錄: {count})")
        except Exception as e:
            st.error(f"❌ 連線失敗: {e}")
        session.close()
        
    if st.button("🆙 執行資料庫欄位升級 (新增原始數據欄位)", use_container_width=True):
        try:
            env = os.environ.copy()
            process = subprocess.Popen(
                ["python3", "scripts/upgrade_db.py"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, env=env, bufsize=1
            )
            out, _ = process.communicate()
            if process.returncode == 0:
                st.success(f"✅ 升級腳本執行完成！\n\n```\n{out}\n```")
            else:
                st.error(f"❌ 執行失敗: {out}")
        except Exception as e:
            st.error(f"❌ 系統錯誤: {e}")
