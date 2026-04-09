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

                    cfg = {"rest_days": 90}
                    config = session.query(SystemConfig).filter_by(key="debut_long_rest_config").first()
                    if config and isinstance(config.value, dict):
                        v = config.value
                        if "rest_days" in v:
                            cfg["rest_days"] = int(v["rest_days"])

                    expected = f"R{cfg['rest_days']}d"
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
                
                if selected_factor in ("騎師＋練馬師合作 (同路程/場地)", "騎師＋練馬師合作 (不論馬匹)", "騎師＋練馬師合作 (綜合)"):
                    st.markdown("---")
                    st.markdown(f"### 💡 演算法說明：騎師＋練馬師合作 (綜合)")
                    st.markdown('''
                    這個條件用於綜合衡量「騎師＋練馬師」的合作表現，分為「全庫歷史合作」與「本駒合作」兩個維度：
                    
                    - **全庫合作**：不論馬匹，該騎練組合的歷史勝率與入圍率。
                    - **本駒合作**：專指該騎練組合在「本場這匹馬」身上的歷史勝率與入圍率。
                    - **最終計分**：由「全庫得分」與「本駒得分」按自訂比例（如各 50%）加權合併後，再於同場內相對百分位標準化成 0–10 分。
                    ''')
                    
                    with st.expander("⚙️ 調整全庫/本駒合作樣本範圍與權重比例 (調整後將即時儲存並重算)", expanded=True):
                        from database.models import SystemConfig
                        
                        cfg = {
                            "global_window": 0, "global_win_w": 0.7, "global_place_w": 0.3,
                            "horse_window": 0, "horse_win_w": 0.7, "horse_place_w": 0.3,
                            "global_weight": 0.5, "horse_weight": 0.5
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
                            
                            submitted = st.form_submit_button("💾 儲存合併參數並為本場重新計分", type="primary")
                            if submitted:
                                new_cfg = {
                                    "global_window": window_options[g_win], "global_win_w": g_ww, "global_place_w": g_pw,
                                    "horse_window": window_options[h_win], "horse_win_w": h_ww, "horse_place_w": h_pw,
                                    "global_weight": gw, "horse_weight": hw
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
                    
                    - 以同條件下的歷史勝率與上名率（前 3）計算原始分
                    - 勝率與上名率可選擇套用時間衰減（半衰期）以降低陳年數據的影響
                    - 透過「可信度降權」避免少量樣本造成分數過高（樣本越多可信度越高）
                    - 可設定時間窗（近 X 日）與樣本下限
                    - 最後在同一場內進行百分位標準化成 0–10 分
                    """)
                    with st.expander("⚙️ 調整時間窗/樣本下限/可信度與勝率權重 (調整後將即時儲存並重算)", expanded=False):
                        from database.models import SystemConfig

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

                        window_options = {"近 180 日": 180, "近 365 日": 365, "近 720 日": 720, "全部": 0}
                        hl_options = {"半衰期 180 日": 180, "半衰期 365 日": 365, "半衰期 720 日": 720, "不衰減": 0}
                        cur_window_label = next((k for k, v in window_options.items() if v == cfg["window_days"]), "近 720 日")
                        cur_hl_label = next((k for k, v in hl_options.items() if v == cfg["half_life_days"]), "半衰期 365 日")

                        with st.form("venue_dist_specialty_config_form"):
                            c1, c2, c3, c4, c5, c6 = st.columns(6)
                            window_label = c1.selectbox("時間窗", list(window_options.keys()), index=list(window_options.keys()).index(cur_window_label))
                            hl_label = c2.selectbox("時間衰減", list(hl_options.keys()), index=list(hl_options.keys()).index(cur_hl_label))
                            min_samples = c3.number_input("樣本下限 N", value=int(cfg["min_samples"]), min_value=0, max_value=30, step=1)
                            confidence_runs = c4.number_input("可信度滿分樣本", value=int(cfg["confidence_runs"]), min_value=1, max_value=50, step=1)
                            win_w = c5.number_input("勝率權重", value=float(cfg["win_w"]), min_value=0.0, max_value=1.0, step=0.05)
                            place_w = c6.number_input("上名率權重", value=float(cfg["place_w"]), min_value=0.0, max_value=1.0, step=0.05)

                            submitted = st.form_submit_button("💾 儲存參數並為本場重新計分", type="primary")
                            if submitted:
                                new_cfg = {
                                    "window_days": int(window_options[window_label]),
                                    "half_life_days": int(hl_options[hl_label]),
                                    "min_samples": int(min_samples),
                                    "confidence_runs": int(confidence_runs),
                                    "win_w": float(win_w),
                                    "place_w": float(place_w),
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

                elif selected_factor == "負磅／評分表現":
                    st.markdown("---")
                    st.markdown("### 💡 演算法說明：負磅／評分表現")
                    st.markdown("""
                    這個條件用於衡量「同路程」下，馬匹是否具備評分/負磅上的優勢，並加入同路程上名率作為輔助。
                    
                    - **主訊號（勝仗評分差）**：找出同路程歷史勝仗中「最高可贏評分」，若目前評分低於該值則加分。
                    - **輔助（同程上名率）**：同路程近 X 日的上名率（前 3）作為 fallback/輔助，並可設定樣本下限 N。
                    - **時間衰減**：對歷史資料乘上半衰期衰減係數，越久以前的表現影響越小。
                    - **最後調整**：同場再做百分位標準化成 0–10 分。
                    """)
                    with st.expander("⚙️ 調整時間窗/半衰期/樣本下限/入圍權重 (調整後將即時儲存並重算)", expanded=False):
                        from database.models import SystemConfig

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

                        window_options = {"近 180 日": 180, "近 365 日": 365, "近 730 日": 730, "全部": 0}
                        hl_options = {"半衰期 90 日": 90, "半衰期 180 日": 180, "半衰期 365 日": 365, "不衰減": 0}
                        n_options = {"3": 3, "5": 5, "8": 8, "10": 10}

                        cur_window_label = next((k for k, v in window_options.items() if v == cfg["window_days"]), "近 365 日")
                        cur_hl_label = next((k for k, v in hl_options.items() if v == cfg["half_life_days"]), "半衰期 180 日")
                        cur_n_label = next((k for k, v in n_options.items() if v == cfg["min_samples"]), "5")

                        with st.form("weight_rating_perf_config_form"):
                            c1, c2, c3, c4 = st.columns(4)
                            window_label = c1.selectbox("時間窗", list(window_options.keys()), index=list(window_options.keys()).index(cur_window_label))
                            hl_label = c2.selectbox("時間衰減", list(hl_options.keys()), index=list(hl_options.keys()).index(cur_hl_label))
                            n_label = c3.selectbox("同程樣本下限 N", list(n_options.keys()), index=list(n_options.keys()).index(cur_n_label))
                            place_weight = c4.number_input("入圍權重 (0-1)", value=float(cfg["place_weight"]), min_value=0.0, max_value=1.0, step=0.05)

                            submitted = st.form_submit_button("💾 儲存參數並為本場重新計分", type="primary")
                            if submitted:
                                new_cfg = {
                                    "window_days": int(window_options[window_label]),
                                    "half_life_days": int(hl_options[hl_label]),
                                    "min_samples": int(n_options[n_label]),
                                    "place_weight": float(place_weight),
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

                elif selected_factor == "檔位偏差 (官方 Draw Statistics)":
                    st.markdown("---")
                    st.markdown("### 💡 演算法說明：檔位偏差 (官方 Draw Statistics)")
                    st.markdown("""
                    這個條件用於評估馬匹排在該檔位是否具有統計上的優勢。
                    
                    - **數據來源**：系統於抓取當日排位時，會同步從馬會官方抓取當日各場次（同場地、同路程、同跑道）的檔位歷史統計數據。
                    - **計分公式**：
                      1. 從當場賽事的檔位統計中，找出**最高勝率**與**最高上名率**作為 100% 基準。
                      2. 計算該馬匹所排檔位的相對表現：`(該檔位勝率 / 最高勝率) × 70% + (該檔位上名率 / 最高上名率) × 30%`。
                      3. 若該檔位樣本數為 0 或尚未載入官方數據，則會退回簡單邏輯（內檔分數較高）。
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

                            show_cols = [c for c in ["draw", "total_runs", "win", "win_rate", "place_rate"] if c in stats_df.columns]
                            st.dataframe(stats_df[show_cols], width="stretch")

                            chart_df = stats_df.set_index("draw")[["win_rate", "place_rate"]] if all(
                                c in stats_df.columns for c in ["draw", "win_rate", "place_rate"]
                            ) else None
                            if chart_df is not None:
                                st.bar_chart(chart_df, width="stretch")
                        else:
                            st.warning("找不到本場次的官方檔位統計（可能尚未爬取或 Key 不匹配）。")
                    else:
                        st.warning("尚未載入當日官方檔位統計，請先執行賽日資料抓取後再檢查。")
                    
            else:
                st.warning("未找到計分條件數據。")
                
        with tab2:
            st.markdown("### 全局因子得分總表")
            st.markdown(f"包含所有馬匹在 {len(available_factors)} 個條件下的原始計算得分（0-10分）。")
            
            # 總表按馬號排序
            full_df = df.sort_values(by="馬號").reset_index(drop=True)
            full_df.index = full_df.index + 1
            st.dataframe(full_df, width="stretch")

session.close()
