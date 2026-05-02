import streamlit as st
import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime
import json
import sys
import os
from pathlib import Path

# 加入專案路徑
root_path = str(Path(__file__).resolve().parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import get_session
from database.models import SystemConfig, Race
from web_ui.utils import _confirm_run

st.set_page_config(page_title="AI 顧問與設定 - HKJC Analytics", page_icon="🤖", layout="wide")

st.markdown("## 🤖 AI 中樞與設定")
st.markdown("在此集中管理 AI 參數設定、批次生成賽事報告，以及檢視賽後反思與進化法則。")

tab_settings, tab_batch, tab_history, tab_reflection, tab_factor = st.tabs([
    "⚙️ AI 參數設定", 
    "⚡ 批次生成報告", 
    "📜 歷史分析報告", 
    "🧠 賽後反思與進化",
    "💡 因子優化建議"
])

session = get_session()
try:
    with tab_settings:
        st.markdown("### ⚙️ LLM 參數與提示詞設定")
        st.caption("在此設定所有 AI 功能（報告生成、因子建議、反思引擎）共用的模型與提示詞。")
        from scoring_engine.ai_advisor import (
            load_ai_settings,
            save_ai_settings,
            load_ai_api_key,
            save_ai_api_key,
            default_ai_system_prompt,
        )

        settings = load_ai_settings(session)
        
        prompt_cfg = session.query(SystemConfig).filter_by(key="ai_race_summary_prompt").first()
        current_summary_prompt = ""
        if prompt_cfg and isinstance(prompt_cfg.value, dict) and "prompt" in prompt_cfg.value:
            current_summary_prompt = prompt_cfg.value["prompt"]
        else:
            current_summary_prompt = (
                "你是專業香港賽馬分析師。現在我提供這場賽事各匹馬的近期走勢評述（FormGuide），以及系統量化出來的客觀數據（包含檔位、負磅、評分、SpeedPRO能量分、騎練合作分、近期狀態分等）。\n"
                "請根據這些質化與量化數據進行深度綜合分析。\n\n"
                "請務必包含以下兩個版本：\n\n"
                "### 【簡潔版分析】\n"
                "- 使用列點方式，直接給出 1-5 匹你認為最值得留意的馬匹（寧缺勿濫），以及你真正有把握淘汰的馬匹。若賽事形勢極度混亂均勢，可不勉強推介，並重點於形勢上作出解說。\n"
                "- 必須標明 `[馬號] 馬名`。\n"
                "- 每匹馬用一句話總結原因（結合客觀因子與走勢評述）。\n\n"
                "### 【完整版分析】\n"
                "包含以下四個部分：\n"
                "1. **👀 焦點馬匹點評**：挑選出狀態正在回勇，或上仗因「意外/受困/走位差/不利步速」而落敗的「可原諒馬匹/黑馬」。必須標明 `[馬號] 馬名`，並結合其客觀因子進行解釋。\n"
                "2. **⚠️ 淘汰風險馬匹分析**：挑選出 1-2 匹你認為今場沒太大可能入圍的馬匹（反向分析）。例如：近期走勢持續疲弱、今仗面對極端不利檔位/步速、或能量數值與評述皆差的馬匹，並解釋為何看淡。\n"
                "3. **🏇 預期賽事形勢**：綜合各駒近仗步速與跑法，預測今場的步速偏快或偏慢？哪幾匹馬可能放頭？\n"
                "4. **💡 綜合結論與投注策略**：給出整體的賽事定調與策略建議。\n\n"
                "請用繁體中文以清晰的 Markdown 格式輸出，直接給出分析，不要包含任何 json 或 markdown code block 標籤。"
            )

        with st.form("ai_llm_settings_form"):
            endpoint = st.text_input("Endpoint（OpenAI-compatible）", value=str(settings.get("endpoint") or "").strip(), placeholder="https://api.openai.com/v1/chat/completions")
            model_id = st.text_input("模型名稱（Model ID）", value=str(settings.get("model_id") or "").strip(), placeholder="gpt-4.1-mini")
            system_prompt = st.text_area(
                "AI 系統提示詞（因子建議用 System Prompt）",
                value=str(settings.get("system_prompt") or default_ai_system_prompt()).strip(),
                height=150,
            )
            summary_prompt = st.text_area(
                "賽前分析 Prompt（賽事總覽用，含反向分析）",
                value=current_summary_prompt.strip(),
                height=300,
            )
            submitted = st.form_submit_button("💾 儲存設定", type="primary")
            if submitted:
                save_ai_settings(session, endpoint=endpoint, model_id=model_id, system_prompt=system_prompt)
                
                p_cfg = session.query(SystemConfig).filter_by(key="ai_race_summary_prompt").first()
                if not p_cfg:
                    p_cfg = SystemConfig(key="ai_race_summary_prompt", description="AI 賽事前瞻分析 Prompt")
                    session.add(p_cfg)
                p_cfg.value = {"prompt": summary_prompt}
                session.commit()
                
                st.success("✅ 已儲存 LLM 設定與分析 Prompt。")
                st.rerun()

        st.markdown("---")
        st.markdown("### 🔑 API Key 設定")
        kinfo = load_ai_api_key(session)
        env_key = str(kinfo.get("env") or "").strip()
        stored_key = str(kinfo.get("stored") or "").strip()
        if env_key:
            st.info("已偵測到環境變數 API Key（AI_API_KEY / OPENAI_API_KEY）。")
        elif stored_key:
            st.warning("未偵測到環境變數，但資料庫內有保存 API Key（不建議長期使用 DB 保存）。")
        else:
            st.warning("目前未設定 API Key。建議在 Railway 設定 AI_API_KEY / OPENAI_API_KEY 環境變數。")

        c1, c2 = st.columns([3, 2])
        api_key_input = c1.text_input("API Key（本次使用，可留空）", value="", type="password", placeholder="留空＝使用環境變數或 DB 保存值")
        use_env_first = c2.checkbox("優先使用環境變數", value=True, key="ai_use_env_first")
        save_db = st.checkbox("將 API Key 儲存到資料庫（不建議）", value=False, key="ai_save_key_db")
        if save_db:
            ok_save = _confirm_run(st, "ai_save_key", label="輸入 RUN 以儲存 API Key")
            btn_save = st.button("💾 儲存 API Key 到資料庫", use_container_width=True, disabled=not ok_save)
            if btn_save:
                key_to_save = str(api_key_input or "").strip()
                if not key_to_save:
                    st.error("❌ 請先輸入 API Key。")
                else:
                    save_ai_api_key(session, key_to_save)
                    st.success("✅ 已儲存。建議改用 Railway 環境變數以提升安全性。")
                    st.rerun()

    with tab_batch:
        st.markdown("### ⚡ 批次生成 AI 賽事前瞻報告")
        st.caption("一次過為選定日期所有場次生成 AI 賽事前瞻，稍後於「獨立條件分析」頁面即可極速查看，無需逐場等待。")
        
        # Date selection
        from sqlalchemy import func
        dates_q = session.query(func.date(Race.race_date)).distinct().order_by(func.date(Race.race_date).desc()).all()
        available_dates = [d[0] for d in dates_q if d and d[0]]
        if not available_dates:
            st.warning("資料庫中無任何賽事日期。")
        else:
            selected_date = st.selectbox("選擇要批次生成的賽事日期", available_dates)
            
            c_confirm, c_btn = st.columns([2, 3])
            ok = _confirm_run(c_confirm, "batch_ai", label="輸入 RUN 以生成報告")
            if c_btn.button("✨ 批次生成該日所有 AI 報告", use_container_width=True, disabled=not ok):
                from scoring_engine.ai_advisor import run_ai_race_summary
                
                api_key = env_key or stored_key
                if not api_key:
                    st.error("❌ 尚未設定 AI API Key，無法生成報告。")
                else:
                    target_date_str = selected_date.strftime("%Y/%m/%d")
                    races = session.query(Race).filter(func.date(Race.race_date) == selected_date).order_by(Race.race_no).all()
                    if not races:
                        st.warning(f"⚠️ {target_date_str} 沒有賽事資料。")
                    else:
                        progress_text = "生成進度"
                        my_bar = st.progress(0, text=progress_text)
                        success_count = 0
                        
                        for i, r in enumerate(races):
                            my_bar.progress((i) / len(races), text=f"正在為 第 {r.race_no} 場 閱讀評述並生成報告...")
                            fg_key = f"speedpro_formguide:{target_date_str}:{r.race_no}"
                            cfg = session.query(SystemConfig).filter_by(key=fg_key).first()
                            
                            if not cfg or not cfg.value:
                                st.warning(f"第 {r.race_no} 場缺乏 FormGuide 資料，跳過。")
                                continue
                                
                            if f"ai_summary_{r.id}" not in st.session_state:
                                res = run_ai_race_summary(session, r.id)
                                if res.get("ok"):
                                    st.session_state[f"ai_summary_{r.id}"] = res.get("summary")
                                    success_count += 1
                                else:
                                    st.error(f"第 {r.race_no} 場生成失敗: {res.get('reason')} - {res.get('error')}")
                            else:
                                success_count += 1
                                
                        my_bar.progress(1.0, text="批次生成完成！")
                        st.success(f"✅ 完成！成功為 {success_count} / {len(races)} 場賽事生成或載入報告。")

    with tab_history:
        st.markdown("### 📜 歷史分析報告總覽")
        reports = session.query(SystemConfig).filter(SystemConfig.key.like("ai_race_report:%")).all()
        
        if not reports:
            st.info("目前尚未有任何 AI 分析報告紀錄。")
        else:
            data = []
            for r in reports:
                parts = r.key.split(":")
                if len(parts) >= 3:
                    date_str = parts[1]
                    race_no = parts[2]
                    
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
                
                c1, c2 = st.columns(2)
                dates = ["全部"] + list(df["Date"].unique())
                selected_date_filter = c1.selectbox("篩選日期", dates, key="tab_hist_date")
                if selected_date_filter != "全部":
                    df = df[df["Date"] == selected_date_filter]
                    
                races = ["全部"] + sorted(list(df["RaceNo"].unique()))
                selected_race_filter = c2.selectbox("篩選場次", races, key="tab_hist_race")
                if selected_race_filter != "全部":
                    df = df[df["RaceNo"] == selected_race_filter]
                    
                st.markdown(f"共找到 **{len(df)}** 份報告。")
                
                for idx, row in df.iterrows():
                    with st.expander(f"📅 {row['Date']} 第 {row['RaceNo']} 場 (建立於: {row['Created']})", expanded=False):
                        val = row["Value"]
                        if isinstance(val, dict) and "report" in val:
                            st.markdown(val["report"])
                        else:
                            st.info("報告內容格式無法解析。")

    with tab_reflection:
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
        st.write("請選擇已經有賽果（且已有賽前 AI 報告）的賽事，讓 AI 對比預測與實際結果，提煉新法則（字數已控制在 200-400 字）。")
        
        races = session.query(Race).order_by(Race.race_date.desc(), Race.race_no).limit(100).all()
        race_opts = {}
        for r in races:
            date_str = r.race_date.strftime("%Y/%m/%d")
            report_key = f"ai_race_report:{date_str}:{r.race_no}"
            report_cfg = session.query(SystemConfig).filter_by(key=report_key).first()
            if report_cfg:
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

    with tab_factor:
        st.markdown("### 💡 AI 因子優化建議")
        st.caption("用途：把命中率、因子重要性、缺失原因等摘要交給 LLM，輸出可執行建議（不會自動改全局）。")
        from scoring_engine.ai_advisor import run_ai_factor_advice
        
        c1, c2 = st.columns(2)
        d1 = c1.date_input("開始日期", value=datetime(2024, 1, 1).date(), key="ai_factor_d1")
        d2 = c2.date_input("結束日期", value=datetime.today().date(), key="ai_factor_d2")
        
        st.markdown("**生成建議**")
        st.caption("省資源建議：只在需要時按一次；日期範圍建議 60～180 日。")
        extra = st.text_area("額外指示（可留空）", value="", height=80, key="ai_extra_instructions")

        c1, c2, c3 = st.columns([2, 2, 3])
        ai_max_w = float(c1.selectbox("建議權重上限", [2.0, 3.0, 4.0, 5.0], index=1, key="ai_tune_max_w"))
        ai_top_k = int(c2.selectbox("TopK 定義", [5], index=0, key="ai_topk"))
        ok_run = _confirm_run(c1, "ai_run", label="輸入 RUN 以呼叫 AI")
        run_ai = c3.button("🤖 呼叫 AI 生成建議", use_container_width=True, key="ai_run_btn", disabled=not ok_run)

        if run_ai:
            key_used = api_key_input or env_key or stored_key

            res_ai = run_ai_factor_advice(
                session,
                d1=d1,
                d2=d2,
                top_k=int(ai_top_k),
                max_suggest_weight=float(ai_max_w),
                endpoint=str(settings.get("endpoint") or "").strip(),
                model_id=str(settings.get("model_id") or "").strip(),
                system_prompt=str(settings.get("system_prompt") or "").strip(),
                api_key=str(key_used or "").strip(),
                extra_instructions=str(extra or "").strip(),
            )
            st.session_state["ai_last_advice_result"] = res_ai

        res_ai = st.session_state.get("ai_last_advice_result")
        if isinstance(res_ai, dict) and res_ai.get("ok") is True:
            parsed = res_ai.get("parsed") if isinstance(res_ai.get("parsed"), dict) else {}
            if parsed.get("ok") is True and isinstance(parsed.get("data"), dict):
                data = parsed.get("data") if isinstance(parsed.get("data"), dict) else {}
                summary = str(data.get("summary") or "").strip()
                if summary:
                    st.success(summary)
                recs = data.get("recommendations") if isinstance(data.get("recommendations"), list) else []
                if recs:
                    rows = []
                    for r in recs:
                        if not isinstance(r, dict):
                            continue
                        rows.append(
                            {
                                "優先級": str(r.get("priority") or ""),
                                "動作": str(r.get("action") or ""),
                                "因子": str(r.get("factor_name") or ""),
                                "預期影響": str(r.get("expected_impact") or ""),
                                "風險": str(r.get("risk") or ""),
                            }
                        )
                    if rows:
                        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
                else:
                    st.info("AI 未輸出 recommendations。")
            else:
                st.error("❌ AI 回傳內容無法解析成 JSON。")

            with st.expander("查看原始回應（Raw）", expanded=False):
                st.code(str(res_ai.get("response_text") or ""), language="json")
                
        elif isinstance(res_ai, dict) and res_ai.get("ok") is False:
            if res_ai.get("reason") == "missing_api_key":
                st.error("❌ 未提供 API Key。")
            else:
                st.error(f"❌ 呼叫失敗：{str(res_ai.get('error') or res_ai.get('reason') or '')}")

finally:
    session.close()
