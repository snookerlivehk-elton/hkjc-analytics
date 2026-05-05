import streamlit as st
import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime
import json
import sys
import os
from pathlib import Path

# 加入專案路徑
root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session
from database.models import SystemConfig, Race
from web_ui.utils import _confirm_run
from web_ui.nav import render_admin_nav

st.set_page_config(page_title="AI 顧問與設定 - HKJC Analytics", page_icon="🤖", layout="wide")

st.markdown("## 🤖 AI 中樞與設定")
st.markdown("在此集中管理 AI 參數設定、批次生成賽事報告，以及檢視賽後反思與進化法則。")
render_admin_nav()

tab_settings, tab_batch, tab_history, tab_reflection, tab_track, tab_factor = st.tabs([
    "⚙️ AI 參數設定", 
    "⚡ 批次生成報告", 
    "📜 歷史分析報告", 
    "🧠 賽後反思與進化",
    "📊 跑道/場地統計",
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
                "4. **💡 綜合結論與觀賽焦點**：給出整體的賽事定調與客觀的數據觀察建議。\n\n"
                "請注意：本報告純屬數據統計與學術研究，絕不構成任何博彩或投注建議。請務必以客觀中立的數據分析師口吻撰寫。\n"
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

            st.markdown("---")
            st.markdown("### 🎯 單場生成 / 補回報告")
            st.caption("用途：針對個別場次（例如第 1 場）獨立生成或補回 AI 報告。")

            from scoring_engine.ai_advisor import run_ai_race_summary
            target_date_str = selected_date.strftime("%Y/%m/%d")
            races1 = session.query(Race).filter(func.date(Race.race_date) == selected_date).order_by(Race.race_no.asc()).all()
            if not races1:
                st.info("該日沒有賽事資料。")
            else:
                opts = []
                by_label = {}
                for r in races1:
                    label = f"第 {int(r.race_no)} 場"
                    opts.append(label)
                    by_label[label] = r
                sel = st.selectbox("選擇場次", options=opts, index=0, key="single_ai_race_sel")
                rr = by_label.get(sel)
                if rr:
                    report_key = f"ai_race_report:{target_date_str}:{int(rr.race_no)}"
                    has_report = bool(session.query(SystemConfig.id).filter_by(key=report_key).first())
                    fg_key = f"speedpro_formguide:{target_date_str}:{int(rr.race_no)}"
                    has_fg = bool(session.query(SystemConfig.id).filter_by(key=fg_key).first())
                    st.markdown(f"- 場次：**{target_date_str} 第 {int(rr.race_no)} 場**")
                    st.markdown(f"- FormGuide：**{'有' if has_fg else '沒有'}**（無資料會無法生成）")
                    st.markdown(f"- AI 報告：**{'已有' if has_report else '未有'}**（可用此功能補回）")

                    c_confirm, c_btn = st.columns([2, 3])
                    ok1 = _confirm_run(c_confirm, "single_ai", label="輸入 RUN 以生成/更新本場報告")
                    if c_btn.button("✨ 生成 / 更新本場 AI 報告", use_container_width=True, disabled=not ok1, key=f"single_ai_btn_{target_date_str}_{int(rr.race_no)}"):
                        api_key = env_key or stored_key
                        if not api_key:
                            st.error("❌ 尚未設定 AI API Key，無法生成報告。")
                        else:
                            with st.spinner("正在生成本場 AI 報告..."):
                                res = run_ai_race_summary(session, int(rr.id))
                            if isinstance(res, dict) and res.get("ok"):
                                st.success("✅ 已生成 / 更新本場 AI 報告。")
                                st.rerun()
                            else:
                                st.error(f"❌ 生成失敗：{res.get('reason') if isinstance(res, dict) else res}")

                    st.markdown("**備用情境（例如落雨）**")
                    from scoring_engine.track_conditions import GOING_CODE_LABELS
                    scenario_opts = [
                        ("WET_SLOW", f"WET_SLOW（{GOING_CODE_LABELS.get('WET_SLOW', '濕慢')}）"),
                        ("WET_FAST", f"WET_FAST（{GOING_CODE_LABELS.get('WET_FAST', '濕快')}）"),
                    ]
                    scen_code = st.selectbox(
                        "假設場地狀態（going_code）",
                        options=[x[0] for x in scenario_opts],
                        format_func=lambda x: dict(scenario_opts).get(x, x),
                        index=0,
                        key=f"single_ai_scenario_code_{target_date_str}_{int(rr.race_no)}",
                    )
                    c_confirm, c_btn = st.columns([2, 3])
                    ok2 = _confirm_run(c_confirm, "single_ai_scenario", label="輸入 RUN 以生成備用情境報告")
                    if c_btn.button(
                        "🌧️ 生成雨戰備用 AI 報告",
                        use_container_width=True,
                        disabled=not ok2,
                        key=f"single_ai_scenario_btn_{target_date_str}_{int(rr.race_no)}_{scen_code}",
                    ):
                        api_key = env_key or stored_key
                        if not api_key:
                            st.error("❌ 尚未設定 AI API Key，無法生成報告。")
                        else:
                            with st.spinner("正在生成備用情境 AI 報告..."):
                                res = run_ai_race_summary(
                                    session,
                                    int(rr.id),
                                    going_code_override=str(scen_code),
                                    scenario_tag=str(scen_code),
                                    save_as_scenario=True,
                                )
                            if isinstance(res, dict) and res.get("ok"):
                                st.success("✅ 已生成備用情境 AI 報告。")
                                st.rerun()
                            else:
                                st.error(f"❌ 生成失敗：{res.get('reason') if isinstance(res, dict) else res}")

                    scen_rows = (
                        session.query(SystemConfig)
                        .filter(SystemConfig.key.like(f"ai_race_report_scenario:{target_date_str}:{int(rr.race_no)}:%"))
                        .all()
                    )
                    if scen_rows:
                        with st.expander("查看本場已生成的備用情境報告", expanded=False):
                            for i, row in enumerate(sorted(scen_rows, key=lambda x: str(x.key or ""))):
                                v = row.value if isinstance(row.value, dict) else {}
                                tag = str(v.get("scenario") or "").strip() or str(row.key).split(":")[-1]
                                st.markdown(f"#### 情境：{tag}")
                                st.markdown(str(v.get("report") or ""))
                                with st.expander("📋 點擊顯示可複製的原始文字", expanded=False):
                                    st.code(str(v.get("report") or ""), language="markdown")

    with tab_history:
        st.markdown("### 📜 歷史分析報告總覽")
        reports = session.query(SystemConfig).filter(SystemConfig.key.like("ai_race_report:%")).all()
        
        if not reports:
            st.info("目前尚未有任何 AI 分析報告紀錄。")
        else:
            import re

            def _uniq_ints(xs):
                out = []
                seen = set()
                for x in xs:
                    try:
                        n = int(x)
                    except Exception:
                        continue
                    if n <= 0:
                        continue
                    if n in seen:
                        continue
                    seen.add(n)
                    out.append(n)
                return out

            def _extract_section_nums(text: str, keywords, max_take: int = 5):
                t = str(text or "")
                lines = t.splitlines()
                start_idx = None
                for i, ln in enumerate(lines):
                    s = ln.strip()
                    for kw in keywords:
                        if kw and (kw in s):
                            start_idx = i
                            break
                    if start_idx is not None:
                        break
                if start_idx is None:
                    return []
                buf = []
                for j in range(start_idx + 1, len(lines)):
                    s = lines[j].strip()
                    if s.startswith("#"):
                        break
                    if s == "":
                        if buf:
                            break
                        continue
                    buf.append(s)
                    if len(buf) > 80:
                        break
                nums = re.findall(r"\[(\d{1,2})\]", "\n".join(buf))
                return _uniq_ints(nums)[: int(max_take or 5)]

            def _try_extract_top5_elim(report_text: str):
                top5 = _extract_section_nums(report_text, ["AI 推薦名單", "簡潔版分析", "推薦", "Top 5", "Top5"], max_take=5)
                elim = _extract_section_nums(report_text, ["AI 淘汰名單", "淘汰", "反向", "看淡"], max_take=20)
                if not top5:
                    all_nums = _uniq_ints(re.findall(r"\[(\d{1,2})\]", str(report_text or "")))
                    top5 = all_nums[:5]
                return top5, elim

            missing_cnt = 0
            for r in reports:
                v = r.value if isinstance(r.value, dict) else {}
                if not isinstance(v, dict):
                    continue
                if "report" not in v:
                    continue
                has_top5 = isinstance(v.get("top5_horse_nos"), list)
                has_elim = isinstance(v.get("eliminated_horse_nos"), list)
                if not (has_top5 and has_elim):
                    missing_cnt += 1

            st.caption(f"缺少 Top5/淘汰 結構化欄位的報告：{missing_cnt} 份（可先嘗試免 AI 成本補回；補不到才逐場重新生成）。")

            c_confirm, c_btn = st.columns([2, 3])
            ok_fix = _confirm_run(c_confirm, "fix_ai_report_struct_fields", label="輸入 RUN 以嘗試補回 Top5/淘汰（免 AI 成本）")
            if c_btn.button("🧩 嘗試補回 Top5 / 淘汰（免 AI 成本）", use_container_width=True, disabled=not ok_fix, key="fix_ai_report_struct_fields_btn"):
                from sqlalchemy import func
                from scoring_engine.ai_stats import calculate_ai_hit_stats

                updated = 0
                skipped = 0
                filled_top5 = 0
                filled_elim = 0
                snap_upd = 0
                for r in reports:
                    if not isinstance(r.value, dict) or "report" not in r.value:
                        continue
                    val = r.value
                    has_top5 = isinstance(val.get("top5_horse_nos"), list)
                    has_elim = isinstance(val.get("eliminated_horse_nos"), list)
                    report_text = str(val.get("report") or "")
                    top5 = val.get("top5_horse_nos") if has_top5 else None
                    elim = val.get("eliminated_horse_nos") if has_elim else None

                    if not has_top5 or not has_elim:
                        t2, e2 = _try_extract_top5_elim(report_text)
                        if not has_top5:
                            top5 = t2
                            val["top5_horse_nos"] = top5
                            filled_top5 += 1
                        if not has_elim:
                            elim = e2
                            val["eliminated_horse_nos"] = elim
                            filled_elim += 1
                        r.value = val
                        updated += 1

                    if not has_top5 and not has_elim and (not top5 and not elim):
                        skipped += 1
                        continue

                    parts = str(r.key or "").split(":")
                    if len(parts) >= 3:
                        date_str = str(parts[1] or "").strip()
                        race_no = str(parts[2] or "").strip()
                        if date_str and race_no:
                            top5_key = f"top5_snapshot:{date_str}:{race_no}"
                            t5_cfg = session.query(SystemConfig).filter_by(key=top5_key).first()
                            if not t5_cfg:
                                t5_cfg = SystemConfig(key=top5_key, description=f"Top 5 預測快照（racedate={date_str} R{race_no}）")
                                session.add(t5_cfg)
                            t5_val = t5_cfg.value if isinstance(t5_cfg.value, dict) else {}
                            if isinstance(top5, list):
                                t5_val["🤖 AI 賽事前瞻"] = top5
                            t5_cfg.value = t5_val

                            elim_key = f"elim_snapshot:{date_str}:{race_no}"
                            e_cfg = session.query(SystemConfig).filter_by(key=elim_key).first()
                            if not e_cfg:
                                e_cfg = SystemConfig(key=elim_key, description=f"反向預測淘汰快照（racedate={date_str} R{race_no}）")
                                session.add(e_cfg)
                            e_val = e_cfg.value if isinstance(e_cfg.value, dict) else {}
                            if isinstance(elim, list):
                                e_val["🤖 AI 賽事前瞻"] = elim
                            e_cfg.value = e_val
                            snap_upd += 1

                session.commit()
                calculate_ai_hit_stats(session)
                st.success(f"✅ 完成：更新 {updated} 份（補回Top5 {filled_top5}／補回淘汰 {filled_elim}），略過 {skipped} 份；同步刷新快照 {snap_upd} 場，並已重算統計。")
                st.rerun()

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
                            report_text = str(val.get("report") or "")
                            if not report_text.lstrip().startswith("# J18.HK AI 賽事前瞻分析"):
                                try:
                                    from sqlalchemy import func
                                    d0 = datetime.strptime(str(row["Date"]), "%Y/%m/%d").date()
                                    rr = (
                                        session.query(Race)
                                        .filter(func.date(Race.race_date) == d0)
                                        .filter(Race.race_no == int(row["RaceNo"]))
                                        .first()
                                    )
                                except Exception:
                                    rr = None

                                if rr:
                                    v = str(getattr(rr, "venue", "") or "").strip().upper()
                                    t = str(getattr(rr, "track_type", "") or "").strip()
                                    loc = "跑馬地" if (v == "HV" or ("跑馬地" in t)) else ("沙田" if (v == "ST" or ("沙田" in t)) else (str(getattr(rr, "venue", "") or "").strip() or "-"))
                                    surface = str(getattr(rr, "surface", "") or "").strip() or "-"
                                    course = str(getattr(rr, "course_type", "") or "").strip()
                                    dist = int(getattr(rr, "distance", 0) or 0)
                                    cls = str(getattr(rr, "race_class", "") or "").strip()

                                    parts = []
                                    if loc and loc != "-":
                                        parts.append(loc)
                                    if surface and surface != "-":
                                        parts.append(surface)
                                    if course:
                                        parts.append(f"跑道{course}")
                                    if dist > 0:
                                        parts.append(f"{dist}米")
                                    if cls:
                                        parts.append(cls)
                                    meta = "｜".join(parts)
                                    meta = f"｜{meta}" if meta else ""
                                    prefix = f"# J18.HK AI 賽事前瞻分析\n**賽事：{str(row['Date'])} 第 {int(row['RaceNo'])} 場{meta}**\n\n"
                                    report_text = prefix + report_text

                            st.markdown(report_text)
                            with st.expander("📋 點擊顯示可複製的原始文字", expanded=False):
                                st.code(report_text, language="markdown")
                            
                            top5 = val.get("top5_horse_nos") if isinstance(val, dict) else None
                            elim = val.get("eliminated_horse_nos") if isinstance(val, dict) else None
                            if isinstance(top5, list) or isinstance(elim, list):
                                with st.expander("📌 AI 結構化輸出（Top5 / 淘汰）", expanded=False):
                                    if isinstance(top5, list):
                                        st.markdown(f"- Top5（馬號）：{', '.join(str(x) for x in top5) if top5 else '（空）'}")
                                    if isinstance(elim, list):
                                        st.markdown(f"- 淘汰（馬號）：{', '.join(str(x) for x in elim) if elim else '（空）'}")
                            else:
                                st.warning("此報告缺少 top5_horse_nos / eliminated_horse_nos（可能為舊版入庫或解析失敗）。可按下方重新生成以補回。")
                            
                            with st.expander("🔍 檢視原始 FormGuide 數據", expanded=False):
                                fg_key = f"speedpro_formguide:{row['Date']}:{int(row['RaceNo'])}"
                                fg_cfg = session.query(SystemConfig).filter_by(key=fg_key).first()
                                if fg_cfg and fg_cfg.value:
                                    st.code(json.dumps(fg_cfg.value, ensure_ascii=False, indent=2), language="json")
                                else:
                                    st.info("找不到此場次的 FormGuide 暫存資料。")

                            c_confirm, c_btn = st.columns([2, 3])
                            ok = _confirm_run(c_confirm, f"regen_ai_report_{row['Date']}_{int(row['RaceNo'])}", label="輸入 RUN 以重新生成本場報告")
                            if c_btn.button(
                                "♻️ 重新生成本場報告",
                                use_container_width=True,
                                disabled=not ok,
                                key=f"regen_ai_report_btn_{row['Date']}_{int(row['RaceNo'])}",
                            ):
                                from sqlalchemy import func
                                from scoring_engine.ai_advisor import run_ai_race_summary

                                try:
                                    d0 = datetime.strptime(str(row["Date"]), "%Y/%m/%d").date()
                                except Exception:
                                    d0 = None
                                if not d0:
                                    st.error("❌ 日期格式無法解析。")
                                else:
                                    rr = (
                                        session.query(Race)
                                        .filter(func.date(Race.race_date) == d0)
                                        .filter(Race.race_no == int(row["RaceNo"]))
                                        .first()
                                    )
                                    if not rr:
                                        st.error("❌ 找不到對應賽事資料（Race）。")
                                    else:
                                        with st.spinner("🤖 正在重新生成報告..."):
                                            res = run_ai_race_summary(session, int(rr.id))
                                        if res.get("ok"):
                                            st.success("✅ 已重新生成並更新報告。")
                                            st.rerun()
                                        else:
                                            st.error(f"❌ 重新生成失敗：{res.get('reason')} {res.get('error')}")
                        else:
                            st.info("報告內容格式無法解析。")

    with tab_reflection:
        st.markdown("### 💡 系統學習到的黃金法則")
        from scoring_engine.ai_reflection import (
            get_learned_rules,
            get_learned_rule_items,
            save_learned_rule_items,
            generate_race_reflection,
            list_reflection_candidates,
            batch_reflect_worst,
        )
        
        items = get_learned_rule_items(session)
        if items:
            st.caption("可在下方管理法則（啟用/停用/刪除），系統只會把「啟用」的法則注入到下一次賽前預測。")
            changed = False
            updated_items = []
            for i, it in enumerate(items, 1):
                rule_text = str(it.get("rule") or "").strip()
                if not rule_text:
                    continue
                enabled_default = bool(it.get("enabled") is not False)
                enabled = st.checkbox(f"啟用｜法則 {i}: {rule_text}", value=enabled_default, key=f"rule_enabled_{i}")
                it2 = dict(it)
                it2["enabled"] = bool(enabled)
                updated_items.append(it2)
                if bool(enabled) != enabled_default:
                    changed = True

            del_opts = [str(x.get("rule") or "").strip() for x in updated_items if str(x.get("rule") or "").strip()]
            to_del = st.multiselect("刪除法則（可多選）", options=del_opts, default=[], key="rule_delete_sel")
            c1, c2 = st.columns([2, 3])
            ok_save = _confirm_run(c1, "save_rules", label="輸入 RUN 以儲存法則變更")
            if c2.button("💾 儲存法則設定", use_container_width=True, disabled=not ok_save):
                final_items = []
                dels = set(str(x).strip() for x in (to_del or []) if str(x).strip())
                for it in updated_items:
                    rt = str(it.get("rule") or "").strip()
                    if not rt or rt in dels:
                        continue
                    final_items.append(it)
                save_learned_rule_items(session, final_items)
                st.success("✅ 已儲存法則設定。")
                st.rerun()

            enabled_rules = get_learned_rules(session)
            if enabled_rules:
                st.markdown("**目前啟用法則（會影響下一次預測）**")
                for r in enabled_rules:
                    st.info(r)
        else:
            st.warning("目前尚未學習到任何法則。請先執行賽後反思。")
            
        st.markdown("---")
        st.markdown("### ⚡ 批次反思（自動挑選最失準場次）")
        st.caption("省資源策略：只挑選同一賽日中最失準的 1～3 場做反思，提升法則品質並避免每場都耗費 AI。")

        from sqlalchemy import func
        dates_q = session.query(func.date(Race.race_date)).distinct().order_by(func.date(Race.race_date).desc()).limit(180).all()
        available_dates = []
        for d in dates_q:
            if d and d[0]:
                try:
                    available_dates.append(d[0].strftime("%Y/%m/%d"))
                except Exception:
                    pass
        if available_dates:
            sel_date = st.selectbox("選擇賽日", options=available_dates, index=0, key="batch_reflect_date")
            top_n = st.slider("最多反思幾場（挑最失準）", min_value=1, max_value=5, value=3, step=1, key="batch_reflect_topn")
            cand = list_reflection_candidates(session, date_str=str(sel_date), only_unreflected=True, limit=100)
            if not cand:
                st.info("該賽日沒有可反思的場次（需同時具備：AI 報告 + 已有賽果 Top4 + 未反思）。")
            else:
                show = cand[: int(top_n)]
                st.markdown("**將會優先反思以下場次（按失準程度排序）**")
                for x in show:
                    st.write(f"- {x.get('date')} 第 {x.get('race_no')} 場｜失準分數 {x.get('score')}（Top4命中 {x.get('hits_in_top4')}/4；錯殺 {x.get('false_elim')}）")

                c1, c2 = st.columns([2, 3])
                ok_run = _confirm_run(c1, "batch_reflect_run", label="輸入 RUN 以批次反思")
                if c2.button("🧠 批次生成反思（只跑最失準）", use_container_width=True, disabled=not ok_run, key="batch_reflect_btn"):
                    with st.spinner("AI 正在批次檢討並提煉法則..."):
                        res = batch_reflect_worst(session, date_str=str(sel_date), top_n=int(top_n))
                    results = res.get("results") if isinstance(res, dict) else []
                    ok_n = 0
                    for item in results or []:
                        rr = item.get("res") if isinstance(item, dict) else {}
                        if isinstance(rr, dict) and rr.get("ok"):
                            ok_n += 1
                    st.success(f"✅ 完成批次反思：成功 {ok_n}/{len(results or [])} 場。")
                    st.rerun()

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

    with tab_track:
        st.markdown("### 📊 跑道 / 場地狀態統計")
        st.caption("用途：統計不同跑道 × 場地狀態下，勝出/入圍馬匹的跑法比率與平均賠率，並可供 AI 賽前分析引用。")

        from scoring_engine.track_profile import compute_track_profiles
        idx_cfg = session.query(SystemConfig).filter_by(key="trkprof_index").first()
        if idx_cfg and isinstance(idx_cfg.value, dict):
            st.info(
                f"最近更新：{str(idx_cfg.value.get('updated_at') or '')}｜已掃描賽事：{int(idx_cfg.value.get('seen_races') or 0)}｜分組：{len(idx_cfg.value.get('items') or [])}"
            )
        else:
            st.warning("尚未建立跑道/場地統計。請先按下方按鈕計算一次。")

        c1, c2, c3 = st.columns([2, 2, 3])
        min_d = c1.date_input("開始日期（可留空＝全量）", value=datetime(2024, 1, 1).date(), key="trkprof_min_d")
        max_d = c2.date_input("結束日期（可留空＝今天）", value=datetime.today().date(), key="trkprof_max_d")
        limit_races = int(c3.selectbox("最多掃描賽事數", [2000, 5000, 10000], index=1, key="trkprof_limit"))

        c1, c2 = st.columns([2, 3])
        ok_run = _confirm_run(c1, "trkprof_compute", label="輸入 RUN 以重新計算")
        if c2.button("🔄 重新計算跑道/場地統計", use_container_width=True, disabled=not ok_run, key="trkprof_compute_btn"):
            with st.spinner("正在重新計算跑道/場地統計..."):
                res = compute_track_profiles(
                    session,
                    min_date=datetime.combine(min_d, datetime.min.time()) if min_d else None,
                    max_date=datetime.combine(max_d, datetime.max.time()) if max_d else None,
                    limit_races=int(limit_races),
                )
            if isinstance(res, dict) and res.get("ok"):
                st.success(f"✅ 完成：掃描 {res.get('seen_races')} 場，產生 {res.get('groups')} 個分組。")
                st.rerun()
            else:
                st.error(f"❌ 計算失敗：{res}")

        st.markdown("---")
        st.markdown("### 🔎 查詢統計")
        idx_cfg = session.query(SystemConfig).filter_by(key="trkprof_index").first()
        items = []
        if idx_cfg and isinstance(idx_cfg.value, dict):
            items = idx_cfg.value.get("items") if isinstance(idx_cfg.value.get("items"), list) else []
        keys = [str(x.get("key") or "") for x in items if isinstance(x, dict) and str(x.get("key") or "")]
        if not keys:
            st.info("尚無可查詢的分組。請先計算。")
        else:
            from scoring_engine.track_conditions import going_code_label

            n_map = {str(x.get("key") or ""): int(x.get("n_races") or 0) for x in items if isinstance(x, dict) and str(x.get("key") or "")}

            def _dist_label(b: str) -> str:
                bb = str(b or "").strip().upper()
                if bb == "S":
                    return "短途≤1200"
                if bb == "M":
                    return "中途1201-1600"
                if bb == "L":
                    return "長途>1600"
                return "未知距離"

            def _venue_label(v: str) -> str:
                vv = str(v or "").strip().upper()
                if vv == "HV":
                    return "跑馬地"
                if vv == "ST":
                    return "沙田"
                return vv or "-"

            def _fmt_trkprof_key(k: str) -> str:
                s = str(k or "").strip()
                parts = s.split(":")
                if len(parts) >= 5 and parts[0] == "trkprof":
                    venue, gcode, course, dist_b = parts[1], parts[2], parts[3], parts[4]
                    n = int(n_map.get(s) or 0)
                    n_txt = f"｜樣本 {n}" if n > 0 else ""
                    return f"{_venue_label(venue)}｜{going_code_label(str(gcode))}({gcode})｜跑道{course}｜{_dist_label(dist_b)}{n_txt}"
                return s

            sel_key = st.selectbox("選擇分組", options=keys, format_func=_fmt_trkprof_key, index=0, key="trkprof_sel_key")
            cfg = session.query(SystemConfig).filter_by(key=str(sel_key)).first()
            if cfg and isinstance(cfg.value, dict):
                v = cfg.value
                st.success(_fmt_trkprof_key(str(sel_key)))
                c1, c2 = st.columns(2)
                c1.markdown("**勝出馬跑法分布**")
                c1.dataframe(pd.DataFrame([v.get("winner_style_composite_pct") or v.get("winner_style_pct") or {}]), use_container_width=True, hide_index=True)
                c2.markdown("**Top4 入圍跑法分布**")
                c2.dataframe(pd.DataFrame([v.get("top4_style_composite_pct") or v.get("top4_style_pct") or {}]), use_container_width=True, hide_index=True)

                c1, c2, c3 = st.columns(3)
                c1.markdown("**Top4 早段跑法**")
                c1.dataframe(pd.DataFrame([v.get("top4_style_early_pct") or {}]), use_container_width=True, hide_index=True)
                c2.markdown("**Top4 中段跑法**")
                c2.dataframe(pd.DataFrame([v.get("top4_style_mid_pct") or {}]), use_container_width=True, hide_index=True)
                c3.markdown("**Top4 末段跑法**")
                c3.dataframe(pd.DataFrame([v.get("top4_style_late_pct") or {}]), use_container_width=True, hide_index=True)

                c1, c2 = st.columns(2)
                c1.markdown(f"**步速分布（勝出；樣本={int(v.get('pace_races') or 0)}）**")
                c1.dataframe(pd.DataFrame([v.get("winner_pace_pct") or {}]), use_container_width=True, hide_index=True)
                c2.markdown(f"**步速分布（Top4；樣本={int(v.get('pace_races') or 0)}）**")
                c2.dataframe(pd.DataFrame([v.get("top4_pace_pct") or {}]), use_container_width=True, hide_index=True)

                st.markdown("**平均賠率（Win Odds）**")
                st.dataframe(
                    pd.DataFrame(
                        [
                            {
                                "勝出馬平均": v.get("winner_win_odds_avg"),
                                "勝出馬中位數": v.get("winner_win_odds_median"),
                                "Top4 平均": v.get("top4_win_odds_avg"),
                                "Top4 中位數": v.get("top4_win_odds_median"),
                            }
                        ]
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.error("找不到該分組資料。")

        st.markdown("---")
        st.markdown("### 🎯 Top3 目標校準（Top5 重排）")
        st.caption("用途：用系統客觀分數對 AI Top5 做低成本重排，目標提升「Top2 勝出率」與「Top3 ≥ 2 入圍率」。不會增加 LLM 成本。")

        from scoring_engine.ai_rerank import (
            load_ai_rerank_config,
            save_ai_rerank_config,
            backtest_rerank,
            tune_rerank_for_bucket,
            load_bucket_rerank_config,
        )
        rr_cfg = load_ai_rerank_config(session)
        c1, c2, c3 = st.columns(3)
        ai_prior_w = c1.number_input("AI 順序權重", value=float(rr_cfg.get("ai_prior_weight") or 0.0), step=0.1, key="rr_ai_prior_w")
        total_score_w = c2.number_input("系統總分權重", value=float(rr_cfg.get("total_score_weight") or 0.0), step=0.1, key="rr_total_score_w")
        speedpro_w = c3.number_input("SpeedPRO 權重", value=float(rr_cfg.get("speedpro_weight") or 0.0), step=0.1, key="rr_speedpro_w")

        c1, c2, c3 = st.columns(3)
        recent_w = c1.number_input("近期狀態 權重", value=float(rr_cfg.get("recent_weight") or 0.0), step=0.1, key="rr_recent_w")
        jt_w = c2.number_input("騎練合作 權重", value=float(rr_cfg.get("jt_weight") or 0.0), step=0.1, key="rr_jt_w")

        c1, c2 = st.columns([2, 3])
        ok_save = _confirm_run(c1, "save_rr_cfg", label="輸入 RUN 以儲存重排參數")
        if c2.button("💾 儲存 Top5 重排參數", use_container_width=True, disabled=not ok_save, key="save_rr_cfg_btn"):
            save_ai_rerank_config(
                session,
                {
                    "ai_prior_weight": float(ai_prior_w),
                    "total_score_weight": float(total_score_w),
                    "speedpro_weight": float(speedpro_w),
                    "recent_weight": float(recent_w),
                    "jt_weight": float(jt_w),
                },
            )
            st.success("✅ 已儲存重排參數。")
            st.rerun()

        st.markdown("#### 📈 回測（Baseline vs 重排）")
        c1, c2, c3 = st.columns([2, 2, 3])
        bt_d1 = c1.date_input("回測開始日期", value=datetime(2026, 4, 8).date(), key="rr_bt_d1")
        bt_d2 = c2.date_input("回測結束日期", value=datetime.today().date(), key="rr_bt_d2")
        bt_n = int(c3.selectbox("最多回測賽事數", [100, 200, 300, 500], index=1, key="rr_bt_n"))
        c1, c2 = st.columns([2, 3])
        ok_bt = _confirm_run(c1, "run_rr_bt", label="輸入 RUN 以回測")
        if c2.button("▶️ 開始回測", use_container_width=True, disabled=not ok_bt, key="run_rr_bt_btn"):
            with st.spinner("正在回測 Top5 重排效果..."):
                res = backtest_rerank(
                    session,
                    d1=datetime.combine(bt_d1, datetime.min.time()) if bt_d1 else None,
                    d2=datetime.combine(bt_d2, datetime.max.time()) if bt_d2 else None,
                    max_races=int(bt_n),
                    cfg={
                        "ai_prior_weight": float(ai_prior_w),
                        "total_score_weight": float(total_score_w),
                        "speedpro_weight": float(speedpro_w),
                        "recent_weight": float(recent_w),
                        "jt_weight": float(jt_w),
                    },
                )
            if isinstance(res, dict) and res.get("ok"):
                b = res.get("base") if isinstance(res.get("base"), dict) else {}
                r = res.get("rerank") if isinstance(res.get("rerank"), dict) else {}
                st.success(f"✅ 回測完成（樣本 {int(b.get('races') or 0)} 場）")
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Baseline Top2 勝出率", f"{b.get('w2_rate', 0.0)}%")
                c2.metric("重排後 Top2 勝出率", f"{r.get('w2_rate', 0.0)}%")
                c3.metric("Baseline Top3≥2入圍率", f"{b.get('top3_2in_rate', 0.0)}%")
                c4.metric("重排後 Top3≥2入圍率", f"{r.get('top3_2in_rate', 0.0)}%")
            else:
                st.error(f"❌ 回測失敗：{res}")

        st.markdown("---")
        st.markdown("#### 🧩 分桶調參（按跑道×場地狀態×距離分桶）")
        st.caption("用途：為不同跑道/場地狀態配置不同重排權重，系統會在生成報告時自動套用對應分桶權重。")

        idx_cfg = session.query(SystemConfig).filter_by(key="trkprof_index").first()
        items = []
        if idx_cfg and isinstance(idx_cfg.value, dict):
            items = idx_cfg.value.get("items") if isinstance(idx_cfg.value.get("items"), list) else []
        keys = [str(x.get("key") or "") for x in items if isinstance(x, dict) and str(x.get("key") or "").startswith("trkprof:")]
        if not keys:
            st.info("尚無跑道/場地分組索引。請先到上方「📊 跑道 / 場地狀態統計」計算一次。")
        else:
            from scoring_engine.track_conditions import going_code_label

            n_map = {str(x.get("key") or ""): int(x.get("n_races") or 0) for x in items if isinstance(x, dict) and str(x.get("key") or "")}

            def _dist_label(b: str) -> str:
                bb = str(b or "").strip().upper()
                if bb == "S":
                    return "短途≤1200"
                if bb == "M":
                    return "中途1201-1600"
                if bb == "L":
                    return "長途>1600"
                return "未知距離"

            def _venue_label(v: str) -> str:
                vv = str(v or "").strip().upper()
                if vv == "HV":
                    return "跑馬地"
                if vv == "ST":
                    return "沙田"
                return vv or "-"

            def _fmt_trkprof_key(k: str) -> str:
                s = str(k or "").strip()
                parts = s.split(":")
                if len(parts) >= 5 and parts[0] == "trkprof":
                    venue, gcode, course, dist_b = parts[1], parts[2], parts[3], parts[4]
                    n = int(n_map.get(s) or 0)
                    n_txt = f"｜樣本 {n}" if n > 0 else ""
                    return f"{_venue_label(venue)}｜{going_code_label(str(gcode))}({gcode})｜跑道{course}｜{_dist_label(dist_b)}{n_txt}"
                return s

            sel = st.selectbox("選擇跑道/場地分桶", options=keys, format_func=_fmt_trkprof_key, index=0, key="rr_bucket_sel")
            parts = str(sel).split(":")
            bparts = None
            if len(parts) >= 5:
                bparts = (parts[1], parts[2], parts[3], parts[4])

            if bparts:
                current_bucket_cfg = load_bucket_rerank_config(session, bparts)
                if current_bucket_cfg:
                    st.success(f"已存在分桶權重：ai_rerank_cfg:{bparts[0]}:{bparts[1]}:{bparts[2]}:{bparts[3]}")
                    st.dataframe(pd.DataFrame([current_bucket_cfg]), use_container_width=True, hide_index=True)
                else:
                    st.warning("此分桶尚未調參，生成報告會回退到全局重排權重。")

                c1, c2, c3 = st.columns(3)
                w2_w = c1.number_input("目標權重：Top2 勝出率", value=0.7, step=0.1, key="rr_obj_w2")
                t2_w = c2.number_input("目標權重：Top3≥2入圍率", value=0.3, step=0.1, key="rr_obj_t2")
                grid = c3.selectbox("搜尋強度", ["fast", "thorough"], index=0, key="rr_grid_preset")

                c1, c2, c3 = st.columns([2, 2, 3])
                bd1 = c1.date_input("調參開始日期", value=datetime(2026, 4, 8).date(), key="rr_bucket_d1")
                bd2 = c2.date_input("調參結束日期", value=datetime.today().date(), key="rr_bucket_d2")
                bn = int(c3.selectbox("最多使用樣本(場)", [50, 100, 150, 200], index=1, key="rr_bucket_n"))

                c1, c2 = st.columns([2, 3])
                ok_tune = _confirm_run(c1, "rr_bucket_tune", label="輸入 RUN 以分桶調參")
                if c2.button("🧠 分桶調參並儲存最佳權重", use_container_width=True, disabled=not ok_tune, key="rr_bucket_tune_btn"):
                    with st.spinner("正在分桶調參（grid search）..."):
                        res = tune_rerank_for_bucket(
                            session,
                            bparts,
                            d1=datetime.combine(bd1, datetime.min.time()) if bd1 else None,
                            d2=datetime.combine(bd2, datetime.max.time()) if bd2 else None,
                            max_races=int(bn),
                            grid_preset=str(grid),
                            objective={"w2_weight": float(w2_w), "top3_2in_weight": float(t2_w)},
                            save=True,
                        )
                    if isinstance(res, dict) and res.get("ok"):
                        st.success(f"✅ 分桶調參完成：樣本 {int(res.get('samples') or 0)} 場")
                        b = res.get("base") if isinstance(res.get("base"), dict) else {}
                        best = res.get("best") if isinstance(res.get("best"), dict) else {}
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("Baseline W2", f"{b.get('w2_rate', 0.0)}%")
                        c2.metric("Best W2", f"{best.get('w2_rate', 0.0)}%")
                        c3.metric("Baseline Top3≥2", f"{b.get('top3_2in_rate', 0.0)}%")
                        c4.metric("Best Top3≥2", f"{best.get('top3_2in_rate', 0.0)}%")
                        st.rerun()
                    else:
                        st.error(f"❌ 分桶調參失敗：{res.get('reason') if isinstance(res, dict) else res}")
                        dbg = res.get("debug") if isinstance(res, dict) else None
                        if isinstance(dbg, dict):
                            st.markdown(f"- 掃描賽事：**{int(dbg.get('scanned') or 0)}**")
                            st.markdown(f"- 命中分桶：**{int(dbg.get('in_bucket') or 0)}**")
                            st.markdown(f"- 有 AI 報告：**{int(dbg.get('with_report') or 0)}**")
                            st.markdown(f"- 有賽果 Top4：**{int(dbg.get('with_results') or 0)}**")
                            ex = dbg.get("missing_examples") if isinstance(dbg.get("missing_examples"), dict) else {}
                            if ex:
                                with st.expander("查看缺失樣本例子", expanded=False):
                                    st.json(ex)
                            hint = str(dbg.get("hint") or "").strip()
                            if hint:
                                st.caption(hint)

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
