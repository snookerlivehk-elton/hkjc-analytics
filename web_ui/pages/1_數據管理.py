import streamlit as st
import pandas as pd
import json
import os
import subprocess
import sys
from pathlib import Path

# 加入專案路徑
root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from scoring_engine.core import ScoringEngine
from scoring_engine.member_stats import HIT_METRICS, METRIC_LABELS
from scoring_engine.job_queue import get_job, list_recent_jobs, peek_queue, rebuild_queue_from_recent_jobs
from web_ui.auth import require_superadmin
from web_ui.nav import render_admin_nav
from web_ui.utils import _confirm_run

st.set_page_config(page_title="數據管理 - HKJC Analytics", page_icon="⚙️", layout="wide")

# 全站列表文字靠左
st.markdown(
    """
    <style>
    div[data-testid="stDataFrame"] div[role="gridcell"],
    div[data-testid="stDataFrame"] div[role="columnheader"] {
      text-align: left !important;
      justify-content: flex-start !important;
    }
    div[data-testid="stDataFrame"] table td,
    div[data-testid="stDataFrame"] table th,
    div[data-testid="stTable"] table td,
    div[data-testid="stTable"] table th {
      text-align: left !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# 初始化資料庫 (確保在雲端環境表結構存在)
init_db()

require_superadmin("🛠️ 數據管理後台")

st.title("🛠️ 數據管理後台")
st.markdown("在此頁面執行數據更新、回填與清理操作。")
render_admin_nav()

def trigger_scraper(target_date: str = None):
    """實時日誌串流輸出"""
    st.markdown("### 🚀 爬蟲執行進度")
    log_placeholder = st.empty() 
    full_log = ""
    try:
        env = os.environ.copy()
        if target_date:
            env["TARGET_DATE"] = target_date
            
        process = subprocess.Popen(
            [sys.executable, "scripts/run_scraper.py"],
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

def trigger_history_backfill(target_date: str = None, mode: str = None):
    """歷史數據回填進度"""
    st.markdown("### 📚 歷史數據回填進度")
    log_placeholder = st.empty()
    full_log = ""
    try:
        env = os.environ.copy()
        if target_date:
            env["TARGET_DATE"] = target_date
        if mode:
            env["BACKFILL_MODE"] = mode
        process = subprocess.Popen(
            [sys.executable, "scripts/fetch_history.py"],
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

def trigger_race_results_fetch(target_date: str = None):
    st.markdown("### 🏁 賽果與派彩抓取進度")
    log_placeholder = st.empty()
    full_log = ""
    try:
        env = os.environ.copy()
        if target_date:
            env["TARGET_DATE"] = target_date
        process = subprocess.Popen(
            [sys.executable, "scripts/fetch_race_results.py"],
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

def trigger_fixture_fetch():
    st.markdown("### 📅 賽期表更新進度")
    log_placeholder = st.empty()
    full_log = ""
    try:
        env = os.environ.copy()
        process = subprocess.Popen(
            [sys.executable, "scripts/fetch_fixture.py"],
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

def trigger_predictions_snapshot(target_date: str):
    st.markdown("### 🧾 Top5 預測快照生成進度")
    log_placeholder = st.empty()
    full_log = ""
    try:
        env = os.environ.copy()
        if target_date:
            env["TARGET_DATE"] = target_date
        process = subprocess.Popen(
            [sys.executable, "scripts/generate_predictions.py"],
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


def trigger_speedpro_fetch(target_date: str, race_nos: str = "", retry_minutes: int = 120, force: bool = True):
    st.markdown("### ⚡ SpeedPRO 能量分抓取進度")
    log_placeholder = st.empty()
    full_log = ""
    try:
        env = os.environ.copy()
        if target_date:
            env["TARGET_DATE"] = target_date
        if race_nos:
            env["RACE_NOS"] = race_nos
        env["SPEEDPRO_RETRY_MINUTES"] = str(int(retry_minutes or 120))
        if force:
            env["FORCE_SPEEDPRO_FETCH"] = "1"
        process = subprocess.Popen(
            [sys.executable, "scripts/cron_speedpro_fetch.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            bufsize=1,
        )
        for line in iter(process.stdout.readline, ""):
            full_log += line
            log_placeholder.code(full_log)
        process.stdout.close()
        return process.wait() == 0
    except Exception as e:
        st.error(f"❌ 系統錯誤: {e}")
        return False

def cleanup_removed_factor_data(session):
    try:
        from database.models import ScoringFactor, ScoringWeight, SystemConfig
        deleted_sf = session.query(ScoringFactor).filter(ScoringFactor.factor_name == "trainer_horse_bond").delete()
        deleted_sw = session.query(ScoringWeight).filter(ScoringWeight.factor_name == "trainer_horse_bond").delete()
        deleted_cfg = session.query(SystemConfig).filter(SystemConfig.key == "trainer_horse_bond_config").delete()
        session.commit()
        return deleted_sf, deleted_sw, deleted_cfg
    except Exception as e:
        session.rollback()
        st.error(f"❌ 清理失敗: {e}")
        return 0, 0, 0

tab_monitor, tab_ops, tab_members, tab_hits = st.tabs(["📡 監察面板", "🛠️ 系統操作", "👥 會員組合", "📈 命中統計"])

with tab_monitor:
    st.subheader("📡 監察面板")
    st.caption("用途：一眼檢查各資料域最後更新時間、缺口與重算狀態；並集中提供常用更新按鈕（分層：主流程／進階修復／維護工具）。")

    from datetime import date, datetime, time as dtime, timedelta
    from database.models import Race, RaceEntry, RaceResult, RaceDividend, RaceTrackCondition, HorseHistory, OddsHistory, ScoringFactor, PredictionTop5, SystemConfig, RaceCoRunning
    from scoring_engine.config_value import unwrap_value
    from scoring_engine.normalization import venue_label

    def _list_race_dates(session, need: int = 180):
        take_rows = max(200, int(need) * 20)
        rows = session.query(Race.race_date).order_by(Race.race_date.desc()).limit(int(take_rows)).all()
        out = []
        seen = set()
        for (dt0,) in rows:
            if not dt0:
                continue
            dd = dt0.date()
            if dd in seen:
                continue
            seen.add(dd)
            out.append(dd)
            if len(out) >= int(need):
                break
        return out

    def _day_range(d: date):
        start = datetime.combine(d, dtime.min)
        end = start + timedelta(days=1)
        return start, end

    def _max_dt(session, model, col_name: str):
        try:
            col = getattr(model, col_name)
            return session.query(col).order_by(col.desc()).limit(1).scalar()
        except Exception:
            return None

    def _syscfg_latest_by_prefix(session, prefix: str):
        try:
            row = (
                session.query(SystemConfig.key, SystemConfig.updated_at, SystemConfig.value)
                .filter(SystemConfig.key.like(f"{str(prefix)}%"))
                .order_by(SystemConfig.updated_at.desc())
                .limit(1)
                .first()
            )
            if not row:
                return None
            k, upd, val = row
            payload, meta = unwrap_value(val)
            return {"key": str(k), "updated_at": upd, "payload": payload, "meta": meta}
        except Exception:
            return None

    def _load_fixture_dates(session, need: int = 365):
        cfg = session.query(SystemConfig).filter_by(key="fixture_dates").first()
        payload, _ = unwrap_value(cfg.value) if cfg else (None, {})
        raw = payload if isinstance(payload, list) else (cfg.value if cfg and isinstance(cfg.value, list) else [])
        out = []
        seen = set()
        for x in raw or []:
            s = str(x or "").strip()
            if not s:
                continue
            try:
                d0 = datetime.strptime(s.replace("-", "/"), "%Y/%m/%d").date()
            except Exception:
                continue
            if d0 in seen:
                continue
            seen.add(d0)
            out.append(d0)
            if len(out) >= int(need or 365):
                break
        out.sort(reverse=True)
        return out

    session_m = get_session()
    try:
        race_dates = _list_race_dates(session_m, need=180)
        fixture_dates = _load_fixture_dates(session_m, need=365)
        mode = st.radio("賽日來源", options=["已入庫", "賽期表", "手動輸入"], horizontal=True, key="monitor_date_mode")
        sel_date = None
        if mode == "賽期表":
            if not fixture_dates:
                st.info("未找到賽期表賽日（可到「維護工具」更新賽期表）。")
            else:
                sel_date = st.selectbox("賽日", options=fixture_dates, index=0, key="monitor_date_fixture")
        elif mode == "手動輸入":
            s_in = st.text_input("賽日（YYYY/MM/DD）", value="", key="monitor_date_manual")
            s_in = str(s_in or "").strip()
            if s_in:
                try:
                    sel_date = datetime.strptime(s_in.replace("-", "/"), "%Y/%m/%d").date()
                except Exception:
                    st.error("日期格式錯誤，請用 YYYY/MM/DD（例如 2026/05/13）。")
        else:
            if not race_dates:
                st.info("資料庫未有任何已入庫賽日。")
            else:
                sel_date = st.selectbox("賽日", options=race_dates, index=0, key="monitor_date")

        if not sel_date:
            st.stop()

        start_dt, end_dt = _day_range(sel_date)
        date_str = sel_date.strftime("%Y/%m/%d")

        st.markdown("#### 🧾 Job 狀態（worker 任務）")
        c_j1, c_j2 = st.columns([1, 3])
        if c_j1.button("刷新", use_container_width=True, key="monitor_jobs_refresh"):
            st.rerun()
        qinfo = peek_queue(session_m)
        qlen = int(qinfo.get("len") or 0)
        st.caption(f"queue_len={qlen}")
        if c_j1.button("修復 queue", use_container_width=True, key="monitor_jobs_rebuild_queue"):
            res = rebuild_queue_from_recent_jobs(session_m, limit=200)
            st.success(f"已修復 queue：added={int(res.get('added') or 0)} len={int(res.get('len') or 0)}")
            st.rerun()
        jobs = list_recent_jobs(session_m, limit=30)
        rows_j = []
        for j in jobs:
            if not isinstance(j, dict):
                continue
            pid = str(j.get("id") or "")
            prog = j.get("progress") if isinstance(j.get("progress"), dict) else {}
            rows_j.append(
                {
                    "id": pid,
                    "type": str(j.get("type") or ""),
                    "status": str(j.get("status") or ""),
                    "current": str(prog.get("current") or ""),
                    "done/total": f"{int(prog.get('done') or 0)}/{int(prog.get('total') or 0)}",
                    "updated_at": str(j.get("updated_at") or ""),
                }
            )
        df_jobs = pd.DataFrame(rows_j)
        if df_jobs.empty:
            st.caption("未有任何 job（或尚未啟動 worker）。")
        else:
            st.dataframe(df_jobs, use_container_width=True, hide_index=True)
            job_ids = [r.get("id") for r in rows_j if str(r.get("id") or "").strip()]
            sel_job = c_j2.selectbox("查看 job 詳情", options=[""] + job_ids, index=0, key="monitor_job_sel")
            if sel_job:
                job = get_job(session_m, str(sel_job))
                if not isinstance(job, dict):
                    st.info("找不到 job。")
                else:
                    st.json({k: job.get(k) for k in ["id", "type", "status", "created_at", "started_at", "finished_at", "progress", "result", "error"]})
                    log = job.get("log") if isinstance(job.get("log"), list) else []
                    if log:
                        st.code("\n".join([str(x) for x in log[-200:]]), language="text")
                    else:
                        st.caption("暫無 log。")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("races.updated_at", str(_max_dt(session_m, Race, "updated_at") or ""))
        c2.metric("race_entries.updated_at", str(_max_dt(session_m, RaceEntry, "updated_at") or ""))
        c3.metric("scoring_factors.updated_at", str(_max_dt(session_m, ScoringFactor, "updated_at") or ""))
        c4.metric("prediction_top5.created_at", str(_max_dt(session_m, PredictionTop5, "created_at") or _max_dt(session_m, PredictionTop5, "race_date") or ""))

        st.markdown("#### 🧾 快照/統計最新狀態")
        cols = st.columns(3)
        latest_trk = _syscfg_latest_by_prefix(session_m, "trkprof:")
        latest_runpos = _syscfg_latest_by_prefix(session_m, "race_runpos:")
        latest_ai = _syscfg_latest_by_prefix(session_m, "ai_race_report:")
        cols[0].write("trkprof:*")
        cols[0].caption((latest_trk or {}).get("key") or "—")
        cols[0].caption(str((latest_trk or {}).get("updated_at") or ""))
        cols[1].write("race_runpos:*")
        cols[1].caption((latest_runpos or {}).get("key") or "—")
        cols[1].caption(str((latest_runpos or {}).get("updated_at") or ""))
        cols[2].write("ai_race_report:*")
        cols[2].caption((latest_ai or {}).get("key") or "—")
        cols[2].caption(str((latest_ai or {}).get("updated_at") or ""))

        st.markdown("#### 🧩 所選賽日：各場資料完整度")
        races = (
            session_m.query(Race)
            .filter(Race.race_date >= start_dt, Race.race_date < end_dt)
            .order_by(Race.race_no.asc(), Race.id.asc())
            .all()
        )
        if not races:
            st.info("該日未有賽事資料（未入庫）。可直接用下方按鈕「抓排位」把該日賽事入庫。")
        else:
            race_ids = [int(getattr(r, "id") or 0) for r in races if int(getattr(r, "id") or 0) > 0]
            race_no_by_id = {int(getattr(r, "id") or 0): int(getattr(r, "race_no") or 0) for r in races if int(getattr(r, "id") or 0) > 0}

            entries_race_ids = set(rid for (rid,) in session_m.query(RaceEntry.race_id).filter(RaceEntry.race_id.in_(race_ids)).distinct().all())
            scores_race_ids = set(
                rid
                for (rid,) in session_m.query(RaceEntry.race_id)
                .filter(RaceEntry.race_id.in_(race_ids))
                .filter(RaceEntry.total_score != None)
                .distinct()
                .all()
            )
            factors_race_ids = set(
                rid
                for (rid,) in session_m.query(RaceEntry.race_id)
                .join(ScoringFactor, ScoringFactor.entry_id == RaceEntry.id)
                .filter(RaceEntry.race_id.in_(race_ids))
                .distinct()
                .all()
            )
            results_race_ids = set(
                rid
                for (rid,) in session_m.query(RaceEntry.race_id)
                .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
                .filter(RaceEntry.race_id.in_(race_ids))
                .distinct()
                .all()
            )
            div_race_ids = set(
                rid for (rid,) in session_m.query(RaceDividend.race_id).filter(RaceDividend.race_id.in_(race_ids)).distinct().all()
            )
            tc_race_ids = set(
                rid for (rid,) in session_m.query(RaceTrackCondition.race_id).filter(RaceTrackCondition.race_id.in_(race_ids)).distinct().all()
            )
            hh_race_ids = set(
                rid
                for (rid,) in session_m.query(RaceEntry.race_id)
                .join(HorseHistory, HorseHistory.horse_id == RaceEntry.horse_id)
                .filter(RaceEntry.race_id.in_(race_ids))
                .distinct()
                .all()
            )
            top5_race_ids = set(
                rid for (rid,) in session_m.query(PredictionTop5.race_id).filter(PredictionTop5.race_id.in_(race_ids)).distinct().all()
            )
            cor_race_ids = set(
                rid for (rid,) in session_m.query(RaceCoRunning.race_id).filter(RaceCoRunning.race_id.in_(race_ids)).distinct().all()
            )

            runpos_keys = [f"race_runpos:{date_str}:{int(race_no_by_id.get(rid) or 0)}" for rid in race_ids if int(race_no_by_id.get(rid) or 0) > 0]
            ai_keys = [f"ai_race_report:{date_str}:{int(race_no_by_id.get(rid) or 0)}" for rid in race_ids if int(race_no_by_id.get(rid) or 0) > 0]
            syscfg_keys = list(dict.fromkeys([k for k in (runpos_keys + ai_keys) if str(k).strip()]))
            syscfg_key_set = set()
            if syscfg_keys:
                syscfg_key_set = set(k for (k,) in session_m.query(SystemConfig.key).filter(SystemConfig.key.in_(syscfg_keys)).all())

            rows = []
            for r in races:
                rid = int(getattr(r, "id") or 0)
                rn = int(getattr(r, "race_no") or 0)
                has_entries = rid in entries_race_ids
                has_scores = rid in scores_race_ids
                has_factors = rid in factors_race_ids
                has_results = rid in results_race_ids
                has_div = rid in div_race_ids
                has_tc = rid in tc_race_ids
                has_hh = rid in hh_race_ids
                has_top5 = rid in top5_race_ids

                runpos_key = f"race_runpos:{date_str}:{rn}"
                has_runpos = runpos_key in syscfg_key_set
                has_cor = rid in cor_race_ids

                rep_key = f"ai_race_report:{date_str}:{rn}"
                has_ai = rep_key in syscfg_key_set

                rows.append(
                    {
                        "RaceNo": rn,
                        "RaceID": rid,
                        "地點": venue_label(getattr(r, "venue", ""), track_type=getattr(r, "track_type", None)),
                        "排位": "✅" if has_entries else "—",
                        "往績": "✅" if has_hh else "—",
                        "計分": "✅" if has_scores else "—",
                        "因子": "✅" if has_factors else "—",
                        "Top5快照": "✅" if has_top5 else "—",
                        "賽果": "✅" if has_results else "—",
                        "派彩": "✅" if has_div else "—",
                        "場地狀況": "✅" if has_tc else "—",
                        "runpos": "✅" if has_runpos else "—",
                        "corunning": "✅" if has_cor else "—",
                        "AI報告": "✅" if has_ai else "—",
                    }
                )

            df = pd.DataFrame(rows).sort_values(["RaceNo"])
            st.dataframe(df, use_container_width=True, hide_index=True)

            st.markdown("#### 🔎 資料內容檢視")
            race_nos = [int(x) for x in df["RaceNo"].tolist() if int(x or 0) > 0]
            sel_rn = st.selectbox("選擇場次", options=race_nos, index=0, key="monitor_race_no")
            rr = next((x for x in races if int(getattr(x, "race_no") or 0) == int(sel_rn)), None)
            sel_rid = int(getattr(rr, "id") or 0) if rr else 0

            v1, v2 = st.columns(2)
            with v1.expander("runpos 快照", expanded=False):
                key = f"race_runpos:{date_str}:{int(sel_rn)}"
                cfg = session_m.query(SystemConfig).filter_by(key=key).first()
                payload, meta = unwrap_value(cfg.value) if cfg else (None, {})
                if not cfg:
                    st.info("未找到 runpos 快照。")
                else:
                    st.caption(f"key={key}｜updated_at={getattr(cfg,'updated_at',None)}")
                    if meta:
                        st.caption("｜".join([f"{k}={str(meta.get(k) or '').strip()}" for k in ["source", "schema", "fetched_at", "saved_at"] if str(meta.get(k) or "").strip()]))
                    st.json(payload if isinstance(payload, dict) else {})

            with v2.expander("corunning（賽後走勢評述）", expanded=False):
                row = session_m.query(RaceCoRunning).filter_by(race_id=int(sel_rid)).first()
                if not row or not isinstance(row.items, dict) or not row.items:
                    st.info("未找到 corunning 資料。")
                else:
                    cap = f"race_id={sel_rid}"
                    try:
                        if getattr(row, "fetched_at", None):
                            cap += f"｜fetched_at={row.fetched_at.isoformat()}"
                    except Exception:
                        pass
                    if str(getattr(row, "source", "") or "").strip():
                        cap += f"｜source={str(getattr(row,'source','') or '').strip()}"
                    meta = row.meta if isinstance(row.meta, dict) else {}
                    if str(meta.get("schema") or "").strip():
                        cap += f"｜schema={str(meta.get('schema') or '').strip()}"
                    st.caption(cap)
                    tbl = []
                    for k, v in row.items.items():
                        if not isinstance(v, dict):
                            continue
                        try:
                            hn = int(k)
                        except Exception:
                            continue
                        tbl.append(
                            {
                                "馬號": hn,
                                "馬名": str(v.get("horse_name") or v.get("name") or "").strip(),
                                "走勢評述": str(v.get("commentary") or v.get("comment") or "").strip(),
                            }
                        )
                    tbl.sort(key=lambda x: int(x.get("馬號") or 0))
                    st.dataframe(tbl, use_container_width=True, hide_index=True)

        st.markdown("#### ⚡ 常用更新（集中）")
        st.caption("主流程建議只用下面幾個按鈕；進階/維護工具收合在下方，避免誤用導致資料不一致。")

        c_confirm, c_btn = st.columns([2, 3])
        ok = _confirm_run(c_confirm, "oneclick_update_monitor", label="輸入 RUN 以執行一鍵完整更新")
        st.markdown("**一鍵完整更新：可選步驟**")
        c_s1, c_s2, c_s3, c_s4, c_s5 = st.columns(5)
        step_scrape = c_s1.checkbox("抓排位", value=True, key="oneclick_step_scrape")
        step_history = c_s2.checkbox("回填往績", value=True, key="oneclick_step_history")
        step_rescore = c_s3.checkbox("重算", value=True, key="oneclick_step_rescore")
        step_snapshot = c_s4.checkbox("Top5快照", value=True, key="oneclick_step_snapshot")
        step_results = c_s5.checkbox("賽果/派彩", value=False, key="oneclick_step_results")

        if c_btn.button("⚡ 一鍵：抓排位 → 回填馬匹往績 → 重算當日 → 生成Top5快照", use_container_width=True, disabled=not ok, key="monitor_oneclick"):
            from scoring_engine.job_queue import enqueue_job

            steps = []
            if step_scrape:
                steps.append("scrape")
            if step_history:
                steps.append("history")
            if step_rescore:
                steps.append("rescore")
            if step_snapshot:
                steps.append("snapshot")
            if step_results:
                steps.append("results")
            if not steps:
                st.error("❌ 請至少勾選一個步驟。")
                st.stop()

            job = enqueue_job(
                session_m,
                "daily_update_pipeline",
                {"date": str(date_str), "steps": steps},
            )
            st.success(f"✅ 已排程一鍵完整更新（job_id={str(job.get('id') or '')}）。請到上方 Job 狀態查看進度。")
            st.rerun()

        c1, c2, c3 = st.columns(3)
        ok2 = _confirm_run(c1, "monitor_fetch_results", label="輸入 RUN")
        if c1.button("🏁 抓取賽果與派彩", use_container_width=True, disabled=not ok2, key="monitor_fetch_results_btn"):
            from scoring_engine.job_queue import enqueue_job

            job = enqueue_job(session_m, "fetch_race_results", {"date": str(date_str)})
            st.success(f"✅ 已排程抓取賽果與派彩（job_id={str(job.get('id') or '')}）。請到上方 Job 狀態查看進度。")
            st.rerun()

        ok3 = _confirm_run(c2, "monitor_snapshot", label="輸入 RUN")
        if c2.button("🧾 生成 Top5 快照", use_container_width=True, disabled=not ok3, key="monitor_snapshot_btn"):
            from scoring_engine.job_queue import enqueue_job

            job = enqueue_job(session_m, "daily_update_pipeline", {"date": str(date_str), "steps": ["snapshot"]})
            st.success(f"✅ 已排程生成 Top5 快照（job_id={str(job.get('id') or '')}）。請到上方 Job 狀態查看進度。")
            st.rerun()

        ok4 = _confirm_run(c3, "monitor_rescore", label="輸入 RUN")
        if c3.button("🚀 重算該日所有場次", use_container_width=True, disabled=not ok4, key="monitor_rescore_btn"):
            from scoring_engine.job_queue import enqueue_job

            job = enqueue_job(session_m, "rescore_race_date", {"date": str(date_str)})
            st.success(f"✅ 已排程重算 {date_str}（job_id={str(job.get('id') or '')}）。")
            st.rerun()

        with st.expander("🧰 進階修復（低頻）", expanded=False):
            st.caption("只有在 cron/一鍵流程失敗時才需要。")
            c1, c2 = st.columns(2)
            ok = _confirm_run(c1, "monitor_scrape", label="輸入 RUN")
            if c1.button("🔄 只抓取該日賽事（排位/即時）", use_container_width=True, disabled=not ok, key="monitor_scrape_btn"):
                from scoring_engine.job_queue import enqueue_job

                job = enqueue_job(session_m, "daily_update_pipeline", {"date": str(date_str), "steps": ["scrape"]})
                st.success(f"✅ 已排程抓取排位（job_id={str(job.get('id') or '')}）。請到上方 Job 狀態查看進度。")
                st.rerun()
            ok = _confirm_run(c2, "monitor_history", label="輸入 RUN")
            if c2.button("📚 只回填該日馬匹往績", use_container_width=True, disabled=not ok, key="monitor_history_btn"):
                from scoring_engine.job_queue import enqueue_job

                job = enqueue_job(session_m, "daily_update_pipeline", {"date": str(date_str), "steps": ["history"]})
                st.success(f"✅ 已排程回填往績（job_id={str(job.get('id') or '')}）。請到上方 Job 狀態查看進度。")
                st.rerun()

        with st.expander("🧨 維護工具（高風險）", expanded=False):
            st.caption("只在排障/遷移/清理時使用。")
            c1, c2, c3 = st.columns(3)
            ok = _confirm_run(c1, "monitor_fixture", label="輸入 RUN")
            if c1.button("📅 更新賽期表 (本月+下月)", use_container_width=True, disabled=not ok, key="monitor_fixture_btn"):
                if trigger_fixture_fetch():
                    st.success("✅ 已更新賽期表。")
                    st.rerun()
            ok = _confirm_run(c2, "monitor_speedpro", label="輸入 RUN")
            if c2.button("⚡ 立即抓取 SpeedPRO（cron 備用）", use_container_width=True, disabled=not ok, key="monitor_speedpro_btn"):
                from scoring_engine.job_queue import enqueue_job

                job = enqueue_job(session_m, "speedpro_fetch", {"date": str(date_str), "race_nos": "", "retry_minutes": 120, "force": True})
                st.success(f"✅ 已排程抓取 SpeedPRO（job_id={str(job.get('id') or '')}）。請到上方 Job 狀態查看進度。")
                st.rerun()
            ok = _confirm_run(c3, "monitor_cleanup", label="輸入 RUN")
            if c3.button("🧹 清理 trainer_horse_bond 舊記錄", use_container_width=True, disabled=not ok, key="monitor_cleanup_btn"):
                deleted_sf, deleted_sw, deleted_cfg = cleanup_removed_factor_data(session_m)
                st.success(f"✅ 已清理：ScoringFactor={deleted_sf} ScoringWeight={deleted_sw} SystemConfig={deleted_cfg}")
                st.rerun()

            st.divider()
            c4, c5 = st.columns([1, 3])
            ok = _confirm_run(c4, "monitor_purge_results", label="輸入 RUN")
            if c4.button("🧯 清除該日賽果/派彩/走位（修復未開賽誤寫）", use_container_width=True, disabled=not ok, key="monitor_purge_results_btn"):
                from datetime import datetime as _dt, time as _dtime, timedelta as _td
                from sqlalchemy import and_
                from database.models import Race, RaceEntry, RaceResult, RaceDividend, RaceTrackCondition, SystemConfig

                d0 = _dt.strptime(str(date_str), "%Y/%m/%d").date()
                start = _dt.combine(d0, _dtime.min)
                end = start + _td(days=1)
                races = session_m.query(Race.id, Race.race_no).filter(and_(Race.race_date >= start, Race.race_date < end)).all()
                race_ids = [int(rid) for rid, _ in races]
                if not race_ids:
                    st.warning("找不到該日 races，無需清除。")
                    st.stop()

                entry_ids = [int(x[0]) for x in session_m.query(RaceEntry.id).filter(RaceEntry.race_id.in_(race_ids)).all()]

                n_rr = 0
                if entry_ids:
                    n_rr = session_m.query(RaceResult).filter(RaceResult.entry_id.in_(entry_ids)).delete(synchronize_session=False)

                n_div = session_m.query(RaceDividend).filter(RaceDividend.race_id.in_(race_ids)).delete(synchronize_session=False)

                n_tc = session_m.query(RaceTrackCondition).filter(RaceTrackCondition.race_id.in_(race_ids)).filter(RaceTrackCondition.source.like("HKJC_LOCALRESULTS%")).delete(synchronize_session=False)

                keys = [f"race_runpos:{str(date_str)}:{int(rno)}" for _, rno in races]
                n_cfg = session_m.query(SystemConfig).filter(SystemConfig.key.in_(keys)).delete(synchronize_session=False)

                session_m.commit()
                st.success(f"✅ 已清除：RaceResult={n_rr} RaceDividend={n_div} RaceTrackCondition={n_tc} SystemConfig(runpos)={n_cfg}")
                st.rerun()
    finally:
        session_m.close()

with tab_ops:
    st.subheader("🛠️ 系統操作（精簡）")
    st.caption("日常更新/重算已集中到「📡 監察面板」。本分頁只保留口徑/權重/校準與維護工具。")

    with st.expander("🧩 因子資料不足策略", expanded=True):
        st.caption("用於識別因子資料是否齊全：可只提示，或在資料覆蓋不足時自動忽略該因子（本場有效權重設為 0）。")
        session_q = get_session()
        try:
            from database.models import SystemConfig, ScoringWeight
            from scoring_engine.constants import DISABLED_FACTORS

            cfg = session_q.query(SystemConfig).filter_by(key="factor_quality_policy").first()
            val = cfg.value if cfg and isinstance(cfg.value, dict) else {}
            default_p = val.get("default") if isinstance(val.get("default"), dict) else {}
            overrides = val.get("overrides") if isinstance(val.get("overrides"), dict) else {}

            def_action = str(default_p.get("action") or "warn").strip().lower()
            def_min_cov = default_p.get("min_coverage")
            try:
                def_min_cov = float(def_min_cov if def_min_cov is not None else 0.7)
            except Exception:
                def_min_cov = 0.7
            if def_min_cov > 1.0:
                def_min_cov = def_min_cov / 100.0
            if def_min_cov < 0.0:
                def_min_cov = 0.0
            if def_min_cov > 1.0:
                def_min_cov = 1.0

            weights = (
                session_q.query(ScoringWeight.factor_name, ScoringWeight.description)
                .filter(ScoringWeight.is_active == True)
                .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
                .order_by(ScoringWeight.factor_name.asc())
                .all()
            )
            factor_rows = []
            for fn, desc in weights:
                code = str(fn or "").strip()
                if not code:
                    continue
                ov = overrides.get(code) if isinstance(overrides.get(code), dict) else {}
                act = str((ov.get("action") if isinstance(ov, dict) else None) or "default").strip().lower()
                mc = ov.get("min_coverage") if isinstance(ov, dict) else None
                try:
                    mc = float(mc) if mc is not None else None
                except Exception:
                    mc = None
                if mc is not None and mc > 1.0:
                    mc = mc / 100.0
                if mc is not None and mc < 0.0:
                    mc = 0.0
                if mc is not None and mc > 1.0:
                    mc = 1.0
                factor_rows.append(
                    {
                        "因子代號": code,
                        "因子名稱": str(desc or code),
                        "模式": act,
                        "門檻(%)": round((mc * 100.0), 0) if mc is not None else None,
                    }
                )

            with st.form("factor_quality_policy_form"):
                c1, c2 = st.columns(2)
                with c1:
                    action_label = "只提示" if def_action != "ignore" else "自動忽略"
                    new_action_label = st.selectbox("預設策略", ["只提示", "自動忽略"], index=0 if action_label == "只提示" else 1)
                with c2:
                    new_min_pct = st.slider("預設門檻(覆蓋率%)", min_value=0, max_value=100, value=int(round(def_min_cov * 100.0)))

                st.markdown("**因子個別設定（可留空＝跟預設）**")
                df_edit = pd.DataFrame(factor_rows)
                edited = st.data_editor(
                    df_edit,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "模式": st.column_config.SelectboxColumn("模式", options=["default", "warn", "ignore"], help="default=跟預設；warn=只提示；ignore=自動忽略"),
                        "門檻(%)": st.column_config.NumberColumn("門檻(%)", min_value=0, max_value=100, step=1, help="留空＝跟預設"),
                    },
                    disabled=["因子代號", "因子名稱"],
                )
                submitted = st.form_submit_button("💾 儲存策略", type="primary")
                if submitted:
                    new_default_action = "ignore" if new_action_label == "自動忽略" else "warn"
                    new_policy = {"default": {"action": new_default_action, "min_coverage": float(new_min_pct) / 100.0}, "overrides": {}}
                    if isinstance(edited, pd.DataFrame):
                        for _, r in edited.iterrows():
                            code = str(r.get("因子代號") or "").strip()
                            if not code:
                                continue
                            mode = str(r.get("模式") or "").strip().lower()
                            mc = r.get("門檻(%)")
                            mc_v = None
                            try:
                                mc_v = float(mc) / 100.0 if mc is not None and str(mc) != "nan" else None
                            except Exception:
                                mc_v = None
                            if mode in ("warn", "ignore") or mc_v is not None:
                                ov = {}
                                if mode in ("warn", "ignore"):
                                    ov["action"] = mode
                                if mc_v is not None:
                                    ov["min_coverage"] = mc_v
                                new_policy["overrides"][code] = ov

                    if not cfg:
                        cfg = SystemConfig(key="factor_quality_policy", description="因子資料不足策略")
                        session_q.add(cfg)
                    cfg.value = new_policy
                    session_q.commit()
                    st.success("✅ 已儲存。新策略會於下一次重新計分後生效。")
                    st.rerun()
        except Exception as e:
            session_q.rollback()
            st.error(f"❌ 策略讀寫失敗: {e}")
        finally:
            session_q.close()

    with st.expander("⚖️ 全局權重設定（ScoringWeight）", expanded=False):
        st.caption("此處係「全局」權重（會影響後台按總分排序、以及以全局權重生成的 Top5/淘汰診斷）。用戶端會員組合係另一套 preset 權重。")
        session_w = get_session()
        try:
            from database.models import ScoringWeight
            from scoring_engine.constants import DISABLED_FACTORS

            rows = (
                session_w.query(ScoringWeight)
                .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
                .order_by(ScoringWeight.factor_name.asc())
                .all()
            )
            items = []
            for w in rows:
                fn = str(getattr(w, "factor_name", "") or "").strip()
                if not fn:
                    continue
                items.append(
                    {
                        "因子代號": fn,
                        "因子名稱": str(getattr(w, "description", "") or fn),
                        "權重": (float(w.weight) if getattr(w, "weight", None) is not None else None),
                        "啟用": bool(getattr(w, "is_active", False)),
                    }
                )

            if not items:
                st.info("目前未找到任何全局權重設定。")
            else:
                dfw = pd.DataFrame(items)
                edited_w = st.data_editor(
                    dfw,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "權重": st.column_config.NumberColumn("權重", step=0.1, help="留空會視作 0；建議一般保持 >0"),
                        "啟用": st.column_config.CheckboxColumn("啟用"),
                    },
                    disabled=["因子代號", "因子名稱"],
                    key="global_weight_editor",
                )
                c_save, c_hint = st.columns([2, 3])
                save_w = c_save.button("💾 儲存全局權重", use_container_width=True, key="save_global_weights")
                c_hint.caption("儲存後需重算相關場次，才會寫回 RaceEntry.total_score。")

                if save_w and isinstance(edited_w, pd.DataFrame):
                    w_by_name = {str(x.factor_name): x for x in rows if getattr(x, "factor_name", None)}
                    for _, r in edited_w.iterrows():
                        code = str(r.get("因子代號") or "").strip()
                        if not code or code not in w_by_name:
                            continue
                        obj = w_by_name[code]
                        v = r.get("權重")
                        try:
                            obj.weight = float(v) if v is not None and str(v) != "nan" else 0.0
                        except Exception:
                            obj.weight = 0.0
                        obj.is_active = bool(r.get("啟用") is True)
                    session_w.commit()
                    st.success("✅ 已儲存全局權重。")
                    st.rerun()
        except Exception as e:
            session_w.rollback()
            st.error(f"❌ 全局權重讀寫失敗: {e}")
        finally:
            session_w.close()

    with st.expander("🎯 勝率校準（Temperature）", expanded=False):
        st.caption("用途：把「總分→預估勝率」的 softmax 溫度做校準，讓勝率分佈更貼近歷史賽果（只影響顯示/勝率欄位，不改排名）。")
        session_cal = get_session()
        try:
            from scoring_engine.calibration import fit_winprob_temperature, load_winprob_temperature, save_winprob_temperature
            from database.models import Race, RaceEntry, RaceResult
            from scoring_engine.core import ScoringEngine
            from datetime import date, timedelta

            current_t = load_winprob_temperature(session_cal)
            if current_t:
                st.info(f"目前 temperature：{float(current_t):.3f}")
            else:
                st.info("目前未設定 temperature（預設 1.0）。")

            drows = (
                session_cal.query(Race.race_date)
                .join(RaceEntry, RaceEntry.race_id == Race.id)
                .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
                .filter(RaceResult.rank != None)
                .order_by(Race.race_date.desc())
                .limit(5000)
                .all()
            )
            available_dates = []
            seen = set()
            for (dt,) in drows:
                if not dt:
                    continue
                dd = dt.date()
                if dd in seen:
                    continue
                seen.add(dd)
                available_dates.append(dd)
                if len(available_dates) >= 365:
                    break
            if not available_dates:
                st.info("目前未有任何已結算賽果可供校準。")
            else:
                end_default = available_dates[0]
                start_default = max(end_default - timedelta(days=60), min(available_dates))
                d1, d2 = st.date_input("校準日期範圍", value=(start_default, end_default), key="calib_dates")
                if isinstance(d1, date) and isinstance(d2, date) and d1 > d2:
                    d1, d2 = d2, d1

                c1, c2 = st.columns([2, 3])
                do_rescore = c1.checkbox("同時重算所選範圍", value=False, key="calib_rescore")
                ok = _confirm_run(c1, "calib_train", label="輸入 RUN 以訓練/保存")
                run = c2.button("訓練並保存 temperature", use_container_width=True, key="calib_train_btn", disabled=not ok)

                if run:
                    res = fit_winprob_temperature(session_cal, d1=d1, d2=d2)
                    if res.get("ok") is True:
                        save_winprob_temperature(session_cal, res)
                        st.success(f"✅ 已保存 temperature={float(res.get('temperature') or 1.0):.3f}（races={int(res.get('races') or 0)} nll={float(res.get('nll') or 0.0):.4f}）")
                        if do_rescore:
                            from datetime import datetime, time as dtime, timedelta
                            start = datetime.combine(d1, dtime.min)
                            end = datetime.combine(d2, dtime.min) + timedelta(days=1)
                            races2 = (
                                session_cal.query(Race)
                                .filter(Race.race_date >= start)
                                .filter(Race.race_date < end)
                                .order_by(Race.race_date.asc(), Race.race_no.asc(), Race.id.asc())
                                .all()
                            )
                            engine = ScoringEngine(session_cal)
                            for r in races2:
                                rid2 = int(getattr(r, "id") or 0)
                                if rid2:
                                    engine.score_race(rid2)
                            st.success("✅ 已重算所選範圍場次。")
                        st.rerun()
                    else:
                        st.error("❌ 訓練失敗：所選範圍內沒有足夠的已結算賽果/計分資料。")
        finally:
            session_cal.close()

    with st.expander("🔌 系統測試與升級", expanded=False):
        st.caption("用於排查連線/結構問題。一般日常不用操作。")
        if st.button("🔌 測試資料庫連線", use_container_width=True, key="db_conn_test_btn"):
            session = get_session()
            try:
                from database.models import ScoringWeight
                count = session.query(ScoringWeight).count()
                st.success(f"✅ 連線正常 (權重紀錄: {count})")
            except Exception as e:
                st.error(f"❌ 連線失敗: {e}")
            session.close()

        c_confirm, c_btn = st.columns([2, 3])
        ok = _confirm_run(c_confirm, "db_upgrade", label="輸入 RUN 以執行升級")
        if c_btn.button("🆙 執行資料庫欄位升級 (新增原始數據欄位)", use_container_width=True, disabled=not ok, key="db_upgrade_btn"):
            try:
                env = os.environ.copy()
                process = subprocess.Popen(
                    [sys.executable, "scripts/upgrade_db.py"],
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

with tab_members:
    st.subheader("👥 會員白名單")
    session_cfg = get_session()
    try:
        from database.models import SystemConfig

        cfg = session_cfg.query(SystemConfig).filter_by(key="member_whitelist_emails").first()
        emails = []
        if cfg and isinstance(cfg.value, list):
            emails = [str(x).strip().lower() for x in cfg.value if str(x).strip()]
        emails = list(dict.fromkeys(emails))
        default_text = "\n".join(emails)

        with st.form("member_whitelist_form"):
            text = st.text_area("允許登入的 Email（每行一個）", value=default_text, height=140, placeholder="name@example.com")
            submitted = st.form_submit_button("💾 儲存白名單", type="primary")
            if submitted:
                new_list = []
                for line in str(text or "").splitlines():
                    e = line.strip().lower()
                    if e:
                        new_list.append(e)
                new_list = list(dict.fromkeys(new_list))
                if not cfg:
                    cfg = SystemConfig(key="member_whitelist_emails", description="會員登入白名單 (email)")
                    session_cfg.add(cfg)
                cfg.value = new_list
                session_cfg.commit()
                st.success(f"已儲存 {len(new_list)} 個 Email。")
                st.rerun()
    except Exception as e:
        session_cfg.rollback()
        st.error(f"❌ 白名單讀寫失敗: {e}")
    finally:
        session_cfg.close()

    with st.expander("📉 會員反向統計總表（回填/重建）", expanded=False):
        st.caption("用途：補回歷史淘汰準確率/錯殺率統計，並覆寫保存到 SystemConfig（member_weight_preset_elim_stats:<email>）。")
        session_elim = get_session()
        try:
            from database.models import SystemConfig
            from scoring_engine.member_stats import rebuild_member_preset_elim_stats
            from datetime import datetime, date, timedelta

            cfg = session_elim.query(SystemConfig).filter_by(key="member_whitelist_emails").first()
            emails = []
            if cfg and isinstance(cfg.value, list):
                emails = [str(x).strip().lower() for x in cfg.value if str(x).strip()]
            emails = list(dict.fromkeys(emails))
            if not emails:
                st.info("未設定會員白名單，無法回填。")
            else:
                end_default = date.today()
                start_default = end_default - timedelta(days=30)
                d1, d2 = st.date_input("回填日期範圍", value=(start_default, end_default), key="admin_elim_rebuild_range")
                if isinstance(d1, date) and isinstance(d2, date) and d1 > d2:
                    d1, d2 = d2, d1

                cols = st.columns([2, 3])
                ok = _confirm_run(cols[0], "admin_elim_rebuild", label="輸入 RUN 以回填/重建")
                if cols[1].button("📉 回填會員反向統計（覆寫）", use_container_width=True, disabled=not ok, key="admin_elim_rebuild_btn"):
                    progress = st.progress(0)
                    done = 0
                    for i, em in enumerate(emails):
                        cfg2 = session_elim.query(SystemConfig).filter_by(key=f"member_weight_presets:{str(em)}").first()
                        presets = cfg2.value if cfg2 and isinstance(cfg2.value, list) else []
                        rebuild_member_preset_elim_stats(
                            session=session_elim,
                            email=str(em),
                            presets=presets,
                            d1=datetime.combine(d1, datetime.min.time()),
                            d2=datetime.combine(d2, datetime.min.time()),
                        )
                        done += 1
                        progress.progress((i + 1) / len(emails))
                    st.success(f"✅ 已回填 {done} 位會員。")
                    st.rerun()
        finally:
            session_elim.close()

    st.subheader("👥 全部會員「儲存組合」列表")
    from database.models import SystemConfig, ScoringWeight
    from scoring_engine.constants import DISABLED_FACTORS
    from scoring_engine.member_stats import load_member_preset_stats

    session_all = get_session()
    try:
        weights = (
            session_all.query(ScoringWeight)
            .filter(ScoringWeight.is_active == True)
            .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
            .all()
        )
        factor_desc = {w.factor_name: w.description for w in weights}

        cfgs = (
            session_all.query(SystemConfig)
            .filter(SystemConfig.key.like("member_weight_presets:%"))
            .order_by(SystemConfig.key.asc())
            .all()
        )

        rows = []
        for cfg in cfgs:
            if not isinstance(cfg.value, list):
                continue
            email = cfg.key.split(":", 1)[1] if ":" in cfg.key else cfg.key
            stats_map = load_member_preset_stats(session_all, email)
            for p in cfg.value[:20]:
                if not isinstance(p, dict):
                    continue
                name = str(p.get("name") or "").strip()
                weights_map = p.get("weights") if isinstance(p.get("weights"), dict) else {}
                stt = stats_map.get(name, {}) if isinstance(stats_map, dict) else {}
                races_n = int(stt.get("races") or 0)
                row = {
                    "Email": email,
                    "組合": name,
                    "更新時間": str(p.get("updated_at") or ""),
                    "樣本(場)": races_n,
                    "_weights": weights_map,
                }
                for k in HIT_METRICS:
                    col = f"{METRIC_LABELS.get(k, k)}%"
                    v = int(stt.get(k) or 0)
                    row[col] = round((v / races_n * 100.0), 1) if races_n else 0.0
                rows.append(row)

        if not rows:
            st.info("目前沒有任何會員儲存組合。")
        else:
            df_overview = []
            for r in rows:
                rr = dict(r)
                rr.pop("_weights", None)
                df_overview.append(rr)
            st.dataframe(
                pd.DataFrame(df_overview), 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "條件": st.column_config.TextColumn(width="medium"),
                    "描述": st.column_config.TextColumn(width="large"),
                    "代號": st.column_config.TextColumn(width="medium"),
                }
            )

            st.markdown("---")
            st.markdown("### 🔎 組合權重參數")
            
            # Group rows by email
            from collections import defaultdict
            grouped_by_email = defaultdict(list)
            for r in rows:
                grouped_by_email[r["Email"]].append(r)
                
            for email, member_rows in grouped_by_email.items():
                with st.expander(f"👤 {email} ({len(member_rows)} 個組合)", expanded=False):
                    for r in member_rows:
                        name = r["組合"]
                        weights_map = r.get("_weights") or {}
                        st.markdown(f"**🔹 {name}**")
                        total_w = sum(float(v) for v in weights_map.values()) if weights_map else 0.0
                        items = []
                        for k, v in weights_map.items():
                            if k in factor_desc:
                                share = (float(v) / total_w * 100.0) if total_w > 0 else 0.0
                                items.append({"條件": factor_desc[k], "代號": k, "權重": round(float(v), 2), "佔比%": round(share, 1)})
                        items = sorted(items, key=lambda x: x["佔比%"], reverse=True)
                        if items:
                            st.dataframe(pd.DataFrame(items), use_container_width=True, hide_index=True)
                        else:
                            st.info("此組合沒有可用的權重資料。")
                        st.markdown("<br>", unsafe_allow_html=True)
    finally:
        session_all.close()

with tab_hits:
    sub_factor, sub_preset = st.tabs(["📈 獨立條件", "👥 會員儲存組合"])

    with sub_factor:
        st.subheader("📈 獨立條件命中率統計")
        from datetime import date, datetime, time as dtime, timedelta
        from database.models import PredictionTop5, RaceResult, RaceEntry, ScoringWeight
        from scoring_engine.constants import DISABLED_FACTORS

        session_hit = get_session()
        try:
            factors = (
                session_hit.query(ScoringWeight.factor_name, ScoringWeight.description)
                .filter(ScoringWeight.is_active == True)
                .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
                .order_by(ScoringWeight.factor_name.asc())
                .all()
            )
            factor_desc = {str(fn): str(desc or fn) for fn, desc in factors}
            factor_names = list(factor_desc.keys())

            drows = (
                session_hit.query(PredictionTop5.race_date)
                .filter(PredictionTop5.predictor_type == "factor")
                .order_by(PredictionTop5.race_date.desc())
                .limit(5000)
                .all()
            )
            available_dates = []
            seen = set()
            for (dt,) in drows:
                if not dt:
                    continue
                dd = dt.date()
                if dd in seen:
                    continue
                seen.add(dd)
                available_dates.append(dd)
                if len(available_dates) >= 90:
                    break
            end_default = available_dates[0] if available_dates else date.today()
            start_default = (
                max(end_default - timedelta(days=30), min(available_dates)) if available_dates else (end_default - timedelta(days=30))
            )
            d1, d2 = st.date_input("統計日期範圍", value=(start_default, end_default), key="hit_factor_range_admin")
            if isinstance(d1, date) and isinstance(d2, date) and d1 > d2:
                d1, d2 = d2, d1

            if not available_dates:
                st.info("目前未有任何獨立條件 Top5 快照。你仍可先設定 AI；要生成建議需先抓排位並生成預測快照，並且有已結算賽果。")

            preds = []
            if available_dates and factor_names:
                start_dt = datetime.combine(d1, dtime.min)
                end_dt = datetime.combine(d2, dtime.min) + timedelta(days=1)
                preds = (
                    session_hit.query(
                        PredictionTop5.race_id,
                        PredictionTop5.predictor_key,
                        PredictionTop5.top5,
                        PredictionTop5.meta,
                    )
                    .filter(PredictionTop5.predictor_type == "factor")
                    .filter(PredictionTop5.predictor_key.in_(factor_names))
                    .filter(PredictionTop5.race_date >= start_dt)
                    .filter(PredictionTop5.race_date < end_dt)
                    .all()
                )

            if preds:
                from scoring_engine.member_stats import _calc_hits

                def actual_top5(race_id: int):
                    rows = (
                        session_hit.query(RaceEntry.horse_no, RaceResult.rank)
                        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
                        .filter(RaceEntry.race_id == race_id)
                        .filter(RaceResult.rank != None)
                        .order_by(RaceResult.rank.asc())
                        .limit(5)
                        .all()
                    )
                    return [int(r[0]) for r in rows]

                agg = {fn: {"races": 0, **{k: 0 for k in HIT_METRICS}} for fn in factor_names}
                cache_act = {}

                for race_id, factor_name, top5, meta in preds:
                    if not isinstance(top5, list) or len(top5) < 5:
                        continue

                    hits = None
                    if isinstance(meta, dict):
                        h = meta.get("hits")
                        if isinstance(h, dict):
                            hits = {str(k).lower(): int(v) for k, v in h.items()}

                    if hits is None:
                        act = cache_act.get(int(race_id))
                        if act is None:
                            act = actual_top5(int(race_id))
                            cache_act[int(race_id)] = act
                        if len(act) < 5:
                            continue
                        hits = _calc_hits([int(x) for x in top5], act)

                    if not hits:
                        continue

                    a = agg.get(str(factor_name))
                    if not a:
                        continue
                    a["races"] += 1
                    for k, v in hits.items():
                        kk = str(k).lower()
                        if kk in a:
                            a[kk] += int(v)

                rows = []
                for fn in factor_names:
                    a = agg[fn]
                    n = int(a["races"] or 0)
                    row = {"條件": factor_desc.get(fn, fn), "代號": fn, "樣本(場)": n}
                    for k in HIT_METRICS:
                        row[f"{METRIC_LABELS.get(k, k)}%"] = round((int(a.get(k) or 0) / n * 100.0), 1) if n else 0.0
                    rows.append(row)
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True, column_config={"條件": st.column_config.TextColumn(width="medium"), "代號": st.column_config.TextColumn(width="medium"), "組合": st.column_config.TextColumn(width="medium")})

                with st.expander("🧩 因子缺資料統計（所選日期範圍）", expanded=False):
                    st.caption("用途：檢查各因子在所選範圍內「無數據/空白」比例，幫你判斷要補數據、降低權重或暫時忽略。")
                    from database.models import Race, ScoringFactor
                    from sqlalchemy import case, func
                    from datetime import datetime, time as dtime, timedelta
                    start = datetime.combine(d1, dtime.min)
                    end = datetime.combine(d2, dtime.min) + timedelta(days=1)

                    with st.container():

                        q = (
                            session_hit.query(
                                ScoringFactor.factor_name.label("factor"),
                                func.count(ScoringFactor.id).label("rows"),
                                func.sum(
                                    case(
                                        (
                                            (ScoringFactor.raw_data_display == None)
                                            | (ScoringFactor.raw_data_display == "")
                                            | (ScoringFactor.raw_data_display == "無數據"),
                                            1,
                                        ),
                                        else_=0,
                                    )
                                ).label("missing_display"),
                                func.sum(case((ScoringFactor.raw_value == None, 1), else_=0)).label("missing_raw"),
                            )
                            .join(RaceEntry, RaceEntry.id == ScoringFactor.entry_id)
                            .join(Race, Race.id == RaceEntry.race_id)
                            .filter(ScoringFactor.factor_name.in_(factor_names))
                            .filter(Race.race_date >= start)
                            .filter(Race.race_date < end)
                            .group_by(ScoringFactor.factor_name)
                            .all()
                        )
                        rowsq = []
                        for factor, rows_n, miss_d, miss_r in q:
                            total = int(rows_n or 0)
                            md = int(miss_d or 0)
                            mr = int(miss_r or 0)
                            cov = (1.0 - (md / total)) if total else 0.0
                            rowsq.append(
                                {
                                    "條件": factor_desc.get(str(factor), str(factor)),
                                    "代號": str(factor),
                                    "樣本(匹)": total,
                                    "缺失顯示(匹)": md,
                                    "缺失顯示(%)": round((md / total * 100.0), 1) if total else 0.0,
                                    "缺失原始(匹)": mr,
                                    "缺失原始(%)": round((mr / total * 100.0), 1) if total else 0.0,
                                    "覆蓋率(%)": round(cov * 100.0, 1),
                                }
                            )
                        if not rowsq:
                            st.info("選定範圍內未找到因子計分資料。")
                        else:
                            st.dataframe(
                                pd.DataFrame(rowsq).sort_values(["缺失顯示(%)", "缺失原始(%)"], ascending=[False, False]),
                                use_container_width=True,
                                hide_index=True,
                            )
                            st.markdown("---")
                            st.markdown("**缺失原因分類（按場次 factor_quality 匯總）**")
                            st.caption("只統計已重新計分過、且已寫入 factor_quality 的場次；舊場次如未重算可能無法顯示原因分類。")
                            from database.models import SystemConfig, HorseHistory
                            from scoring_engine.core import ScoringEngine as _SE

                            if factor_names:
                                selected_factor = st.selectbox(
                                    "選擇因子",
                                    options=factor_names,
                                    format_func=lambda x: f"{factor_desc.get(x, x)} ({x})",
                                    key="missing_reason_factor",
                                )
                                race_ids = [
                                    int(r[0])
                                    for r in (
                                        session_hit.query(Race.id)
                                        .filter(Race.race_date >= start)
                                        .filter(Race.race_date < end)
                                        .all()
                                    )
                                    if r and r[0]
                                ]
                                keys = [f"factor_quality:{rid}" for rid in race_ids]
                                cfgs = []
                                if keys:
                                    cfgs = session_hit.query(SystemConfig.key, SystemConfig.value).filter(SystemConfig.key.in_(keys)).all()
                                agg_reason = {}

                with st.expander("🧪 因子成效評估（單因子）", expanded=False):
                    st.caption("用途：評估每個獨立條件「單獨使用」時的命中率與資料覆蓋，協助你判斷因子是否值得保留/加權/補數據。")
                    from scoring_engine.diagnostics import active_factor_names, factor_label_map
                    from scoring_engine.factor_evaluation import evaluate_factors

                    label_map2 = factor_label_map(session_hit)
                    factor_names2 = active_factor_names(session_hit)
                    if not factor_names2:
                        st.info("未找到可用因子（請先確認 ScoringWeight/is_active 與 get_available_factors）。")
                    else:
                        c1, c2, c3 = st.columns([2, 2, 3])
                        top_k = c1.selectbox("TopK（用於命中）", [5, 4], index=0, key="factor_eval_topk")
                        use_cache = c2.checkbox("使用快取", value=True, key="factor_eval_use_cache")
                        ok = _confirm_run(c3, "factor_eval_run", label="輸入 RUN 以產生成效表")
                        run = c3.button("產生成效評估表", use_container_width=True, disabled=not ok)

                        if run:
                            cache_key = ""
                            if use_cache:
                                cache_key = f"factor_eval:{d1.isoformat()}:{d2.isoformat()}:top{int(top_k)}:v2"
                            res = evaluate_factors(
                                session_hit,
                                d1=d1,
                                d2=d2,
                                factor_names=factor_names2,
                                top_k=int(top_k),
                                cache_key=cache_key,
                                save_cache=bool(use_cache),
                            )
                            if not isinstance(res, dict) or res.get("ok") is not True:
                                st.error(f"❌ 評估失敗：{str(res.get('reason') if isinstance(res, dict) else '')}")
                            else:
                                rows_eval = res.get("rows") if isinstance(res.get("rows"), list) else []
                                if not rows_eval:
                                    st.info("所選範圍內沒有足夠已結算賽果/計分資料可評估。")
                                else:
                                    rows2 = []
                                    for it in rows_eval:
                                        if not isinstance(it, dict):
                                            continue
                                        fn = str(it.get("factor_name") or "").strip()
                                        if not fn:
                                            continue
                                        rows2.append(
                                            {
                                                "條件": label_map2.get(fn, fn),
                                                "代號": fn,
                                                "命中樣本(場)": int(it.get("races") or 0),
                                                "覆蓋率(%)": round(float(it.get("coverage_pct") or 0.0), 1) if it.get("coverage_pct") is not None else None,
                                                "缺失顯示(%)": round(float(it.get("missing_display_pct") or 0.0), 1) if it.get("missing_display_pct") is not None else None,
                                                "W2(%)": round(float(it.get("w2_rate") or 0.0), 1) if it.get("w2_rate") is not None else None,
                                                "PQ(3)(%)": round(float(it.get("pq3_rate") or 0.0), 1) if it.get("pq3_rate") is not None else None,
                                                "P2(%)": round(float(it.get("p2_rate") or 0.0), 1) if it.get("p2_rate") is not None else None,
                                                "AUC(W2)": round(float(it.get("auc_w2") or 0.0), 3) if it.get("auc_w2") is not None else None,
                                                "AUC(Top3)": round(float(it.get("auc_top3") or 0.0), 3) if it.get("auc_top3") is not None else None,
                                            }
                                        )
                                    df_eval = pd.DataFrame(rows2)
                                    st.dataframe(df_eval, use_container_width=True, hide_index=True)

                                    payload = {"meta": {k: v for k, v in res.items() if k != "rows"}, "rows": rows_eval}
                                    st.download_button(
                                        "下載 JSON",
                                        data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
                                        file_name=f"factor_eval_{d1.isoformat()}_{d2.isoformat()}_top{int(top_k)}.json",
                                        mime="application/json",
                                        use_container_width=True,
                                        key="factor_eval_dl_json",
                                    )
                                    st.download_button(
                                        "下載 CSV",
                                        data=df_eval.to_csv(index=False).encode("utf-8"),
                                        file_name=f"factor_eval_{d1.isoformat()}_{d2.isoformat()}_top{int(top_k)}.csv",
                                        mime="text/csv",
                                        use_container_width=True,
                                        key="factor_eval_dl_csv",
                                    )
                                total_missing = 0
                                cfg_key_set = set()
                                race_ids_with_reason = set()
                                for k, v in cfgs:
                                    ks = None
                                    try:
                                        ks = str(k)
                                        cfg_key_set.add(ks)
                                    except Exception:
                                        ks = None
                                    rid = None
                                    try:
                                        if ks and ks.startswith("factor_quality:"):
                                            rid = int(ks.split(":", 1)[1])
                                    except Exception:
                                        rid = None
                                    if not isinstance(v, dict):
                                        continue
                                    fs = v.get("factors") if isinstance(v.get("factors"), dict) else {}
                                    fv = fs.get(selected_factor) if isinstance(fs, dict) else None
                                    if not isinstance(fv, dict):
                                        continue
                                    if rid is not None:
                                        race_ids_with_reason.add(int(rid))
                                    reasons = fv.get("reasons") if isinstance(fv.get("reasons"), dict) else {}
                                    for rk, rv in reasons.items():
                                        n = int(rv or 0)
                                        agg_reason[str(rk)] = int(agg_reason.get(str(rk)) or 0) + n
                                        total_missing += n

                                missing_race_ids = [rid for rid in race_ids if int(rid) not in race_ids_with_reason]
                                if missing_race_ids:
                                    engine = _SE(session_hit)
                                    miss_rows = (
                                        session_hit.query(
                                            ScoringFactor.raw_data_display,
                                            RaceEntry.draw,
                                            RaceEntry.rating,
                                            RaceEntry.actual_weight,
                                            RaceEntry.horse_id,
                                            Race.race_date,
                                            Race.race_no,
                                        )
                                        .join(RaceEntry, RaceEntry.id == ScoringFactor.entry_id)
                                        .join(Race, Race.id == RaceEntry.race_id)
                                        .filter(RaceEntry.race_id.in_(missing_race_ids))
                                        .filter(ScoringFactor.factor_name == selected_factor)
                                        .all()
                                    )

                                    horse_ids = []
                                    race_keys = []
                                    for disp, draw, rating, wt, hid, rd, rno in miss_rows:
                                        dd = str(disp or "").strip()
                                        if dd not in {"", "無數據"}:
                                            continue
                                        try:
                                            if hid is not None:
                                                horse_ids.append(int(hid))
                                        except Exception:
                                            pass
                                        try:
                                            if rd is not None and hasattr(rd, "date") and int(rno or 0) > 0:
                                                date_str = rd.date().strftime("%Y/%m/%d")
                                                race_keys.append((date_str, int(rno or 0)))
                                        except Exception:
                                            pass

                                    horse_ids = sorted(set([x for x in horse_ids if x > 0]))
                                    horse_has_history = {hid: False for hid in horse_ids}
                                    if horse_ids:
                                        rows_h = (
                                            session_hit.query(HorseHistory.horse_id, func.count(HorseHistory.id))
                                            .filter(HorseHistory.horse_id.in_(horse_ids))
                                            .group_by(HorseHistory.horse_id)
                                            .all()
                                        )
                                        for hid, cnt in rows_h:
                                            try:
                                                horse_has_history[int(hid)] = int(cnt or 0) > 0
                                            except Exception:
                                                continue

                                    race_keys = sorted(set([rk for rk in race_keys if rk and rk[0] and rk[1]]))
                                    sp_key_list = []
                                    for ds, rno in race_keys:
                                        sp_key_list.append(f"speedpro_energy:{ds}:{rno}")
                                        sp_key_list.append(f"speedpro_retry:{ds}:{rno}")
                                    sp_cfg = {}
                                    if sp_key_list:
                                        sp_rows = session_hit.query(SystemConfig.key, SystemConfig.value).filter(SystemConfig.key.in_(sp_key_list)).all()
                                        for kk, vv in sp_rows:
                                            try:
                                                sp_cfg[str(kk)] = vv
                                            except Exception:
                                                continue

                                    speedpro_state_by_race = {}
                                    for ds, rno in race_keys:
                                        sp = sp_cfg.get(f"speedpro_energy:{ds}:{rno}")
                                        rr = sp_cfg.get(f"speedpro_retry:{ds}:{rno}")
                                        rv = rr if isinstance(rr, dict) else {}
                                        speedpro_state_by_race[(ds, rno)] = {
                                            "has_data": bool(isinstance(sp, dict) and sp),
                                            "had_retry": bool(isinstance(rv, dict) and rv),
                                            "last_error": (rv.get("last_error") if isinstance(rv, dict) else None),
                                        }

                                    for disp, draw, rating, wt, hid, rd, rno in miss_rows:
                                        dd = str(disp or "").strip()
                                        if dd not in {"", "無數據"}:
                                            continue
                                        date_str = None
                                        try:
                                            if rd is not None and hasattr(rd, "date"):
                                                date_str = rd.date().strftime("%Y/%m/%d")
                                        except Exception:
                                            date_str = None
                                        sp_state = speedpro_state_by_race.get((date_str, int(rno or 0))) if date_str else None
                                        if not isinstance(sp_state, dict):
                                            sp_state = {"has_data": False, "had_retry": False, "last_error": None}
                                        row = {"draw": draw, "rating": rating, "weight": wt, "horse_id": hid}
                                        r = engine._missing_reason(
                                            factor_name=selected_factor,
                                            display=dd,
                                            row=row,
                                            speedpro_state=sp_state,
                                            horse_has_history=horse_has_history,
                                        )
                                        agg_reason[str(r)] = int(agg_reason.get(str(r)) or 0) + 1
                                        total_missing += 1

                                if not agg_reason:
                                    st.info("所選範圍內暫無缺失原因分類資料（可先對該範圍場次重新計分）。")
                                else:
                                    rr = []
                                    for rk, n in sorted(agg_reason.items(), key=lambda x: (-(int(x[1] or 0)), str(x[0]))):
                                        rr.append(
                                            {
                                                "原因": rk,
                                                "缺失(匹)": int(n or 0),
                                                "佔缺失(%)": round((int(n or 0) / total_missing * 100.0), 1) if total_missing else 0.0,
                                            }
                                        )
                                    st.dataframe(pd.DataFrame(rr), use_container_width=True, hide_index=True)

            elif available_dates:
                st.info("選定範圍內沒有任何獨立條件 Top5 快照。")

            with st.expander("🤖 權重建議（Top3 重心模型）", expanded=False):
                if not factor_names:
                    st.info("目前沒有可用的獨立條件因子。")
                else:
                    st.caption("用所選日期範圍的歷史賽果自動估計各因子重要性，目標聚焦 Top2 勝出率＋PQ(3)（後台只作分析與下載）。")
                    st.markdown(
                        """
**方法說明（自動估計因子重要性）**
- **資料來源**：使用所選日期範圍內、已結算賽果的場次；每匹馬取資料庫 `ScoringFactor` 的各因子分數與 `raw_data_display`。
- **目標定義**：同一份資料會學兩個目標：`勝出(名次=1)` 與 `入圍Top3(名次≤3)`（更貼近 PQ(3)），再按目標權重加總。
- **特徵**：每個因子會產生 2 個特徵：
  - `分數`：該因子在該場的相對分數（0–10）。
  - `缺失`：若 `raw_data_display` 為空白/無數據 → 1，否則 0。
- **缺失處理**：若某因子分數缺失，分數以 5.0（中間值）補上；同時 `缺失=1` 讓模型學到「缺資料時應該如何調整」。
- **模型**：Logistic Regression（二分類），並用 `class_weight=balanced` 減少正負例比例不均造成的偏差。
- **建議權重**：把兩個模型的正向係數按目標權重加總，再按「最大值」比例縮放到你選的「建議權重上限」。
- **指標**：回算同一批資料的 Top2 勝出率與 PQ(3)（in-sample）作方向參考；建議以不同日期範圍反覆驗證。
                        """.strip()
                    )
                    from scoring_engine.weight_tuning import tune_weights_top3_focus
                    import json

                    w_rows = (
                        session_hit.query(ScoringWeight.factor_name, ScoringWeight.weight)
                        .filter(ScoringWeight.is_active == True)
                        .filter(ScoringWeight.factor_name.in_(factor_names))
                        .all()
                    )
                    current_w = {str(fn): float(w or 0.0) for fn, w in w_rows if fn}

                    c1, c2, c3 = st.columns([2, 2, 3])
                    max_w = float(c1.selectbox("建議權重上限", [2.0, 3.0, 4.0, 5.0], index=1, key="tune_max_w"))
                    w2_w = float(c2.selectbox("目標權重：Top2 勝出率", [0.5, 0.7, 0.9], index=1, key="tune_w2_w"))
                    run = c3.button("生成建議", use_container_width=True, key="tune_run_btn")
                    t2_w = float(st.selectbox("目標權重：PQ(3)", [0.1, 0.3, 0.5], index=1, key="tune_t2_w"))

                    if run:
                        res = tune_weights_top3_focus(
                            session_hit,
                            d1=d1,
                            d2=d2,
                            factor_names=factor_names,
                            max_suggest_weight=max_w,
                            objective={"w2_weight": float(w2_w), "pq3_weight": float(t2_w)},
                        )
                        st.session_state["tune_top5_result"] = res

                    res = st.session_state.get("tune_top5_result")
                    if isinstance(res, dict) and res.get("ok") is True:
                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("樣本(匹)", int(res.get("rows") or 0))
                        m2.metric("樣本(場)", int(res.get("races") or 0))
                        ins = res.get("in_sample") if isinstance(res.get("in_sample"), dict) else {}
                        m3.metric("Top2 勝出率", f"{float(ins.get('w2_rate') or 0.0):.1f}%")
                        m4.metric("PQ(3)", f"{float(ins.get('pq3_rate') or ins.get('top3_2in_rate') or 0.0):.1f}%")

                        sugg = res.get("suggested_weights") if isinstance(res.get("suggested_weights"), dict) else {}
                        cs = res.get("coef_win_score") if isinstance(res.get("coef_win_score"), dict) else {}
                        cm = res.get("coef_win_missing") if isinstance(res.get("coef_win_missing"), dict) else {}

                        out_rows = []
                        for fn in factor_names:
                            out_rows.append(
                                {
                                    "條件": factor_desc.get(fn, fn),
                                    "代號": fn,
                                    "目前權重": round(float(current_w.get(fn) or 0.0), 3),
                                    "建議權重": round(float(sugg.get(fn) or 0.0), 3),
                                    "係數(分數)": round(float(cs.get(fn) or 0.0), 4),
                                    "係數(缺失)": round(float(cm.get(fn) or 0.0), 4),
                                }
                            )
                        df_out = pd.DataFrame(out_rows).sort_values(["建議權重", "目前權重"], ascending=[False, False])
                        st.dataframe(
                            df_out, 
                            use_container_width=True, 
                            hide_index=True,
                            column_config={
                                "條件": st.column_config.TextColumn(width="medium"),
                                "代號": st.column_config.TextColumn(width="medium")
                            }
                        )

                        payload = {
                            "date_range": {"from": d1.isoformat(), "to": d2.isoformat()},
                            "objective": res.get("objective"),
                            "metrics": {"rows": res.get("rows"), "races": res.get("races"), "in_sample": res.get("in_sample")},
                            "suggested_weights": {str(k): float(v) for k, v in (sugg or {}).items()},
                        }
                        st.download_button(
                            "下載建議權重 JSON",
                            data=json.dumps(payload, ensure_ascii=False, indent=2),
                            file_name=f"tuned_weights_top3focus_{d1.isoformat()}_{d2.isoformat()}.json",
                            mime="application/json",
                            use_container_width=True,
                            key="tune_download_btn",
                        )
                    elif isinstance(res, dict) and res.get("ok") is False and res.get("reason"):
                        st.info("選定範圍內未找到足夠的已結算賽果 + 計分資料，無法生成建議。")

        finally:
            session_hit.close()

    with sub_preset:
        st.subheader("👥 會員儲存組合命中率統計")
        from datetime import date, datetime, time as dtime, timedelta
        from database.models import PredictionTop5

        session_p = get_session()
        try:
            drows = (
                session_p.query(PredictionTop5.race_date)
                .filter(PredictionTop5.predictor_type == "preset")
                .order_by(PredictionTop5.race_date.desc())
                .limit(5000)
                .all()
            )
            available_dates = []
            seen = set()
            for (dt,) in drows:
                if not dt:
                    continue
                dd = dt.date()
                if dd in seen:
                    continue
                seen.add(dd)
                available_dates.append(dd)
                if len(available_dates) >= 90:
                    break
            if not available_dates:
                st.info("目前未有任何會員組合 Top5 快照。請先抓取排位並生成預測快照。")
            else:
                end_default = available_dates[0]
                start_default = max(end_default - timedelta(days=30), min(available_dates))
                d1, d2 = st.date_input("統計日期範圍", value=(start_default, end_default), key="preset_hit_range")
                if isinstance(d1, date) and isinstance(d2, date) and d1 > d2:
                    d1, d2 = d2, d1

                start_dt = datetime.combine(d1, dtime.min)
                end_dt = datetime.combine(d2, dtime.min) + timedelta(days=1)
                preds = (
                    session_p.query(
                        PredictionTop5.member_email,
                        PredictionTop5.predictor_key,
                        PredictionTop5.meta,
                    )
                    .filter(PredictionTop5.predictor_type == "preset")
                    .filter(PredictionTop5.race_date >= start_dt)
                    .filter(PredictionTop5.race_date < end_dt)
                    .all()
                )
                if not preds:
                    st.info("選定範圍內沒有任何會員組合 Top5 快照。")
                else:
                    agg = {}
                    for email, preset_name, meta in preds:
                        email_k = str(email or "").strip().lower()
                        preset_k = str(preset_name or "").strip()
                        if not email_k or not preset_k:
                            continue
                        h = None
                        if isinstance(meta, dict):
                            h = meta.get("hits")
                        if not isinstance(h, dict):
                            continue
                        key = (email_k, preset_k)
                        a = agg.get(key)
                        if a is None:
                            a = {"races": 0, **{k: 0 for k in HIT_METRICS}}
                            agg[key] = a
                        a["races"] += 1
                        for mk, mv in h.items():
                            kk = str(mk).lower()
                            if kk in a:
                                a[kk] += int(mv or 0)

                    rows = []
                    for (email_k, preset_k), a in agg.items():
                        n = int(a["races"] or 0)
                        row = {"Email": email_k, "組合": preset_k, "樣本(場)": n}
                        for k in HIT_METRICS:
                            row[f"{METRIC_LABELS.get(k, k)}%"] = round((int(a.get(k) or 0) / n * 100.0), 1) if n else 0.0
                        rows.append(row)
                    if not rows:
                        st.info("目前未有任何已結算（已抓賽果）的會員組合命中資料。")
                    else:
                        st.dataframe(
                            pd.DataFrame(rows), 
                            use_container_width=True, 
                            hide_index=True, 
                            column_config={
                                "Email": st.column_config.TextColumn(width="medium"), 
                                "組合": st.column_config.TextColumn(width="medium")
                            }
                        )
        finally:
            session_p.close()
