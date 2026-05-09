import streamlit as st
import pandas as pd
import os
import sys
from pathlib import Path
from datetime import date, datetime, time as dtime, timedelta

root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.models import Race, RaceCoRunning, SystemConfig
from scoring_engine.config_value import unwrap_value
from scoring_engine.normalization import bucket_parts, venue_label
from scoring_engine.track_profile import compute_track_profiles
from web_ui.nav import render_admin_nav

st.set_page_config(page_title="重算/回填狀態 - HKJC Analytics", page_icon="🧭", layout="wide")

init_db()

if not st.session_state.get("is_superadmin", False):
    st.title("🧭 重算/回填狀態")
    st.markdown("🔐 需要 Superadmin 登入後才能查看。")
    super_pw = os.environ.get("SUPERADMIN_PASSWORD", "")
    if not super_pw:
        st.error("❌ 未設定 SUPERADMIN_PASSWORD 環境變數，無法登入後台。")
        st.stop()

    with st.form("superadmin_login_form_status"):
        pw = st.text_input("Superadmin 密碼", value="", type="password")
        submitted = st.form_submit_button("登入", type="primary")
        if submitted:
            if str(pw) == super_pw:
                st.session_state["is_superadmin"] = True
                st.rerun()
            else:
                st.error("❌ 密碼錯誤")
    st.stop()

render_admin_nav(active="status")
st.title("🧭 重算/回填狀態")


def _list_race_dates(session, need: int = 180):
    take_rows = max(200, int(need) * 20)
    rows = session.query(Race.race_date).order_by(Race.race_date.desc()).limit(int(take_rows)).all()
    out = []
    seen = set()
    for (dt,) in rows:
        if not dt:
            continue
        dd = dt.date()
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


session = get_session()
try:
    dates = _list_race_dates(session, need=180)
    if not dates:
        st.info("資料庫未有任何賽日。")
        st.stop()

    sel_date = st.selectbox("賽日", options=dates, index=0, key="status_date")
    start_dt, end_dt = _day_range(sel_date)
    date_str = sel_date.strftime("%Y/%m/%d")

    races = (
        session.query(Race)
        .filter(Race.race_date >= start_dt, Race.race_date < end_dt)
        .order_by(Race.race_no.asc())
        .all()
    )
    if not races:
        st.info("該日未有賽事。")
        st.stop()

    cfg_idx = session.query(SystemConfig).filter_by(key="trkprof_index").first()
    idx_payload, idx_meta = unwrap_value(cfg_idx.value) if cfg_idx else (None, {})
    idx_payload = idx_payload if isinstance(idx_payload, dict) else {}

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("賽事數", int(len(races)))
    c2.metric("trkprof groups", int(len(idx_payload.get("groups") or [])) if isinstance(idx_payload.get("groups"), list) else 0)
    c3.metric("trkprof seen_races", int(idx_payload.get("seen_races") or 0))
    c4.metric("trkprof updated_at", str(idx_payload.get("updated_at") or "").strip() or "未知")

    if idx_meta:
        cap = []
        for k in ["source", "schema", "fetched_at", "saved_at"]:
            v = str(idx_meta.get(k) or "").strip()
            if v:
                cap.append(f"{k}={v}")
        if cap:
            st.caption("trkprof_index｜" + "｜".join(cap))

    rows = []
    ok_runpos = 0
    ok_cor = 0
    ok_prof = 0

    for r in races:
        rn = int(getattr(r, "race_no", 0) or 0)
        key_runpos = f"race_runpos:{date_str}:{rn}"
        cfg_run = session.query(SystemConfig).filter_by(key=key_runpos).first()
        run_payload, run_meta = unwrap_value(cfg_run.value) if cfg_run else (None, {})
        run_payload = run_payload if isinstance(run_payload, dict) else {}
        runpos = run_payload.get("runpos") if isinstance(run_payload.get("runpos"), dict) else {}
        has_runpos = bool(runpos)
        if has_runpos:
            ok_runpos += 1

        cor = session.query(RaceCoRunning).filter_by(race_id=int(r.id)).first()
        items = cor.items if (cor and isinstance(cor.items, dict)) else {}
        has_cor = bool(items)
        if has_cor:
            ok_cor += 1

        parts = bucket_parts(session, r)
        trk_key = ""
        has_prof = False
        style_samples = ""
        if parts:
            v, g, c, d = parts
            trk_key = f"trkprof:{v}:{g}:{c}:{d}"
            cfg_p = session.query(SystemConfig).filter_by(key=trk_key).first()
            p_payload, _ = unwrap_value(cfg_p.value) if cfg_p else (None, {})
            p_payload = p_payload if isinstance(p_payload, dict) else {}
            has_prof = bool(p_payload)
            if has_prof:
                ok_prof += 1
            ws = int(p_payload.get("winner_style_samples") or 0)
            ts = int(p_payload.get("top4_style_samples") or 0)
            if ws or ts:
                style_samples = f"win={ws},top4={ts}"

        rows.append(
            {
                "RaceNo": rn,
                "RaceID": int(r.id),
                "地點": venue_label(getattr(r, "venue", ""), track_type=getattr(r, "track_type", None)),
                "runpos": "✅" if has_runpos else "—",
                "runpos筆數": int(len(runpos)) if isinstance(runpos, dict) else 0,
                "corunning": "✅" if has_cor else "—",
                "corunning筆數": int(len(items)) if isinstance(items, dict) else 0,
                "trkprof_key": trk_key,
                "trkprof": "✅" if has_prof else "—",
                "style_samples": style_samples,
                "runpos_meta": str(run_meta.get("fetched_at") or run_meta.get("saved_at") or "").strip(),
                "corunning_fetched_at": (cor.fetched_at.isoformat() if (cor and getattr(cor, "fetched_at", None)) else ""),
            }
        )

    c1, c2, c3 = st.columns(3)
    c1.metric("runpos 完整", f"{ok_runpos}/{len(races)}")
    c2.metric("corunning 完整", f"{ok_cor}/{len(races)}")
    c3.metric("trkprof key 命中", f"{ok_prof}/{len(races)}")

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("🔄 快捷操作")
    c1, c2 = st.columns([2, 3])
    if c1.button("重算 trkprof（只限此賽日）", type="primary", key="recompute_trkprof_day"):
        compute_track_profiles(session, min_date=start_dt, max_date=end_dt, limit_races=200)
        st.success("已觸發 trkprof 重算（請稍後重新整理查看）。")
        st.rerun()

    c2.info("runpos 與 corunning 來源係「抓取賽果與派彩」。如缺失，先確保該日已成功抓取賽果。")
finally:
    session.close()

