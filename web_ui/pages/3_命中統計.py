import streamlit as st
import pandas as pd
import sys
from pathlib import Path
from datetime import date, timedelta
from sqlalchemy import func

root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import get_session, init_db
from database.models import PredictionTop5, RaceEntry, RaceResult, ScoringWeight
from scoring_engine.constants import DISABLED_FACTORS
from scoring_engine.diagnostics import (
    actual_ranks_by_horse_no,
    actual_topk,
    compute_elim_n,
    compute_top_n,
    factor_label_map,
    field_size,
    predicted_bottomk_by_factor,
    predicted_bottomk_by_total,
    predicted_topk_by_factor,
    predicted_topk_by_total,
    reverse_stats_for_race,
    summarize_entry_reason_fields,
)
from scoring_engine.prediction_snapshots import finalize_prediction_top5_hits_for_race_date
from web_ui.auth import require_superadmin
from web_ui.nav import render_admin_nav

st.set_page_config(page_title="命中統計 - HKJC Analytics", layout="wide")

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

init_db()

require_superadmin("📈 命中統計總覽")

st.title("📈 命中統計總覽")
render_admin_nav()


def _actual_top5(session, race_id: int):
    rows = (
        session.query(RaceEntry.horse_no, RaceResult.rank)
        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
        .filter(RaceEntry.race_id == race_id)
        .filter(RaceResult.rank != None)
        .order_by(RaceResult.rank.asc())
        .limit(5)
        .all()
    )
    return [int(r[0]) for r in rows]


tab_factor, tab_preset, tab_diag = st.tabs(["📈 獨立條件", "👥 會員儲存組合", "🧠 診斷"])

with tab_factor:
    session = get_session()
    try:
        factors = (
            session.query(ScoringWeight.factor_name, ScoringWeight.description)
            .filter(ScoringWeight.is_active == True)
            .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
            .order_by(ScoringWeight.factor_name.asc())
            .all()
        )
        factor_desc = {str(fn): str(desc or fn) for fn, desc in factors}
        factor_names = list(factor_desc.keys())

        drows = (
            session.query(func.date(PredictionTop5.race_date))
            .filter(PredictionTop5.predictor_type == "factor")
            .distinct()
            .order_by(func.date(PredictionTop5.race_date).desc())
            .limit(180)
            .all()
        )
        available_dates = [r[0] for r in drows if r and r[0]]
        if not available_dates:
            st.info("目前未有任何獨立條件 Top5 快照。")
        else:
            st.markdown("### 🧾 分享字段（獨立條件 Top5）")
            share_c1, share_c2, share_c3 = st.columns([2, 4, 2])
            share_date = share_c1.selectbox(
                "賽日",
                available_dates,
                index=0,
                format_func=lambda x: x.isoformat() if hasattr(x, "isoformat") else str(x),
            )
            share_factor = share_c2.selectbox(
                "獨立條件",
                factor_names,
                index=0,
                format_func=lambda x: f"{factor_desc.get(str(x), str(x))} ({x})",
            )

            if share_c3.button("生成分享字段", use_container_width=True, key="share_factor_text_only"):
                rows = (
                    session.query(PredictionTop5.race_no, PredictionTop5.top5)
                    .filter(PredictionTop5.predictor_type == "factor")
                    .filter(PredictionTop5.predictor_key == str(share_factor))
                    .filter(func.date(PredictionTop5.race_date) == share_date.isoformat())
                    .order_by(PredictionTop5.race_no.asc())
                    .all()
                )

                races = []
                for rn, top5 in rows:
                    races.append(
                        {
                            "race_no": int(rn or 0),
                            "top5": [int(x) for x in (top5 or []) if str(x).strip().isdigit()],
                        }
                    )
                races.sort(key=lambda x: x["race_no"])

                if not races:
                    st.info("該賽日未找到此獨立條件的 Top5 快照。")
                else:
                    factor_label = factor_desc.get(str(share_factor), str(share_factor))

                    import json

                    payload = {
                        "race_date": share_date.isoformat(),
                        "factor_code": str(share_factor),
                        "factor_name": factor_label,
                        "races": races,
                    }
                    txt_lines = [
                        f"獨立條件：{factor_label} ({share_factor})",
                        f"賽日：{share_date.isoformat()}",
                    ]
                    for r in races:
                        top5_s = ",".join(str(x) for x in (r.get("top5") or [])[:5])
                        txt_lines.append(f"第{int(r.get('race_no') or 0)}場：{top5_s}")
                    txt = "\n".join(txt_lines) + "\n"

                    st.code(txt, language="text")
                    st.download_button(
                        "下載 TXT",
                        data=txt.encode("utf-8"),
                        file_name=f"factor_top5_{share_factor}_{share_date.isoformat()}.txt",
                        mime="text/plain",
                        use_container_width=False,
                        key="share_factor_txt_download",
                    )
                    st.download_button(
                        "下載 JSON",
                        data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
                        file_name=f"factor_top5_{share_factor}_{share_date.isoformat()}.json",
                        mime="application/json",
                        use_container_width=False,
                        key="share_factor_json_download",
                    )

            end_default = available_dates[0]
            start_default = max(end_default - timedelta(days=30), min(available_dates))
            range_key = "hit_factor_range"
            if range_key not in st.session_state:
                st.session_state[range_key] = (start_default, end_default)

            b1, b2, b3, b4 = st.columns(4)
            if b1.button("前30日", use_container_width=True):
                st.session_state[range_key] = (max(end_default - timedelta(days=30), min(available_dates)), end_default)
                st.rerun()
            if b2.button("前60日", use_container_width=True):
                st.session_state[range_key] = (max(end_default - timedelta(days=60), min(available_dates)), end_default)
                st.rerun()
            if b3.button("前180日", use_container_width=True):
                st.session_state[range_key] = (max(end_default - timedelta(days=180), min(available_dates)), end_default)
                st.rerun()
            if b4.button("最長日子", use_container_width=True):
                st.session_state[range_key] = (min(available_dates), end_default)
                st.rerun()

            d1, d2 = st.date_input("統計日期範圍", value=st.session_state[range_key], key=range_key)
            if isinstance(d1, date) and isinstance(d2, date) and d1 > d2:
                d1, d2 = d2, d1

            if st.button(f"🧾 結算 Top5 命中（{d2.isoformat()}）", use_container_width=False):
                res = finalize_prediction_top5_hits_for_race_date(session, d2.strftime("%Y/%m/%d"))
                st.success(f"完成：updated={res.get('updated')} skipped={res.get('skipped')}")
                st.rerun()

            preds = (
                session.query(
                    PredictionTop5.race_id,
                    PredictionTop5.predictor_key,
                    PredictionTop5.top5,
                    PredictionTop5.meta,
                )
                .filter(PredictionTop5.predictor_type == "factor")
                .filter(PredictionTop5.predictor_key.in_(factor_names))
                .filter(func.date(PredictionTop5.race_date) >= d1.isoformat())
                .filter(func.date(PredictionTop5.race_date) <= d2.isoformat())
                .all()
            )
            if not preds:
                st.info("選定範圍內沒有任何獨立條件 Top5 快照。")
            else:
                from scoring_engine.member_stats import _calc_hits

                agg = {
                    fn: {"races": 0, "win": 0, "p": 0, "q1": 0, "pq": 0, "t3e": 0, "t3": 0, "f4": 0, "f4q": 0, "b5w": 0, "b5p": 0}
                    for fn in factor_names
                }
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
                            act = _actual_top5(session, int(race_id))
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
                    rows.append(
                        {
                            "條件": factor_desc.get(fn, fn),
                            "代號": fn,
                            "樣本(場)": n,
                            "WIN%": round((a["win"] / n * 100.0), 1) if n else 0.0,
                            "P%": round((a["p"] / n * 100.0), 1) if n else 0.0,
                            "Q1%": round((a["q1"] / n * 100.0), 1) if n else 0.0,
                            "PQ%": round((a["pq"] / n * 100.0), 1) if n else 0.0,
                            "T3E%": round((a["t3e"] / n * 100.0), 1) if n else 0.0,
                            "T3%": round((a["t3"] / n * 100.0), 1) if n else 0.0,
                            "F4%": round((a["f4"] / n * 100.0), 1) if n else 0.0,
                            "F4Q%": round((a["f4q"] / n * 100.0), 1) if n else 0.0,
                            "B5W%": round((a["b5w"] / n * 100.0), 1) if n else 0.0,
                            "B5P%": round((a["b5p"] / n * 100.0), 1) if n else 0.0,
                        }
                    )
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    finally:
        session.close()

with tab_preset:
    session = get_session()
    try:
        drows = (
            session.query(func.date(PredictionTop5.race_date))
            .filter(PredictionTop5.predictor_type == "preset")
            .distinct()
            .order_by(func.date(PredictionTop5.race_date).desc())
            .limit(180)
            .all()
        )
        available_dates = [r[0] for r in drows if r and r[0]]
        if not available_dates:
            st.info("目前未有任何會員組合 Top5 快照。")
        else:
            st.markdown("### 🧾 分享字段（會員組合 Top5）")
            s1, s2, s3, s4 = st.columns([2, 3, 3, 2])
            share_date = s1.selectbox(
                "賽日",
                available_dates,
                index=0,
                format_func=lambda x: x.isoformat() if hasattr(x, "isoformat") else str(x),
                key="preset_share_date",
            )

            opt_rows = (
                session.query(PredictionTop5.member_email, PredictionTop5.predictor_key)
                .filter(PredictionTop5.predictor_type == "preset")
                .filter(func.date(PredictionTop5.race_date) == share_date.isoformat())
                .distinct()
                .order_by(PredictionTop5.member_email.asc(), PredictionTop5.predictor_key.asc())
                .all()
            )
            emails = sorted({str(e or "").strip().lower() for e, _ in opt_rows if str(e or "").strip()})
            presets = sorted({str(p or "").strip() for _, p in opt_rows if str(p or "").strip()})

            if not emails or not presets:
                st.info("該賽日未找到可分享的會員組合 Top5 快照。")
            else:
                share_email = s2.selectbox("會員 Email", emails, index=0, key="preset_share_email")
                share_preset = s3.selectbox("組合名稱", presets, index=0, key="preset_share_preset")

                if s4.button("生成分享字段", use_container_width=True, key="preset_share_text"):
                    rows = (
                        session.query(PredictionTop5.race_no, PredictionTop5.top5)
                        .filter(PredictionTop5.predictor_type == "preset")
                        .filter(PredictionTop5.member_email == str(share_email))
                        .filter(PredictionTop5.predictor_key == str(share_preset))
                        .filter(func.date(PredictionTop5.race_date) == share_date.isoformat())
                        .order_by(PredictionTop5.race_no.asc())
                        .all()
                    )

                    races = []
                    for rn, top5 in rows:
                        races.append(
                            {
                                "race_no": int(rn or 0),
                                "top5": [int(x) for x in (top5 or []) if str(x).strip().isdigit()],
                            }
                        )
                    races.sort(key=lambda x: x["race_no"])

                    if not races:
                        st.info("該賽日未找到此會員組合的 Top5 快照。")
                    else:
                        import json
                        from database.models import SystemConfig

                        preset_weights = None
                        cfg = session.query(SystemConfig).filter_by(key=f"member_weight_presets:{str(share_email)}").first()
                        if cfg and isinstance(cfg.value, list):
                            for item in cfg.value:
                                if isinstance(item, dict) and str(item.get("name", "")).strip() == str(share_preset):
                                    preset_weights = item.get("weights")
                                    break

                        payload = {
                            "race_date": share_date.isoformat(),
                            "member_email": str(share_email),
                            "preset_name": str(share_preset),
                            "preset_weights": preset_weights,
                            "races": races,
                        }
                        txt_lines = [
                            f"會員：{share_email}",
                            f"組合：{share_preset}",
                            f"賽日：{share_date.isoformat()}",
                        ]
                        for r in races:
                            top5_s = ",".join(str(x) for x in (r.get("top5") or [])[:5])
                            txt_lines.append(f"第{int(r.get('race_no') or 0)}場：{top5_s}")
                        txt = "\n".join(txt_lines) + "\n"

                        st.code(txt, language="text")
                        st.download_button(
                            "下載 TXT",
                            data=txt.encode("utf-8"),
                            file_name=f"preset_top5_{share_email}_{share_preset}_{share_date.isoformat()}.txt",
                            mime="text/plain",
                            use_container_width=False,
                            key="preset_share_txt_download",
                        )
                        st.download_button(
                            "下載 JSON",
                            data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
                            file_name=f"preset_top5_{share_email}_{share_preset}_{share_date.isoformat()}.json",
                            mime="application/json",
                            use_container_width=False,
                            key="preset_share_json_download",
                        )

            end_default = available_dates[0]
            start_default = max(end_default - timedelta(days=30), min(available_dates))
            d1, d2 = st.date_input("統計日期範圍", value=(start_default, end_default), key="hit_preset_range")
            if isinstance(d1, date) and isinstance(d2, date) and d1 > d2:
                d1, d2 = d2, d1

            if st.button(f"🧾 結算 Top5 命中（{d2.isoformat()}）", use_container_width=False, key="settle_preset"):
                res = finalize_prediction_top5_hits_for_race_date(session, d2.strftime("%Y/%m/%d"))
                st.success(f"完成：updated={res.get('updated')} skipped={res.get('skipped')}")
                st.rerun()

            preds = (
                session.query(
                    PredictionTop5.member_email,
                    PredictionTop5.predictor_key,
                    PredictionTop5.meta,
                )
                .filter(PredictionTop5.predictor_type == "preset")
                .filter(func.date(PredictionTop5.race_date) >= d1.isoformat())
                .filter(func.date(PredictionTop5.race_date) <= d2.isoformat())
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
                        a = {"races": 0, "win": 0, "p": 0, "q1": 0, "pq": 0, "t3e": 0, "t3": 0, "f4": 0, "f4q": 0, "b5w": 0, "b5p": 0}
                        agg[key] = a
                    a["races"] += 1
                    for mk, mv in h.items():
                        kk = str(mk).lower()
                        if kk in a:
                            a[kk] += int(mv or 0)

                rows = []
                for (email_k, preset_k), a in agg.items():
                    n = int(a["races"] or 0)
                    rows.append(
                        {
                            "Email": email_k,
                            "組合": preset_k,
                            "樣本(場)": n,
                            "WIN%": round((a["win"] / n * 100.0), 1) if n else 0.0,
                            "P%": round((a["p"] / n * 100.0), 1) if n else 0.0,
                            "Q1%": round((a["q1"] / n * 100.0), 1) if n else 0.0,
                            "PQ%": round((a["pq"] / n * 100.0), 1) if n else 0.0,
                            "T3E%": round((a["t3e"] / n * 100.0), 1) if n else 0.0,
                            "T3%": round((a["t3"] / n * 100.0), 1) if n else 0.0,
                            "F4%": round((a["f4"] / n * 100.0), 1) if n else 0.0,
                            "F4Q%": round((a["f4q"] / n * 100.0), 1) if n else 0.0,
                            "B5W%": round((a["b5w"] / n * 100.0), 1) if n else 0.0,
                            "B5P%": round((a["b5p"] / n * 100.0), 1) if n else 0.0,
                        }
                    )
                if not rows:
                    st.info("目前未有任何已結算（已抓賽果）的會員組合命中資料。")
                else:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    finally:
        session.close()


with tab_diag:
    st.markdown("### 🧠 單場診斷（反向統計 + 失準原因）")
    st.caption("選擇賽日/場次後，可檢視：預測Top5 命中/誤推/漏網，以及 BottomN 淘汰是否錯殺。")
    st.caption("推高原因的 +分值＝該因子對總分的加權貢獻（因子分×權重），數值越大越推高排名。")

    session = get_session()
    try:
        label_map = factor_label_map(session)
        drows = (
            session.query(func.date(PredictionTop5.race_date))
            .distinct()
            .order_by(func.date(PredictionTop5.race_date).desc())
            .limit(365)
            .all()
        )
        available_dates = [r[0] for r in drows if r and r[0]]
        if not available_dates:
            st.info("目前未有任何預測快照資料可供診斷。")
        else:
            c1, c2, c3, c4 = st.columns([2, 2, 3, 3])
            sel_date = c1.selectbox(
                "賽日",
                available_dates,
                index=0,
                format_func=lambda x: x.isoformat() if hasattr(x, "isoformat") else str(x),
                key="diag_date",
            )

            race_nos = (
                session.query(PredictionTop5.race_no)
                .filter(func.date(PredictionTop5.race_date) == sel_date.isoformat())
                .distinct()
                .order_by(PredictionTop5.race_no.asc())
                .all()
            )
            race_nos = [int(r[0] or 0) for r in race_nos if r and int(r[0] or 0) > 0]
            if not race_nos:
                st.info("該賽日未找到可診斷的場次。")
            else:
                sel_race_no = c2.selectbox("場次", race_nos, index=0, key="diag_race_no")
                mode = c3.selectbox("診斷模式", ["總分(組合/整體)", "單一因子"], index=0, key="diag_mode")
                bottom_pct = float(c4.selectbox("淘汰 BottomN%", [10, 15, 20, 25, 30], index=2, key="diag_bottom_pct"))

                factor_name = None
                if mode == "單一因子":
                    factors = (
                        session.query(ScoringWeight.factor_name, ScoringWeight.description)
                        .filter(ScoringWeight.is_active == True)
                        .filter(~ScoringWeight.factor_name.in_(DISABLED_FACTORS))
                        .order_by(ScoringWeight.factor_name.asc())
                        .all()
                    )
                    factor_desc = {str(fn): str(desc or fn) for fn, desc in factors}
                    factor_names = [str(x[0]) for x in factors if x and x[0]]
                    factor_name = c3.selectbox(
                        "因子",
                        factor_names,
                        index=0 if factor_names else None,
                        format_func=lambda x: f"{factor_desc.get(str(x), str(x))} ({x})",
                        key="diag_factor_name",
                    )

                race_id_row = (
                    session.query(PredictionTop5.race_id)
                    .filter(func.date(PredictionTop5.race_date) == sel_date.isoformat())
                    .filter(PredictionTop5.race_no == int(sel_race_no or 0))
                    .order_by(PredictionTop5.race_id.asc())
                    .first()
                )
                if not race_id_row:
                    st.warning("⚠️ 找不到該場次的 race_id。")
                else:
                    rid = int(race_id_row[0])
                    n_field = field_size(session, rid)
                    elim_n = compute_elim_n(n_field, bottom_pct)
                    top_n = compute_top_n(n_field, bottom_pct)
                    actual_rank = actual_ranks_by_horse_no(session, rid)
                    actual_t5 = actual_topk(session, rid, 5)
                    actual_t5_set = set(actual_t5)
                    actual_pos = actual_topk(session, rid, top_n)

                    if mode == "單一因子" and factor_name:
                        pred_t5 = predicted_topk_by_factor(session, rid, factor_name, 5)
                        pred_b = predicted_bottomk_by_factor(session, rid, factor_name, elim_n)
                    else:
                        pred_t5 = predicted_topk_by_total(session, rid, 5)
                        pred_b = predicted_bottomk_by_total(session, rid, elim_n)

                    rs = reverse_stats_for_race(actual_positive=actual_pos, predicted_negative=pred_b)
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("參賽馬數", int(n_field or 0))
                    m2.metric("預測Top5", len(pred_t5))
                    m3.metric("淘汰N", f"{int(rs.get('pred_neg') or 0)} ({int(bottom_pct)}%)")
                    m4.metric(
                        f"淘汰準確率(不入Top{int(top_n or 0)})",
                        f"{(rs.get('neg_accuracy') or 0.0):.1%}" if rs.get("neg_accuracy") is not None else "-",
                    )

                    all_hns = sorted({int(x) for x in list(actual_rank.keys()) + pred_t5 + pred_b if int(x or 0) > 0})
                    rows = []
                    pred_t5_set = set(pred_t5)
                    pred_b_set = set(pred_b)
                    for hn in all_hns:
                        rk = actual_rank.get(int(hn))
                        rows.append(
                            {
                                "馬號": int(hn),
                                "實際名次": int(rk) if rk else None,
                                "實際Top5": bool(int(hn) in actual_t5_set),
                                "預測Top5": bool(int(hn) in pred_t5_set),
                                "預測淘汰": bool(int(hn) in pred_b_set),
                            }
                        )
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                    fp = [x for x in pred_t5 if x not in actual_t5_set]
                    fn = [x for x in actual_t5 if x not in pred_t5_set]

                    st.markdown("### ❌ 誤推 / ✅ 漏網（主要因子貢獻）")
                    left, right = st.columns(2)
                    with left:
                        st.markdown("**誤推Top5（預測Top5但未入實際Top5）**")
                        if not fp:
                            st.caption("無")
                        else:
                            rows2 = []
                            for hn in fp:
                                entry_id = (
                                    session.query(RaceEntry.id)
                                    .filter(RaceEntry.race_id == rid)
                                    .filter(RaceEntry.horse_no == int(hn))
                                    .first()
                                )
                                reason = summarize_entry_reason_fields(session, int(entry_id[0]), label_map=label_map, top_n=3) if entry_id else {}
                                tops = reason.get("tops") if isinstance(reason, dict) else []
                                miss = int(reason.get("missing_count") or 0) if isinstance(reason, dict) else 0
                                t1 = tops[0] if len(tops) > 0 else {}
                                t2 = tops[1] if len(tops) > 1 else {}
                                t3 = tops[2] if len(tops) > 2 else {}
                                rows2.append(
                                    {
                                        "馬號": int(hn),
                                        "實際名次": actual_rank.get(int(hn)),
                                        "推高原因1": (f"{t1.get('name')} +{float(t1.get('contrib') or 0.0):.2f}" if t1 else ""),
                                        "推高原因2": (f"{t2.get('name')} +{float(t2.get('contrib') or 0.0):.2f}" if t2 else ""),
                                        "推高原因3": (f"{t3.get('name')} +{float(t3.get('contrib') or 0.0):.2f}" if t3 else ""),
                                        "缺資料(項)": miss,
                                    }
                                )
                            st.dataframe(pd.DataFrame(rows2), use_container_width=True, hide_index=True)
                    with right:
                        st.markdown("**漏網馬（實際Top5但未入預測Top5）**")
                        if not fn:
                            st.caption("無")
                        else:
                            rows3 = []
                            for hn in fn:
                                entry_id = (
                                    session.query(RaceEntry.id)
                                    .filter(RaceEntry.race_id == rid)
                                    .filter(RaceEntry.horse_no == int(hn))
                                    .first()
                                )
                                reason = summarize_entry_reason_fields(session, int(entry_id[0]), label_map=label_map, top_n=3) if entry_id else {}
                                tops = reason.get("tops") if isinstance(reason, dict) else []
                                miss = int(reason.get("missing_count") or 0) if isinstance(reason, dict) else 0
                                t1 = tops[0] if len(tops) > 0 else {}
                                t2 = tops[1] if len(tops) > 1 else {}
                                t3 = tops[2] if len(tops) > 2 else {}
                                rows3.append(
                                    {
                                        "馬號": int(hn),
                                        "實際名次": actual_rank.get(int(hn)),
                                        "推高原因1": (f"{t1.get('name')} +{float(t1.get('contrib') or 0.0):.2f}" if t1 else ""),
                                        "推高原因2": (f"{t2.get('name')} +{float(t2.get('contrib') or 0.0):.2f}" if t2 else ""),
                                        "推高原因3": (f"{t3.get('name')} +{float(t3.get('contrib') or 0.0):.2f}" if t3 else ""),
                                        "缺資料(項)": miss,
                                    }
                                )
                            st.dataframe(pd.DataFrame(rows3), use_container_width=True, hide_index=True)
    finally:
        session.close()
