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


tab_factor, tab_preset = st.tabs(["📈 獨立條件", "👥 會員儲存組合"])

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
