import streamlit as st
import pandas as pd
import sys
from pathlib import Path
from datetime import date, timedelta
from sqlalchemy import func, case

root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from database.connection import get_session, init_db
from database.models import PredictionTop5, Race, RaceEntry, RaceResult, ScoringWeight, ScoringFactor
from scoring_engine.constants import DISABLED_FACTORS
from scoring_engine.diagnostics import (
    actual_ranks_by_horse_no,
    actual_topk,
    compute_elim_n,
    factor_label_map,
    field_size,
    active_factor_names,
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

st.set_page_config(page_title="命中統計 - HKJC Analytics", page_icon="📈", layout="wide")

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


def _venue_label(venue: str, track_type: str) -> str:
    v = str(venue or "").strip().upper()
    t = str(track_type or "").strip()
    if v == "HV" or ("跑馬地" in t):
        return "跑馬地"
    if v == "ST" or ("沙田" in t):
        return "沙田"
    return str(venue or "").strip() or "-"


def _surface_label(track_type: str) -> str:
    t = str(track_type or "").strip()
    if any(x in t for x in ["全天候", "ALL WEATHER", "A/W", "AW"]):
        return "泥"
    if any(x in t for x in ["草地", "TURF"]):
        return "草"
    if "泥" in t:
        return "泥"
    if "草" in t:
        return "草"
    return "-"


tab_factor, tab_preset, tab_range, tab_diag = st.tabs(["📈 獨立條件", "👥 會員儲存組合", "📊 反向統計", "🧠 診斷"])

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

            if share_c3.button("生成分享字段", width="stretch", key="share_factor_text_only"):
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
                        width="content",
                        key="share_factor_txt_download",
                    )
                    st.download_button(
                        "下載 JSON",
                        data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
                        file_name=f"factor_top5_{share_factor}_{share_date.isoformat()}.json",
                        mime="application/json",
                        width="content",
                        key="share_factor_json_download",
                    )

            end_default = available_dates[0]
            start_default = max(end_default - timedelta(days=30), min(available_dates))
            range_key = "hit_factor_range"
            if range_key not in st.session_state:
                st.session_state[range_key] = (start_default, end_default)

            b1, b2, b3, b4 = st.columns(4)
            if b1.button("前30日", width="stretch"):
                st.session_state[range_key] = (max(end_default - timedelta(days=30), min(available_dates)), end_default)
                st.rerun()
            if b2.button("前60日", width="stretch"):
                st.session_state[range_key] = (max(end_default - timedelta(days=60), min(available_dates)), end_default)
                st.rerun()
            if b3.button("前180日", width="stretch"):
                st.session_state[range_key] = (max(end_default - timedelta(days=180), min(available_dates)), end_default)
                st.rerun()
            if b4.button("最長日子", width="stretch"):
                st.session_state[range_key] = (min(available_dates), end_default)
                st.rerun()

            d1, d2 = st.date_input("統計日期範圍", value=st.session_state[range_key], key=range_key)
            if isinstance(d1, date) and isinstance(d2, date) and d1 > d2:
                d1, d2 = d2, d1

            if st.button(f"🧾 結算 Top5 命中（{d2.isoformat()}）", width="content"):
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
                st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
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

                if s4.button("生成分享字段", width="stretch", key="preset_share_text"):
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
                            width="content",
                            key="preset_share_txt_download",
                        )
                        st.download_button(
                            "下載 JSON",
                            data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
                            file_name=f"preset_top5_{share_email}_{share_preset}_{share_date.isoformat()}.json",
                            mime="application/json",
                            width="content",
                            key="preset_share_json_download",
                        )

            drows2 = (
                session.query(func.date(Race.race_date))
                .join(RaceEntry, RaceEntry.race_id == Race.id)
                .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
                .filter(RaceResult.rank != None)
                .distinct()
                .order_by(func.date(Race.race_date).desc())
                .limit(365)
                .all()
            )
            available_dates2 = [r[0] for r in drows2 if r and r[0]]
            if not available_dates2:
                st.info("目前未有任何已抓取賽果的場次可供統計。")
            else:
                end_default = available_dates2[0]
                start_default = max(end_default - timedelta(days=30), min(available_dates2))
                d1, d2 = st.date_input("統計日期範圍", value=(start_default, end_default), key="hit_preset_range")
                if isinstance(d1, date) and isinstance(d2, date) and d1 > d2:
                    d1, d2 = d2, d1

                st.caption("本區塊以「會員已儲存組合」+「已結算賽果」即時計算，不依賴 Top5 快照是否已結算。")

                from scoring_engine.member_stats import _calc_hits
                from database.models import SystemConfig

                cfg_rows = session.query(SystemConfig.key, SystemConfig.value).filter(SystemConfig.key.like("member_weight_presets:%")).all()
                preset_defs = []
                for k, v in cfg_rows:
                    key_s = str(k or "")
                    if ":" not in key_s:
                        continue
                    email_k = key_s.split(":", 1)[1].strip().lower()
                    if not email_k:
                        continue
                    if not isinstance(v, list):
                        continue
                    for item in v[:3]:
                        if not isinstance(item, dict):
                            continue
                        name = str(item.get("name") or "").strip()
                        weights = item.get("weights") if isinstance(item.get("weights"), dict) else {}
                        if not name or not weights:
                            continue
                        preset_defs.append((email_k, name, {str(fn): float(w or 0.0) for fn, w in weights.items()}))

                if not preset_defs:
                    st.info("目前未找到任何會員已儲存組合。")
                else:
                    race_rows = (
                        session.query(Race.id, Race.race_date, Race.race_no)
                        .join(RaceEntry, RaceEntry.race_id == Race.id)
                        .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
                        .filter(RaceResult.rank != None)
                        .filter(func.date(Race.race_date) >= d1.isoformat())
                        .filter(func.date(Race.race_date) <= d2.isoformat())
                        .distinct()
                        .order_by(Race.race_date.asc(), Race.race_no.asc(), Race.id.asc())
                        .all()
                    )
                    race_ids = [int(r[0]) for r in race_rows if r and int(r[0] or 0) > 0]
                    if not race_ids:
                        st.info("選定範圍內沒有任何已抓取賽果的場次。")
                    else:
                        used_factors = set()
                        for _, _, w in preset_defs:
                            for fn, ww in (w or {}).items():
                                if abs(float(ww or 0.0)) > 1e-12:
                                    used_factors.add(str(fn))
                        used_factors = sorted(used_factors)

                        horses_by_race = {}
                        for rid, hn in session.query(RaceEntry.race_id, RaceEntry.horse_no).filter(RaceEntry.race_id.in_(race_ids)).all():
                            rr = horses_by_race.get(int(rid))
                            if rr is None:
                                rr = []
                                horses_by_race[int(rid)] = rr
                            try:
                                rr.append(int(hn or 0))
                            except Exception:
                                rr.append(0)
                        for rid in list(horses_by_race.keys()):
                            horses_by_race[rid] = [x for x in horses_by_race[rid] if int(x or 0) > 0]

                        actual_by_race = {}
                        rr_rows = (
                            session.query(RaceEntry.race_id, RaceEntry.horse_no, RaceResult.rank)
                            .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
                            .filter(RaceEntry.race_id.in_(race_ids))
                            .filter(RaceResult.rank != None)
                            .order_by(RaceEntry.race_id.asc(), RaceResult.rank.asc())
                            .all()
                        )
                        for rid, hn, rk in rr_rows:
                            rid_i = int(rid or 0)
                            if rid_i <= 0:
                                continue
                            a = actual_by_race.get(rid_i)
                            if a is None:
                                a = []
                                actual_by_race[rid_i] = a
                            if len(a) >= 5:
                                continue
                            try:
                                a.append(int(hn or 0))
                            except Exception:
                                a.append(0)
                        for rid in list(actual_by_race.keys()):
                            actual_by_race[rid] = [x for x in actual_by_race[rid] if int(x or 0) > 0][:5]

                        score_map = {}
                        if used_factors:
                            sf_rows = (
                                session.query(RaceEntry.race_id, RaceEntry.horse_no, ScoringFactor.factor_name, ScoringFactor.score)
                                .join(ScoringFactor, ScoringFactor.entry_id == RaceEntry.id)
                                .filter(RaceEntry.race_id.in_(race_ids))
                                .filter(ScoringFactor.factor_name.in_(used_factors))
                                .all()
                            )
                            for rid, hn, fn, sc in sf_rows:
                                rid_i = int(rid or 0)
                                if rid_i <= 0:
                                    continue
                                hn_i = int(hn or 0)
                                if hn_i <= 0:
                                    continue
                                rmap = score_map.get(rid_i)
                                if rmap is None:
                                    rmap = {}
                                    score_map[rid_i] = rmap
                                hmap = rmap.get(hn_i)
                                if hmap is None:
                                    hmap = {}
                                    rmap[hn_i] = hmap
                                hmap[str(fn)] = float(sc or 0.0)

                        def _ranked_horses_for_preset(rid: int, weights: dict):
                            horses = horses_by_race.get(int(rid)) or []
                            rmap = score_map.get(int(rid)) or {}
                            items = []
                            for hn in horses:
                                m = rmap.get(int(hn)) or {}
                                total = 0.0
                                for fn, ww in (weights or {}).items():
                                    total += float(m.get(str(fn), 0.0)) * float(ww or 0.0)
                                items.append((int(hn), float(total)))
                            items.sort(key=lambda x: (-x[1], x[0]))
                            return [hn for hn, _ in items]

                        st.markdown("### 📊 會員組合命中率（Top5）")
                        agg = {}
                        for email_k, preset_k, w in preset_defs:
                            key = (email_k, preset_k)
                            a = agg.get(key)
                            if a is None:
                                a = {"races": 0, "win": 0, "p": 0, "q1": 0, "pq": 0, "t3e": 0, "t3": 0, "f4": 0, "f4q": 0, "b5w": 0, "b5p": 0}
                                agg[key] = a
                            for rid in race_ids:
                                act = actual_by_race.get(int(rid)) or []
                                if len(act) < 5:
                                    continue
                                ranked = _ranked_horses_for_preset(int(rid), w)
                                if len(ranked) < 5:
                                    continue
                                pred = ranked[:5]
                                hits = _calc_hits(pred, act)
                                if not hits:
                                    continue
                                a["races"] += 1
                                for mk, mv in hits.items():
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
                        rows = [r for r in rows if int(r.get("樣本(場)") or 0) > 0]
                        if not rows:
                            st.info("目前未有足夠資料計算命中率（可能尚未抓賽果或未重新計分）。")
                        else:
                            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

                        st.markdown("### 📉 會員組合反向表現（淘汰準確率）")
                        pct = 35.0
                        agg2 = {}
                        for email_k, preset_k, w in preset_defs:
                            key = (email_k, preset_k)
                            a = agg2.get(key)
                            if a is None:
                                a = {"races": 0, "pred": 0, "tn": 0, "fp": 0}
                                agg2[key] = a
                            for rid in race_ids:
                                act = actual_by_race.get(int(rid)) or []
                                if len(act) < 5:
                                    continue
                                ranked = _ranked_horses_for_preset(int(rid), w)
                                if not ranked:
                                    continue
                                elim_n = compute_elim_n(len(horses_by_race.get(int(rid)) or []), float(pct))
                                if int(elim_n or 0) <= 0:
                                    continue
                                pred_neg = ranked[-int(elim_n):]
                                rs = reverse_stats_for_race(actual_positive=act, predicted_negative=pred_neg)
                                if rs.get("pred_neg") is None:
                                    continue
                                a["races"] += 1
                                a["pred"] += int(rs.get("pred_neg") or 0)
                                a["tn"] += int(rs.get("tn") or 0)
                                a["fp"] += int(rs.get("fp") or 0)

                        rows2 = []
                        for (email_k, preset_k), a in agg2.items():
                            n = int(a["races"] or 0)
                            pred_n = int(a["pred"] or 0)
                            tn = int(a["tn"] or 0)
                            fp = int(a["fp"] or 0)
                            if n <= 0 or pred_n <= 0:
                                continue
                            acc = (tn / pred_n) if pred_n else None
                            fp_rate = (fp / pred_n) if pred_n else None
                            rows2.append(
                                {
                                    "Email": email_k,
                                    "組合": preset_k,
                                    "樣本(場)": n,
                                    "淘汰(匹)": pred_n,
                                    "淘汰準確率(不入Top5)": (round(acc * 100.0, 1) if acc is not None else None),
                                    "錯殺率": (round(fp_rate * 100.0, 1) if fp_rate is not None else None),
                                    "正確淘汰(匹)": tn,
                                    "錯殺(匹)": fp,
                                }
                            )
                        if not rows2:
                            st.info("目前未有足夠資料計算淘汰統計（可能尚未抓賽果或未重新計分）。")
                        else:
                            st.dataframe(pd.DataFrame(rows2).sort_values(["淘汰準確率(不入Top5)", "錯殺率"], ascending=[False, True]), width="stretch", hide_index=True)
    finally:
        session.close()


with tab_range:
    st.markdown("### 📊 反向統計（日期範圍）")
    st.caption("用 BottomN%（按每場參賽馬數計算 N）評估：你淘汰的馬匹是否真的不入 Top5。")

    session = get_session()
    try:
        label_map = factor_label_map(session)
        factor_names = active_factor_names(session)

        drows = (
            session.query(func.date(Race.race_date))
            .join(RaceEntry, RaceEntry.race_id == Race.id)
            .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
            .filter(RaceResult.rank != None)
            .distinct()
            .order_by(func.date(Race.race_date).desc())
            .limit(365)
            .all()
        )
        available_dates = [r[0] for r in drows if r and r[0]]
        if not available_dates:
            st.info("目前未有任何已抓取賽果的場次可供統計。")
        else:
            end_default = available_dates[0]
            start_default = max(end_default - timedelta(days=30), min(available_dates))

            c1, c2, c3, c4 = st.columns([3, 2, 2, 3])
            d1, d2 = c1.date_input("統計日期範圍", value=(start_default, end_default), key="rev_range_dates")
            if isinstance(d1, date) and isinstance(d2, date) and d1 > d2:
                d1, d2 = d2, d1
            mode = c2.selectbox("模式", ["總分(組合/整體)", "單一因子"], index=0, key="rev_range_mode")
            bottom_pct = 35.0

            seg_opts = ["地點", "草/泥", "距離", "班次"]
            segs = c4.multiselect("分桶維度", options=seg_opts, default=["地點", "距離"], key="rev_range_segs")

            factor_name = None
            if mode == "單一因子":
                factor_name = st.selectbox(
                    "選擇因子",
                    options=factor_names,
                    format_func=lambda x: f"{label_map.get(x, x)} ({x})",
                    key="rev_range_factor",
                )

            races = (
                session.query(Race)
                .join(RaceEntry, RaceEntry.race_id == Race.id)
                .join(RaceResult, RaceResult.entry_id == RaceEntry.id)
                .filter(RaceResult.rank != None)
                .filter(func.date(Race.race_date) >= d1.isoformat())
                .filter(func.date(Race.race_date) <= d2.isoformat())
                .distinct()
                .order_by(Race.race_date.asc(), Race.race_no.asc(), Race.id.asc())
                .all()
            )
            if not races:
                st.info("選定範圍內沒有任何已抓取賽果的場次。")
            else:
                rows = []
                for r in races:
                    rid = int(getattr(r, "id") or 0)
                    if not rid:
                        continue
                    n_field = field_size(session, rid)
                    elim_n = compute_elim_n(n_field, bottom_pct)
                    top_k = 5
                    if elim_n <= 0 or top_k <= 0:
                        continue

                    actual_pos = actual_topk(session, rid, top_k)
                    if len(actual_pos) < top_k:
                        continue

                    if mode == "單一因子" and factor_name:
                        pred_neg = predicted_bottomk_by_factor(session, rid, factor_name, elim_n)
                    else:
                        pred_neg = predicted_bottomk_by_total(session, rid, elim_n)
                    rs = reverse_stats_for_race(actual_positive=actual_pos, predicted_negative=pred_neg)
                    if rs.get("pred_neg") is None:
                        continue

                    venue = str(getattr(r, "venue", "") or "").strip()
                    distance = getattr(r, "distance", None)
                    race_class = str(getattr(r, "race_class", "") or "").strip()
                    track_type = str(getattr(r, "track_type", "") or "").strip()
                    loc = _venue_label(venue=venue, track_type=track_type)
                    surf = _surface_label(track_type=track_type)

                    seg_vals = []
                    if "地點" in segs:
                        seg_vals.append(f"地點:{loc}")
                    if "草/泥" in segs:
                        seg_vals.append(f"草泥:{surf}")
                    if "距離" in segs:
                        seg_vals.append(f"距離:{int(distance or 0) or '-'}")
                    if "班次" in segs:
                        seg_vals.append(f"班次:{race_class or '-'}")
                    seg_key = "｜".join(seg_vals) if seg_vals else "全部"

                    rd = getattr(r, "race_date", None)
                    rd_s = rd.date().isoformat() if hasattr(rd, "date") else str(rd or "")
                    rows.append(
                        {
                            "seg": seg_key,
                            "race_id": rid,
                            "date": rd_s,
                            "race_no": int(getattr(r, "race_no", 0) or 0),
                            "venue": venue,
                            "loc": loc,
                            "distance": int(distance or 0) if distance is not None else None,
                            "class": race_class,
                            "track": track_type,
                            "surface": surf,
                            "field": int(n_field or 0),
                            "elim_n": int(elim_n or 0),
                            "top_k": int(top_k or 0),
                            "pred_neg": int(rs.get("pred_neg") or 0),
                            "tn": int(rs.get("tn") or 0),
                            "fp": int(rs.get("fp") or 0),
                        }
                    )

                if not rows:
                    st.info("選定範圍內沒有足夠資料（可能尚未重新計分或賽果未齊）。")
                else:
                    df = pd.DataFrame(rows)
                    df["neg_accuracy"] = df["tn"] / df["pred_neg"]
                    df["false_elim_rate"] = df["fp"] / df["pred_neg"]

                    st.markdown("### 篩選")
                    fcols = st.columns([2, 2, 6])
                    df_f = df
                    if "地點" in segs:
                        loc_opts = sorted([x for x in df_f["loc"].dropna().unique().tolist() if str(x).strip()])
                        loc_sel = fcols[0].multiselect("地點", options=loc_opts, default=loc_opts, key="rev_range_filter_loc")
                        if loc_sel:
                            df_f = df_f[df_f["loc"].isin(loc_sel)]
                        else:
                            df_f = df_f.iloc[0:0]
                    if "草/泥" in segs:
                        surf_opts = sorted([x for x in df_f["surface"].dropna().unique().tolist() if str(x).strip()])
                        surf_sel = fcols[1].multiselect("草/泥", options=surf_opts, default=surf_opts, key="rev_range_filter_surf")
                        if surf_sel:
                            df_f = df_f[df_f["surface"].isin(surf_sel)]
                        else:
                            df_f = df_f.iloc[0:0]

                    if df_f.empty:
                        st.info("篩選條件下沒有資料。")
                    else:
                        df = df_f

                    total_pred = int(df["pred_neg"].sum() or 0)
                    total_tn = int(df["tn"].sum() or 0)
                    total_fp = int(df["fp"].sum() or 0)
                    overall_acc = (total_tn / total_pred) if total_pred else None
                    overall_fp_rate = (total_fp / total_pred) if total_pred else None

                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("樣本(場)", int(df["race_id"].nunique() or 0))
                    m2.metric("淘汰總匹數", total_pred)
                    m3.metric("淘汰準確率", f"{overall_acc:.1%}" if overall_acc is not None else "-")
                    m4.metric("錯殺率", f"{overall_fp_rate:.1%}" if overall_fp_rate is not None else "-")

                    st.markdown("### 分桶表現")
                    min_races = st.slider("最少樣本(場)", min_value=1, max_value=50, value=10, step=1, key="rev_range_min_races")
                    g = (
                        df.groupby("seg", dropna=False)
                        .agg(
                            races=("race_id", "nunique"),
                            pred_neg=("pred_neg", "sum"),
                            tn=("tn", "sum"),
                            fp=("fp", "sum"),
                        )
                        .reset_index()
                    )
                    g["neg_accuracy"] = g["tn"] / g["pred_neg"]
                    g["false_elim_rate"] = g["fp"] / g["pred_neg"]
                    g = g[g["races"] >= int(min_races or 0)]
                    if g.empty:
                        st.info("未達到最少樣本門檻，請降低最少樣本(場)或擴大日期範圍。")
                    else:
                        show_cols = ["seg", "races", "pred_neg", "neg_accuracy", "false_elim_rate", "tn", "fp"]
                        out = g[show_cols].sort_values(["neg_accuracy", "false_elim_rate"], ascending=[True, False])
                        out = out.rename(
                            columns={
                                "seg": "分桶",
                                "races": "樣本(場)",
                                "pred_neg": "淘汰(匹)",
                                "neg_accuracy": "淘汰準確率",
                                "false_elim_rate": "錯殺率",
                                "tn": "正確淘汰(匹)",
                                "fp": "錯殺(匹)",
                            }
                        )
                        out["淘汰準確率"] = out["淘汰準確率"].map(lambda x: f"{float(x):.1%}")
                        out["錯殺率"] = out["錯殺率"].map(lambda x: f"{float(x):.1%}")
                        st.dataframe(out, width="stretch", hide_index=True)

                    st.markdown("### 場次明細（按錯殺率排序）")
                    df_show = df.copy()
                    df_show = df_show.sort_values(["false_elim_rate", "neg_accuracy"], ascending=[False, True]).head(80)
                    df_show = df_show.rename(
                        columns={
                            "date": "賽日",
                            "race_no": "場次",
                            "field": "參賽馬數",
                            "elim_n": "淘汰N",
                            "top_k": "Top5",
                            "pred_neg": "淘汰(匹)",
                            "tn": "正確淘汰(匹)",
                            "fp": "錯殺(匹)",
                            "neg_accuracy": "淘汰準確率",
                            "false_elim_rate": "錯殺率",
                            "seg": "分桶",
                        }
                    )
                    df_show["淘汰準確率"] = df_show["淘汰準確率"].map(lambda x: f"{float(x):.1%}")
                    df_show["錯殺率"] = df_show["錯殺率"].map(lambda x: f"{float(x):.1%}")
                    st.dataframe(
                        df_show[
                            [
                                "賽日",
                                "場次",
                                "分桶",
                                "參賽馬數",
                                "淘汰N",
                                "Top5",
                                "淘汰(匹)",
                                "淘汰準確率",
                                "錯殺率",
                                "錯殺(匹)",
                            ]
                        ],
                        width="stretch",
                        hide_index=True,
                    )
    finally:
        session.close()


with tab_diag:
    st.markdown("### 🧠 單場診斷（反向統計 + 失準原因）")
    st.caption("選擇賽日/場次後，可檢視：預測Top4 命中/誤推/漏網，以及 BottomN 淘汰是否錯殺。")
    st.caption("推高原因的 +分值＝該因子對總分的加權貢獻（因子分×權重），數值越大越推高排名。")

    session = get_session()
    try:
        label_map = factor_label_map(session)
        from database.models import SystemConfig
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
                bottom_pct = 35.0

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
                    srow = (
                        session.query(
                            func.count(RaceEntry.id),
                            func.sum(case((RaceEntry.total_score == None, 1), else_=0)),
                            func.min(RaceEntry.total_score),
                            func.max(RaceEntry.total_score),
                            func.count(func.distinct(RaceEntry.total_score)),
                        )
                        .filter(RaceEntry.race_id == int(rid))
                        .first()
                    )
                    if srow:
                        total_n = int(srow[0] or 0)
                        null_n = int(srow[1] or 0)
                        min_s = srow[2]
                        max_s = srow[3]
                        distinct_s = int(srow[4] or 0)
                        non_null_n = max(0, total_n - null_n)
                        m1s, m2s, m3s, m4s = st.columns(4)
                        m1s.metric("總分筆數", total_n)
                        m2s.metric("總分缺失", null_n)
                        m3s.metric("總分範圍", f"{float(min_s):.3f}~{float(max_s):.3f}" if (min_s is not None and max_s is not None) else "-")
                        m4s.metric("總分分散度", f"{distinct_s} 值" if non_null_n else "-")
                        if non_null_n == 0:
                            st.warning("本場未有 total_score（未重新計分/未保存）。預測Top5/淘汰會退化成任意排序。請先在「數據管理後台」重算該日賽事。")
                        elif distinct_s <= 1:
                            st.warning("本場 total_score 幾乎全部相同，Top5/淘汰會高度重疊甚至看似一樣。通常原因：有效權重接近 0（因子覆蓋不足且策略=自動忽略）、或因子分數全同/全缺。")
                    n_field = field_size(session, rid)
                    elim_n = compute_elim_n(n_field, bottom_pct)
                    actual_rank = actual_ranks_by_horse_no(session, rid)
                    top_k = 4
                    actual_tk = actual_topk(session, rid, top_k)
                    actual_tk_set = set(actual_tk)
                    actual_pos = actual_tk

                    if mode == "單一因子" and factor_name:
                        pred_t5 = predicted_topk_by_factor(session, rid, factor_name, top_k)
                        pred_b = predicted_bottomk_by_factor(session, rid, factor_name, elim_n)
                    else:
                        pred_t5 = predicted_topk_by_total(session, rid, top_k)
                        pred_b = predicted_bottomk_by_total(session, rid, elim_n)

                    rs = reverse_stats_for_race(actual_positive=actual_pos, predicted_negative=pred_b)
                    overlap = sorted(set(pred_t5) & set(pred_b))
                    if overlap:
                        st.warning(f"預測Top4 與 預測淘汰 有重疊（{len(overlap)} 匹）：{', '.join(str(x) for x in overlap[:12])}{'...' if len(overlap) > 12 else ''}。通常因 total_score 同分/缺失造成邊界選取退化。")
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("參賽馬數", int(n_field or 0))
                    m2.metric("預測Top4", len(pred_t5))
                    m3.metric("淘汰N", f"{int(rs.get('pred_neg') or 0)} ({int(bottom_pct)}%)")
                    m4.metric(
                        "淘汰準確率(不入Top4)",
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
                                "實際Top4": bool(int(hn) in actual_tk_set),
                                "預測Top4": bool(int(hn) in pred_t5_set),
                                "預測淘汰": bool(int(hn) in pred_b_set),
                            }
                        )
                    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

                    q_cfg = session.query(SystemConfig).filter_by(key=f"factor_quality:{rid}").first()
                    qv = q_cfg.value if q_cfg and isinstance(q_cfg.value, dict) else {}
                    qf = qv.get("factors") if isinstance(qv, dict) else {}
                    if isinstance(qf, dict) and qf:
                        st.markdown("### 🧩 因子資料完整度（本場）")
                        st.caption("缺失判斷以 raw_data_display 為「無數據/空白」計算；覆蓋率越低，代表該因子本場越多馬匹欠缺參考。")
                        qrows = []
                        eff_sum = 0.0
                        for k, v in qf.items():
                            if not isinstance(v, dict):
                                continue
                            try:
                                eff_sum += float(v.get("effective_weight") or 0.0)
                            except Exception:
                                pass
                            reasons = v.get("reasons") if isinstance(v.get("reasons"), dict) else {}
                            top_reasons = []
                            if isinstance(reasons, dict) and reasons:
                                for rk, rv in sorted(reasons.items(), key=lambda x: (-(int(x[1] or 0)), str(x[0]))):
                                    if len(top_reasons) >= 2:
                                        break
                                    top_reasons.append(f"{rk}({int(rv or 0)})")
                            qrows.append(
                                {
                                    "因子": k,
                                    "名稱": label_map.get(k, k),
                                    "覆蓋率": round(float(v.get("coverage") or 0.0) * 100.0, 1),
                                    "缺失(匹)": int(v.get("missing") or 0),
                                    "缺失原因": "；".join(top_reasons),
                                    "門檻(%)": round(float(v.get("min_coverage") or 0.0) * 100.0, 0),
                                    "策略": "自動忽略" if str(v.get("action") or "") == "ignore" else "只提示",
                                    "已忽略": bool(v.get("ignored") is True),
                                    "有效權重": round(float(v.get("effective_weight") or 0.0), 3),
                                }
                            )
                        if qrows:
                            qdf = pd.DataFrame(qrows).sort_values(["已忽略", "覆蓋率"], ascending=[False, True])
                            st.dataframe(qdf, width="stretch", hide_index=True)
                            if abs(float(eff_sum or 0.0)) < 1e-9:
                                st.warning("本場所有因子有效權重合計為 0（策略=自動忽略 + 覆蓋不足）→ total_score 會全部相同，Top5/淘汰結果將退化並重疊。")

                    fp = [x for x in pred_t5 if x not in actual_tk_set]
                    fn = [x for x in actual_tk if x not in pred_t5_set]

                    st.markdown("### ❌ 誤推 / ✅ 漏網（主要因子貢獻）")
                    left, right = st.columns(2)
                    with left:
                        st.markdown("**誤推Top4（預測Top4但未入實際Top4）**")
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
                            st.dataframe(pd.DataFrame(rows2), width="stretch", hide_index=True)
                    with right:
                        st.markdown("**漏網馬（實際Top4但未入預測Top4）**")
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
                            st.dataframe(pd.DataFrame(rows3), width="stretch", hide_index=True)
    finally:
        session.close()
