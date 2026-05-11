from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st
from sqlalchemy import and_, func
import sys
from pathlib import Path

# 加入專案路徑
root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.models import SearchDocument, Race, SystemConfig
from scoring_engine.job_queue import enqueue_job
from scoring_engine.track_conditions import GOING_CODE_LABELS, normalize_going
from scoring_engine.search_index import index_system_config_doc
from web_ui.nav import render_admin_nav


def _parse_date_token(t: str) -> Optional[str]:
    s = str(t or "").strip()
    if not s:
        return None
    s2 = s.replace("-", "/")
    for fmt in ("%Y/%m/%d", "%Y/%m/%d"):
        try:
            datetime.strptime(s2, fmt)
            return s2
        except Exception:
            pass
    return None


def _parse_query(q: str) -> Tuple[Optional[str], Optional[str], Optional[str], List[str]]:
    tokens = [x.strip() for x in str(q or "").strip().split() if x.strip()]
    date_str = None
    going_code = None
    venue = None
    keep: List[str] = []
    for t in tokens:
        ds = _parse_date_token(t)
        if ds and not date_str:
            date_str = ds
            continue
        v = str(t).strip().upper()
        if v in {"ST", "HV"} and not venue:
            venue = v
            continue
        if str(t).strip() in {"沙田", "跑馬地"} and not venue:
            venue = "ST" if str(t).strip() == "沙田" else "HV"
            continue
        raw, gc = normalize_going(str(t))
        gc2 = str(gc or "").strip()
        gc_norm = gc2.upper()
        if (gc_norm in GOING_CODE_LABELS) and not going_code:
            going_code = gc_norm
            continue
        keep.append(t)
    return date_str, going_code, venue, keep


def _date_range_from_str(ds: str):
    d0 = datetime.strptime(ds, "%Y/%m/%d").date()
    return d0


def main():
    st.set_page_config(page_title="全站搜尋", layout="wide")
    render_admin_nav(active="search")

    st.title("🔎 全站搜尋")
    init_db()

    q = st.text_input("輸入關鍵字（預設 AND；可混合日期/馬名/騎師/練馬師/going）", value="", placeholder="例：2026/04/26 文家良 或 朗日自強 好地至黏地")
    date_str, going_code, venue, keep = _parse_query(q)

    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    with c1:
        limit = st.selectbox("最多結果", options=[50, 100, 200, 500], index=1)
    with c2:
        show_payload = st.checkbox("顯示內容", value=True)
    with c3:
        only_race = st.checkbox("只顯示賽事相關", value=False)
    with c4:
        doc_types = st.multiselect(
            "doc_type 篩選",
            options=[
                "race",
                "race_entry",
                "race_reportext",
                "windtracker",
                "runpos",
                "corunning",
                "ai_report",
                "ai_reflection",
                "ai_learned_rules",
                "trkprof",
                "trkprof_index",
            ],
            default=[],
        )

    show_stats = st.checkbox("顯示索引統計", value=False)
    with st.expander("索引維護", expanded=False):
        d_default = None
        if date_str:
            try:
                d_default = datetime.strptime(date_str, "%Y/%m/%d").date()
            except Exception:
                d_default = None
        if not d_default:
            d_default = datetime.utcnow().date()
        d_from = st.date_input("回填日期（from）", value=d_default, key="bf_from")
        d_to = st.date_input("回填日期（to）", value=d_default, key="bf_to")
        limit_races_bf = st.selectbox("回填最多 races", options=[50, 200, 1000, 5000], index=1, key="bf_limit")
        c_bf1, c_bf2 = st.columns([1, 1])
        with c_bf1:
            if st.button("回填搜尋索引（需要 worker）", type="primary"):
                ds1 = str(d_from.strftime("%Y/%m/%d"))
                ds2 = str(d_to.strftime("%Y/%m/%d"))
                s_job = get_session()
                try:
                    job = enqueue_job(s_job, "search_backfill", {"from": ds1, "to": ds2, "limit_races": int(limit_races_bf or 200)})
                    st.success(f"已提交回填 job_id={str(job.get('id') or '')}")
                finally:
                    s_job.close()
        with c_bf2:
            if st.button("即時回填 AI 索引（唔需 worker）"):
                ds1 = str(d_from.strftime("%Y/%m/%d"))
                ds2 = str(d_to.strftime("%Y/%m/%d"))
                s0 = get_session()
                try:
                    before = int(s0.query(func.count(SystemConfig.id)).filter(SystemConfig.key.like("ai_race_report:%")).scalar() or 0)
                    before_idx = int(s0.query(func.count(SearchDocument.id)).filter(SearchDocument.doc_type == "ai_report").scalar() or 0)

                    start = datetime.combine(d_from, datetime.min.time())
                    end = datetime.combine(d_to, datetime.min.time())
                    end = end.replace(hour=0, minute=0, second=0, microsecond=0)
                    end = end + timedelta(days=1)
                    races = (
                        s0.query(Race)
                        .filter(Race.race_date >= start)
                        .filter(Race.race_date < end)
                        .order_by(Race.race_date.asc(), Race.race_no.asc())
                        .limit(int(limit_races_bf or 200))
                        .all()
                    )
                    n_docs = 0
                    for r in races:
                        rn = int(getattr(r, "race_no", 0) or 0)
                        try:
                            ds = r.race_date.strftime("%Y/%m/%d")
                        except Exception:
                            ds = ""
                        if not ds or not rn:
                            continue
                        index_system_config_doc(s0, f"ai_race_report:{ds}:{rn}", doc_type="ai_report", title=f"{ds} R{rn} AI report")
                        index_system_config_doc(s0, f"ai_race_reflection:{ds}:{rn}", doc_type="ai_reflection", title=f"{ds} R{rn} AI reflection")
                        scenario_keys = (
                            s0.query(SystemConfig.key)
                            .filter(SystemConfig.key.like(f"ai_race_report_scenario:{ds}:{rn}:%"))
                            .order_by(SystemConfig.key.asc())
                            .all()
                        )
                        for (k,) in scenario_keys:
                            if not k:
                                continue
                            index_system_config_doc(s0, str(k), doc_type="ai_report", title=str(k))
                        n_docs += 1
                    s0.commit()
                    after_idx = int(s0.query(func.count(SearchDocument.id)).filter(SearchDocument.doc_type == "ai_report").scalar() or 0)
                    st.success(f"完成：races={len(races)}（處理={n_docs}） ai_report索引 {before_idx} → {after_idx}（SystemConfig ai_race_report總數={before}）")
                finally:
                    s0.close()

    session = get_session()
    try:
        if show_stats:
            rows2 = (
                session.query(SearchDocument.doc_type, func.count(SearchDocument.id).label("n"))
                .group_by(SearchDocument.doc_type)
                .order_by(func.count(SearchDocument.id).desc())
                .all()
            )
            st.write({str(dt or ""): int(n or 0) for dt, n in rows2})

        qq = session.query(SearchDocument)
        if doc_types:
            qq = qq.filter(SearchDocument.doc_type.in_([str(x) for x in doc_types]))
        if only_race:
            qq = qq.filter(SearchDocument.race_id.isnot(None))
        if date_str:
            d0 = _date_range_from_str(date_str)
            qq = qq.filter(SearchDocument.race_date_day == d0)
        if going_code:
            qq = qq.filter(SearchDocument.going_code == str(going_code))
        if venue:
            qq = qq.filter(SearchDocument.venue == str(venue))
        for t in keep:
            s = str(t).strip()
            if not s:
                continue
            qq = qq.filter(SearchDocument.search_text.ilike(f"%{s}%"))

        qq = qq.order_by(SearchDocument.race_date_day.desc().nullslast(), SearchDocument.updated_at.desc())
        rows = qq.limit(int(limit or 100)).all()

        st.caption(
            "解析結果："
            + " ".join(
                [
                    f"date={date_str}" if date_str else "",
                    f"going_code={going_code}" if going_code else "",
                    f"venue={venue}" if venue else "",
                    f"tokens={' '.join(keep)}" if keep else "",
                ]
            ).strip()
        )

        st.write(f"命中：{len(rows)}")

        for r in rows:
            head = " | ".join(
                [
                    str(r.doc_type or ""),
                    str(r.race_date_day or ""),
                    str(r.title or ""),
                    str(r.ref_key or ""),
                ]
            )
            with st.expander(head, expanded=False):
                st.write(
                    {
                        "doc_type": r.doc_type,
                        "ref_key": r.ref_key,
                        "entity_type": r.entity_type,
                        "entity_key": r.entity_key,
                        "race_id": r.race_id,
                        "race_date_day": str(r.race_date_day) if r.race_date_day else None,
                        "race_no": r.race_no,
                        "venue": r.venue,
                        "surface_code": r.surface_code,
                        "course_type": r.course_type,
                        "going_code": r.going_code,
                        "horse_name": r.horse_name,
                        "jockey_name": r.jockey_name,
                        "trainer_name": r.trainer_name,
                        "updated_at": str(r.updated_at) if r.updated_at else None,
                    }
                )
                if show_payload and isinstance(r.payload_excerpt, dict):
                    st.json(r.payload_excerpt)
                st.text((r.search_text or "")[:4000])
    finally:
        session.close()


if __name__ == "__main__":
    main()
