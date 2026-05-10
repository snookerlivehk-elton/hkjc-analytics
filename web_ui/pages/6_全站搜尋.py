from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st
from sqlalchemy import and_

from database.connection import get_session, init_db
from database.models import SearchDocument
from scoring_engine.track_conditions import normalize_going
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
        if gc and not going_code:
            going_code = str(gc).strip()
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

    session = get_session()
    try:
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

