import sys
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import func

# 加入專案路徑
root_path = str(Path(__file__).resolve().parent.parent.parent)
if root_path not in sys.path:
    sys.path.insert(0, root_path)

from database.connection import get_session, init_db
from database.models import Horse, HorseHistory, Race, RaceEntry, RaceResult
from web_ui.nav import render_admin_nav


def main():
    st.set_page_config(page_title="馬匹往績", layout="wide")
    render_admin_nav(active="horse_history")
    st.title("🐴 馬匹往績")
    init_db()

    q = st.text_input("輸入馬名（中文）或馬匹編號（例如 H123）", value="", placeholder="例如：北地烈馬")
    limit = st.selectbox("最多顯示往績", options=[20, 50, 100, 200], index=1)

    session = get_session()
    try:
        qs = str(q or "").strip()
        if not qs:
            st.caption("提示：先輸入馬名或馬匹編號。")
            return

        hq = session.query(Horse)
        if qs.upper().startswith("H"):
            hq = hq.filter(func.upper(Horse.code) == qs.upper())
        else:
            hq = hq.filter(Horse.name_ch.ilike(f"%{qs}%"))
        horses = hq.order_by(Horse.name_ch.asc(), Horse.code.asc()).limit(50).all()

        if not horses:
            st.error("搵唔到匹配馬匹。")
            st.caption("如果係新馬名，先去「數據管理」做一次抓排位/回填往績。")
            return

        if len(horses) == 1:
            horse = horses[0]
        else:
            opts = {f"{h.name_ch} ({h.code})": h.id for h in horses}
            pick = st.selectbox("選擇馬匹", options=list(opts.keys()), index=0)
            hid = int(opts[pick])
            horse = next((h for h in horses if int(h.id) == hid), horses[0])

        st.subheader(f"{str(horse.name_ch)} ({str(horse.code)})")

        hh = (
            session.query(HorseHistory)
            .filter(HorseHistory.horse_id == int(horse.id))
            .order_by(HorseHistory.race_date.desc())
            .limit(int(limit or 50))
            .all()
        )

        if not hh:
            st.warning("未有呢匹馬嘅歷史往績（horse_histories）記錄。")
            st.caption("可到「數據管理」執行『歷史數據回填／回填馬匹往績』，再返嚟重試。")
        else:
            rows = []
            for r in hh:
                dt = getattr(r, "race_date", None)
                rows.append(
                    {
                        "日期": dt.strftime("%Y/%m/%d") if hasattr(dt, "strftime") else str(dt or ""),
                        "場地": str(getattr(r, "venue", "") or ""),
                        "跑道": str(getattr(r, "surface", "") or ""),
                        "班次": str(getattr(r, "race_class", "") or ""),
                        "路程": int(getattr(r, "distance", 0) or 0) or None,
                        "名次": int(getattr(r, "rank", 0) or 0) or None,
                        "檔位": int(getattr(r, "draw", 0) or 0) or None,
                        "騎師": str(getattr(r, "jockey_name", "") or ""),
                        "練馬師": str(getattr(r, "trainer_name", "") or ""),
                        "負磅": int(getattr(r, "weight", 0) or 0) or None,
                        "評分": int(getattr(r, "rating", 0) or 0) or None,
                        "完成時間": str(getattr(r, "finish_time", "") or ""),
                    }
                )
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        st.divider()
        st.subheader("本系統賽事入庫紀錄（race_entries / race_results）")
        entries = (
            session.query(RaceEntry, Race, RaceResult)
            .join(Race, Race.id == RaceEntry.race_id)
            .outerjoin(RaceResult, RaceResult.entry_id == RaceEntry.id)
            .filter(RaceEntry.horse_id == int(horse.id))
            .order_by(Race.race_date.desc(), Race.race_no.desc())
            .limit(200)
            .all()
        )
        if not entries:
            st.caption("未有入庫賽事紀錄（通常只會有你抓過嘅賽日）。")
        else:
            out = []
            for e, r, rr in entries:
                dt = getattr(r, "race_date", None)
                out.append(
                    {
                        "日期": dt.strftime("%Y/%m/%d") if hasattr(dt, "strftime") else str(dt or ""),
                        "場地": str(getattr(r, "venue", "") or ""),
                        "場次": int(getattr(r, "race_no", 0) or 0) or None,
                        "路程": int(getattr(r, "distance", 0) or 0) or None,
                        "馬號": int(getattr(e, "horse_no", 0) or 0) or None,
                        "檔位": int(getattr(e, "draw", 0) or 0) or None,
                        "名次": int(getattr(rr, "rank", 0) or 0) if rr else None,
                        "win_odds": float(getattr(rr, "win_odds", 0) or 0) if rr and getattr(rr, "win_odds", None) is not None else None,
                        "entry_id": int(getattr(e, "id", 0) or 0),
                        "race_id": int(getattr(r, "id", 0) or 0),
                    }
                )
            st.dataframe(pd.DataFrame(out), use_container_width=True, hide_index=True)
    finally:
        session.close()


if __name__ == "__main__":
    main()

