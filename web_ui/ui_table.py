from __future__ import annotations

import html
from typing import Any, Optional

import pandas as pd
import streamlit as st


def _inject_css_once():
    if st.session_state.get("_ui_table_css", False):
        return
    st.session_state["_ui_table_css"] = True
    st.markdown(
        """
        <style>
        table.hkjc-table {
          width: 100%;
          border-collapse: collapse;
          border-spacing: 0;
          font-size: 0.92rem;
        }
        table.hkjc-table th,
        table.hkjc-table td {
          text-align: left;
          padding: 8px 10px;
          vertical-align: top;
          border-bottom: 1px solid rgba(148, 163, 184, 0.20);
          white-space: nowrap;
        }
        table.hkjc-table th {
          font-weight: 600;
          background: rgba(148, 163, 184, 0.08);
        }
        table.hkjc-table tr:hover td {
          background: rgba(148, 163, 184, 0.06);
        }
        table.hkjc-table td.wrap {
          white-space: normal;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_table(df: pd.DataFrame, *, key: Optional[str] = None, wrap: bool = False) -> None:
    _inject_css_once()

    if df is None or df.empty:
        st.info("沒有資料")
        return

    df2 = df.copy()
    df2 = df2.fillna("")

    cols = [str(c) for c in df2.columns]
    rows = df2.to_dict(orient="records")

    table_id = f"hkjc-table-{html.escape(str(key))}" if key else ""
    td_cls = "wrap" if wrap else ""

    thead = "<tr>" + "".join([f"<th>{html.escape(c)}</th>" for c in cols]) + "</tr>"
    tbody_parts = []
    for r in rows:
        tds = []
        for c in cols:
            v = r.get(c, "")
            tds.append(f"<td class='{td_cls}'>{html.escape(str(v))}</td>")
        tbody_parts.append("<tr>" + "".join(tds) + "</tr>")
    tbody = "".join(tbody_parts)

    html_table = f"<table id='{table_id}' class='hkjc-table'><thead>{thead}</thead><tbody>{tbody}</tbody></table>"
    st.markdown(html_table, unsafe_allow_html=True)


def render_dividends(dividends: Any, *, key: Optional[str] = None) -> None:
    _inject_css_once()

    if not isinstance(dividends, list) or not dividends:
        st.info("本場尚未有派彩資料。")
        return

    df = pd.DataFrame(dividends)
    if df.empty:
        st.info("本場尚未有派彩資料。")
        return

    df = df.rename(columns={"pool": "彩池", "combination": "勝出組合", "dividend": "派彩(HK$)"})
    for col in ("彩池", "勝出組合"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    if "派彩(HK$)" in df.columns:
        df["派彩(HK$)"] = df["派彩(HK$)"].apply(lambda x: f"{x:.1f}" if isinstance(x, (int, float)) else str(x or ""))

    cols = [c for c in ["彩池", "勝出組合", "派彩(HK$)"] if c in df.columns]
    df = df[cols].copy()

    pools = [p for p in df["彩池"].tolist() if p] if "彩池" in df.columns else []
    pool_order = list(dict.fromkeys(pools))
    palette = ["#2563eb", "#16a34a", "#f59e0b", "#9333ea", "#0ea5e9", "#db2777"]
    pool_to_color = {p: palette[i % len(palette)] for i, p in enumerate(pool_order)}

    table_id = f"hkjc-div-{html.escape(str(key))}" if key else ""
    thead = "<tr>" + "".join([f"<th>{html.escape(c)}</th>" for c in cols]) + "</tr>"

    prev_pool = None
    tbody_parts = []
    for _, row in df.iterrows():
        pool = str(row.get("彩池", "") or "")
        combo = str(row.get("勝出組合", "") or "")
        divv = str(row.get("派彩(HK$)", "") or "")

        raw_pool = pool
        show_pool = "" if (prev_pool == raw_pool and raw_pool) else raw_pool
        prev_pool = raw_pool if raw_pool else prev_pool

        color = pool_to_color.get(raw_pool, "#94a3b8")
        style = f"border-left: 6px solid {color};"
        tds = []
        for c in cols:
            if c == "彩池":
                tds.append(f"<td style='{style}'>{html.escape(show_pool)}</td>")
            elif c == "勝出組合":
                tds.append(f"<td class='wrap'>{html.escape(combo)}</td>")
            else:
                tds.append(f"<td>{html.escape(divv)}</td>")
        tbody_parts.append("<tr>" + "".join(tds) + "</tr>")

    tbody = "".join(tbody_parts)
    html_table = f"<table id='{table_id}' class='hkjc-table'><thead>{thead}</thead><tbody>{tbody}</tbody></table>"
    st.markdown(html_table, unsafe_allow_html=True)

