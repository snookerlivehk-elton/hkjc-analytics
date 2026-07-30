import streamlit as st
from pathlib import Path


st.set_page_config(page_title="會議決策清單 - HKJC Analytics", page_icon="🧾", layout="wide")


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception as e:
        return f"讀取失敗：{e}"


ROOT = Path(__file__).resolve().parent.parent.parent
CHECKLIST_PATH = ROOT / "MEETING_DECISION_CHECKLIST.md"
REPORT_PATH = ROOT / "OPTIMIZATION_REPORT_V2_STATS_AI_KELLY_20260730.md"
STYLE_SPEC_PATH = ROOT / "SECTIONAL_STYLE_LABEL_SPEC.md"

st.title("🧾 會議決策清單")
st.caption("供會議快速拍板用，先看優先級與決策點，再按需要查看詳細規格。")

st.markdown(
    """
    **建議閱讀順序**

    1. 先看 `會議決策清單`
    2. 再看 `總體優化報告`
    3. 最後看 `分段時間 / 跑法 / 標籤規格`
    """
)

c1, c2, c3, c4 = st.columns(4)
c1.metric("P0", "本次必拍板", "6 項")
c2.metric("P1", "第一期必做", "7 項")
c3.metric("P2", "第二期再做", "4 項")
c4.metric("核心定位", "統計先行", "AI 補充")

tab1, tab2, tab3 = st.tabs(["會議決策清單", "總體優化報告", "分段時間 / 跑法 / 標籤規格"])

with tab1:
    st.markdown(_read_text(CHECKLIST_PATH))

with tab2:
    st.markdown(_read_text(REPORT_PATH))

with tab3:
    st.markdown(_read_text(STYLE_SPEC_PATH))
