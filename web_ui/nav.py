import streamlit as st


def render_admin_nav():
    c1, c2, c3 = st.columns([1, 1, 1])

    if c1.button("🛠️ 數據管理", use_container_width=True):
        try:
            st.switch_page("pages/1_數據管理.py")
        except Exception:
            st.markdown("[🛠️ 數據管理](/%E6%95%B8%E6%93%9A%E7%AE%A1%E7%90%86)")

    if c2.button("📊 獨立條件分析", use_container_width=True):
        try:
            st.switch_page("pages/2_獨立條件分析.py")
        except Exception:
            st.markdown("[📊 獨立條件分析](/%E7%8D%A8%E7%AB%8B%E6%A2%9D%E4%BB%B6%E5%88%86%E6%9E%90)")

    if c3.button("📈 命中統計", use_container_width=True):
        try:
            st.switch_page("pages/3_命中統計.py")
        except Exception:
            st.markdown("[📈 命中統計](/%E5%91%BD%E4%B8%AD%E7%B5%B1%E8%A8%88)")
