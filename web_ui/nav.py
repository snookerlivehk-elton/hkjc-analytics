import streamlit as st

from web_ui.auth import clear_auth_query_param


def render_admin_nav(show_logout: bool = True, active: str = ""):
    cols = st.columns([1, 1, 1, 1, 1, 1, 1, 1, 1, 0.8] if show_logout else [1, 1, 1, 1, 1, 1, 1, 1, 1])
    c1, c2, c3, c4, c5, c6, c7, c8, c9 = cols[0], cols[1], cols[2], cols[3], cols[4], cols[5], cols[6], cols[7], cols[8]

    if c1.button("🛠️ 數據管理", use_container_width=True):
        try:
            st.switch_page("pages/1_數據管理.py")
        except Exception:
            if hasattr(st, "page_link"):
                st.page_link("pages/1_數據管理.py", label="🛠️ 數據管理")
            else:
                st.markdown("[🛠️ 數據管理](/%E6%95%B8%E6%93%9A%E7%AE%A1%E7%90%86)")

    if c2.button("📊 獨立條件分析", use_container_width=True):
        try:
            st.switch_page("pages/2_獨立條件分析.py")
        except Exception:
            if hasattr(st, "page_link"):
                st.page_link("pages/2_獨立條件分析.py", label="📊 獨立條件分析")
            else:
                st.markdown("[📊 獨立條件分析](/%E7%8D%A8%E7%AB%8B%E6%A2%9D%E4%BB%B6%E5%88%86%E6%9E%90)")

    if c3.button("📈 命中統計", use_container_width=True):
        try:
            st.switch_page("pages/3_命中統計.py")
        except Exception:
            if hasattr(st, "page_link"):
                st.page_link("pages/3_命中統計.py", label="📈 命中統計")
            else:
                st.markdown("[📈 命中統計](/%E5%91%BD%E4%B8%AD%E7%B5%B1%E8%A8%88)")

    if c4.button("🤖 AI 中樞與設定", use_container_width=True):
        try:
            st.switch_page("pages/4_AI_中樞與設定.py")
        except Exception:
            if hasattr(st, "page_link"):
                st.page_link("pages/4_AI_中樞與設定.py", label="🤖 AI 中樞與設定")
            else:
                st.markdown("[🤖 AI 中樞與設定](/%E4%B8%AD%E6%A8%9E%E8%88%87%E8%A8%AD%E5%AE%9A)")

    if c5.button("🧭 重算/回填狀態", use_container_width=True):
        try:
            st.switch_page("pages/5_重算回填狀態.py")
        except Exception:
            if hasattr(st, "page_link"):
                st.page_link("pages/5_重算回填狀態.py", label="🧭 重算/回填狀態")
            else:
                st.markdown("[🧭 重算/回填狀態](/%E9%87%8D%E7%AE%97%E5%9B%9E%E5%A1%AB%E7%8B%80%E6%85%8B)")

    if c6.button("🔎 全站搜尋", use_container_width=True):
        try:
            st.switch_page("pages/6_全站搜尋.py")
        except Exception:
            if hasattr(st, "page_link"):
                st.page_link("pages/6_全站搜尋.py", label="🔎 全站搜尋")
            else:
                st.markdown("[🔎 全站搜尋](/%E5%85%A8%E7%AB%99%E6%90%9C%E5%B0%8B)")

    if show_logout:
        c10 = cols[9]
        if c10.button("🚪 登出", use_container_width=True):
            st.session_state["is_superadmin"] = False
            st.session_state.pop("auth_token", None)
            clear_auth_query_param()
            st.rerun()

    if c7.button("🐴 馬匹往績", use_container_width=True):
        try:
            st.switch_page("pages/7_馬匹往績.py")
        except Exception:
            if hasattr(st, "page_link"):
                st.page_link("pages/7_馬匹往績.py", label="🐴 馬匹往績")
            else:
                st.markdown("[🐴 馬匹往績](/%E9%A6%AC%E5%8C%B9%E5%BE%80%E7%B8%BE)")

    if c8.button("📌 Top5賠率統計", use_container_width=True):
        try:
            st.switch_page("pages/8_會員Top5賠率統計.py")
        except Exception:
            if hasattr(st, "page_link"):
                st.page_link("pages/8_會員Top5賠率統計.py", label="📌 Top5賠率統計")
            else:
                st.markdown("[📌 Top5賠率統計](/%E6%9C%83%E5%93%A1Top5%E8%B3%A0%E7%8E%87%E7%B5%B1%E8%A8%88)")

    if c9.button("💡 貼士設定", use_container_width=True):
        try:
            st.switch_page("pages/9_貼士設定.py")
        except Exception:
            if hasattr(st, "page_link"):
                st.page_link("pages/9_貼士設定.py", label="💡 貼士設定")
            else:
                st.markdown("[💡 貼士設定](/%E8%B2%BC%E5%A3%AB%E8%A8%AD%E5%AE%9A)")

    cols2 = st.columns([1, 1, 1, 1, 1, 1, 1, 1])
    if cols2[0].button("💡 貼士列表", use_container_width=True):
        try:
            st.switch_page("pages/10_貼士列表.py")
        except Exception:
            if hasattr(st, "page_link"):
                st.page_link("pages/10_貼士列表.py", label="💡 貼士列表")
            else:
                st.markdown("[💡 貼士列表](/%E8%B2%BC%E5%A3%AB%E5%88%97%E8%A1%A8)")

    if cols2[1].button("🎯 檔位統計", use_container_width=True):
        try:
            st.switch_page("pages/11_檔位統計.py")
        except Exception:
            if hasattr(st, "page_link"):
                st.page_link("pages/11_檔位統計.py", label="🎯 檔位統計")
            else:
                st.markdown("[🎯 檔位統計](/%E6%AA%94%E4%BD%8D%E7%B5%B1%E8%A8%88)")

    if cols2[2].button("🏇 騎師統計", use_container_width=True):
        try:
            st.switch_page("pages/12_騎師統計.py")
        except Exception:
            if hasattr(st, "page_link"):
                st.page_link("pages/12_騎師統計.py", label="🏇 騎師統計")
            else:
                st.markdown("[🏇 騎師統計](/%E9%A8%8E%E5%B8%AB%E7%B5%B1%E8%A8%88)")

    if cols2[3].button("🏇 練馬師統計", use_container_width=True):
        try:
            st.switch_page("pages/13_練馬師統計.py")
        except Exception:
            if hasattr(st, "page_link"):
                st.page_link("pages/13_練馬師統計.py", label="🏇 練馬師統計")
            else:
                st.markdown("[🏇 練馬師統計](/%E7%B7%B4%E9%A6%AC%E5%B8%AB%E7%B5%B1%E8%A8%88)")
