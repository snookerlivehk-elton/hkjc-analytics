import os

import streamlit as st


def require_superadmin(page_title: str):
    if st.session_state.get("is_superadmin", False):
        return

    st.title(page_title)
    st.error("❌ 此頁面目前僅限 Superadmin 使用。請先登入。")

    super_pw = os.environ.get("SUPERADMIN_PASSWORD", "")
    if not super_pw:
        st.info("未設定 SUPERADMIN_PASSWORD 環境變數，無法登入。")
        st.stop()

    with st.form("superadmin_login_form_inline"):
        pw = st.text_input("Superadmin 密碼", value="", type="password")
        submitted = st.form_submit_button("登入", type="primary")
        if submitted:
            if str(pw) == super_pw:
                st.session_state["is_superadmin"] = True
                st.rerun()
            else:
                st.error("❌ 密碼錯誤")

    if st.button("➡️ 前往數據管理後台", use_container_width=False):
        try:
            st.switch_page("pages/1_數據管理.py")
        except Exception:
            st.markdown("[➡️ 前往數據管理後台](/%E6%95%B8%E6%93%9A%E7%AE%A1%E7%90%86)")

    st.stop()

