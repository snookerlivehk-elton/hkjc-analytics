import os
import base64
import hashlib
import hmac
import json
import time
from typing import Any, Dict, Optional, Tuple

import streamlit as st


def _now_ts() -> int:
    return int(time.time())


def _get_secret() -> str:
    s = str(os.environ.get("AUTH_TOKEN_SECRET") or "").strip()
    if s:
        return s
    s2 = str(os.environ.get("SUPERADMIN_PASSWORD") or "").strip()
    if s2:
        return s2
    return "dev-unsafe-secret"


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * ((4 - (len(s) % 4)) % 4)
    return base64.urlsafe_b64decode((s or "") + pad)


def _sign(payload_b64: str) -> str:
    key = _get_secret().encode("utf-8")
    msg = payload_b64.encode("utf-8")
    sig = hmac.new(key, msg, hashlib.sha256).digest()
    return _b64url_encode(sig)


def _encode_token(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    payload_b64 = _b64url_encode(raw)
    sig_b64 = _sign(payload_b64)
    return f"{payload_b64}.{sig_b64}"


def _decode_token(token: str) -> Optional[Dict[str, Any]]:
    t = str(token or "").strip()
    if not t or "." not in t:
        return None
    payload_b64, sig_b64 = t.split(".", 1)
    if not payload_b64 or not sig_b64:
        return None
    exp_sig = _sign(payload_b64)
    if not hmac.compare_digest(exp_sig, sig_b64):
        return None
    try:
        raw = _b64url_decode(payload_b64)
        obj = json.loads(raw.decode("utf-8"))
        if not isinstance(obj, dict):
            return None
        exp = obj.get("exp")
        if exp is None:
            return None
        if int(exp) < _now_ts():
            return None
        return obj
    except Exception:
        return None


def _get_query_params() -> Dict[str, Any]:
    if hasattr(st, "query_params"):
        qp = dict(st.query_params)
        out: Dict[str, Any] = {}
        for k, v in qp.items():
            out[str(k)] = v
        return out
    return dict(st.experimental_get_query_params())


def _set_query_params(params: Dict[str, Any]):
    if hasattr(st, "query_params"):
        st.query_params.clear()
        for k, v in params.items():
            if v is None:
                continue
            st.query_params[str(k)] = str(v)
        return
    st.experimental_set_query_params(**{str(k): str(v) for k, v in params.items() if v is not None})


def clear_auth_query_param():
    qp = _get_query_params()
    if "auth" in qp:
        qp.pop("auth", None)
        _set_query_params(qp)


def restore_auth_from_query_param():
    qp = _get_query_params()
    tok = qp.get("auth")
    if isinstance(tok, list):
        tok = tok[0] if tok else ""
    tok = str(tok or "").strip()
    if not tok:
        return
    payload = _decode_token(tok)
    if not payload:
        return
    role = str(payload.get("role") or "").strip()
    if role == "superadmin":
        st.session_state["is_superadmin"] = True
        st.session_state["auth_token"] = tok
        return
    if role == "member":
        email = str(payload.get("email") or "").strip().lower()
        if email:
            st.session_state["member_email"] = email
            st.session_state["auth_token"] = tok


def persist_auth_to_query_param():
    tok = str(st.session_state.get("auth_token") or "").strip()
    if not tok:
        return
    qp = _get_query_params()
    cur = qp.get("auth")
    if isinstance(cur, list):
        cur = cur[0] if cur else ""
    cur = str(cur or "").strip()
    if cur == tok:
        return
    qp["auth"] = tok
    _set_query_params(qp)


def set_member_authenticated(email: str, ttl_days: int = 30):
    e = str(email or "").strip().lower()
    exp = _now_ts() + int(ttl_days) * 86400
    tok = _encode_token({"role": "member", "email": e, "exp": exp})
    st.session_state["member_email"] = e
    st.session_state["auth_token"] = tok
    persist_auth_to_query_param()


def set_superadmin_authenticated(ttl_days: int = 30):
    exp = _now_ts() + int(ttl_days) * 86400
    tok = _encode_token({"role": "superadmin", "exp": exp})
    st.session_state["is_superadmin"] = True
    st.session_state["auth_token"] = tok
    persist_auth_to_query_param()


def require_superadmin(page_title: str):
    restore_auth_from_query_param()
    persist_auth_to_query_param()
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
                set_superadmin_authenticated()
                st.rerun()
            else:
                st.error("❌ 密碼錯誤")

    if st.button("➡️ 前往數據管理後台", width="content"):
        try:
            st.switch_page("pages/1_數據管理.py")
        except Exception:
            st.markdown("[➡️ 前往數據管理後台](/%E6%95%B8%E6%93%9A%E7%AE%A1%E7%90%86)")

    st.stop()
