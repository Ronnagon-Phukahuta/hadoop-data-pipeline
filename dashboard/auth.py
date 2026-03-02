# auth.py
import streamlit as st
from streamlit_cookies_manager import EncryptedCookieManager
import os
import json
import secrets
from datetime import datetime, timedelta

TOKEN_FILE = "/app/history/auth_tokens.json"
TOKEN_EXPIRY_HOURS = 24
COOKIE_PASSWORD = os.environ.get("COOKIE_SECRET", "finance-itsc-secret-key-2026")


def _get_cookies():
    cookies = EncryptedCookieManager(
        prefix="finance_itsc_",
        password=COOKIE_PASSWORD,
    )
    if not cookies.ready():
        st.stop()
    return cookies


def _load_tokens() -> dict:
    if not os.path.exists(TOKEN_FILE):
        return {}
    try:
        with open(TOKEN_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_tokens(tokens: dict):
    os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
    with open(TOKEN_FILE, "w") as f:
        json.dump(tokens, f)


def _create_token(username: str) -> str:
    token = secrets.token_urlsafe(32)
    tokens = _load_tokens()
    tokens[token] = {
        "username": username,
        "expires": (datetime.now() + timedelta(hours=TOKEN_EXPIRY_HOURS)).isoformat()
    }
    _save_tokens(tokens)
    return token


def _validate_token(token: str):
    tokens = _load_tokens()
    record = tokens.get(token)
    if not record:
        return None
    if datetime.now() > datetime.fromisoformat(record["expires"]):
        del tokens[token]
        _save_tokens(tokens)
        return None
    return record["username"]


def _delete_token(token: str):
    tokens = _load_tokens()
    tokens.pop(token, None)
    _save_tokens(tokens)


def check_login(username: str, password: str) -> bool:
    users = st.secrets.get("users", {})
    return users.get(username) == password


def login_page(cookies):
    st.markdown("""
        <style>
            [data-testid="stSidebar"] { display: none; }
            [data-testid="collapsedControl"] { display: none; }
        </style>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("<br><br><br>", unsafe_allow_html=True)
        st.title("💰 Finance ITSC Dashboard")
        st.markdown("---")
        st.subheader("🔐 เข้าสู่ระบบ")

        with st.form("login_form"):
            username = st.text_input("ชื่อผู้ใช้", placeholder="username")
            password = st.text_input("รหัสผ่าน", type="password", placeholder="password")
            submitted = st.form_submit_button("เข้าสู่ระบบ", use_container_width=True)

            if submitted:
                if check_login(username, password):
                    token = _create_token(username)
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    st.session_state.auth_token = token
                    cookies["auth_token"] = token
                    cookies.save()
                    st.rerun()
                else:
                    st.error("ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง")


def require_auth():
    cookies = _get_cookies()

    # session state ยังอยู่ → ผ่านเลย
    if st.session_state.get("authenticated"):
        return

    # ลอง restore จาก cookie
    token = cookies.get("auth_token", "")
    if token:
        username = _validate_token(token)
        if username:
            st.session_state.authenticated = True
            st.session_state.username = username
            st.session_state.auth_token = token
            return

    # ไม่มี token → login
    login_page(cookies)
    st.stop()


def logout():
    cookies = _get_cookies()
    token = st.session_state.get("auth_token", "")
    if token:
        _delete_token(token)
    cookies["auth_token"] = ""
    cookies.save()
    st.session_state.authenticated = False
    st.session_state.username = None
    st.session_state.auth_token = None
    st.session_state.messages = []
    st.session_state.current_chat_id = None
    st.rerun()