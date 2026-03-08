# components/sidebar.py
import streamlit as st
from auth import logout
from utils.history import load_chat_history, clear_history
from services.hive_gpt import execute_query_df


@st.cache_data(ttl=300)
def load_available_years() -> list:
    """ดึงรายการปีที่มีข้อมูลใน Hive"""
    try:
        df = execute_query_df(
            "SELECT DISTINCT year FROM finance_itsc_long ORDER BY year DESC")
        return df["year"].tolist()
    except Exception:
        return [2024]


@st.cache_data(ttl=300)
def load_quick_stats(year: int):
    """ดึงข้อมูล Quick Stats จาก Hive"""
    if year is None:
        return 0, 0

    df_budget = execute_query_df(f"""
        SELECT SUM(amount) as total 
        FROM finance_itsc_long 
        WHERE details = 'budget' AND year = {year}
        AND `date` = 'all-year-budget'
    """)
    total_budget = df_budget['total'].iloc[0] or 0

    df_spent = execute_query_df(f"""
        SELECT SUM(amount) as total 
        FROM finance_itsc_long 
        WHERE details = 'spent' AND year = {year}
    """)
    total_spent = df_spent['total'].iloc[0] or 0

    return total_budget, total_spent


@st.cache_data(ttl=300)
def load_negative_categories(year: int) -> list:
    """ดึงหมวดที่ remaining ติดลบ จาก row สุดท้าย"""
    if year is None:
        return []

    try:
        df = execute_query_df(f"""
            SELECT category, amount
            FROM (
                SELECT t.category, t.amount
                FROM finance_itsc_long t
                JOIN (
                    SELECT category, MAX(`date`) AS max_date
                    FROM finance_itsc_long
                    WHERE details = 'remaining'
                    AND year = {year}
                    GROUP BY category
                ) latest ON t.category = latest.category 
                    AND t.`date` = latest.max_date
                WHERE t.details = 'remaining'
                AND t.year = {year}
            ) sub
            WHERE amount < 0
            ORDER BY amount ASC
        """)
        return df.to_dict("records")
    except Exception:
        return []


def render_sidebar():
    with st.sidebar:
        _render_user_section()
        st.markdown("---")
        _render_year_selector()
        st.markdown("---")
        _render_quick_stats()
        _render_negative_alert()
        st.markdown("---")
        _render_history_section()


def _render_user_section():
    st.markdown(f"👤 **{st.session_state.get('username', '')}**")
    if st.button("🚪 ออกจากระบบ", use_container_width=True):
        logout()


def _render_year_selector():
    """Year selector — เก็บใน session_state เพื่อให้ทุก component ใช้ได้"""
    st.header("📅 เลือกปีงบประมาณ")
    years = load_available_years()

    if "selected_year" not in st.session_state:
        st.session_state.selected_year = years[0] if years else 2024

    selected = st.selectbox(
        "ปีงบประมาณ",
        options=years,
        index=years.index(
            st.session_state.selected_year) if st.session_state.selected_year in years else 0,
        label_visibility="collapsed"
    )

    if selected != st.session_state.selected_year:
        st.session_state.selected_year = selected
        # clear cache เมื่อเปลี่ยนปี
        load_quick_stats.clear()
        load_negative_categories.clear()
        st.rerun()


def _render_quick_stats():
    st.header("📊 Quick Stats")

    # ดึง year หลังจาก _render_year_selector set แล้วเสมอ
    year = st.session_state.get("selected_year")
    if year is None:
        st.caption("รอเลือกปีงบประมาณ...")
        return

    st.caption(f"ปีงบประมาณ {year}")

    try:
        total_budget, total_spent = load_quick_stats(year)
        remaining = total_budget - total_spent
        percent_used = (total_spent / total_budget *
                        100) if total_budget > 0 else 0

        st.metric("งบประมาณทั้งหมด", f"{total_budget:,.0f} ฿")
        st.metric("ใช้ไปแล้ว", f"{total_spent:,.0f} ฿", f"{percent_used:.1f}%")
        st.metric(
            "คงเหลือ",
            f"{remaining:,.0f} ฿",
            delta_color="inverse" if remaining < 0 else "normal"
        )
        st.progress(min(percent_used / 100, 1.0))

    except Exception as e:
        st.error(f"โหลดข้อมูลไม่ได้: {e}")


def _render_negative_alert():
    year = st.session_state.get("selected_year")
    if year is None:
        return

    neg_cats = load_negative_categories(year)

    if not neg_cats:
        return

    st.markdown("---")
    with st.expander(f"⚠️ หมวดงบติดลบ ({len(neg_cats)} หมวด)", expanded=True):
        for item in neg_cats:
            st.error(
                f"**{item['category']}**  \n"
                f"remaining: {item['amount']:,.0f} ฿"
            )

def _render_history_section():
    st.header("📜 ประวัติการถาม")
    history = load_chat_history()

    if not history:
        st.info("ยังไม่มีประวัติการถาม")
        return

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🗑️ ล้างประวัติ", use_container_width=True):
            clear_history()
            st.session_state.messages = []
            st.session_state.current_chat_id = None
            st.rerun()
    with col2:
        if st.button("➕ Chat ใหม่", use_container_width=True):
            st.session_state.messages = []
            st.session_state.current_chat_id = None
            st.rerun()

    for i, item in enumerate(reversed(history)):
        idx = len(history) - 1 - i
        short_q = item['question'][:25] + \
            "..." if len(item['question']) > 25 else item['question']
        if st.button(f"🕐 {item['timestamp']}\n{short_q}", key=f"history_{i}", use_container_width=True):
            st.session_state.current_chat_id = idx
            st.session_state.messages = []
            st.rerun()