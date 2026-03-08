# dashboard/pages/hdfs_browser.py
"""
HDFS Browser — Browse folders and files in HDFS Data Lake
"""

import streamlit as st
from auth import require_auth
from components.hdfs_browser import render_hdfs_browser

st.set_page_config(
    page_title="HDFS Browser — Finance ITSC",
    page_icon="🗂️",
    layout="wide",
)

require_auth()

col_title, col_refresh = st.columns([6, 1])
with col_title:
    st.title("🗂️ HDFS Browser")
with col_refresh:
    st.markdown("<div style='padding-top:14px'>", unsafe_allow_html=True)
    if st.button("🔄 Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

render_hdfs_browser(namenode_url="http://namenode:50070")