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

st.title("🗂️ HDFS Browser")

if st.button("🔄 Refresh", type="primary"):
    st.rerun()

render_hdfs_browser(namenode_url="http://namenode:50070")