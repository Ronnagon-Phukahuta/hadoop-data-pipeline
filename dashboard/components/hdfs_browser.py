# dashboard/components/hdfs_browser.py
"""
HDFS File Browser — Python fetches WebHDFS server-side (no CORS issue)
Renders with native Streamlit widgets
"""

import streamlit as st
import requests
from datetime import datetime


# ── WebHDFS helpers ────────────────────────────────────────────────────────────

def _fetch(namenode_url: str, path: str) -> list:
    url = f"{namenode_url}/webhdfs/v1{path}?op=LISTSTATUS"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()["FileStatuses"]["FileStatus"]


def _fmt_size(b: int) -> str:
    if not b:
        return "—"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def _fmt_date(ms: int) -> str:
    if not ms:
        return "—"
    return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M")


def _type_icon(item: dict) -> str:
    if item["type"] == "DIRECTORY":
        return "📁"
    name = item["pathSuffix"]
    if name.endswith(".done"):
        return "✅"
    if name.endswith(".failed"):
        return "❌"
    ext = name.rsplit(".", 1)[-1].lower()
    return {"csv": "📄", "parquet": "🗃️", "json": "📋", "py": "🐍", "xml": "📝"}.get(ext, "📄")


# ── Main component ─────────────────────────────────────────────────────────────

def render_hdfs_browser(namenode_url: str = "http://namenode:50070"):
    """
    HDFS File Browser — server-side fetch via WebHDFS REST API.
    No CORS issues since requests run in Python, not the browser.
    """

    # Session state
    if "hdfs_path" not in st.session_state:
        st.session_state.hdfs_path = "/"
    if "hdfs_sort_by" not in st.session_state:
        st.session_state.hdfs_sort_by = "name"
    if "hdfs_sort_asc" not in st.session_state:
        st.session_state.hdfs_sort_asc = True

    current_path = st.session_state.hdfs_path

    # ── Breadcrumb ─────────────────────────────────────────────────────────────
    path_parts = [p for p in current_path.split("/") if p]
    crumb_labels = ["⌂"] + path_parts
    crumb_cols = st.columns(len(crumb_labels) * 2 - 1)

    for i, label in enumerate(crumb_labels):
        target = "/" if i == 0 else "/" + "/".join(path_parts[:i])
        with crumb_cols[i * 2]:
            if i == len(crumb_labels) - 1:
                st.markdown(f"**`{label}`**")
            else:
                if st.button(label, key=f"bc_{i}_{current_path}"):
                    st.session_state.hdfs_path = target
                    st.rerun()
        if i < len(crumb_labels) - 1:
            with crumb_cols[i * 2 + 1]:
                st.markdown("<p style='margin:6px 0;color:#666'>/</p>", unsafe_allow_html=True)

    # ── Fetch ──────────────────────────────────────────────────────────────────
    try:
        items = _fetch(namenode_url, current_path)
    except requests.exceptions.ConnectionError:
        st.error(f"❌ เชื่อมต่อ NameNode ไม่ได้: `{namenode_url}`")
        st.info("ตรวจสอบว่า container `namenode` รันอยู่และ port 9870 เปิดอยู่")
        return
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ WebHDFS error: {e}")
        return
    except Exception as e:
        st.error(f"❌ {e}")
        return

    # ── Stats ──────────────────────────────────────────────────────────────────
    dirs = [i for i in items if i["type"] == "DIRECTORY"]
    files = [i for i in items if i["type"] == "FILE"]
    total_size = sum(f.get("length", 0) for f in files)

    m1, m2, m3 = st.columns(3)
    m1.metric("📁 Folders", len(dirs))
    m2.metric("📄 Files", len(files))
    m3.metric("💾 Total Size", _fmt_size(total_size))

    # ── Sort controls ──────────────────────────────────────────────────────────
    sc1, sc2, sc3 = st.columns([2, 2, 6])
    with sc1:
        sort_by = st.selectbox(
            "Sort by",
            ["name", "size", "date"],
            index=["name", "size", "date"].index(st.session_state.hdfs_sort_by),
            key="hdfs_sort_select",
        )
    with sc2:
        sort_dir = st.selectbox(
            "Order",
            ["↑ Asc", "↓ Desc"],
            index=0 if st.session_state.hdfs_sort_asc else 1,
            key="hdfs_order_select",
        )

    sort_asc = sort_dir == "↑ Asc"
    if sort_by != st.session_state.hdfs_sort_by or sort_asc != st.session_state.hdfs_sort_asc:
        st.session_state.hdfs_sort_by = sort_by
        st.session_state.hdfs_sort_asc = sort_asc
        st.rerun()

    # ── Sort items ─────────────────────────────────────────────────────────────
    def sort_key(item):
        if sort_by == "name":
            return item["pathSuffix"].lower()
        elif sort_by == "size":
            return item.get("length", 0)
        elif sort_by == "date":
            return item.get("modificationTime", 0)
        return item["pathSuffix"].lower()

    sorted_items = sorted(items, key=sort_key, reverse=not sort_asc)
    # Dirs always first
    sorted_items = (
        [i for i in sorted_items if i["type"] == "DIRECTORY"] +
        [i for i in sorted_items if i["type"] != "DIRECTORY"]
    )

    st.divider()

    # ── Back button ────────────────────────────────────────────────────────────
    if current_path != "/":
        parent = "/" + "/".join(current_path.strip("/").split("/")[:-1])
        if st.button("📁 .. (กลับ)", key="hdfs_back"):
            st.session_state.hdfs_path = parent or "/"
            st.rerun()

    # ── File listing ───────────────────────────────────────────────────────────
    if not sorted_items:
        st.info("📭 Empty directory")
        return

    # Header
    hc = st.columns([4, 1, 1, 1, 2])
    for col, label in zip(hc, ["Name", "Size", "Permission", "Owner", "Modified"]):
        col.markdown(f"<small style='color:#888;font-weight:600;text-transform:uppercase;letter-spacing:.05em'>{label}</small>", unsafe_allow_html=True)

    st.markdown("<hr style='margin:4px 0;border-color:rgba(128,128,128,0.2)'>", unsafe_allow_html=True)

    # Rows
    for item in sorted_items:
        is_dir = item["type"] == "DIRECTORY"
        name = item["pathSuffix"]
        full_path = f"{current_path.rstrip('/')}/{name}"
        icon = _type_icon(item)

        rc = st.columns([4, 1, 1, 1, 2])

        with rc[0]:
            if is_dir:
                if st.button(f"{icon} {name}", key=f"nav_{full_path}", use_container_width=True):
                    st.session_state.hdfs_path = full_path
                    st.rerun()
            else:
                st.markdown(
                    f"<div style='padding:5px 0;font-family:monospace;font-size:13px'>{icon} {name}</div>",
                    unsafe_allow_html=True,
                )

        with rc[1]:
            st.markdown(
                f"<div style='padding:5px 0;font-family:monospace;font-size:12px;color:#666'>"
                f"{'—' if is_dir else _fmt_size(item.get('length', 0))}</div>",
                unsafe_allow_html=True,
            )

        with rc[2]:
            st.markdown(
                f"<div style='padding:5px 0;font-family:monospace;font-size:12px;color:#666'>"
                f"{item.get('permission', '—')}</div>",
                unsafe_allow_html=True,
            )

        with rc[3]:
            st.markdown(
                f"<div style='padding:5px 0;font-family:monospace;font-size:12px;color:#666'>"
                f"{item.get('owner', '—')}</div>",
                unsafe_allow_html=True,
            )

        with rc[4]:
            st.markdown(
                f"<div style='padding:5px 0;font-family:monospace;font-size:12px;color:#666'>"
                f"{_fmt_date(item.get('modificationTime', 0))}</div>",
                unsafe_allow_html=True,
            )