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


def _stat_card(label: str, value: str, icon: str) -> str:
    return (
        f"<div style='display:flex;flex-direction:column;gap:4px;"
        f"background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);"
        f"border-radius:10px;padding:14px 18px'>"
        f"<span style='font-size:11px;color:#64748B;text-transform:uppercase;"
        f"letter-spacing:.08em;font-weight:600'>{icon} {label}</span>"
        f"<span style='font-size:24px;font-weight:700;color:#F1F5F9;font-family:monospace'>{value}</span>"
        f"</div>"
    )


# ── Main component ─────────────────────────────────────────────────────────────

def render_hdfs_browser(namenode_url: str = "http://namenode:50070"):
    """
    HDFS File Browser — server-side fetch via WebHDFS REST API.
    No CORS issues since requests run in Python, not the browser.
    """

    if "hdfs_path" not in st.session_state:
        st.session_state.hdfs_path = "/"
    if "hdfs_sort_by" not in st.session_state:
        st.session_state.hdfs_sort_by = "name"
    if "hdfs_sort_asc" not in st.session_state:
        st.session_state.hdfs_sort_asc = True

    current_path = st.session_state.hdfs_path

    # ── Path bar + sort controls (1 row) ──────────────────────────────────────
    path_parts = [p for p in current_path.split("/") if p]

    crumb_html = "<span style='color:#64748B'>⌂</span>"
    for i, part in enumerate(path_parts):
        crumb_html += "<span style='color:#334155;margin:0 6px'>/</span>"
        if i < len(path_parts) - 1:
            crumb_html += f"<span style='color:#64748B'>{part}</span>"
        else:
            crumb_html += f"<span style='color:#F1F5F9;font-weight:600'>{part}</span>"

    col_path, col_sort, col_order = st.columns([4, 1, 1])
    with col_path:
        st.markdown(
            f"<div style='background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);"
            f"border-radius:8px;padding:10px 16px;font-family:monospace;font-size:13px;line-height:1.8'>"
            f"{crumb_html}</div>",
            unsafe_allow_html=True,
        )
    with col_sort:
        sort_by = st.selectbox(
            "Sort",
            ["name", "size", "date"],
            index=["name", "size", "date"].index(st.session_state.hdfs_sort_by),
            key="hdfs_sort_select",
            label_visibility="collapsed",
        )
    with col_order:
        sort_dir = st.selectbox(
            "Order",
            ["↑ Asc", "↓ Desc"],
            index=0 if st.session_state.hdfs_sort_asc else 1,
            key="hdfs_order_select",
            label_visibility="collapsed",
        )

    sort_asc = sort_dir == "↑ Asc"
    if sort_by != st.session_state.hdfs_sort_by or sort_asc != st.session_state.hdfs_sort_asc:
        st.session_state.hdfs_sort_by = sort_by
        st.session_state.hdfs_sort_asc = sort_asc
        st.rerun()

    # ── Breadcrumb nav (เฉพาะเมื่อไม่ได้อยู่ root) ──────────────────────────
    if path_parts:
        nav_items = [("⌂ root", "/")] + [
            (f"↑ {part}", "/" + "/".join(path_parts[:i + 1]))
            for i, part in enumerate(path_parts[:-1])
        ]
        nav_cols = st.columns(min(len(nav_items), 8))
        for i, (label, target) in enumerate(nav_items):
            with nav_cols[i]:
                if st.button(label, key=f"bc_{i}_{current_path}", use_container_width=True):
                    st.session_state.hdfs_path = target
                    st.rerun()

    st.markdown("")

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
    dirs  = [i for i in items if i["type"] == "DIRECTORY"]
    files = [i for i in items if i["type"] == "FILE"]
    total_size = sum(f.get("length", 0) for f in files)

    s1, s2, s3 = st.columns(3)
    s1.markdown(_stat_card("Folders",    str(len(dirs)),        "📁"), unsafe_allow_html=True)
    s2.markdown(_stat_card("Files",      str(len(files)),       "📄"), unsafe_allow_html=True)
    s3.markdown(_stat_card("Total Size", _fmt_size(total_size), "💾"), unsafe_allow_html=True)

    st.markdown("")

    # ── Sort items ─────────────────────────────────────────────────────────────
    def sort_key(item):
        if sort_by == "name":
            return item["pathSuffix"].lower()
        if sort_by == "size": 
            return item.get("length", 0)
        if sort_by == "date": 
            return item.get("modificationTime", 0)
        return item["pathSuffix"].lower()

    sorted_items = sorted(items, key=sort_key, reverse=not sort_asc)
    sorted_items = (
        [i for i in sorted_items if i["type"] == "DIRECTORY"] +
        [i for i in sorted_items if i["type"] != "DIRECTORY"]
    )

    # ── File listing ───────────────────────────────────────────────────────────
    if not sorted_items:
        st.markdown(
            "<div style='padding:32px;text-align:center;color:#475569;"
            "border:1px dashed rgba(255,255,255,0.08);border-radius:10px'>"
            "📭 Empty directory</div>",
            unsafe_allow_html=True,
        )
        return

    # Header
    hc = st.columns([4, 1, 1, 1, 2])
    for col, label in zip(hc, ["NAME", "SIZE", "PERM", "OWNER", "MODIFIED"]):
        col.markdown(
            f"<div style='padding:0 0 6px 0;font-size:10px;color:#475569;"
            f"font-weight:700;letter-spacing:.1em'>{label}</div>",
            unsafe_allow_html=True,
        )
    st.markdown(
        "<hr style='margin:0 0 4px 0;border:none;border-top:1px solid rgba(255,255,255,0.06)'>",
        unsafe_allow_html=True,
    )

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
                    f"<div style='padding:6px 0;font-family:monospace;font-size:13px'>"
                    f"{icon} {name}</div>",
                    unsafe_allow_html=True,
                )

        with rc[1]:
            st.markdown(
                f"<div style='padding:6px 0;font-family:monospace;font-size:12px;color:#64748B'>"
                f"{'—' if is_dir else _fmt_size(item.get('length', 0))}</div>",
                unsafe_allow_html=True,
            )
        with rc[2]:
            st.markdown(
                f"<div style='padding:6px 0;font-family:monospace;font-size:12px;color:#64748B'>"
                f"{item.get('permission', '—')}</div>",
                unsafe_allow_html=True,
            )
        with rc[3]:
            st.markdown(
                f"<div style='padding:6px 0;font-family:monospace;font-size:12px;color:#64748B'>"
                f"{item.get('owner', '—')}</div>",
                unsafe_allow_html=True,
            )
        with rc[4]:
            st.markdown(
                f"<div style='padding:6px 0;font-family:monospace;font-size:12px;color:#64748B'>"
                f"{_fmt_date(item.get('modificationTime', 0))}</div>",
                unsafe_allow_html=True,
            )