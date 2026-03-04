# dashboard/pages/upload.py
"""
Upload — Excel → GPT → CSV → Column Mapping → HDFS
"""

import os
import re
import requests
import streamlit as st
from collections import Counter
from datetime import datetime
from auth import require_auth
import tempfile
import openpyxl
from services.schema_service import (
    infer_table_name,
    get_schema,
    compare_columns,
    translate_columns_to_thai,
)
from services.hdfs_upload import upload_csv_to_hdfs
from services.excel_service import convert_excel

WEBHDFS_URL = os.environ.get("WEBHDFS_URL", "http://namenode:50070")

st.set_page_config(
    page_title="Upload — Finance ITSC",
    page_icon="📤",
    layout="wide",
)

require_auth()

st.title("📤 Upload ข้อมูลเข้า HDFS")

# ═══════════════════════════════════════════════════
# STEP 1 — เลือก folder และ upload ไฟล์
# ═══════════════════════════════════════════════════
st.header("1️⃣ เลือก Folder ปลายทาง")

ROOT_PATH = "/datalake/raw"

if "upload_browse_path" not in st.session_state:
    st.session_state.upload_browse_path = ROOT_PATH

browse_path = st.session_state.upload_browse_path

@st.cache_data(ttl=15)
def list_hdfs_dirs(path: str) -> list[str]:
    try:
        url = f"{WEBHDFS_URL}/webhdfs/v1{path}?op=LISTSTATUS"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        items = resp.json()["FileStatuses"]["FileStatus"]
        return [i["pathSuffix"] for i in items if i["type"] == "DIRECTORY"]
    except Exception:
        return []

# ── Path bar ──────────────────────────────────────
relative = browse_path.replace(ROOT_PATH, "") or "/"
st.markdown(
    f"<div style='background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);"
    f"border-radius:8px;padding:8px 14px;font-family:monospace;font-size:13px;color:#94A3B8'>"
    f"<span style='color:#475569'>datalake/raw</span>"
    f"<span style='color:#E2E8F0'>{relative}</span></div>",
    unsafe_allow_html=True,
)
st.markdown("")

# ── Breadcrumb (เฉพาะ path ใต้ raw) ──────────────
rel_parts = [p for p in relative.strip("/").split("/") if p]

if rel_parts:
    crumbs = ["raw"] + rel_parts
    bc_html = ""
    for i, part in enumerate(crumbs):
        if i < len(crumbs) - 1:
            target = ROOT_PATH + ("/" + "/".join(rel_parts[:i]) if i > 0 else "")
            bc_html += f"<button onclick=\"\" style='background:none;border:none;color:#64748B;font-family:monospace;font-size:12px;cursor:pointer;padding:2px 4px'>{part}</button>"
            bc_html += "<span style='color:#334155;margin:0 2px'>/</span>"
        else:
            bc_html += f"<span style='color:#E2E8F0;font-family:monospace;font-size:12px;font-weight:600;padding:2px 4px'>{part}</span>"

    # ทำ breadcrumb ด้วย st.button แทน (HTML onclick ใน st.markdown ทำงานไม่ได้)
    bc_cols = st.columns(len(crumbs) * 2 - 1)
    for i, part in enumerate(crumbs):
        target = ROOT_PATH if i == 0 else ROOT_PATH + "/" + "/".join(rel_parts[:i])
        with bc_cols[i * 2]:
            if i < len(crumbs) - 1:
                if st.button(part, key=f"ubc_{i}", use_container_width=False):
                    st.session_state.upload_browse_path = target
                    st.session_state.pop("upload_selected_path", None)
                    st.rerun()
            else:
                st.markdown(f"**{part}**")
        if i < len(crumbs) - 1:
            with bc_cols[i * 2 + 1]:
                st.markdown("<span style='color:#475569'>/</span>", unsafe_allow_html=True)

# ── Subfolder grid ────────────────────────────────
subdirs = list_hdfs_dirs(browse_path)

if subdirs:
    cols = st.columns(min(len(subdirs), 4))
    for i, d in enumerate(subdirs):
        with cols[i % 4]:
            if st.button(f"📁 {d}", key=f"udir_{browse_path}_{d}", use_container_width=True):
                st.session_state.upload_browse_path = f"{browse_path}/{d}"
                st.session_state.pop("upload_selected_path", None)
                st.rerun()
else:
    st.caption("— ไม่มี subfolder —")

st.markdown("")

# ── Actions ───────────────────────────────────────
col_sel, col_new = st.columns([1, 1])

with col_sel:
    label = browse_path.replace(ROOT_PATH, "raw") or "raw"
    if st.button(f"✅ เลือก `{label}`", use_container_width=True, type="primary"):
        st.session_state.upload_selected_path = browse_path

with col_new:
    with st.popover("📁 สร้าง folder ใหม่", use_container_width=True):
        new_folder_name = st.text_input("ชื่อ folder", placeholder="เช่น year=2026", key="new_folder_input")
        if st.button("➕ สร้าง", key="btn_create_folder", type="primary"):
            if new_folder_name:
                new_path = f"{browse_path}/{new_folder_name}"
                try:
                    url = f"{WEBHDFS_URL}/webhdfs/v1{new_path}?op=MKDIRS"
                    resp = requests.put(url, timeout=10)
                    resp.raise_for_status()
                    st.success(f"✅ สร้าง `{new_folder_name}` สำเร็จ")
                    st.session_state.upload_browse_path = new_path
                    st.session_state.upload_selected_path = new_path
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")

# ── Selected path display ─────────────────────────
if "upload_selected_path" not in st.session_state:
    st.info("👆 เลือก folder หรือสร้างใหม่ก่อน")
    st.stop()

selected_folder_path = st.session_state.upload_selected_path
selected_folder = selected_folder_path.replace(ROOT_PATH + "/", "").split("/")[0]
st.success(f"📍 Folder ที่เลือก: `{selected_folder_path.replace('/datalake/', 'datalake/')}`")

# Infer + validate table
table_name = infer_table_name(selected_folder)

col_info, col_status = st.columns([3, 1])
with col_info:
    st.markdown(f"**Hive Table:** `{table_name}`" if table_name else "**Hive Table:** ไม่พบ table ที่ตรงกัน")
with col_status:
    if table_name:
        st.success("✅ พบ table")
    else:
        st.error("❌ ไม่พบ table ที่ตรงกับ folder นี้ใน Hive")

if not table_name:
    st.stop()

# แสดง schema ปัจจุบัน
with st.expander("📋 ดู Schema ปัจจุบันของ Table"):
    hive_schema = get_schema(table_name)
    schema_rows = [{"Column": k, "Type": v} for k, v in hive_schema.items()]
    st.dataframe(schema_rows, use_container_width=True, hide_index=True)

st.divider()

# ═══════════════════════════════════════════════════
# STEP 2 — Upload Excel
# ═══════════════════════════════════════════════════
st.header("2️⃣ Upload ไฟล์ Excel")

uploaded_file = st.file_uploader(
    "เลือกไฟล์ Excel (.xlsx)",
    type=["xlsx"],
    help="GPT จะแปลง merge cell และ format ให้เป็น CSV อัตโนมัติ",
)

if not uploaded_file:
    st.stop()

# บันทึกไฟล์ชั่วคราวเพื่ออ่าน sheet names
with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
    tmp.write(uploaded_file.read())
    tmp_path = tmp.name

wb = openpyxl.load_workbook(tmp_path, read_only=True)
sheet_names = wb.sheetnames
wb.close()

selected_sheet = st.selectbox("เลือก Sheet", sheet_names)

# แปลง Excel → DataFrame ด้วย GPT
if st.button("🤖 แปลงด้วย GPT", type="secondary"):
    with st.spinner("🤖 GPT กำลังแปลง Excel → CSV... (อาจใช้เวลาสักครู่)"):
        try:
            df_converted = convert_excel(tmp_path, selected_sheet)
            st.session_state["df_converted"] = df_converted
            st.session_state["original_filename"] = uploaded_file.name
        except Exception as e:
            st.error(f"❌ แปลงไฟล์ไม่สำเร็จ: {e}")
            st.stop()

if "df_converted" not in st.session_state:
    st.stop()

df_converted = st.session_state["df_converted"]
csv_columns = df_converted.columns.tolist()
csv_content = df_converted.to_csv(index=False, encoding="utf-8")

st.success(f"✅ แปลงสำเร็จ — {len(csv_columns)} columns, {len(df_converted)} rows")

# Preview CSV
with st.expander("👁️ Preview CSV (10 rows แรก)"):
    st.dataframe(df_converted.head(10), use_container_width=True)

st.divider()

# ═══════════════════════════════════════════════════
# STEP 3 — Column Mapping
# ═══════════════════════════════════════════════════
st.header("3️⃣ Column Mapping")

hive_schema = get_schema(table_name)
hive_cols = list(hive_schema.keys())
diff = compare_columns(hive_schema, csv_columns)

# แปลชื่อ column เป็นไทยสำหรับแสดงผล
with st.spinner("🤖 กำลังแปลชื่อ column เป็นภาษาไทย..."):
    all_cols = list(set(hive_cols + csv_columns))
    thai_labels = translate_columns_to_thai(all_cols)

def th(col: str) -> str:
    """คืนชื่อไทย (ถ้ามี) พ่วงท้ายด้วยชื่อ column จริง"""
    label = thai_labels.get(col, "")
    return f"{label} ({col})" if label else col

# ❌ Critical missing → BLOCK ก่อนเลย
if diff["critical_missing"]:
    st.error(
        f"❌ ขาด critical columns: {', '.join(f'`{c}`' for c in diff['critical_missing'])}\n\n"
        "ไม่สามารถ upload ได้ เพราะ column เหล่านี้ห้าม null"
    )
    st.stop()

st.markdown("กำหนดว่า column แต่ละตัวใน CSV จะ map กับ column ไหนใน Hive หรือจะสร้างใหม่")

# dropdown options: hive columns + "🆕 สร้าง column ใหม่"
NEW_COL = "🆕 สร้าง column ใหม่"
dropdown_options = hive_cols + [NEW_COL]

# header
hc = st.columns([3, 3, 2])
hc[0].markdown("<small style='color:#888;font-weight:600;text-transform:uppercase'>CSV Column</small>", unsafe_allow_html=True)
hc[1].markdown("<small style='color:#888;font-weight:600;text-transform:uppercase'>Map to Hive Column</small>", unsafe_allow_html=True)
hc[2].markdown("<small style='color:#888;font-weight:600;text-transform:uppercase'>Status</small>", unsafe_allow_html=True)

st.markdown("<hr style='margin:4px 0;border-color:rgba(128,128,128,0.2)'>", unsafe_allow_html=True)

col_mapping = {}

# dropdown แสดงชื่อไทย แต่ value ยังเป็น column จริง
dropdown_display = [th(c) for c in hive_cols] + [NEW_COL]
dropdown_values = hive_cols + [NEW_COL]

for csv_col in csv_columns:
    rc = st.columns([3, 3, 2])

    with rc[0]:
        st.markdown(
            f"<div style='padding:8px 0;font-size:13px'>"
            f"<span style='font-family:monospace'>`{csv_col}`</span><br>"
            f"<small style='color:#888'>{thai_labels.get(csv_col, '')}</small></div>",
            unsafe_allow_html=True,
        )

    with rc[1]:
        default_idx = hive_cols.index(csv_col) if csv_col in hive_cols else len(dropdown_values) - 1
        selected_display = st.selectbox(
            label=csv_col,
            options=dropdown_display,
            index=default_idx,
            key=f"map_{csv_col}",
            label_visibility="collapsed",
        )
        # แปลง display กลับเป็น value จริง
        selected = dropdown_values[dropdown_display.index(selected_display)]
        col_mapping[csv_col] = selected

    with rc[2]:
        if selected == NEW_COL:
            st.markdown("🆕 สร้างใหม่")
        elif selected == csv_col:
            st.markdown("✅ ตรงกัน")
        else:
            st.markdown(f"🔀 → `{selected}`")

# หลัง loop — เช็ค duplicate แบบ real-time
current_targets = [v for v in col_mapping.values() if v != NEW_COL]
dup_now = {t for t, c in Counter(current_targets).items() if c > 1}
if dup_now:
    st.warning(
        "⚠️ Hive column ที่ถูก map ซ้ำ: "
        + ", ".join(f"`{t}`" for t in dup_now)
        + " — กรุณาเปลี่ยนให้ไม่ซ้ำกัน"
    )

# ⚠️ เช็ค duplicate — Hive column เดียวกันถูก map มากกว่า 1 CSV column
mapped_targets = [v for v in col_mapping.values() if v != NEW_COL]
dup_targets = {t for t, count in Counter(mapped_targets).items() if count > 1}

if dup_targets:
    st.error(
        "❌ มี Hive column ที่ถูก map ซ้ำกัน: "
        + ", ".join(f"`{t}`" for t in dup_targets)
        + "\n\nแต่ละ Hive column สามารถรับได้แค่ 1 CSV column เท่านั้น กรุณาแก้ไขก่อน upload"
    )
    can_upload = False
mapped_hive_cols = {v for v in col_mapping.values() if v != NEW_COL}
unmapped_hive_cols = [c for c in hive_cols if c not in mapped_hive_cols]

can_upload = True
if unmapped_hive_cols:
    st.markdown("---")
    st.warning(
        f"⚠️ {len(unmapped_hive_cols)} columns ใน Hive ที่ไม่ได้ถูก map: "
        + ", ".join(f"`{c}`" for c in unmapped_hive_cols)
        + "\n\nจะถูก set เป็น `null`"
    )
    can_upload = st.checkbox(
        "รับทราบและต้องการ upload ต่อ (columns ที่ไม่ได้ map จะเป็น null)",
        value=False,
    )

st.divider()

# ═══════════════════════════════════════════════════
# STEP 4 — Confirm และ Upload
# ═══════════════════════════════════════════════════
st.header("4️⃣ Confirm Upload")

new_cols_to_add = [csv_col for csv_col, target in col_mapping.items() if target == NEW_COL]
remapped_cols = {csv_col: target for csv_col, target in col_mapping.items()
                 if target != NEW_COL and target != csv_col}

st.markdown(f"""
| | |
|---|---|
| **ไฟล์** | `{st.session_state.get('original_filename', '')}` |
| **Folder** | `{selected_folder_path}/` |
| **Table** | `{table_name}` |
| **Matched** | {len([v for v in col_mapping.values() if v != NEW_COL])} columns |
| **Remap** | {len(remapped_cols)} columns |
| **สร้างใหม่ (ALTER TABLE)** | {len(new_cols_to_add)} columns |
| **Null** | {len(unmapped_hive_cols)} columns |
""")

hdfs_filename = st.session_state.get("original_filename", "data.xlsx").replace(".xlsx", ".csv")

# Sanitize ชื่อไฟล์ — ถ้ามีภาษาไทยให้ใช้ชื่อจาก table + timestamp แทน

def sanitize_filename(name: str) -> str:
    has_thai = any('\u0e00' <= c <= '\u0e7f' for c in name)
    if has_thai:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{table_name}_{timestamp}.csv"
    # ถ้าไม่มีไทย แค่แทน space และอักขระพิเศษ
    name = re.sub(r'[^\w\-.]', '_', name)
    return name

hdfs_filename = sanitize_filename(hdfs_filename)
st.markdown(f"**ชื่อไฟล์ใน HDFS:** `{hdfs_filename}`")
hdfs_dest = f"{selected_folder_path}/{hdfs_filename}"
st.markdown(f"**Destination:** `{hdfs_dest}`")

if not can_upload and unmapped_hive_cols:
    st.info("กรุณา confirm ด้านบนก่อนถึงจะ upload ได้")
    st.stop()

if st.button("🚀 Upload เข้า HDFS", type="primary", use_container_width=True):
    # Rename columns ตาม mapping ที่ user เลือก
    rename_map = {csv_col: target for csv_col, target in col_mapping.items()
                  if target != NEW_COL and target != csv_col}
    df_upload = df_converted.rename(columns=rename_map)

    # Upload CSV เข้า HDFS — ไม่ ALTER TABLE ที่นี่
    # pipeline จะ detect column ใหม่และ ALTER TABLE เองตอนรัน
    with st.spinner("📤 กำลัง upload เข้า HDFS..."):
        try:
            csv_bytes = df_upload.to_csv(index=False, encoding="utf-8").encode("utf-8")
            upload_csv_to_hdfs(csv_bytes, hdfs_dest)
            st.success(f"✅ Upload สำเร็จ: `{hdfs_dest}`")
            if new_cols_to_add:
                st.info(
                    "ℹ️ Pipeline จะ ALTER TABLE เพิ่ม columns อัตโนมัติเมื่อรัน: "
                    + ", ".join(f"`{c}`" for c in new_cols_to_add)
                )
            st.balloons()
        except Exception as e:
            st.error(f"❌ Upload ล้มเหลว: {e}")
            st.stop()

    st.info("💡 Pipeline จะรันตาม schedule ของ Airflow เพื่อ process ไฟล์นี้")