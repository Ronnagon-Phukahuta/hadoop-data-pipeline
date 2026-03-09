# dashboard/pages/upload.py
"""
Upload — Excel → GPT → CSV → Column Mapping → HDFS

Changes:
- generate_dataset_yaml() สร้างแค่ slim yaml (pipeline config only)
- หลัง upload สำเร็จ → write_column_metadata() + write_nlp_config() เข้า Hive
- dataset เดิม: ถ้า remap columns → update notes ใน column_metadata
- dataset ใหม่: write_column_metadata จาก GPT-translated thai names
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
    dataset_yaml_exists,
    generate_dataset_yaml,
)
from services.hdfs_upload import upload_csv_to_hdfs
from services.excel_service import convert_excel
from services.hive_metadata import (
    get_column_metadata,
    write_column_metadata,
    write_nlp_config,
    ColumnMeta,
)

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


# ── Path bar + Breadcrumb + New Folder (inline) ──────────────
relative = browse_path.replace(ROOT_PATH, "") or "/"
rel_parts = [p for p in relative.strip("/").split("/") if p]

crumb_html = ""
all_parts = ["raw"] + rel_parts
for i, part in enumerate(all_parts):
    if i < len(all_parts) - 1:
        crumb_html += (
            f"<span style='color:#64748B'>{part}</span>"
            f"<span style='color:#334155;margin:0 8px'>/</span>"
        )
    else:
        crumb_html += f"<span style='color:#F1F5F9;font-weight:600'>{part}</span>"

col_path, col_new = st.columns([5, 1])
with col_path:
    st.markdown(
        f"<div style='background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);"
        f"border-radius:8px;padding:10px 16px;font-family:monospace;font-size:13px;line-height:1.8'>"
        f"{crumb_html}</div>",
        unsafe_allow_html=True,
    )
with col_new:
    with st.popover("＋ Folder", use_container_width=True):
        new_folder_name = st.text_input(
            "ชื่อ folder", placeholder="เช่น year=2026", key="new_folder_input"
        )
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

# ── Back navigation ───────────────────────────────────────────
if rel_parts:
    nav_cols = st.columns(min(len(rel_parts) + 1, 6))
    for i, part in enumerate(all_parts[:-1]):
        target = ROOT_PATH if i == 0 else ROOT_PATH + "/" + "/".join(rel_parts[:i])
        with nav_cols[i]:
            if st.button(f"↑ {part}", key=f"ubc_{i}", use_container_width=True):
                st.session_state.upload_browse_path = target
                st.session_state.pop("upload_selected_path", None)
                st.rerun()
    st.markdown("")

# ── Subfolder grid ────────────────────────────────────────────
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
    st.markdown(
        "<div style='margin:8px 0;padding:14px;text-align:center;color:#475569;"
        "border:1px dashed rgba(255,255,255,0.08);border-radius:8px;font-size:13px'>"
        "ไม่มี subfolder</div>",
        unsafe_allow_html=True,
    )

st.markdown("")

if st.button("✅ เลือก folder นี้เป็นปลายทาง", use_container_width=True):
    st.session_state.upload_selected_path = browse_path

if "upload_selected_path" not in st.session_state:
    st.info("👆 เลือก folder หรือสร้างใหม่ก่อน")
    st.stop()

selected_folder_path = st.session_state.upload_selected_path
selected_folder = selected_folder_path.replace(ROOT_PATH + "/", "").split("/")[0]
st.success(f"📍 Folder ที่เลือก: `{selected_folder_path.replace('/datalake/', 'datalake/')}`")

# ── Infer dataset name และ table ──────────────────────────────
dataset_name = selected_folder.replace("-", "_").replace(" ", "_").lower()
table_name = infer_table_name(selected_folder)

col_info, col_status = st.columns([3, 1])
with col_info:
    st.markdown(f"**Dataset:** `{dataset_name}`")
    if table_name:
        st.markdown(f"**Hive Table:** `{table_name}`")
    else:
        st.markdown("**Hive Table:** ยังไม่มี table")
with col_status:
    if table_name:
        st.success("✅ พบ table")
    else:
        st.warning("🆕 Dataset ใหม่")

# ═══════════════════════════════════════════════════
# NEW DATASET FLOW
# ═══════════════════════════════════════════════════
if not table_name:
    st.divider()
    st.subheader("🆕 Dataset ใหม่ตรวจพบ")
    st.info(
        f"ยังไม่มี Hive table สำหรับ **`{dataset_name}`**\n\n"
        "ถ้าสร้าง dataset ใหม่ ระบบจะ:\n"
        f"- สร้างไฟล์ `{dataset_name}.yaml` อัตโนมัติ\n"
        "- บันทึก column metadata เข้า Hive\n"
        "- Pipeline จะสร้าง Hive table ให้ตอนรันครั้งแรก"
    )

    if dataset_yaml_exists(dataset_name):
        st.success(f"✅ พบไฟล์ `{dataset_name}.yaml` แล้ว — ดำเนินการต่อได้เลย")
        table_name = f"{dataset_name}_wide"
        is_new_dataset = True
    else:
        col_yes, col_no = st.columns(2)
        with col_yes:
            if st.button("✅ สร้าง Dataset ใหม่", type="primary", use_container_width=True):
                st.session_state["create_new_dataset"] = True
                st.rerun()
        with col_no:
            if st.button("❌ ยกเลิก", use_container_width=True):
                st.session_state.pop("upload_selected_path", None)
                st.session_state.pop("create_new_dataset", None)
                st.rerun()

        if not st.session_state.get("create_new_dataset"):
            st.stop()

        st.info("📋 Upload Excel ก่อนเพื่อ detect columns แล้วระบบจะสร้าง yaml และ metadata ให้อัตโนมัติ")
        table_name = f"{dataset_name}_wide"
        is_new_dataset = True
else:
    is_new_dataset = False

# แสดง schema ปัจจุบัน (เฉพาะ dataset เดิม)
if not is_new_dataset:
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

with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
    tmp.write(uploaded_file.read())
    tmp_path = tmp.name

wb = openpyxl.load_workbook(tmp_path, read_only=True)
sheet_names = wb.sheetnames
wb.close()

selected_sheet = st.selectbox("เลือก Sheet", sheet_names)

if st.button("🤖 แปลงด้วย GPT", type="secondary"):
    with st.spinner("🤖 GPT กำลังแปลง Excel → CSV... (อาจใช้เวลาสักครู่)"):
        try:
            df_converted = convert_excel(tmp_path, selected_sheet)
            st.session_state["df_converted"] = df_converted
            st.session_state["original_filename"] = uploaded_file.name
        except Exception as e:
            import traceback
            st.error(f"❌ แปลงไฟล์ไม่สำเร็จ: {e}")
            st.code(traceback.format_exc())
            st.stop()

if "df_converted" not in st.session_state:
    st.stop()

df_converted = st.session_state["df_converted"]
csv_columns = df_converted.columns.tolist()

st.success(f"✅ แปลงสำเร็จ — {len(csv_columns)} columns, {len(df_converted)} rows")

with st.expander("👁️ Preview CSV (10 rows แรก)"):
    st.dataframe(df_converted.head(10), use_container_width=True)

# ── Auto-generate yaml (slim) + write Hive metadata ──────────
if is_new_dataset and not dataset_yaml_exists(dataset_name):
    with st.spinner(f"⚙️ กำลังสร้าง `{dataset_name}.yaml` และ column metadata..."):
        # 1) สร้าง slim yaml
        try:
            yaml_path = generate_dataset_yaml(dataset_name, selected_folder_path, csv_columns)
            st.success(f"✅ สร้าง `{yaml_path.name}` สำเร็จ")
        except Exception as e:
            st.error(f"❌ สร้าง yaml ไม่สำเร็จ: {e}")
            st.stop()

        # 2) แปลชื่อ column เป็นภาษาไทยด้วย GPT
        try:
            thai_map = translate_columns_to_thai(csv_columns)
        except Exception:
            thai_map = {}

        # 3) สร้าง ColumnMeta list แล้ว write เข้า Hive
        id_cols_set = {"date", "details", "year"}
        col_metas = [
            ColumnMeta(
                col_name=c,
                col_type="STRING" if c in {"date", "details"} else "DOUBLE",
                thai_name=thai_map.get(c, c),
                description="",
                is_amount=c not in id_cols_set,
                is_id=c in id_cols_set,
                is_date=c == "date",
                nullable=True,
                reserved_keyword=c == "date",
            )
            for c in csv_columns
            if c != "year"  # year เป็น partition col
        ]

        try:
            write_column_metadata(dataset_name, col_metas)
            write_nlp_config(
                dataset_name,
                rules=[
                    "`date` เป็น reserved keyword ต้องใส่ backtick ทุกครั้ง",
                    "'budget' และ 'spent' → SUM ได้ปกติ",
                    "'remaining' → คือ running balance ห้าม SUM เด็ดขาด",
                ],
                examples=[],
            )
            st.success("✅ บันทึก column metadata เข้า Hive สำเร็จ")
        except Exception as e:
            st.warning(
                f"⚠️ บันทึก metadata ไม่สำเร็จ: {e}\n\n"
                "Pipeline ยังทำงานได้ตามปกติ แต่ควรรัน migrate_yaml_to_hive.py ภายหลัง"
            )

st.divider()

# ═══════════════════════════════════════════════════
# STEP 3 — Column Mapping
# ═══════════════════════════════════════════════════
st.header("3️⃣ Column Mapping")

if is_new_dataset:
    hive_schema = {}
    hive_cols = []
    diff = {
        "matched": [],
        "new": csv_columns,
        "missing": [],
        "critical_missing": [],
    }
else:
    hive_schema = get_schema(table_name)
    hive_cols = list(hive_schema.keys())
    diff = compare_columns(hive_schema, csv_columns)

with st.spinner("🤖 กำลังแปลชื่อ column เป็นภาษาไทย..."):
    all_cols = list(set(hive_cols + csv_columns))
    thai_labels = translate_columns_to_thai(all_cols)


def th(c: str) -> str:
    label = thai_labels.get(c, "")
    return f"{label} ({c})" if label else c


if diff["critical_missing"]:
    st.error(
        f"❌ ขาด critical columns: {', '.join(f'`{c}`' for c in diff['critical_missing'])}\n\n"
        "ไม่สามารถ upload ได้ เพราะ column เหล่านี้ห้าม null"
    )
    st.stop()

if is_new_dataset:
    st.info("🆕 Dataset ใหม่ — ทุก column จะถูก map เป็น column ใหม่ใน Hive table ที่จะสร้างขึ้น")
else:
    st.markdown("กำหนดว่า column แต่ละตัวใน CSV จะ map กับ column ไหนใน Hive หรือจะสร้างใหม่")

NEW_COL = "🆕 สร้าง column ใหม่"

hc = st.columns([3, 3, 2])
hc[0].markdown(
    "<small style='color:#888;font-weight:600;text-transform:uppercase'>CSV Column</small>",
    unsafe_allow_html=True,
)
hc[1].markdown(
    "<small style='color:#888;font-weight:600;text-transform:uppercase'>Map to Hive Column</small>",
    unsafe_allow_html=True,
)
hc[2].markdown(
    "<small style='color:#888;font-weight:600;text-transform:uppercase'>Status</small>",
    unsafe_allow_html=True,
)
st.markdown(
    "<hr style='margin:4px 0;border-color:rgba(128,128,128,0.2)'>",
    unsafe_allow_html=True,
)

col_mapping = {}
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
        if is_new_dataset:
            st.markdown(
                "<div style='padding:8px 0;font-size:13px;color:#94A3B8'>"
                "<em>column ใหม่</em></div>",
                unsafe_allow_html=True,
            )
            selected = NEW_COL
        else:
            default_idx = (
                hive_cols.index(csv_col) if csv_col in hive_cols
                else len(dropdown_values) - 1
            )
            selected_display = st.selectbox(
                label=csv_col,
                options=dropdown_display,
                index=default_idx,
                key=f"map_{csv_col}",
                label_visibility="collapsed",
            )
            selected = dropdown_values[dropdown_display.index(selected_display)]
        col_mapping[csv_col] = selected

    with rc[2]:
        if selected == NEW_COL:
            st.markdown("🆕 สร้างใหม่")
        elif selected == csv_col:
            st.markdown("✅ ตรงกัน")
        else:
            st.markdown(f"🔀 → `{selected}`")

# เช็ค duplicate
if not is_new_dataset:
    current_targets = [v for v in col_mapping.values() if v != NEW_COL]
    dup_now = {t for t, c in Counter(current_targets).items() if c > 1}
    if dup_now:
        st.warning(
            "⚠️ Hive column ที่ถูก map ซ้ำ: "
            + ", ".join(f"`{t}`" for t in dup_now)
            + " — กรุณาเปลี่ยนให้ไม่ซ้ำกัน"
        )

    mapped_targets = [v for v in col_mapping.values() if v != NEW_COL]
    dup_targets = {t for t, count in Counter(mapped_targets).items() if count > 1}
    if dup_targets:
        st.error(
            "❌ มี Hive column ที่ถูก map ซ้ำกัน: "
            + ", ".join(f"`{t}`" for t in dup_targets)
            + "\n\nกรุณาแก้ไขก่อน upload"
        )

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
remapped_cols = {
    csv_col: target for csv_col, target in col_mapping.items()
    if target != NEW_COL and target != csv_col
}

st.markdown(f"""
| | |
|---|---|
| **ไฟล์** | `{st.session_state.get('original_filename', '')}` |
| **Folder** | `{selected_folder_path}/` |
| **Dataset** | `{dataset_name}` {'🆕 ใหม่' if is_new_dataset else ''} |
| **Table** | `{table_name}` |
| **Matched** | {len([v for v in col_mapping.values() if v != NEW_COL])} columns |
| **Remap** | {len(remapped_cols)} columns |
| **สร้างใหม่** | {len(new_cols_to_add)} columns |
| **Null** | {len(unmapped_hive_cols)} columns |
""")

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
original_filename = st.session_state.get("original_filename", "data.xlsx")


def sanitize_filename(name: str, ext: str) -> str:
    has_thai = any("\u0e00" <= c <= "\u0e7f" for c in name)
    base = (
        f"{dataset_name}_{timestamp}" if has_thai
        else re.sub(r"[^\w\-]", "_", name.rsplit(".", 1)[0])
    )
    return f"{base}.{ext}"


csv_filename  = sanitize_filename(original_filename, "csv")
xlsx_filename  = sanitize_filename(original_filename, "xlsx")
hdfs_dest_csv  = f"{selected_folder_path}/{csv_filename}"
hdfs_dest_orig = f"/datalake/original/{dataset_name}/{xlsx_filename}"

st.markdown(
    f"<div style='background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);"
    f"border-radius:8px;padding:14px 18px;font-size:13px;line-height:2'>"
    f"<div><span style='color:#64748B'>CSV (pipeline)&nbsp;&nbsp;</span>"
    f"<code style='color:#7DD3FC'>{hdfs_dest_csv}</code></div>"
    f"<div><span style='color:#64748B'>Original (xlsx)&nbsp;</span>"
    f"<code style='color:#86EFAC'>{hdfs_dest_orig}</code></div>"
    f"</div>",
    unsafe_allow_html=True,
)

if not can_upload and unmapped_hive_cols:
    st.info("กรุณา confirm ด้านบนก่อนถึงจะ upload ได้")
    st.stop()

if st.button("🚀 Upload เข้า HDFS", type="primary", use_container_width=True):
    rename_map = {
        csv_col: target for csv_col, target in col_mapping.items()
        if target != NEW_COL and target != csv_col
    }
    df_upload = df_converted.rename(columns=rename_map)

    with st.spinner("📤 กำลัง upload เข้า HDFS..."):

        # 1) Original .xlsx → /datalake/original/
        try:
            with open(tmp_path, "rb") as f:
                xlsx_bytes = f.read()
            upload_csv_to_hdfs(xlsx_bytes, hdfs_dest_orig)
            st.success(f"✅ Original saved: `{hdfs_dest_orig}`")
        except Exception as e:
            st.warning(f"⚠️ บันทึก original ไม่สำเร็จ: {e}")

        # 2) Converted .csv → /datalake/raw/
        try:
            csv_bytes = df_upload.to_csv(index=False, encoding="utf-8").encode("utf-8")
            upload_csv_to_hdfs(csv_bytes, hdfs_dest_csv)
            st.success(f"✅ CSV uploaded: `{hdfs_dest_csv}`")
        except Exception as e:
            st.error(f"❌ Upload CSV ล้มเหลว: {e}")
            st.stop()

        # 3) Update column_metadata ใน Hive (dataset เดิมที่มี remap)
        if not is_new_dataset and remapped_cols:
            try:
                existing_metas = get_column_metadata(dataset_name)
                meta_dict = {m.col_name: m for m in existing_metas}
                for csv_col, hive_col in remapped_cols.items():
                    if hive_col in meta_dict:
                        # บันทึก remap history ไว้ใน notes
                        existing_note = meta_dict[hive_col].notes or ""
                        meta_dict[hive_col].notes = (
                            f"remapped from: {csv_col} "
                            f"(uploaded {timestamp})"
                            + (f" | {existing_note}" if existing_note else "")
                        )
                write_column_metadata(dataset_name, list(meta_dict.values()))
                st.success("✅ อัปเดต column metadata สำเร็จ")
            except Exception as e:
                st.warning(f"⚠️ อัปเดต column metadata ไม่สำเร็จ: {e}")

        # 4) เพิ่ม new columns เข้า column_metadata (dataset เดิมที่เพิ่ม column ใหม่)
        if not is_new_dataset and new_cols_to_add:
            try:
                existing_metas = get_column_metadata(dataset_name)
                existing_names = {m.col_name for m in existing_metas}
                id_cols_set = {"date", "details", "year"}
                new_metas = [
                    ColumnMeta(
                        col_name=c,
                        col_type="DOUBLE",
                        thai_name=thai_labels.get(c, c),
                        description="(new column added via upload)",
                        is_amount=c not in id_cols_set,
                        is_id=c in id_cols_set,
                        is_date=False,
                        nullable=True,
                        reserved_keyword=False,
                    )
                    for c in new_cols_to_add
                    if c not in existing_names
                ]
                if new_metas:
                    write_column_metadata(dataset_name, existing_metas + new_metas)
                    st.success(
                        f"✅ เพิ่ม {len(new_metas)} column ใหม่เข้า metadata: "
                        + ", ".join(f"`{m.col_name}`" for m in new_metas)
                    )
            except Exception as e:
                st.warning(f"⚠️ เพิ่ม column metadata ไม่สำเร็จ: {e}")

        # ── Messages ──────────────────────────────────────────
        if is_new_dataset:
            st.info(
                f"ℹ️ Pipeline จะสร้าง Hive table `{dataset_name}_wide` และ `{dataset_name}_long` "
                "อัตโนมัติเมื่อรันครั้งแรก"
            )
        elif new_cols_to_add:
            st.info(
                "ℹ️ Pipeline จะ ALTER TABLE เพิ่ม columns อัตโนมัติเมื่อรัน: "
                + ", ".join(f"`{c}`" for c in new_cols_to_add)
            )

        st.balloons()
        st.session_state.pop("create_new_dataset", None)

    st.info("💡 Pipeline จะรันตาม schedule ของ Airflow เพื่อ process ไฟล์นี้")