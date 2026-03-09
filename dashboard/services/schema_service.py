# dashboard/services/schema_service.py
"""
Schema Service — ดึง schema จาก Hive โดยตรง
"""

import os
import streamlit as st
from pyhive import hive
from openai import OpenAI
import json
import yaml
from pathlib import Path

HIVE_HOST = os.environ.get("HIVE_HOST", "hive-server")
HIVE_PORT = int(os.environ.get("HIVE_PORT", 10000))
DATASETS_DIR = Path(os.environ.get("DATASETS_DIR", "/jobs/datasets"))

CRITICAL_COLUMNS = {"date", "details"}


def _get_conn():
    return hive.connect(host=HIVE_HOST, port=HIVE_PORT, database="default")


def get_tables() -> list[str]:
    with _get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        return [row[0] for row in cursor.fetchall()]


def get_schema(table_name: str) -> dict[str, str]:
    with _get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE `{table_name}`")
        rows = cursor.fetchall()

    schema = {}
    for row in rows:
        col_name = row[0].strip()
        col_type = row[1].strip() if row[1] else ""
        if not col_name or col_name.startswith("#"):
            break
        schema[col_name] = col_type
    return schema


def infer_table_name(folder_name: str) -> str | None:
    base = folder_name.replace("-", "_").replace(" ", "_").lower()
    tables = get_tables()
    wide_table = f"{base}_wide"
    if wide_table in tables:
        return wide_table
    matches = [t for t in tables if t.startswith(base)]
    return matches[0] if matches else None


def validate_table_exists(table_name: str) -> bool:
    return table_name in get_tables()


def dataset_yaml_exists(dataset_name: str) -> bool:
    return (DATASETS_DIR / f"{dataset_name}.yaml").exists()


def generate_dataset_yaml(
    dataset_name: str,
    folder_path: str,
    csv_columns: list[str],
) -> Path:
    """
    Auto-generate {dataset_name}.yaml จาก folder path และ CSV columns
    detect date/details column จากชื่อจริง ไม่ hardcode
    """
    raw_base = f"/datalake/raw/{dataset_name}"

    # detect date column — ชื่อ "date" exact ก่อน
    date_col = next(
        (c for c in csv_columns if c == "date"),
        None
    )

    # detect details column — ชื่อ "details" exact ก่อน
    details_col = next(
        (c for c in csv_columns if c == "details"),
        None
    )

    # id_cols เฉพาะที่มีจริงใน csv
    id_cols = [c for c in [date_col, details_col, "year"] if c and c in csv_columns + ["year"]]
    if not id_cols:
        id_cols = ["year"]

    exclude_cols = ["total_amount"] if "total_amount" in csv_columns else []

    # metadata columns ที่ไม่ใช่ amount
    metadata_patterns = {
        "edoc", "dataowner", "doc_number", "document_number",
        "fund_type", "expense_category", "budget_plan", "carry_over"
    }
    amount_cols = [
        c for c in csv_columns
        if c not in id_cols
        and c not in exclude_cols
        and c != "year"
        and not any(c.startswith(p) for p in metadata_patterns)
    ]

    critical_cols = [c for c in [date_col, details_col] if c]
    required_cols = critical_cols + (["total_amount"] if "total_amount" in csv_columns else [])

    def _infer_type(c: str) -> str:
        if c == "year":
            return "INT"
        if c in {date_col, details_col}:
            return "STRING"
        return "DOUBLE"

    schema = [
        {"name": c, "type": _infer_type(c), "thai_name": c, "description": ""}
        for c in csv_columns
        if c != "year"
    ]

    data = {
        "dataset": dataset_name,
        "owner": "auto-generated",
        "paths": {
            "raw":      raw_base,
            "original": f"/datalake/original/{dataset_name}",
            "staging":  f"/datalake/staging/{dataset_name}_wide",
            "curated":  f"/datalake/curated/{dataset_name}_long",
            "versions": f"/datalake/versions/{dataset_name}",
            "trash":    "/datalake/trash",
        },
        "tables": {
            "database": "default",
            "staging":  f"{dataset_name}_wide",
            "curated":  f"{dataset_name}_long",
        },
        "pipeline": {
            "critical_columns": critical_cols,
            "required_columns": required_cols,
            "partition_by":     "year",
            "id_columns":       id_cols,
            "exclude_columns":  exclude_cols,
            "date_column":      date_col,
            "amount_columns":   amount_cols,
        },
        "schema": schema,
        "category_mapping": {},
        "nlp_rules": [
            "`date` เป็น reserved keyword ต้องใส่ backtick ทุกครั้ง",
            "'budget' และ 'spent' → SUM ได้ปกติ",
            "'remaining' → คือ running balance ห้าม SUM เด็ดขาด",
        ],
        "example_queries": [],
    }

    yaml_path = DATASETS_DIR / f"{dataset_name}.yaml"
    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    with yaml_path.open("w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, sort_keys=False, default_flow_style=False)

    return yaml_path


_openai_client = None


def _get_openai():
    global _openai_client
    if _openai_client is None:
        _openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    return _openai_client


@st.cache_data(ttl=3600)
def translate_columns_to_thai(columns: list[str]) -> dict[str, str]:
    client = _get_openai()
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": """แปลงชื่อ column ภาษาอังกฤษแบบ snake_case เป็นชื่อภาษาไทยที่เข้าใจง่าย
สำหรับระบบงบประมาณของมหาวิทยาลัย

กฎ:
1. ตอบเป็น JSON เท่านั้น format: {"col_name": "ชื่อไทย", ...}
2. ชื่อไทยต้องสั้น กระชับ เข้าใจง่าย ไม่เกิน 30 ตัวอักษร
3. ถ้าเป็นชื่อระบบหรือคำเฉพาะ ให้ทับศัพท์ได้ เช่น WiFi, Cloud
4. ไม่ต้องอธิบาย ตอบ JSON อย่างเดียว"""
            },
            {
                "role": "user",
                "content": f"แปลงชื่อ columns เหล่านี้:\n{json.dumps(columns, ensure_ascii=False)}"
            }
        ]
    )
    result = response.choices[0].message.content.strip()
    if result.startswith("```"):
        result = "\n".join(line for line in result.split("\n") if not line.startswith("```")).strip()
    return json.loads(result)


def compare_columns(
    hive_schema: dict[str, str],
    csv_columns: list[str],
) -> dict:
    hive_cols = set(hive_schema.keys())
    csv_cols = set(csv_columns)
    matched = hive_cols & csv_cols
    new_cols = csv_cols - hive_cols
    missing = hive_cols - csv_cols
    critical_missing = missing & CRITICAL_COLUMNS
    return {
        "matched": sorted(matched),
        "new": sorted(new_cols),
        "missing": sorted(missing),
        "critical_missing": sorted(critical_missing),
    }