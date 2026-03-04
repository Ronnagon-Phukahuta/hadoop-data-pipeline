# dashboard/services/schema_service.py
"""
Schema Service — ดึง schema จาก Hive โดยตรง
"""

import os
import streamlit as st
from pyhive import hive
from openai import OpenAI
import json

HIVE_HOST = os.environ.get("HIVE_HOST", "hive-server")
HIVE_PORT = int(os.environ.get("HIVE_PORT", 10000))

CRITICAL_COLUMNS = {"date", "details"}


def _get_conn():
    return hive.connect(host=HIVE_HOST, port=HIVE_PORT, database="default")


def get_tables() -> list[str]:
    """SHOW TABLES — คืน list ชื่อ table ทั้งหมดใน Hive"""
    with _get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        return [row[0] for row in cursor.fetchall()]


def get_schema(table_name: str) -> dict[str, str]:
    """
    DESCRIBE TABLE — คืน dict {column_name: data_type}
    เช่น {"date": "string", "details": "string", "amount": "double"}
    """
    with _get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE `{table_name}`")
        rows = cursor.fetchall()

    schema = {}
    for row in rows:
        col_name = row[0].strip()
        col_type = row[1].strip() if row[1] else ""
        # หยุดเมื่อเจอ partition info หรือ empty row
        if not col_name or col_name.startswith("#"):
            break
        schema[col_name] = col_type
    return schema


def infer_table_name(folder_name: str) -> str | None:
    """
    แปลงชื่อ folder เป็น table name โดย match กับ _wide table
    เช่น finance_itsc → finance_itsc_wide
    คืน None ถ้าหา table ไม่เจอ
    """
    base = folder_name.replace("-", "_").replace(" ", "_").lower()
    tables = get_tables()

    # หา table ที่ขึ้นต้นด้วย base และลงท้ายด้วย _wide
    wide_table = f"{base}_wide"
    if wide_table in tables:
        return wide_table

    # fallback: หา table ที่ขึ้นต้นด้วย base ตัวไหนก็ได้
    matches = [t for t in tables if t.startswith(base)]
    return matches[0] if matches else None


def validate_table_exists(table_name: str) -> bool:
    """เช็คว่า table มีอยู่ใน Hive จริงไหม"""
    return table_name in get_tables()

_openai_client = None

def _get_openai():
    global _openai_client
    if _openai_client is None:
        _openai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    return _openai_client


@st.cache_data(ttl=3600)
def translate_columns_to_thai(columns: list[str]) -> dict[str, str]:
    """
    แปลงชื่อ column snake_case เป็นภาษาไทยสำหรับแสดงผลใน UI
    เช่น general_fund_admin_wifi_grant → งบทั่วไป (WiFi ฝ่ายบริหาร)
    คืน dict {col_name: thai_label}
    """
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
    """
    เปรียบเทียบ column ระหว่าง Hive schema กับ CSV ที่ได้จาก GPT

    คืน:
    {
        "matched":   ["date", "details", ...],   # ตรงกันทั้งสองฝั่ง
        "new":       ["col_d"],                   # มีใน CSV แต่ไม่มีใน Hive → ALTER TABLE
        "missing":   ["col_c"],                   # มีใน Hive แต่ไม่มีใน CSV → set null
        "critical_missing": ["date"],             # missing ที่เป็น critical → block
    }
    """
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