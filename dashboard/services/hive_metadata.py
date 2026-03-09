# -*- coding: utf-8 -*-
"""
dashboard/services/hive_metadata.py
------------------------------------
Read layer สำหรับ Hive metadata tables:
  - column_metadata
  - category_mapping_meta
  - nlp_config

ใช้แทนการอ่าน yaml schema/mapping/nlp โดยตรง
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import streamlit as st
from pyhive import hive

HIVE_HOST = os.environ.get("HIVE_HOST", "hive-server")
HIVE_PORT = int(os.environ.get("HIVE_PORT", 10000))


def _conn():
    return hive.connect(host=HIVE_HOST, port=HIVE_PORT, database="default")


# ── Dataclasses ──────────────────────────────────────────────

@dataclass
class ColumnMeta:
    col_name: str
    col_type: str
    thai_name: str = ""
    description: str = ""
    is_amount: bool = False
    is_id: bool = False
    is_date: bool = False
    nullable: bool = True
    reserved_keyword: bool = False
    allowed_values: List[str] = field(default_factory=list)
    notes: str = ""


@dataclass
class DatasetMeta:
    dataset_name: str
    columns: List[ColumnMeta] = field(default_factory=list)
    category_mapping: Dict[str, List[str]] = field(default_factory=dict)
    nlp_rules: List[str] = field(default_factory=list)
    example_queries: List[Dict] = field(default_factory=list)

    # ── Convenience ─────────────────────────────────────────

    @property
    def amount_columns(self) -> List[str]:
        return [c.col_name for c in self.columns if c.is_amount]

    @property
    def id_columns(self) -> List[str]:
        return [c.col_name for c in self.columns if c.is_id]

    @property
    def date_column(self) -> Optional[str]:
        cols = [c.col_name for c in self.columns if c.is_date]
        return cols[0] if cols else None

    @property
    def schema_dict(self) -> Dict[str, str]:
        """คืน {col_name: col_type} — ใช้แทน get_schema()"""
        return {c.col_name: c.col_type for c in self.columns}

    def col(self, name: str) -> Optional[ColumnMeta]:
        return next((c for c in self.columns if c.col_name == name), None)


# ── Readers ──────────────────────────────────────────────────

def _decode(s: str) -> str:
    """decode __SQ__ กลับเป็น single quote"""
    return (s or "").replace("__SQ__", "'")


def get_column_metadata(dataset_name: str) -> List[ColumnMeta]:
    """อ่าน column_metadata จาก Hive"""
    try:
        with _conn() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT col_name, col_type, thai_name, description,
                       is_amount, is_id, is_date, nullable,
                       reserved_keyword, allowed_values, notes
                FROM column_metadata
                WHERE ds = '{dataset_name}'
                ORDER BY col_name
            """)
            rows = cursor.fetchall()

        result = []
        for row in rows:
            try:
                allowed = json.loads(row[9]) if row[9] else []
            except Exception:
                allowed = []
            result.append(ColumnMeta(
                col_name=row[0],
                col_type=row[1] or "STRING",
                thai_name=_decode(row[2]) or row[0],
                description=_decode(row[3]),
                is_amount=bool(row[4]),
                is_id=bool(row[5]),
                is_date=bool(row[6]),
                nullable=bool(row[7]) if row[7] is not None else True,
                reserved_keyword=bool(row[8]),
                allowed_values=allowed,
                notes=_decode(row[10]),
            ))
        return result
    except Exception:
        # fallback gracefully — คืน empty list
        return []


@st.cache_data(ttl=300)
def get_category_mapping(dataset_name: str) -> Dict[str, List[str]]:
    """อ่าน category_mapping_meta จาก Hive → {col_name: [alias...]}"""
    try:
        with _conn() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT col_name, thai_alias
                FROM category_mapping_meta
                WHERE dataset_name = '{dataset_name}'
                ORDER BY col_name
            """)
            rows = cursor.fetchall()

        result: Dict[str, List[str]] = {}
        for col_name, alias in rows:
            result.setdefault(col_name, []).append(_decode(alias))
        return result
    except Exception:
        return {}


@st.cache_data(ttl=300)
def get_nlp_config(dataset_name: str) -> tuple[List[str], List[Dict]]:
    """
    อ่าน nlp_config จาก Hive
    Returns: (rules: List[str], examples: List[{q, sql}])
    """
    try:
        with _conn() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT config_type, content, sort_order
                FROM nlp_config
                WHERE dataset_name = '{dataset_name}'
                ORDER BY config_type, sort_order
            """)
            rows = cursor.fetchall()

        rules, examples = [], []
        for config_type, content, _ in rows:
            content = _decode(content)
            if config_type == "rule":
                rules.append(content)
            elif config_type == "example":
                try:
                    examples.append(json.loads(content))
                except Exception:
                    pass
        return rules, examples
    except Exception:
        return [], []


def get_dataset_meta(dataset_name: str) -> DatasetMeta:
    """
    รวม read ทั้ง 3 tables เป็น DatasetMeta เดียว
    ใช้ใน registry.py และ chat.py
    """
    columns = get_column_metadata(dataset_name)
    cat_map = get_category_mapping(dataset_name)
    rules, examples = get_nlp_config(dataset_name)

    return DatasetMeta(
        dataset_name=dataset_name,
        columns=columns,
        category_mapping=cat_map,
        nlp_rules=rules,
        example_queries=examples,
    )


# ── Writers ──────────────────────────────────────────────────

def write_column_metadata(dataset_name: str, columns: List[ColumnMeta]) -> None:
    """
    Upsert column_metadata สำหรับ dataset นี้
    ใช้ตอน upload dataset ใหม่ หรือแก้ thai_name
    """
    from datetime import datetime
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with _conn() as conn:
        cursor = conn.cursor()
        # ลบ rows เดิมของ dataset นี้
        cursor.execute(
            f"ALTER TABLE column_metadata DROP IF EXISTS PARTITION (ds='{dataset_name}')"
        )
        for col in columns:
            allowed = json.dumps(col.allowed_values, ensure_ascii=False).replace("'", "__SQ__")
            notes = (col.notes or "").replace("'", "__SQ__").replace("\n", " ")
            desc = (col.description or "").replace("'", "__SQ__")
            thai = (col.thai_name or col.col_name).replace("'", "__SQ__")
            cursor.execute(f"""
                INSERT INTO column_metadata PARTITION (ds='{dataset_name}') VALUES (
                  '{col.col_name}', '{col.col_type}',
                  '{thai}', '{desc}',
                  {str(col.is_amount).upper()}, {str(col.is_id).upper()},
                  {str(col.is_date).upper()}, {str(col.nullable).upper()},
                  {str(col.reserved_keyword).upper()},
                  '{allowed}', '{notes}', '{now}'
                )
            """)

    # clear cache
    try:
        get_column_metadata.clear()
    except Exception:
        st.cache_data.clear()


def write_category_mapping(dataset_name: str, mapping: Dict[str, List[str]]) -> None:
    """Upsert category_mapping_meta"""
    with _conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SET hive.exec.dynamic.partition.mode=nonstrict")
        cursor.execute(
            f"ALTER TABLE category_mapping_meta DROP IF EXISTS PARTITION (dataset_name='{dataset_name}')"
        )
        for col_name, aliases in mapping.items():
            for alias in aliases:
                alias_esc = alias.replace("'", "__SQ__")
                cursor.execute(
                    f"INSERT INTO category_mapping_meta PARTITION (dataset_name='{dataset_name}') "
                    f"VALUES ('{col_name}', '{alias_esc}')"
                )
    try:
        get_category_mapping.clear()
    except Exception:
        st.cache_data.clear()


def write_nlp_config(
    dataset_name: str,
    rules: List[str],
    examples: List[Dict],
) -> None:
    """Upsert nlp_config"""
    with _conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SET hive.exec.dynamic.partition.mode=nonstrict")
        cursor.execute(
            f"ALTER TABLE nlp_config DROP IF EXISTS PARTITION (dataset_name='{dataset_name}')"
        )
        for i, rule in enumerate(rules):
            rule_esc = rule.replace("'", "__SQ__")
            cursor.execute(
                f"INSERT INTO nlp_config PARTITION (dataset_name='{dataset_name}') "
                f"VALUES ('rule', '{rule_esc}', {i})"
            )
        for i, ex in enumerate(examples):
            ex_content = json.dumps(ex, ensure_ascii=False).replace("'", "__SQ__")
            cursor.execute(
                f"INSERT INTO nlp_config PARTITION (dataset_name='{dataset_name}') "
                f"VALUES ('example', '{ex_content}', {i})"
            )
    try:
        get_nlp_config.clear()
    except Exception:
        st.cache_data.clear()