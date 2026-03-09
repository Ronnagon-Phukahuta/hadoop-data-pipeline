# -*- coding: utf-8 -*-
"""
datasets/registry.py
---------------------
Dataset Registry

แยก source of truth:
  yaml  → pipeline config (paths, tables, pipeline rules)
  Hive  → schema metadata, category_mapping, nlp_rules, examples

Usage:
    from datasets.registry import load_dataset, build_schema_prompt

    ds = load_dataset("finance_itsc")
    # ds.schema / ds.category_mapping / ds.nlp_rules โหลดจาก Hive อัตโนมัติ
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from pyhive import hive

DATASETS_DIR = Path(os.getenv("DATASETS_DIR", Path(__file__).parent))
HIVE_HOST = os.environ.get("HIVE_HOST", "hive-server")
HIVE_PORT = int(os.environ.get("HIVE_PORT", 10000))


def _conn():
    return hive.connect(host=HIVE_HOST, port=HIVE_PORT, database="default")


# ── Dataclasses ──────────────────────────────────────────────

class ColumnDef:
    """
    Column definition — รับทั้ง col_type= และ type= (backward compat กับ tests)
    และรับ partition= kwarg
    """
    def __init__(self, name, col_type=None, type=None, **kwargs):
        self.name = name
        self.col_type = col_type or type or "STRING"
        self.thai_name = kwargs.get("thai_name", "")
        self.description = kwargs.get("description", "")
        self.is_amount = kwargs.get("is_amount", False)
        self.is_id = kwargs.get("is_id", False)
        self.is_date = kwargs.get("is_date", False)
        self.nullable = kwargs.get("nullable", True)
        self.reserved_keyword = kwargs.get("reserved_keyword", False)
        self.allowed_values = kwargs.get("allowed_values", [])
        self.notes = kwargs.get("notes", "")
        self.partition = kwargs.get("partition", False)

    # backward-compat: pipeline.py ใช้ col.type
    @property
    def type(self) -> str:
        return self.col_type


@dataclass
class DatasetConfig:
    dataset: str
    owner: str
    paths: Dict[str, str]
    tables: Dict[str, str]
    pipeline: Dict
    # โหลดจาก Hive — ว่างถ้ายังไม่ migrate
    schema: List[ColumnDef] = field(default_factory=list)
    category_mapping: Dict[str, List[str]] = field(default_factory=dict)
    nlp_rules: List[str] = field(default_factory=list)
    example_queries: List[Dict[str, str]] = field(default_factory=list)

    # ── Convenience ─────────────────────────────────────────

    @property
    def curated_table(self) -> str:
        return self.tables["curated"]

    @property
    def staging_table(self) -> str:
        return self.tables["staging"]

    @property
    def database(self) -> str:
        return self.tables["database"]

    @property
    def critical_columns(self) -> List[str]:
        return self.pipeline.get("critical_columns", [])

    @property
    def required_columns(self) -> List[str]:
        return self.pipeline.get("required_columns", [])

    @property
    def id_columns(self) -> List[str]:
        return self.pipeline.get("id_columns", ["date", "details", "year"])

    @property
    def exclude_columns(self) -> List[str]:
        return self.pipeline.get("exclude_columns", [])

    @property
    def partition_by(self) -> str:
        return self.pipeline.get("partition_by", "year")

    @property
    def date_column(self) -> Optional[str]:
        """
        column ที่ใช้เป็น date — None ถ้า dataset ไม่มี date
        อ่านจาก pipeline.date_column ใน yaml (override)
        หรือ fallback จาก Hive column_metadata (is_date=TRUE)
        """
        # explicit yaml override ก่อน
        yaml_val = self.pipeline.get("date_column", "__unset__")
        if yaml_val != "__unset__":
            return yaml_val or None  # null ใน yaml → None

        # fallback: หา is_date จาก schema ที่โหลดมา
        for col in self.schema:
            if col.is_date:
                return col.name
        return None

    @property
    def has_date(self) -> bool:
        return self.date_column is not None

    @property
    def amount_columns(self) -> List[str]:
        # อ่านจาก yaml ก่อน (backward compat + explicit override)
        yaml_amounts = self.pipeline.get("amount_columns")
        if yaml_amounts:
            return yaml_amounts
        # fallback: จาก Hive schema
        return [c.name for c in self.schema if c.is_amount]

    def col(self, name: str) -> Optional[ColumnDef]:
        return next((c for c in self.schema if c.name == name), None)


# ── Hive Loaders ─────────────────────────────────────────────

def _load_schema_from_hive(dataset_name: str) -> List[ColumnDef]:
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
            result.append(ColumnDef(
                name=row[0],
                col_type=row[1] or "STRING",
                thai_name=row[2] or row[0],
                description=row[3] or "",
                is_amount=bool(row[4]),
                is_id=bool(row[5]),
                is_date=bool(row[6]),
                nullable=bool(row[7]) if row[7] is not None else True,
                reserved_keyword=bool(row[8]),
                allowed_values=allowed,
                notes=row[10] or "",
            ))
        return result
    except Exception:
        return []


def _load_category_mapping_from_hive(dataset_name: str) -> Dict[str, List[str]]:
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
            result.setdefault(col_name, []).append(alias)
        return result
    except Exception:
        return {}


def _load_nlp_from_hive(dataset_name: str) -> tuple[List[str], List[Dict]]:
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


# ── Loader ───────────────────────────────────────────────────

def load_dataset(name: str, datasets_dir: Path = DATASETS_DIR) -> DatasetConfig:
    """
    โหลด DatasetConfig:
      - pipeline config  ← yaml
      - schema/mapping/nlp ← Hive (fallback: yaml ถ้ายังไม่ migrate)
    """
    candidates = [
        datasets_dir / f"{name}.yaml",
        datasets_dir / f"{name.replace('_itsc', '')}.yaml",
    ]
    yaml_path = next((p for p in candidates if p.exists()), None)
    if yaml_path is None:
        raise FileNotFoundError(
            f"Dataset '{name}' not found. Searched: {[str(p) for p in candidates]}"
        )

    with yaml_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    # โหลด schema/mapping/nlp จาก Hive ก่อน
    hive_schema = _load_schema_from_hive(name)
    hive_mapping = _load_category_mapping_from_hive(name)
    hive_rules, hive_examples = _load_nlp_from_hive(name)

    # fallback: ถ้า Hive ยังไม่มีข้อมูล ใช้จาก yaml (backward compat)
    if hive_schema:
        schema = hive_schema
    else:
        schema = [
            ColumnDef(
                name=col["name"],
                col_type=col.get("type", "STRING"),
                thai_name=col.get("thai_name", ""),
                description=col.get("description", ""),
                reserved_keyword=col.get("reserved_keyword", False),
                allowed_values=col.get("allowed_values", []),
                notes=col.get("notes", "") or "",
                partition=col.get("partition", False),
                is_date=col.get("is_date", False),
                is_amount=col.get("is_amount", False),
            )
            for col in raw.get("schema", [])
        ]

    category_mapping = hive_mapping or raw.get("category_mapping", {})
    nlp_rules = hive_rules or raw.get("nlp_rules", [])
    example_queries = hive_examples or raw.get("example_queries", [])

    return DatasetConfig(
        dataset=raw["dataset"],
        owner=raw.get("owner", ""),
        paths=raw["paths"],
        tables=raw["tables"],
        pipeline=raw.get("pipeline", {}),
        schema=schema,
        category_mapping=category_mapping,
        nlp_rules=nlp_rules,
        example_queries=example_queries,
    )


# ── Prompt Builders ──────────────────────────────────────────

def build_category_mapping_text(ds: DatasetConfig) -> str:
    lines = ["Mapping ชื่อภาษาไทย → ชื่อ column ในฐานข้อมูล:\n"]
    for col_name, aliases in ds.category_mapping.items():
        lines.append(f"{col_name:<60} = {' / '.join(aliases)}")
    return "\n".join(lines)


def build_schema_prompt(ds: DatasetConfig) -> str:
    table = ds.curated_table
    category_text = build_category_mapping_text(ds)

    col_lines = []
    for col in ds.schema:
        name_fmt = f"`{col.name}`" if col.reserved_keyword else col.name
        col_lines.append(f"- {name_fmt} ({col.col_type}): {col.description}")

    rules_block = "\n".join(
        f"{i+1}. {rule}" for i, rule in enumerate(ds.nlp_rules)
    )

    example_block = ""
    if ds.example_queries:
        parts = [
            f"Q: {ex['q']}\nA:\n{ex['sql'].rstrip()}"
            for ex in ds.example_queries
        ]
        example_block = "\n\nตัวอย่าง Query ที่ถูกต้อง:\n\n" + "\n\n".join(parts)

    return f"""Table: {table}
Columns:
{chr(10).join(col_lines)}

{category_text}

กฎที่ต้องปฏิบัติตามเสมอ:
{rules_block}{example_block}"""