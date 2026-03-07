# datasets/registry.py
"""
Dataset Registry — โหลด dataset config จาก YAML
ใช้แทน hardcode ใน config.py

Usage:
    from datasets.registry import load_dataset, get_table_schema_prompt

    ds = load_dataset("finance_itsc")
    prompt = get_table_schema_prompt(ds)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml

# default: datasets/ อยู่ใน root ของ project
DATASETS_DIR = Path(os.getenv("DATASETS_DIR", Path(__file__).parent))


# ── Dataclasses ──────────────────────────────────────────────

@dataclass
class ColumnDef:
    name: str
    type: str
    thai_name: str = ""
    description: str = ""
    reserved_keyword: bool = False
    partition: bool = False
    allowed_values: List[str] = field(default_factory=list)
    notes: str = ""


@dataclass
class DatasetConfig:
    dataset: str
    owner: str
    paths: Dict[str, str]
    tables: Dict[str, str]
    pipeline: Dict
    schema: List[ColumnDef]
    category_mapping: Dict[str, List[str]]
    nlp_rules: List[str]
    example_queries: List[Dict[str, str]]

    # ── convenience properties ───────────────────────────────

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
    def id_columns(self) -> List[str]:
        return self.pipeline.get("id_columns", ["date", "details", "year"])

    @property
    def exclude_columns(self) -> List[str]:
        return self.pipeline.get("exclude_columns", [])

    @property
    def partition_by(self) -> str:
        return self.pipeline.get("partition_by", "year")

    def col(self, name: str) -> Optional[ColumnDef]:
        """ดึง ColumnDef ตามชื่อ column"""
        return next((c for c in self.schema if c.name == name), None)


# ── Loader ───────────────────────────────────────────────────

def load_dataset(name: str, datasets_dir: Path = DATASETS_DIR) -> DatasetConfig:
    """
    โหลด dataset config จาก <datasets_dir>/<name>.yaml

    Args:
        name: ชื่อ dataset เช่น 'finance_itsc'
        datasets_dir: path ของ folder datasets (override สำหรับ test)

    Returns:
        DatasetConfig

    Raises:
        FileNotFoundError: ถ้าไม่พบไฟล์ yaml
        KeyError: ถ้า yaml ขาด field ที่จำเป็น
    """
    # รองรับทั้ง 'finance_itsc' และ 'finance' (ชื่อไฟล์ finance.yaml)
    candidates = [
        datasets_dir / f"{name}.yaml",
        datasets_dir / f"{name.replace('_itsc', '')}.yaml",
    ]
    yaml_path = next((p for p in candidates if p.exists()), None)
    if yaml_path is None:
        searched = [str(p) for p in candidates]
        raise FileNotFoundError(
            f"Dataset '{name}' not found. Searched: {searched}"
        )

    with yaml_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    schema = [
        ColumnDef(
            name=col["name"],
            type=col["type"],
            thai_name=col.get("thai_name", ""),
            description=col.get("description", ""),
            reserved_keyword=col.get("reserved_keyword", False),
            partition=col.get("partition", False),
            allowed_values=col.get("allowed_values", []),
            notes=col.get("notes", ""),
        )
        for col in raw.get("schema", [])
    ]

    return DatasetConfig(
        dataset=raw["dataset"],
        owner=raw.get("owner", ""),
        paths=raw["paths"],
        tables=raw["tables"],
        pipeline=raw.get("pipeline", {}),
        schema=schema,
        category_mapping=raw.get("category_mapping", {}),
        nlp_rules=raw.get("nlp_rules", []),
        example_queries=raw.get("example_queries", []),
    )


# ── Prompt Builders (สำหรับ NLP / GPT) ──────────────────────

def build_category_mapping_text(ds: DatasetConfig) -> str:
    """สร้าง mapping text สำหรับใส่ใน GPT prompt"""
    lines = [
        "Mapping ชื่อภาษาไทย → ชื่อ column ในฐานข้อมูล (ใช้ชื่อ column ใน SQL เสมอ):\n"
    ]
    for col_name, aliases in ds.category_mapping.items():
        alias_str = " / ".join(aliases)
        lines.append(f"{col_name:<60} = {alias_str}")
    return "\n".join(lines)


def build_schema_prompt(ds: DatasetConfig) -> str:
    """
    สร้าง TABLE_SCHEMA prompt สำหรับส่งให้ GPT
    รวม schema + category mapping + nlp_rules + example_queries
    """
    table = ds.curated_table
    category_text = build_category_mapping_text(ds)

    # Schema columns
    col_lines = []
    for col in ds.schema:
        name_fmt = f"`{col.name}`" if col.reserved_keyword else col.name
        col_lines.append(f"- {name_fmt} ({col.type}): {col.description}")
    schema_block = "\n".join(col_lines)

    # Rules
    rules_block = "\n".join(
        f"{i+1}. {rule}" for i, rule in enumerate(ds.nlp_rules)
    )

    # Examples
    example_block = ""
    if ds.example_queries:
        parts = []
        for ex in ds.example_queries:
            parts.append(f"Q: {ex['q']}\nA:\n{ex['sql'].rstrip()}")
        example_block = "\n\nตัวอย่าง Query ที่ถูกต้อง:\n\n" + "\n\n".join(parts)

    return f"""Table: {table}
Columns:
{schema_block}

{category_text}

กฎที่ต้องปฏิบัติตามเสมอ:
{rules_block}{example_block}"""