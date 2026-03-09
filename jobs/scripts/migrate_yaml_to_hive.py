#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import yaml
from pyhive import hive

HIVE_HOST = os.environ.get("HIVE_HOST", "hive-server")
HIVE_PORT = int(os.environ.get("HIVE_PORT", 10000))
DATASETS_DIR = Path(os.environ.get("DATASETS_DIR", "/jobs/datasets"))

# Hive 2.3 ไม่รองรับ ACID (DELETE/UPDATE)
# ใช้ TEXTFILE + INSERT OVERWRITE แทน
# strategy: สร้าง temp table → INSERT ทั้งหมดใหม่ → DROP temp

DDL_COLUMN_METADATA = """
CREATE TABLE IF NOT EXISTS column_metadata (
  dataset_name       STRING,
  col_name           STRING,
  col_type           STRING,
  thai_name          STRING,
  description        STRING,
  is_amount          BOOLEAN,
  is_id              BOOLEAN,
  is_date            BOOLEAN,
  nullable           BOOLEAN,
  reserved_keyword   BOOLEAN,
  allowed_values     STRING,
  notes              STRING,
  updated_at         STRING
)
PARTITIONED BY (ds STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
"""

DDL_CATEGORY_MAPPING_META = """
CREATE TABLE IF NOT EXISTS category_mapping_meta (
  col_name      STRING,
  thai_alias    STRING
)
PARTITIONED BY (dataset_name STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
"""

DDL_NLP_CONFIG = """
CREATE TABLE IF NOT EXISTS nlp_config (
  config_type   STRING,
  content       STRING,
  sort_order    INT
)
PARTITIONED BY (dataset_name STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
"""


def _conn():
    return hive.connect(host=HIVE_HOST, port=HIVE_PORT, database="default")


def create_tables(cursor):
    print("Creating Hive metadata tables...")
    cursor.execute("SET hive.exec.dynamic.partition.mode=nonstrict")
    for ddl in [DDL_COLUMN_METADATA, DDL_CATEGORY_MAPPING_META, DDL_NLP_CONFIG]:
        cursor.execute(ddl)
    print("Tables ready.")


def migrate_dataset(cursor, dataset_name, yaml_data):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pipeline = yaml_data.get("pipeline", {})
    id_cols = set(pipeline.get("id_columns", ["date", "details", "year"]))
    amount_cols = set(pipeline.get("amount_columns", []))
    date_col = pipeline.get("date_column", "date")

    print("\nMigrating dataset: %s" % dataset_name)

    cursor.execute("SET hive.exec.dynamic.partition.mode=nonstrict")

    # ── column_metadata — overwrite partition ds=dataset_name ──
    schema = yaml_data.get("schema", [])
    if not schema:
        print("  WARNING: no schema in yaml, skipping column_metadata")
    else:
        # DROP partition เดิมก่อน แล้ว INSERT ใหม่
        cursor.execute(
            "ALTER TABLE column_metadata DROP IF EXISTS PARTITION (ds='%s')" % dataset_name
        )
        count = 0
        for col in schema:
            name = col["name"]
            allowed = json.dumps(col.get("allowed_values", []), ensure_ascii=False).replace("'", "")
            notes = (col.get("notes") or "").replace("'", "").replace("\n", " ")
            desc = (col.get("description") or "").replace("'", "")
            thai = (col.get("thai_name") or name).replace("'", "")
            cursor.execute(
                "INSERT INTO column_metadata PARTITION (ds='%s') VALUES "
                "('%s','%s','%s','%s',%s,%s,%s,TRUE,%s,'%s','%s','%s')"
                % (
                    dataset_name,
                    name, col.get("type", "STRING"), thai, desc,
                    str(name in amount_cols).upper(),
                    str(name in id_cols).upper(),
                    str(name == date_col).upper(),
                    str(col.get("reserved_keyword", False)).upper(),
                    allowed, notes, now,
                )
            )
            count += 1
        print("  column_metadata: %d rows" % count)

    # ── category_mapping_meta ────────────────────────────────
    cat_map = yaml_data.get("category_mapping", {})
    if not cat_map:
        print("  WARNING: no category_mapping in yaml, skipping")
    else:
        cursor.execute(
            "ALTER TABLE category_mapping_meta DROP IF EXISTS PARTITION (dataset_name='%s')" % dataset_name
        )
        count = 0
        for col_name, aliases in cat_map.items():
            for alias in aliases:
                alias_clean = alias.replace("'", "")
                cursor.execute(
                    "INSERT INTO category_mapping_meta PARTITION (dataset_name='%s') VALUES ('%s','%s')"
                    % (dataset_name, col_name, alias_clean)
                )
                count += 1
        print("  category_mapping_meta: %d rows" % count)

    # ── nlp_config ───────────────────────────────────────────
    cursor.execute(
        "ALTER TABLE nlp_config DROP IF EXISTS PARTITION (dataset_name='%s')" % dataset_name
    )
    count = 0
    for i, rule in enumerate(yaml_data.get("nlp_rules", [])):
        rule_clean = rule.replace("'", "")
        cursor.execute(
            "INSERT INTO nlp_config PARTITION (dataset_name='%s') VALUES ('rule','%s',%d)"
            % (dataset_name, rule_clean, i)
        )
        count += 1
    for i, ex in enumerate(yaml_data.get("example_queries", [])):
        content = json.dumps(ex, ensure_ascii=False).replace("'", "")
        cursor.execute(
            "INSERT INTO nlp_config PARTITION (dataset_name='%s') VALUES ('example','%s',%d)"
            % (dataset_name, content, i)
        )
        count += 1
    print("  nlp_config: %d rows" % count)


def run(dataset_names):
    with _conn() as conn:
        cursor = conn.cursor()
        create_tables(cursor)
        for name in dataset_names:
            candidates = [
                DATASETS_DIR / ("%s.yaml" % name),
                DATASETS_DIR / ("%s.yaml" % name.replace("_itsc", "")),
            ]
            yaml_path = next((p for p in candidates if p.exists()), None)
            if yaml_path is None:
                print("ERROR: yaml not found for '%s', skipping" % name)
                continue
            with yaml_path.open("r", encoding="utf-8") as f:
                yaml_data = yaml.safe_load(f)
            migrate_dataset(cursor, name, yaml_data)
    print("\nMigration complete.")


def list_datasets():
    return [p.stem for p in DATASETS_DIR.glob("*.yaml")]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dataset", help="dataset name e.g. finance_itsc")
    group.add_argument("--all", action="store_true")
    args = parser.parse_args()

    if args.all:
        datasets = list_datasets()
        if not datasets:
            print("ERROR: no yaml files found in %s" % DATASETS_DIR)
            sys.exit(1)
        print("Found %d datasets: %s" % (len(datasets), datasets))
    else:
        datasets = [args.dataset]

    run(datasets)