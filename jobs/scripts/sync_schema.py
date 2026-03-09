"""
sync_schema.py — Sync Hive table schema → dataset yaml

ทำงาน:
  1. ต่อ Hive และ DESCRIBE table (staging + curated)
  2. เปรียบเทียบกับ schema section ใน finance.yaml
  3. column ใหม่ใน Hive → เพิ่มใน yaml
  4. column ใน yaml ที่ Hive ไม่มีแล้ว → แจ้งเตือน (ไม่ลบอัตโนมัติ)

Usage:
    spark-submit /jobs/scripts/sync_schema.py
    spark-submit /jobs/scripts/sync_schema.py --dry-run
    spark-submit /jobs/scripts/sync_schema.py --dataset finance_itsc
    spark-submit /jobs/scripts/sync_schema.py --dataset finance_itsc --dry-run
"""

import argparse
import os
import sys

import yaml
from pyspark.sql import SparkSession


DATASETS_DIR = os.path.join(os.path.dirname(__file__), "..", "datasets")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _save_yaml(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, sort_keys=False, default_flow_style=False)


def _describe_table(spark, database: str, table: str) -> dict:
    """
    คืน {col_name: data_type} จาก Hive DESCRIBE
    ข้าม partition info และ comment rows (#)
    """
    try:
        rows = spark.sql(f"DESCRIBE {database}.{table}").collect()
        return {
            row["col_name"]: row["data_type"]
            for row in rows
            if row["col_name"]
            and not row["col_name"].startswith("#")
            and row["col_name"].strip()
        }
    except Exception as e:
        print(f"  ⚠️  ไม่สามารถ DESCRIBE {database}.{table}: {e}")
        return {}


def _yaml_schema_cols(schema: list) -> dict:
    """คืน {col_name: entry} จาก yaml schema list"""
    return {entry["name"]: entry for entry in (schema or [])}


def _hive_type_to_yaml(hive_type: str) -> str:
    """แปลง Hive type → yaml type ที่ใช้ใน schema"""
    mapping = {
        "string": "STRING",
        "int": "INT",
        "bigint": "BIGINT",
        "double": "DOUBLE",
        "float": "FLOAT",
        "boolean": "BOOLEAN",
        "timestamp": "TIMESTAMP",
        "date": "DATE",
    }
    t = hive_type.lower().split("(")[0].strip()
    if t.startswith("decimal"):
        return "DECIMAL"
    return mapping.get(t, hive_type.upper())


# ── Core sync logic ───────────────────────────────────────────────────────────

def sync_table(
    spark,
    yaml_data: dict,
    database: str,
    table: str,
    dry_run: bool,
) -> bool:
    """
    Sync schema ของ curated table เปรียบเทียบกับ yaml schema
    คืน True ถ้ามีการเปลี่ยนแปลง
    """
    print(f"\n📋 DESCRIBE {database}.{table}")
    hive_cols = _describe_table(spark, database, table)
    if not hive_cols:
        print("  ⏭️  ข้าม (ไม่สามารถ DESCRIBE ได้)")
        return False

    yaml_schema = yaml_data.get("schema", [])
    yaml_cols = _yaml_schema_cols(yaml_schema)
    partition_col = yaml_data.get("pipeline", {}).get("partition_by", "year")
    skip_cols = {partition_col}

    # ── 1. column ใหม่ใน Hive ที่ไม่มีใน yaml ─────────────────────────────
    new_cols = [
        (col, dtype)
        for col, dtype in hive_cols.items()
        if col not in yaml_cols and col not in skip_cols
    ]

    # ── 2. column ใน yaml ที่ Hive ไม่มีแล้ว ──────────────────────────────
    removed_cols = [
        col
        for col in yaml_cols
        if col not in hive_cols and col not in skip_cols
    ]

    changed = bool(new_cols or removed_cols)

    if not changed:
        print("  ✅ schema ตรงกัน ไม่มีการเปลี่ยนแปลง")
        return False

    if new_cols:
        print(f"\n  🆕 column ใหม่ใน Hive ({len(new_cols)} รายการ):")
        for col, dtype in new_cols:
            print(f"     + {col}: {_hive_type_to_yaml(dtype)}")

    if removed_cols:
        print(f"\n  ⚠️  column ใน yaml ที่ไม่มีใน Hive ({len(removed_cols)} รายการ):")
        for col in removed_cols:
            print(f"     ? {col}  ← ตรวจสอบว่า drop จาก Hive แล้วหรือยังไม่ได้สร้าง")

    if dry_run:
        print("\n  🔍 [dry-run] ไม่มีการแก้ไขไฟล์")
        return False

    # เพิ่ม column ใหม่เข้า yaml schema
    for col, dtype in new_cols:
        yaml_type = _hive_type_to_yaml(dtype)
        yaml_data["schema"].append({
            "name": col,
            "type": yaml_type,
            "thai_name": col,  # ให้ user แก้ชื่อไทยเอง
            "description": f"(auto-synced from Hive {table})",
        })
        print(f"  ✏️  เพิ่ม '{col}' ({yaml_type}) ใน yaml schema แล้ว")

    return True


def check_staging(spark, yaml_data: dict, database: str, table: str) -> None:
    """แจ้งเตือน column ใน staging ที่ยังไม่มีใน yaml (ไม่แก้ไข yaml)"""
    print(f"\n{'─' * 60}")
    print(f"ℹ️  staging table ({table}) — ตรวจสอบเฉพาะแจ้งเตือน")

    staging_cols = _describe_table(spark, database, table)
    if not staging_cols:
        return

    yaml_cols = _yaml_schema_cols(yaml_data.get("schema", []))
    partition_col = yaml_data.get("pipeline", {}).get("partition_by", "year")
    amount_cols = set(yaml_data.get("pipeline", {}).get("amount_columns", []))

    # staging มี amount columns ที่ไม่ได้อยู่ใน curated schema — นั่นเป็นเรื่องปกติ
    # แจ้งเฉพาะ column ที่ไม่ใช่ amount_col และไม่ใช่ partition
    unexpected = [
        col for col in staging_cols
        if col not in yaml_cols
        and col not in amount_cols
        and col != partition_col
    ]

    if unexpected:
        print(f"  ⚠️  column ใน staging ที่ไม่มีใน yaml schema หรือ amount_columns ({len(unexpected)} รายการ):")
        for col in unexpected:
            print(f"     ? {col}")
    else:
        print("  ✅ staging columns ครบถ้วนแล้ว")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Sync Hive schema → dataset yaml")
    parser.add_argument(
        "--dataset",
        default="finance_itsc",
        help="ชื่อ dataset (default: finance_itsc)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="แสดงผลเฉยๆ ไม่แก้ไขไฟล์",
    )
    args = parser.parse_args()

    yaml_path = os.path.join(DATASETS_DIR, f"{args.dataset}.yaml")
    if not os.path.exists(yaml_path):
        print(f"❌ ไม่พบไฟล์ {yaml_path}")
        sys.exit(1)

    print(f"📂 Dataset : {args.dataset}")
    print(f"📄 YAML    : {yaml_path}")
    if args.dry_run:
        print("🔍 Mode    : dry-run")

    yaml_data = _load_yaml(yaml_path)
    database = yaml_data.get("tables", {}).get("database", "default")
    staging_table = yaml_data.get("tables", {}).get("staging")
    curated_table = yaml_data.get("tables", {}).get("curated")

    spark = (
        SparkSession.builder
        .appName(f"sync_schema_{args.dataset}")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # sync curated (source of truth สำหรับ NLP schema)
    changed = False
    if curated_table:
        changed = sync_table(spark, yaml_data, database, curated_table, args.dry_run)

    # ตรวจสอบ staging (แจ้งเตือนเท่านั้น)
    if staging_table:
        check_staging(spark, yaml_data, database, staging_table)

    # บันทึก yaml
    if changed and not args.dry_run:
        _save_yaml(yaml_path, yaml_data)
        print(f"\n💾 บันทึก {yaml_path} แล้ว")
        print("⚠️  กรุณาแก้ไข thai_name และ description ของ column ใหม่ด้วย")
    elif not changed:
        print("\n✅ ไม่มีการเปลี่ยนแปลง")

    spark.stop()


if __name__ == "__main__":
    main()