#!/usr/bin/env python3
# manage.py — Dataset CLI สำหรับ Finance ITSC Pipeline
#
# Usage (รันผ่าน spark-submit):
#   spark-submit /jobs/manage.py --dataset finance_itsc versions 2024
#   spark-submit /jobs/manage.py --dataset finance_itsc diff 2024 v_20260301_120000 v_20260306_095749
#   spark-submit /jobs/manage.py --dataset finance_itsc restore 2024 v_20260301_120000
#   spark-submit /jobs/manage.py --dataset finance_itsc trash 2024
#   spark-submit /jobs/manage.py --dataset finance_itsc cleanup 2024 --keep 5
#
#   ถ้าไม่ระบุ --dataset จะใช้ finance_itsc (default)

import sys
import json
import argparse

sys.path.insert(0, "/jobs")

from pyspark.sql import SparkSession


def get_spark():
    return (
        SparkSession.builder
        .appName("manage")
        .enableHiveSupport()
        .getOrCreate()
    )


def load_dataset(dataset_name: str):
    """โหลด DatasetConfig จาก registry"""
    from datasets.registry import load_dataset as _load
    try:
        return _load(dataset_name)
    except Exception as e:
        print(f"❌ โหลด dataset '{dataset_name}' ไม่สำเร็จ: {e}")
        print("   ตรวจสอบว่า /jobs/datasets/{dataset_name}.yaml มีอยู่จริง")
        sys.exit(1)


# ── commands ──────────────────────────────────────────────────────

def cmd_versions(sc, args):
    """แสดง versions ทั้งหมดของปีที่ระบุ"""
    from utils.versioning import list_versions

    versions = list_versions(sc, year=args.year)
    if not versions:
        print(f"ไม่มี version สำหรับ year={args.year}")
        return

    print(f"\n{'─'*80}")
    print(f"  [{args.dataset}] Versions for year={args.year}  ({len(versions)} versions)")
    print(f"{'─'*80}")
    print(f"  {'VERSION':<25} {'TIMESTAMP':<25} {'ROWS':>6}  {'SCHEMA_HASH':<14}  SOURCE")
    print(f"{'─'*80}")
    for v in versions:
        schema_hash = v.get("schema_hash", "N/A")
        if schema_hash != "N/A":
            schema_hash = schema_hash[:12]
        print(
            f"  {v['version']:<25} "
            f"{v['timestamp'][:19]:<25} "
            f"{v['row_count']:>6}  "
            f"{schema_hash:<14}  "
            f"{v.get('source_file', 'N/A')}"
        )
    print(f"{'─'*80}\n")


def cmd_diff(sc, args):
    """เปรียบเทียบ 2 versions"""
    from utils.versioning import diff_versions

    print(f"\n=== [{args.dataset}] Diff: {args.version_a} → {args.version_b} (year={args.year}) ===")
    diff = diff_versions(sc, args.version_a, args.version_b, year=args.year)

    print(f"  schema_changed : {diff['schema_changed']}")
    if diff["added_columns"]:
        print(f"  added_columns  : {diff['added_columns']}")
    if diff["removed_columns"]:
        print(f"  removed_columns: {diff['removed_columns']}")
    print(f"  row_count      : {diff['row_count_a']} → {diff['row_count_b']} (diff={diff['row_diff']:+d})")
    print(f"  source_a       : {diff['source_a']}")
    print(f"  source_b       : {diff['source_b']}")
    print(f"  same_source    : {diff['same_source']}")
    print()

    if args.json:
        print(json.dumps(diff, indent=2, ensure_ascii=False))


def cmd_restore(sc, spark, ds, args):
    """Restore version ที่ระบุกลับไปเป็น staging"""
    from utils.versioning import restore_version

    staging_table = ds.staging_table
    staging_path  = f"hdfs://namenode:8020{ds.paths['staging']}"

    print(f"\n=== [{args.dataset}] Restore: {args.version_id} → year={args.year} ===")
    print(f"  target_table: {staging_table}")
    print(f"  target_path : {staging_path}")

    if not args.yes:
        print("\nต้องใส่ --yes เพื่อยืนยัน restore")
        print(f"  spark-submit /jobs/manage.py --dataset {args.dataset} restore {args.year} {args.version_id} --yes")
        return

    restore_version(
        spark,
        version_id=args.version_id,
        year=args.year,
        target_table=staging_table,
        target_path=staging_path,
    )
    print(f"✅ Restored {args.version_id} → {staging_table} สำเร็จ\n")


def cmd_trash(sc, args):
    """แสดง versions ที่อยู่ใน trash"""
    from utils.hdfs import hdfs_ls_recursive

    trash_base = "hdfs://namenode:8020/datalake/trash"
    files = hdfs_ls_recursive(sc, trash_base)
    version_dirs = [
        f for f in files
        if f"year={args.year}" in f and "_version.json" in f
    ]

    if not version_dirs:
        print(f"\nไม่มี version ใน trash สำหรับ year={args.year}\n")
        return

    print(f"\n=== [{args.dataset}] Trash for year={args.year} ({len(version_dirs)} versions) ===")
    for f in sorted(version_dirs):
        print(f"  {f}")
    print()


def cmd_cleanup(sc, args):
    """cleanup versions เก่า เก็บไว้แค่ N อัน"""
    from utils.versioning import cleanup_old_versions, list_versions

    versions = list_versions(sc, year=args.year)
    keep = args.keep
    to_delete = versions[keep:]

    if not to_delete:
        print(f"\nไม่มี version ที่ต้องลบ (มีอยู่ {len(versions)} versions, keep={keep})\n")
        return

    print(f"\n=== [{args.dataset}] Cleanup year={args.year}: เก็บ {keep} versions, ลบ {len(to_delete)} versions ===")
    for v in to_delete:
        print(f"  ลบ: {v['version']}  ({v['timestamp'][:19]})")

    if not args.yes:
        print("\nต้องใส่ --yes เพื่อยืนยัน cleanup")
        print(f"  spark-submit /jobs/manage.py --dataset {args.dataset} cleanup {args.year} --keep {args.keep} --yes")
        return

    cleanup_old_versions(sc, year=args.year, keep=keep)
    print("✅ Cleanup สำเร็จ\n")


def cmd_info(ds, args):
    """แสดงข้อมูล dataset จาก registry"""
    print(f"\n{'─'*60}")
    print(f"  Dataset: {ds.dataset}")
    print(f"{'─'*60}")
    print(f"  staging_table : {ds.staging_table}")
    print(f"  curated_table : {ds.curated_table}")
    print(f"  date_column   : {ds.date_column or '(none)'}")
    print(f"  id_columns    : {ds.id_columns}")
    print(f"  amount_columns: {len(ds.amount_columns)} columns")
    print("  paths:")
    for k, v in ds.paths.items():
        print(f"    {k:<12}: {v}")
    print(f"{'─'*60}\n")


# ── main ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        prog="manage.py",
        description="Dataset CLI สำหรับ Finance ITSC Pipeline",
    )
    parser.add_argument(
        "--dataset", "-d",
        default="finance_itsc",
        help="ชื่อ dataset (default: finance_itsc)",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # info
    sub.add_parser("info", help="แสดงข้อมูล dataset จาก registry")

    # versions
    p_versions = sub.add_parser("versions", help="แสดง versions ทั้งหมดของปีที่ระบุ")
    p_versions.add_argument("year", type=int)

    # diff
    p_diff = sub.add_parser("diff", help="เปรียบเทียบ 2 versions")
    p_diff.add_argument("year", type=int)
    p_diff.add_argument("version_a")
    p_diff.add_argument("version_b")
    p_diff.add_argument("--json", action="store_true", help="output เป็น JSON ด้วย")

    # restore
    p_restore = sub.add_parser("restore", help="restore version กลับไป staging")
    p_restore.add_argument("year", type=int)
    p_restore.add_argument("version_id")
    p_restore.add_argument("--yes", "-y", action="store_true", help="ไม่ต้อง confirm")

    # trash
    p_trash = sub.add_parser("trash", help="แสดง versions ที่อยู่ใน trash")
    p_trash.add_argument("year", type=int)

    # cleanup
    p_cleanup = sub.add_parser("cleanup", help="cleanup versions เก่า")
    p_cleanup.add_argument("year", type=int)
    p_cleanup.add_argument("--keep", type=int, default=5)
    p_cleanup.add_argument("--yes", "-y", action="store_true", help="ไม่ต้อง confirm")

    args = parser.parse_args()

    # โหลด dataset config จาก registry
    ds = load_dataset(args.dataset)

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    try:
        if args.command == "info":
            cmd_info(ds, args)
        elif args.command == "versions":
            cmd_versions(sc, args)
        elif args.command == "diff":
            cmd_diff(sc, args)
        elif args.command == "restore":
            cmd_restore(sc, spark, ds, args)
        elif args.command == "trash":
            cmd_trash(sc, args)
        elif args.command == "cleanup":
            cmd_cleanup(sc, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()