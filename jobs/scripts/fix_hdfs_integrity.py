# -*- coding: utf-8 -*-
# scripts/fix_hdfs_integrity.py
#
# ใช้งาน:
#   spark-submit /jobs/scripts/fix_hdfs_integrity.py --dataset finance_itsc
#   spark-submit /jobs/scripts/fix_hdfs_integrity.py --dataset finance_itsc_test_01
#   spark-submit /jobs/scripts/fix_hdfs_integrity.py  (default: finance_itsc)

import sys
import argparse

sys.path.insert(0, "/jobs")

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser(description="Fix HDFS Integrity")
    parser.add_argument(
        "--dataset", "-d",
        default="finance_itsc",
        help="ชื่อ dataset (default: finance_itsc)",
    )
    return parser.parse_args()


def get_spark():
    return (
        SparkSession.builder
        .appName("Fix HDFS Integrity")
        .enableHiveSupport()
        .getOrCreate()
    )


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def _get_fs(sc):
    URI = sc._jvm.java.net.URI
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    conf = sc._jsc.hadoopConfiguration()
    return FileSystem.get(URI("hdfs://namenode:8020"), conf)


def hdfs_delete(sc, path, recursive=False):
    Path = sc._jvm.org.apache.hadoop.fs.Path
    fs = _get_fs(sc)
    return fs.delete(Path(path), recursive)


def hdfs_ls(sc, path):
    Path = sc._jvm.org.apache.hadoop.fs.Path
    fs = _get_fs(sc)
    try:
        statuses = fs.listStatus(Path(path))
        return [str(s.getPath()) for s in statuses]
    except Exception:
        return []


def hdfs_find_recursive(sc, path, suffix):
    results = []
    for entry in hdfs_ls(sc, path):
        name = entry.split("/")[-1]
        if name.endswith(suffix):
            results.append(entry)
        elif "." not in name:
            results.extend(hdfs_find_recursive(sc, entry, suffix))
    return results


def main():
    args = parse_args()

    from datasets.registry import load_dataset
    try:
        ds = load_dataset(args.dataset)
    except Exception as e:
        print(f"❌ โหลด dataset '{args.dataset}' ไม่สำเร็จ: {e}")
        sys.exit(1)

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    staging_path = f"hdfs://namenode:8020{ds.paths['staging']}"
    curated_path = f"hdfs://namenode:8020{ds.paths['curated']}"
    raw_path     = f"hdfs://namenode:8020{ds.paths['raw']}"
    wide_table   = ds.staging_table
    long_table   = ds.curated_table
    database     = ds.database
    date_col     = ds.date_column

    print(f"\n{'='*60}")
    print(f"  Fix HDFS Integrity: {ds.dataset}")
    print(f"  wide_table : {wide_table}")
    print(f"  long_table : {long_table}")
    print(f"  date_column: {date_col or '(none)'}")
    print(f"{'='*60}")

    # ============================================================
    # FIX 1: MSCK REPAIR — sync Hive metastore กับ HDFS
    # ============================================================
    section("FIX 1: MSCK REPAIR TABLE")

    for table in [wide_table, long_table]:
        try:
            print(f"  Running MSCK REPAIR on {table}...")
            spark.sql(f"MSCK REPAIR TABLE {database}.{table}")
            print(f"  ✅ {table} repaired")
        except Exception as e:
            print(f"  ❌ MSCK REPAIR {table} failed: {e}")

    print("\n  📊 Partitions หลัง REPAIR:")
    try:
        spark.sql(f"SHOW PARTITIONS {database}.{wide_table}").show(truncate=False)
    except Exception as e:
        print(f"  ❌ SHOW PARTITIONS failed: {e}")

    # ============================================================
    # FIX 2: ลบ partition ที่มี date year ไม่ตรง (Closure Bug)
    # ============================================================
    section("FIX 2: Clean up partitions ที่ผิดจาก Closure Bug")

    if not date_col:
        print("  ⏭️  ข้าม (dataset ไม่มี date_column)")
    else:
        try:
            all_years = [
                row["year"] for row in
                spark.sql(f"SELECT DISTINCT year FROM {database}.{wide_table}").collect()
            ]

            bad_years = []
            for year in all_years:
                cnt = spark.sql(
                    f"SELECT COUNT(*) as cnt FROM {database}.{wide_table} "
                    f"WHERE year = {year} "
                    f"AND `{date_col}` RLIKE '^[0-9]{{4}}-[0-9]{{2}}' "
                    f"AND CAST(SUBSTR(`{date_col}`,1,4) AS INT) != {year}"
                ).collect()[0]["cnt"]
                if cnt > 0:
                    bad_years.append(year)
                    print(f"  ❌ year={year} มี {cnt} rows ที่ {date_col} ไม่ตรง — จะลบ partition นี้")

            if not bad_years:
                print("  ✅ ไม่พบ partition ที่ผิด")
            else:
                for year in bad_years:
                    try:
                        spark.sql(f"ALTER TABLE {database}.{wide_table} DROP IF EXISTS PARTITION (year={year})")
                        spark.sql(f"ALTER TABLE {database}.{long_table} DROP IF EXISTS PARTITION (year={year})")
                        hdfs_delete(sc, f"{staging_path}/year={year}", recursive=True)
                        hdfs_delete(sc, f"{curated_path}/year={year}", recursive=True)
                        print(f"  🗑️  ลบ partition year={year} ออกจาก wide, long และ HDFS แล้ว")
                    except Exception as e:
                        print(f"  ❌ ลบ partition year={year} ไม่สำเร็จ: {e}")

                # ลบ .done files เพื่อให้ pipeline re-process
                print(f"\n  🔄 ลบ .done files ของ years {bad_years} เพื่อ re-process...")
                done_files = hdfs_find_recursive(sc, raw_path, ".done")
                removed = 0
                for f in done_files:
                    for year in bad_years:
                        if f"year={year}/" in f:
                            try:
                                hdfs_delete(sc, f, recursive=False)
                                print(f"    🗑️  ลบ {f}")
                                removed += 1
                            except Exception as e:
                                print(f"    ❌ ลบ {f} ไม่สำเร็จ: {e}")
                print(f"  ลบ .done files ทั้งหมด {removed} files")

        except Exception as e:
            print(f"  ❌ Fix 2 failed: {e}")

    # ============================================================
    # FIX 3: .done / .failed audit
    # ============================================================
    section("FIX 3: .done / .failed audit")

    try:
        csv_files    = hdfs_find_recursive(sc, raw_path, ".csv")
        done_files   = set(hdfs_find_recursive(sc, raw_path, ".done"))
        failed_files = set(hdfs_find_recursive(sc, raw_path, ".failed"))

        print(f"\n  📂 CSV files    : {len(csv_files)}")
        print(f"  ✅ .done files  : {len(done_files)}")
        print(f"  ❌ .failed files: {len(failed_files)}")

        pending = [f for f in csv_files
                   if f + ".done" not in done_files
                   and f + ".failed" not in failed_files]
        if pending:
            print(f"\n  ⚠️  {len(pending)} CSV ยังไม่ถูก process:")
            for f in pending:
                print(f"    - {f}")
        else:
            print("  ✅ ทุก CSV มี .done หรือ .failed แล้ว")

        if failed_files:
            print("\n  ⚠️  .failed files ที่ค้างอยู่:")
            for f in sorted(failed_files):
                print(f"    - {f}")
    except Exception as e:
        print(f"  ❌ Audit failed: {e}")

    # ============================================================
    # VERIFY: ตรวจหลัง fix
    # ============================================================
    section(f"VERIFY: ตรวจสอบหลัง fix — {ds.dataset}")

    try:
        print("\n  📊 Wide table partitions:")
        spark.sql(f"SHOW PARTITIONS {database}.{wide_table}").show(truncate=False)

        print("  📊 Row count per year (wide):")
        spark.sql(
            f"SELECT year, COUNT(*) as rows FROM {database}.{wide_table} GROUP BY year ORDER BY year"
        ).show(truncate=False)

        print("  📊 Row count per year (long):")
        spark.sql(
            f"SELECT year, COUNT(*) as rows FROM {database}.{long_table} GROUP BY year ORDER BY year"
        ).show(truncate=False)
    except Exception as e:
        print(f"  ❌ Verify failed: {e}")

    print("\n  ✅ Fix script เสร็จสิ้น — รัน pipeline ใหม่ได้เลยครับ\n")
    spark.stop()


if __name__ == "__main__":
    main()