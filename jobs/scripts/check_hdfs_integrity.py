# -*- coding: utf-8 -*-
# scripts/check_hdfs_integrity.py
#
# ใช้งาน:
#   spark-submit /jobs/scripts/check_hdfs_integrity.py --dataset finance_itsc
#   spark-submit /jobs/scripts/check_hdfs_integrity.py --dataset finance_itsc_test_01
#   spark-submit /jobs/scripts/check_hdfs_integrity.py  (default: finance_itsc)

import sys
import argparse
import subprocess

sys.path.insert(0, "/jobs")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


def parse_args():
    parser = argparse.ArgumentParser(description="HDFS Integrity Check")
    parser.add_argument(
        "--dataset", "-d",
        default="finance_itsc",
        help="ชื่อ dataset (default: finance_itsc)",
    )
    return parser.parse_args()


def get_spark():
    return (
        SparkSession.builder
        .appName("HDFS Integrity Check")
        .enableHiveSupport()
        .getOrCreate()
    )


issues = []


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def ok(msg):   print(f"  ✅ {msg}")
def warn(msg): 
    print(f"  ⚠️  {msg}")
    issues.append(f"[WARN] {msg}")
def err(msg):  
    print(f"  ❌ {msg}")
    issues.append(f"[ERROR] {msg}")


def hdfs_find(path, pattern):
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-find", path, "-name", pattern],
            capture_output=True, text=True
        )
        return [line.strip() for line in result.stdout.strip().split("\n") if line.strip()]
    except Exception:
        return []


def hdfs_ls_years(path):
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-ls", path],
            capture_output=True, text=True
        )
        lines = result.stdout.strip().split("\n")
        entries = [line.split()[-1].split("/")[-1] for line in lines if "year=" in line]
        return set(int(e.split("=")[1]) for e in entries if "=" in e)
    except Exception:
        return set()


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

    staging_path = f"hdfs://namenode:8020{ds.paths['staging']}"
    raw_path     = f"hdfs://namenode:8020{ds.paths['raw']}"
    wide_table   = ds.staging_table
    long_table   = ds.curated_table
    database     = ds.database
    date_col     = ds.date_column

    print(f"\n{'='*60}")
    print(f"  HDFS Integrity Check: {ds.dataset}")
    print(f"  wide_table : {wide_table}")
    print(f"  long_table : {long_table}")
    print(f"  date_column: {date_col or '(none)'}")
    print(f"{'='*60}")

    # ============================================================
    # CHECK 1: Partition directories vs Hive metadata
    # ============================================================
    section("CHECK 1: Partition directories vs Hive metadata")
    try:
        hive_partitions = spark.sql(f"SHOW PARTITIONS {database}.{wide_table}") \
            .rdd.flatMap(lambda r: r).collect()
        hive_years = set(int(p.split("=")[1]) for p in hive_partitions if "=" in p)

        hdfs_years = hdfs_ls_years(staging_path)

        only_in_hive = hive_years - hdfs_years
        only_in_hdfs = hdfs_years - hive_years

        if only_in_hive:
            err(f"Partitions ใน Hive แต่ไม่มีใน HDFS: {sorted(only_in_hive)}")
        if only_in_hdfs:
            warn(f"Partitions ใน HDFS แต่ไม่มีใน Hive: {sorted(only_in_hdfs)} — ควร MSCK REPAIR")
        if not only_in_hive and not only_in_hdfs:
            ok(f"Hive และ HDFS ตรงกัน: years = {sorted(hive_years)}")
    except Exception as e:
        err(f"ไม่สามารถตรวจ partition sync ได้: {e}")

    # ============================================================
    # CHECK 2: year column vs partition (เฉพาะ dataset ที่มี date_col)
    # ============================================================
    section("CHECK 2: year column vs partition value")
    try:
        df_wide = spark.sql(f"SELECT * FROM {database}.{wide_table}")

        year_check = df_wide.groupBy("year").agg(
            count("*").alias("row_count"),
        ).orderBy("year")

        print("\n  📊 Row count per year partition:")
        year_check.show(truncate=False)

        empty_partitions = year_check.filter(col("row_count") == 0).collect()
        for row in empty_partitions:
            err(f"Partition year={row['year']} ว่างเปล่า (0 rows)")
        if not empty_partitions:
            ok("ทุก partition มีข้อมูล")

        if date_col and date_col in df_wide.columns:
            date_year_check = df_wide.filter(
                col(date_col).rlike(r"^\d{4}-\d{2}")
            ).withColumn(
                "date_year", col(date_col).substr(1, 4).cast("int")
            ).filter(
                col("date_year") != col("year")
            )
            mismatch_count = date_year_check.count()
            if mismatch_count > 0:
                err(f"พบ {mismatch_count} rows ที่ date year ไม่ตรงกับ partition year")
                date_year_check.select(date_col, "year").show(20, truncate=False)
            else:
                ok(f"ค่า {date_col} ตรงกับ partition year ทุก row")
        else:
            ok(f"ข้าม date/year check (date_column={date_col or 'none'})")

    except Exception as e:
        err(f"ไม่สามารถตรวจ year column ได้: {e}")

    # ============================================================
    # CHECK 3: Wide vs Long row consistency
    # ============================================================
    section("CHECK 3: Wide vs Long — row consistency per year")
    try:
        wide_counts = spark.sql(
            f"SELECT year, COUNT(*) as wide_rows FROM {database}.{wide_table} GROUP BY year"
        )
        long_counts = spark.sql(
            f"SELECT year, COUNT(*) as long_rows FROM {database}.{long_table} GROUP BY year"
        )
        joined = wide_counts.join(long_counts, on="year", how="full_outer").orderBy("year")

        print("\n  📊 Wide vs Long row count:")
        joined.show(truncate=False)

        missing_in_long = joined.filter(col("long_rows").isNull()).collect()
        for row in missing_in_long:
            err(f"year={row['year']} มีข้อมูลใน wide แต่ไม่มีใน long table")

        orphan_in_long = joined.filter(col("wide_rows").isNull()).collect()
        for row in orphan_in_long:
            warn(f"year={row['year']} มีข้อมูลใน long แต่ไม่มีใน wide (orphan)")

        if not missing_in_long and not orphan_in_long:
            ok("Wide และ Long table มี year partition ตรงกัน")
    except Exception as e:
        err(f"ไม่สามารถตรวจ wide vs long ได้: {e}")

    # ============================================================
    # CHECK 4: .done / .failed file audit
    # ============================================================
    section("CHECK 4: .done / .failed file audit")
    try:
        csv_files    = hdfs_find(raw_path, "*.csv")
        done_files   = set(hdfs_find(raw_path, "*.done"))
        failed_files = set(hdfs_find(raw_path, "*.failed"))

        print(f"\n  📂 CSV files    : {len(csv_files)}")
        print(f"  ✅ .done files  : {len(done_files)}")
        print(f"  ❌ .failed files: {len(failed_files)}")

        pending = [f for f in csv_files
                   if f + ".done" not in done_files
                   and f + ".failed" not in failed_files]
        if pending:
            warn(f"มี {len(pending)} CSV files ที่ยังไม่ถูก process:")
            for f in pending:
                print(f"    - {f}")
        else:
            ok("ทุก CSV file มี .done หรือ .failed แล้ว")

        if failed_files:
            warn(f"มี {len(failed_files)} files ที่ failed:")
            for f in sorted(failed_files):
                print(f"    - {f}")
    except Exception as e:
        err(f"ไม่สามารถ audit .done/.failed ได้: {e}")

    # ============================================================
    # CHECK 5: Long table — null/negative amount audit
    # ============================================================
    section("CHECK 5: Long table — null/negative amount audit")
    try:
        df_long = spark.sql(f"SELECT * FROM {database}.{long_table}")
        null_amounts     = df_long.filter(col("amount").isNull()).count()
        negative_amounts = df_long.filter(col("amount") < 0).count()
        total            = df_long.count()

        print(f"\n  📊 Total rows   : {total:,}")
        print(f"  ⚠️  Null amount  : {null_amounts:,}")
        print(f"  ⚠️  Neg amount   : {negative_amounts:,}")

        if null_amounts > 0:
            warn(f"Long table มี {null_amounts:,} rows ที่ amount เป็น null")
        else:
            ok("ไม่มี null amount ใน long table")

        if negative_amounts > 0:
            warn(f"Long table มี {negative_amounts:,} rows ที่ amount ติดลบ")
        else:
            ok("ไม่มี negative amount ใน long table")
    except Exception as e:
        err(f"ไม่สามารถตรวจ long table ได้: {e}")

    # ============================================================
    # SUMMARY
    # ============================================================
    section(f"SUMMARY — {ds.dataset}")

    if not issues:
        print("\n  🎉 ไม่พบปัญหาใดๆ — ข้อมูลใน HDFS สมบูรณ์!")
    else:
        print(f"\n  พบปัญหาทั้งหมด {len(issues)} รายการ:\n")
        for i, issue in enumerate(issues, 1):
            print(f"  {i}. {issue}")

        print("\n  📋 แนะนำ action:")
        print("  - date year ไม่ตรง partition → ลบ partition และ re-run pipeline")
        print("  - orphan partition ใน long    → DROP PARTITION แล้ว re-run PART 2")
        print("  - .failed files               → ลบ .failed เพื่อ re-process")
        print(f"  - Hive ไม่ sync HDFS          → MSCK REPAIR TABLE {wide_table};")

    print("\n")
    spark.stop()


if __name__ == "__main__":
    main()