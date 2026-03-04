# -*- coding: utf-8 -*-
# scripts/check_hdfs_integrity.py
#
# ใช้งาน: spark-submit check_hdfs_integrity.py
# หรือ:   python check_hdfs_integrity.py  (ถ้ามี SparkSession อยู่แล้ว)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct
import subprocess

spark = SparkSession.builder \
    .appName("HDFS Integrity Check") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext

# ===== Config (ต้องตรงกับ pipeline) =====
staging_path  = "hdfs://namenode:8020/datalake/staging/finance_itsc_wide"
curated_path  = "hdfs://namenode:8020/datalake/curated/finance_itsc_long"
raw_path      = "hdfs://namenode:8020/datalake/raw/finance_itsc"
wide_table    = "finance_itsc_wide"
long_table    = "finance_itsc_long"
database_name = "default"

issues = []  # รวม issues ทั้งหมดไว้ report ท้าย script


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def ok(msg):
    print(f"  ✅ {msg}")


def warn(msg):
    print(f"  ⚠️  {msg}")
    issues.append(msg)


def err(msg):
    print(f"  ❌ {msg}")
    issues.append(f"[ERROR] {msg}")


def hdfs_ls(path):
    """Return list of subdirectory/file names under path."""
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-ls", path],
            capture_output=True, text=True
        )
        lines = result.stdout.strip().split("\n")
        return [line.split()[-1] for line in lines if line.startswith("d") or line.startswith("-")]
    except Exception:
        return []


# ============================================================
# CHECK 1: Partition directories vs Hive metadata
# ============================================================
section("CHECK 1: Partition directories vs Hive metadata")

try:
    # partitions ใน Hive
    hive_partitions = spark.sql(f"SHOW PARTITIONS {database_name}.{wide_table}") \
        .rdd.flatMap(lambda r: r).collect()
    hive_years = set(int(p.split("=")[1]) for p in hive_partitions if "=" in p)

    # partitions ใน HDFS จริงๆ
    hdfs_entries = hdfs_ls(staging_path)
    hdfs_years = set(
        int(e.split("=")[1])
        for e in hdfs_entries
        if "year=" in e.split("/")[-1]
    )

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
# CHECK 2: year column vs partition ตรงกันไหม (Closure Bug)
# ============================================================
section("CHECK 2: year column vs partition value (ตรวจ Closure Bug)")

try:
    df_wide = spark.sql(f"SELECT * FROM {database_name}.{wide_table}")

    # ใช้ Hive partition pruning — ดึง year จาก partition แล้วเทียบกับ col year
    mismatch = df_wide.filter(col("year") != col("year").cast("int"))

    # วิธีที่แม่นกว่า: group by year แล้วดู distinct values
    year_check = df_wide.groupBy("year").agg(
        count("*").alias("row_count"),
        countDistinct("year").alias("distinct_year_in_partition")
    ).orderBy("year")

    print("\n  📊 Row count per year partition:")
    year_check.show(truncate=False)

    # ตรวจว่า partition ไหนมี row_count = 0
    empty_partitions = year_check.filter(col("row_count") == 0).collect()
    if empty_partitions:
        for row in empty_partitions:
            err(f"Partition year={row['year']} ว่างเปล่า (0 rows)")
    else:
        ok("ทุก partition มีข้อมูล")

    # ตรวจว่าค่า year ใน column ตรงกับ partition จริงๆ
    # โดยเทียบ min/max date ว่าสอดคล้องกับ year ไหม
    date_year_check = df_wide.filter(
        col("date").rlike(r"^\d{4}-\d{2}$")
    ).withColumn(
        "date_year", col("date").substr(1, 4).cast("int")
    ).filter(
        col("date_year") != col("year")
    )

    mismatch_count = date_year_check.count()
    if mismatch_count > 0:
        err(f"พบ {mismatch_count} rows ที่ date year ไม่ตรงกับ partition year — อาจเกิดจาก Closure Bug!")
        print("\n  ตัวอย่าง rows ที่ผิด:")
        date_year_check.select("date", "year").show(20, truncate=False)
    else:
        ok("ค่า date ใน column ตรงกับ partition year ทุก row")

except Exception as e:
    err(f"ไม่สามารถตรวจ year column ได้: {e}")


# ============================================================
# CHECK 3: Wide vs Long row consistency
# ============================================================
section("CHECK 3: Wide vs Long — row consistency per year")

try:
    wide_counts = spark.sql(
        f"SELECT year, COUNT(*) as wide_rows FROM {database_name}.{wide_table} GROUP BY year"
    )
    long_counts = spark.sql(
        f"SELECT year, COUNT(*) as long_rows FROM {database_name}.{long_table} GROUP BY year"
    )

    joined = wide_counts.join(long_counts, on="year", how="full_outer").orderBy("year")

    print("\n  📊 Wide vs Long row count:")
    joined.show(truncate=False)

    # year ที่มีใน wide แต่ไม่มีใน long
    missing_in_long = joined.filter(col("long_rows").isNull()).collect()
    for row in missing_in_long:
        err(f"year={row['year']} มีข้อมูลใน wide แต่ไม่มีใน long table")

    # year ที่มีใน long แต่ไม่มีใน wide (orphan)
    orphan_in_long = joined.filter(col("wide_rows").isNull()).collect()
    for row in orphan_in_long:
        warn(f"year={row['year']} มีข้อมูลใน long แต่ไม่มีใน wide (orphan partition)")

    if not missing_in_long and not orphan_in_long:
        ok("Wide และ Long table มี year partition ตรงกัน")

except Exception as e:
    err(f"ไม่สามารถตรวจ wide vs long ได้: {e}")


# ============================================================
# CHECK 4: .done / .failed file audit
# ============================================================
section("CHECK 4: .done / .failed file audit")

try:
    result = subprocess.run(
        ["hdfs", "dfs", "-find", raw_path, "-name", "*.csv"],
        capture_output=True, text=True
    )
    csv_files = [line.strip() for line in result.stdout.strip().split("\n") if line.endswith(".csv")]

    result_done = subprocess.run(
        ["hdfs", "dfs", "-find", raw_path, "-name", "*.done"],
        capture_output=True, text=True
    )
    done_files = set(line.strip() for line in result_done.stdout.strip().split("\n") if line.endswith(".done"))

    result_failed = subprocess.run(
        ["hdfs", "dfs", "-find", raw_path, "-name", "*.failed"],
        capture_output=True, text=True
    )
    failed_files = set(line.strip() for line in result_failed.stdout.strip().split("\n") if line.endswith(".failed"))

    print(f"\n  📂 CSV files    : {len(csv_files)}")
    print(f"  ✅ .done files  : {len(done_files)}")
    print(f"  ❌ .failed files: {len(failed_files)}")

    # CSV ที่ไม่มี .done และไม่มี .failed = pending (ยังไม่ถูก process)
    pending = [f for f in csv_files if f + ".done" not in done_files and f + ".failed" not in failed_files]
    if pending:
        warn(f"มี {len(pending)} CSV files ที่ยังไม่ถูก process:")
        for f in pending:
            print(f"    - {f}")
    else:
        ok("ทุก CSV file มี .done หรือ .failed แล้ว")

    if failed_files:
        warn(f"มี {len(failed_files)} files ที่ failed — ตรวจสอบและ re-process หรือ clear .failed ถ้าแก้ bug แล้ว:")
        for f in sorted(failed_files):
            print(f"    - {f}")

except Exception as e:
    err(f"ไม่สามารถ audit .done/.failed ได้: {e}")


# ============================================================
# CHECK 5: Long table — null amount audit
# ============================================================
section("CHECK 5: Long table — null/negative amount audit")

try:
    df_long = spark.sql(f"SELECT * FROM {database_name}.{long_table}")

    null_amounts = df_long.filter(col("amount").isNull()).count()
    negative_amounts = df_long.filter(col("amount") < 0).count()
    total = df_long.count()

    print(f"\n  📊 Total rows   : {total:,}")
    print(f"  ⚠️  Null amount  : {null_amounts:,}")
    print(f"  ⚠️  Neg amount   : {negative_amounts:,}")

    if null_amounts > 0:
        warn(f"Long table มี {null_amounts:,} rows ที่ amount เป็น null")
    else:
        ok("ไม่มี null amount ใน long table")

    if negative_amounts > 0:
        warn(f"Long table มี {negative_amounts:,} rows ที่ amount ติดลบ — ควรตรวจว่าถูกต้องหรือไม่")
    else:
        ok("ไม่มี negative amount ใน long table")

except Exception as e:
    err(f"ไม่สามารถตรวจ long table ได้: {e}")


# ============================================================
# SUMMARY
# ============================================================
section("SUMMARY")

if not issues:
    print("\n  🎉 ไม่พบปัญหาใดๆ — ข้อมูลใน HDFS สมบูรณ์!")
else:
    print(f"\n  พบปัญหาทั้งหมด {len(issues)} รายการ:\n")
    for i, issue in enumerate(issues, 1):
        print(f"  {i}. {issue}")

    print("\n  📋 แนะนำ action:")
    print("  - ถ้า date year ไม่ตรง partition → ลบ partition และ re-run pipeline")
    print("  - ถ้ามี orphan partition ใน long → DROP PARTITION แล้ว re-run PART 2")
    print("  - ถ้ามี .failed files → ตรวจ log แล้วลบ .failed เพื่อ re-process")
    print("  - ถ้า Hive ไม่ sync HDFS → รัน: MSCK REPAIR TABLE finance_itsc_wide;")

print("\n")
spark.stop()