# -*- coding: utf-8 -*-
# scripts/fix_hdfs_integrity.py
#
# ใช้งาน: spark-submit fix_hdfs_integrity.py
#
# FIX ที่ทำ:
#   1. MSCK REPAIR TABLE เพื่อ sync Hive metastore กับ HDFS จริง
#   2. ลบ partition ที่มี date year ไม่ตรง แล้วลบ .done เพื่อ re-process
#   3. แก้ CHECK 3 (.done audit) ให้ใช้ Spark แทน subprocess

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Fix HDFS Integrity") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext

# ===== Config =====
staging_path  = "hdfs://namenode:8020/datalake/staging/finance_itsc_wide"
curated_path  = "hdfs://namenode:8020/datalake/curated/finance_itsc_long"
raw_path      = "hdfs://namenode:8020/datalake/raw/finance_itsc"
wide_table    = "finance_itsc_wide"
long_table    = "finance_itsc_long"
database_name = "default"


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def hdfs_delete(path, recursive=False):
    URI = sc._jvm.java.net.URI
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    conf = sc._jsc.hadoopConfiguration()
    fs = FileSystem.get(URI(path.split("/user")[0] if "/user" in path else "hdfs://namenode:8020"), conf)
    return fs.delete(Path(path), recursive)


def hdfs_ls(path):
    """List files/dirs under path using Spark's Hadoop FileSystem API."""
    URI = sc._jvm.java.net.URI
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    conf = sc._jsc.hadoopConfiguration()
    fs = FileSystem.get(URI("hdfs://namenode:8020"), conf)
    try:
        statuses = fs.listStatus(Path(path))
        return [str(s.getPath()) for s in statuses]
    except Exception:
        return []


def hdfs_find_recursive(path, suffix):
    """Recursively find files ending with suffix."""
    results = []
    entries = hdfs_ls(path)
    for entry in entries:
        name = entry.split("/")[-1]
        if name.endswith(suffix):
            results.append(entry)
        elif "." not in name:  # likely a directory
            results.extend(hdfs_find_recursive(entry, suffix))
    return results


# ============================================================
# FIX 1: MSCK REPAIR — sync Hive metastore กับ HDFS
# ============================================================
section("FIX 1: MSCK REPAIR TABLE")

try:
    print(f"  Running MSCK REPAIR on {wide_table}...")
    spark.sql(f"MSCK REPAIR TABLE {database_name}.{wide_table}")
    print(f"  ✅ {wide_table} repaired")
except Exception as e:
    print(f"  ❌ MSCK REPAIR {wide_table} failed: {e}")

try:
    print(f"  Running MSCK REPAIR on {long_table}...")
    spark.sql(f"MSCK REPAIR TABLE {database_name}.{long_table}")
    print(f"  ✅ {long_table} repaired")
except Exception as e:
    print(f"  ❌ MSCK REPAIR {long_table} failed: {e}")

# แสดง partitions หลัง repair
print("\n  📊 Partitions หลัง REPAIR:")
spark.sql(f"SHOW PARTITIONS {database_name}.{wide_table}").show(truncate=False)


# ============================================================
# FIX 2: ลบ partition ที่มี date year ไม่ตรง (Closure Bug)
# ============================================================
section("FIX 2: Clean up partitions ที่ผิดจาก Closure Bug")

try:
    df_wide = spark.sql(f"SELECT DISTINCT year FROM {database_name}.{wide_table}")
    all_years = [row["year"] for row in df_wide.collect()]

    bad_years = []
    for year in all_years:
        df_year = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {database_name}.{wide_table} "
            f"WHERE year = {year} "
            f"AND date RLIKE '^[0-9]{{4}}-[0-9]{{2}}$' "
            f"AND CAST(SUBSTR(date,1,4) AS INT) != {year}"
        )
        cnt = df_year.collect()[0]["cnt"]
        if cnt > 0:
            bad_years.append(year)
            print(f"  ❌ year={year} มี {cnt} rows ที่ date ไม่ตรง — จะลบ partition นี้")

    if not bad_years:
        print("  ✅ ไม่พบ partition ที่ผิด")
    else:
        for year in bad_years:
            try:
                # ลบ partition ใน Hive
                spark.sql(
                    f"ALTER TABLE {database_name}.{wide_table} DROP IF EXISTS PARTITION (year={year})"
                )
                spark.sql(
                    f"ALTER TABLE {database_name}.{long_table} DROP IF EXISTS PARTITION (year={year})"
                )

                # ลบ HDFS directory
                hdfs_delete(f"{staging_path}/year={year}", recursive=True)
                hdfs_delete(f"{curated_path}/year={year}", recursive=True)

                print(f"  🗑️  ลบ partition year={year} ออกจาก wide, long และ HDFS แล้ว")
            except Exception as e:
                print(f"  ❌ ลบ partition year={year} ไม่สำเร็จ: {e}")

        # ลบ .done files เพื่อให้ pipeline re-process
        print(f"\n  🔄 ลบ .done files ของ years {bad_years} เพื่อ re-process...")
        done_files = hdfs_find_recursive(raw_path, ".done")
        removed = 0
        for f in done_files:
            for year in bad_years:
                if f"/{year}/" in f or f"_{year}_" in f or f"-{year}-" in f:
                    try:
                        hdfs_delete(f, recursive=False)
                        print(f"    🗑️  ลบ {f}")
                        removed += 1
                    except Exception as e:
                        print(f"    ❌ ลบ {f} ไม่สำเร็จ: {e}")
        print(f"  ลบ .done files ทั้งหมด {removed} files")

except Exception as e:
    print(f"  ❌ Fix 2 failed: {e}")


# ============================================================
# FIX 3: .done / .failed audit ด้วย Spark (แทน subprocess)
# ============================================================
section("FIX 3: .done / .failed audit (via Spark FileSystem API)")

try:
    csv_files    = hdfs_find_recursive(raw_path, ".csv")
    done_files   = set(hdfs_find_recursive(raw_path, ".done"))
    failed_files = set(hdfs_find_recursive(raw_path, ".failed"))

    print(f"\n  📂 CSV files    : {len(csv_files)}")
    print(f"  ✅ .done files  : {len(done_files)}")
    print(f"  ❌ .failed files: {len(failed_files)}")

    pending = [f for f in csv_files if f + ".done" not in done_files and f + ".failed" not in failed_files]
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
section("VERIFY: ตรวจสอบหลัง fix")

try:
    print("\n  📊 Wide table partitions:")
    spark.sql(f"SHOW PARTITIONS {database_name}.{wide_table}").show(truncate=False)

    print("  📊 Row count per year (wide):")
    spark.sql(
        f"SELECT year, COUNT(*) as rows FROM {database_name}.{wide_table} GROUP BY year ORDER BY year"
    ).show(truncate=False)

    print("  📊 Row count per year (long):")
    spark.sql(
        f"SELECT year, COUNT(*) as rows FROM {database_name}.{long_table} GROUP BY year ORDER BY year"
    ).show(truncate=False)

except Exception as e:
    print(f"  ❌ Verify failed: {e}")


print("\n  ✅ Fix script เสร็จสิ้น — รัน pipeline ใหม่ได้เลยครับ\n")
spark.stop()