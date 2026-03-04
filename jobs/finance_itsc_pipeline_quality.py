# -*- coding: utf-8 -*-
# jobs/finance_itsc_pipeline_quality.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from typing import List, Dict

from data_quality import run_quality_checks
from utils.hdfs import hdfs_ls_recursive, hdfs_touch, extract_year_from_path
from utils.alerts import send_quality_alert
from utils.retry import atomic_write_table, with_retry
from utils.versioning import create_version, cleanup_old_versions
from logger import setup_logger

log = setup_logger("etl")

spark = SparkSession.builder \
    .appName("Finance ITSC ETL") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext

# ===== Config =====
raw_path      = "hdfs://namenode:8020/datalake/raw/finance-itsc"
staging_path  = "hdfs://namenode:8020/datalake/staging/finance-itsc_wide"
curated_path  = "hdfs://namenode:8020/datalake/curated/finance-itsc_long"
wide_table    = "finance_itsc_wide"
long_table    = "finance_itsc_long"
database_name = "default"


# ============================================================
# PART 1: Raw -> Staging (Wide) — Incremental
# ============================================================
log.info("PART 1 started: Raw -> Staging (Wide) — Incremental")

# ── Step 1: Scan HDFS ────────────────────────────────────────
all_files = with_retry(hdfs_ls_recursive, sc, raw_path, label="scan HDFS")

csv_files    = [f for f in all_files if f.endswith(".csv")]
done_files   = set(f for f in all_files if f.endswith(".done"))
failed_files = set(f for f in all_files if f.endswith(".failed"))

pending_files = [
    f for f in csv_files
    if f + ".done" not in done_files and f + ".failed" not in failed_files
]

log.info(
    "File scan complete",
    csv_found=len(csv_files),
    already_processed=len([f for f in csv_files if f + ".done" in done_files]),
    dq_failed=len([f for f in csv_files if f + ".failed" in failed_files]),
    pending=len(pending_files),
)

pending_by_year: Dict[int, List[str]] = {}

if not pending_files:
    log.info("No new files to process — skipping Part 1")
else:
    for f in pending_files:
        year = extract_year_from_path(f)
        if year:
            pending_by_year.setdefault(year, []).append(f)
        else:
            log.warning("Cannot extract year from file — skipping", file=f)

    log.info("Years to update", years=sorted(pending_by_year.keys()))
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # FIX 3: ใช้ failed_years แทน del pending_by_year ใน loop
    failed_years = set()

    for year, files in sorted(pending_by_year.items()):
        log.info("Processing year", year=year, files=len(files))

        # ── Step 2: Read CSV ─────────────────────────────────
        try:
            # FIX 1: แก้ closure bug โดยผูก year และ files เป็น default args
            def _read_csv(_files=files, _year=year):
                # ปิด inferSchema เพื่อป้องกัน date/details ถูก infer เป็น double
                # แล้ว cast เองทั้งหมด
                df = spark.read.option("header", "true").option("inferSchema", "false").csv(_files)
                df = df.withColumn("year", lit(_year).cast("int"))
                for c in df.columns:
                    if c in ["date", "details"]:
                        df = df.withColumn(c, col(c).cast("string"))
                    elif c != "year":
                        df = df.withColumn(c, col(c).cast("double"))
                return df

            df = with_retry(_read_csv, label=f"read CSV year={year}")
        except Exception as e:
            log.error("Failed to read CSV — skipping year", year=year, error=str(e))
            failed_years.add(year)
            continue

        # ── Step 3: Data Quality ──────────────────────────────
        dq_report = {}
        try:
            # FIX 2: with_retry ไม่รับ positional args ของ fn — ใช้ lambda แทน
            dq_passed, dq_report = with_retry(
                lambda: run_quality_checks(df, files[0]),
                label=f"data quality year={year}"
            )
        except Exception as e:
            log.error("Data quality check error — skipping year", year=year, error=str(e))
            for f in files:
                hdfs_touch(sc, f + ".failed")
            send_quality_alert(files[0], dq_report)
            failed_years.add(year)
            continue

        if not dq_passed:
            log.error("DQ failed — skipping load", year=year)
            for f in files:
                hdfs_touch(sc, f + ".failed")
            send_quality_alert(files[0], dq_report)
            failed_years.add(year)
            continue

        # ── Step 4: Atomic Write ──────────────────────────────
        try:
            from utils.schema_evolution import sync_schema
            df = sync_schema(spark, df, wide_table, database_name, skip_cols=["year"])

            atomic_write_table(
                df=df,
                table_path=staging_path,
                table_name=wide_table,
                database=database_name,
                partition_col="year",
                partition_value=year,
            )
            for f in files:
                with_retry(hdfs_touch, sc, f + ".done", label=f"touch .done {f}")

            # ===== VERSIONING =====
            create_version(sc, df, files[0], year)
            cleanup_old_versions(sc, year)

            log.info("Year written to staging", year=year)
        except Exception as e:
            log.error("Failed to write staging after retries — skipping", year=year, error=str(e))
            failed_years.add(year)
            continue

    # FIX 3: กรอง pending_by_year โดยเอาเฉพาะ year ที่สำเร็จ
    for year in failed_years:
        pending_by_year.pop(year, None)

log.info("PART 1 completed")


# ============================================================
# PART 2: Staging (Wide) -> Curated (Long)
# ============================================================
log.info("PART 2 started: Staging (Wide) -> Curated (Long)")

years_to_update = set(pending_by_year.keys())

# เพิ่ม years ที่อยู่ใน staging แล้วแต่ยังไม่มีใน curated
# (กรณี PART 2 fail แล้ว re-run — PART 1 จะ skip แต่ PART 2 ต้องรันต่อได้)
try:
    staging_years = set(
        int(row[0].split("=")[1])
        for row in spark.sql(f"SHOW PARTITIONS {database_name}.{wide_table}").collect()
        if "year=" in row[0]
    )
    curated_years = set(
        int(row[0].split("=")[1])
        for row in spark.sql(f"SHOW PARTITIONS {database_name}.{long_table}").collect()
        if "year=" in row[0]
    )
    incomplete_years = staging_years - curated_years
    if incomplete_years:
        log.info("พบ years ใน staging แต่ไม่มีใน curated — เพิ่มเข้า queue", years=sorted(incomplete_years))
        years_to_update |= incomplete_years
except Exception as e:
    log.warning("ไม่สามารถตรวจ staging/curated years ได้", error=str(e))

years_to_update = sorted(years_to_update)

if not years_to_update:
    log.info("No new data — skipping Part 2")
else:
    id_columns      = ["date", "details", "year"]
    exclude_columns = ["total_amount"]
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    for year in years_to_update:
        log.info("Converting wide -> long", year=year)

        # ── Step 5: Read Wide ─────────────────────────────────
        try:
            # FIX 1: แก้ closure bug โดยผูก year เป็น default arg
            # อ่านตรงจาก parquet แทน spark.sql เพื่อหลีกเลี่ยง Hive schema type mismatch
            def _read_wide(_year=year):
                df = spark.read.option("mergeSchema", "true").parquet(f"{staging_path}/year={_year}")
                return df.filter(
                    col("date").rlike(r"^\d{4}-\d{2}$") | (col("date") == "all-year-budget")
                )

            df_wide = with_retry(_read_wide, label=f"read wide year={year}")
        except Exception as e:
            log.error("Failed to read wide table — skipping year", year=year, error=str(e))
            continue

        # ── Step 6: Transform Wide -> Long ────────────────────
        try:
            # FIX 1: แก้ closure bug โดยผูก df_wide เป็น default arg
            def _transform(_df_wide=df_wide, _id_columns=id_columns, _exclude_columns=exclude_columns):
                amount_cols = [c for c in _df_wide.columns if c not in _id_columns + _exclude_columns]

                # cast ทุก amount column เป็น double ก่อน เพราะ schema evolution อาจเพิ่ม STRING column
                df_casted = _df_wide.select(
                    *_id_columns,
                    *[col(c).cast("double").alias(c) for c in amount_cols]
                )

                stack_expr = ", ".join([f"'{c}', `{c}`" for c in amount_cols])
                id_exprs = [f"`{c}`" for c in _id_columns]
                return df_casted.selectExpr(
                    *id_exprs,
                    f"stack({len(amount_cols)}, {stack_expr}) as (category, amount)"
                ).filter("amount is not null")

            df_long = with_retry(_transform, label=f"transform wide->long year={year}")
        except Exception as e:
            log.error("Failed to transform wide->long — skipping year", year=year, error=str(e))
            continue

        long_rows = df_long.count()

        # ── Step 7: Atomic Write ──────────────────────────────
        try:
            atomic_write_table(
                df=df_long,
                table_path=curated_path,
                table_name=long_table,
                database=database_name,
                partition_col="year",
                partition_value=year,
            )
            log.info("Long table updated", year=year, rows=long_rows)
        except Exception as e:
            log.error("Failed to write curated after retries", year=year, error=str(e))

log.info("ETL Pipeline completed")