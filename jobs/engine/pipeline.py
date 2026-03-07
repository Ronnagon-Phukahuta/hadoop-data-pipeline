# -*- coding: utf-8 -*-
# engine/pipeline.py
"""
Generic ETL Pipeline Engine
รับ DatasetConfig แทนการ hardcode finance_itsc

Usage:
    from engine.pipeline import run_pipeline
    from datasets.registry import load_dataset

    ds = load_dataset("finance_itsc")
    run_pipeline(spark, sc, ds)
"""

from typing import Dict, List, Set
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit

from data_quality import run_quality_checks
from utils.hdfs import (
    hdfs_ls_recursive, hdfs_touch, hdfs_write_done,
    is_already_processed, compute_file_checksum, extract_year_from_path,
)
from utils.alerts import send_quality_alert
from utils.retry import atomic_write_table, with_retry
from utils.versioning import create_version, cleanup_old_versions
from utils.schema_evolution import sync_schema
from logger import setup_logger, step_log
from datasets.registry import DatasetConfig


def run_pipeline(spark: SparkSession, sc, ds: DatasetConfig) -> None:
    """
    รัน ETL pipeline ทั้งหมดสำหรับ dataset ที่กำหนด

    Args:
        spark: SparkSession
        sc:    SparkContext
        ds:    DatasetConfig จาก registry
    """
    log = setup_logger(f"etl.{ds.dataset}")
    dataset_label = ds.dataset

    # ── Paths & Tables จาก registry ─────────────────────────
    raw_path     = f"hdfs://namenode:8020{ds.paths['raw']}"
    staging_path = f"hdfs://namenode:8020{ds.paths['staging']}"
    curated_path = f"hdfs://namenode:8020{ds.paths['curated']}"
    wide_table   = ds.staging_table
    long_table   = ds.curated_table
    database     = ds.database
    id_cols      = ds.id_columns
    exclude_cols = ds.exclude_columns

    log.info(f"[dataset={dataset_label}] [step=pipeline] START")

    # ============================================================
    # PART 1: Raw -> Staging (Wide) — Incremental
    # ============================================================
    pending_by_year: Dict[int, List[str]] = {}

    with step_log(log, "scan_hdfs", dataset=dataset_label) as ctx:
        all_files = with_retry(hdfs_ls_recursive, sc, raw_path, label="scan HDFS")
        csv_files    = [f for f in all_files if f.endswith(".csv")]
        failed_files = set(f for f in all_files if f.endswith(".failed"))

        pending_files = [
            f for f in csv_files
            if f + ".failed" not in failed_files
            and not is_already_processed(sc, f)
        ]

        ctx["csv_found"]    = len(csv_files)
        ctx["pending"]      = len(pending_files)
        ctx["dq_failed"]    = len([f for f in csv_files if f + ".failed" in failed_files])
        ctx["already_done"] = len(csv_files) - len(pending_files) - ctx["dq_failed"]

    if not pending_files:
        log.info(f"[dataset={dataset_label}] [step=pipeline] No new files — skipping Part 1")
    else:
        for f in pending_files:
            year = extract_year_from_path(f)
            if year:
                pending_by_year.setdefault(year, []).append(f)
            else:
                log.warning(f"[dataset={dataset_label}] [step=scan_hdfs] Cannot extract year",
                            file=f)

        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        failed_years: Set[int] = set()

        for year, files in sorted(pending_by_year.items()):

            # ── Read CSV ─────────────────────────────────────
            try:
                with step_log(log, "read_csv", dataset=dataset_label,
                              year=year, files=len(files)) as ctx:
                    def _read_csv(_files=files, _year=year, _id=id_cols):
                        df = (spark.read
                              .option("header", "true")
                              .option("inferSchema", "false")
                              .csv(_files))
                        df = df.withColumn("year", lit(_year).cast("int"))
                        for c in df.columns:
                            if c in _id:
                                df = df.withColumn(c, col(c).cast("string"))
                            elif c != "year":
                                df = df.withColumn(c, col(c).cast("double"))
                        return df

                    df = with_retry(_read_csv, label=f"read CSV year={year}")
                    ctx["columns"] = len(df.columns)
            except Exception as e:
                log.error(f"[dataset={dataset_label}] [step=read_csv] FAILED — skipping year",
                          year=year, error=str(e))
                failed_years.add(year)
                continue

            # ── Data Quality ──────────────────────────────────
            dq_report = {}
            try:
                with step_log(log, "data_quality", dataset=dataset_label, year=year) as ctx:
                    dq_passed, dq_report = with_retry(
                        lambda: run_quality_checks(df, files[0]),
                        label=f"data quality year={year}"
                    )
                    ctx["passed"] = dq_passed
            except Exception as e:
                log.error(f"[dataset={dataset_label}] [step=data_quality] FAILED — skipping year",
                          year=year, error=str(e))
                for f in files:
                    hdfs_touch(sc, f + ".failed")
                send_quality_alert(files[0], dq_report)
                failed_years.add(year)
                continue

            if not dq_passed:
                log.error(f"[dataset={dataset_label}] [step=data_quality] DQ failed — skipping load",
                          year=year)
                for f in files:
                    hdfs_touch(sc, f + ".failed")
                send_quality_alert(files[0], dq_report)
                failed_years.add(year)
                continue

            # ── Atomic Write ──────────────────────────────────
            try:
                with step_log(log, "atomic_write", dataset=dataset_label, year=year) as ctx:
                    df = sync_schema(spark, df, wide_table, database, skip_cols=["year"])
                    atomic_write_table(
                        df=df,
                        table_path=staging_path,
                        table_name=wide_table,
                        database=database,
                        partition_col="year",
                        partition_value=year,
                    )
                    ctx["rows"] = df.count()

                with step_log(log, "write_done", dataset=dataset_label, year=year):
                    for f in files:
                        checksum = with_retry(
                            compute_file_checksum, sc, f,
                            label=f"checksum {f.split('/')[-1]}"
                        )
                        with_retry(
                            hdfs_write_done, sc, f, checksum,
                            label=f"write .done {f.split('/')[-1]}"
                        )

                with step_log(log, "versioning", dataset=dataset_label, year=year) as ctx:
                    version_id = create_version(sc, df, files[0], year)
                    cleanup_old_versions(sc, year)
                    ctx["version"] = version_id

            except Exception as e:
                log.error(f"[dataset={dataset_label}] [step=atomic_write] FAILED — skipping year",
                          year=year, error=str(e))
                failed_years.add(year)
                continue

        for year in failed_years:
            pending_by_year.pop(year, None)

    log.info(f"[dataset={dataset_label}] [step=pipeline] PART 1 completed")

    # ============================================================
    # PART 2: Staging (Wide) -> Curated (Long)
    # ============================================================
    log.info(f"[dataset={dataset_label}] [step=pipeline] PART 2 started")

    years_to_update = set(pending_by_year.keys())

    try:
        with step_log(log, "check_partitions", dataset=dataset_label) as ctx:
            staging_years = set(
                int(row[0].split("=")[1])
                for row in spark.sql(
                    f"SHOW PARTITIONS {database}.{wide_table}"
                ).collect()
                if "year=" in row[0]
            )
            curated_years = set(
                int(row[0].split("=")[1])
                for row in spark.sql(
                    f"SHOW PARTITIONS {database}.{long_table}"
                ).collect()
                if "year=" in row[0]
            )
            incomplete_years = staging_years - curated_years
            if incomplete_years:
                years_to_update |= incomplete_years
            ctx["staging_years"]    = sorted(staging_years)
            ctx["incomplete_years"] = sorted(incomplete_years)
    except Exception as e:
        log.warning(f"[dataset={dataset_label}] [step=check_partitions] Cannot check partitions",
                    error=str(e))

    years_to_update = sorted(years_to_update)

    if not years_to_update:
        log.info(f"[dataset={dataset_label}] [step=pipeline] No new data — skipping Part 2")
    else:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        for year in years_to_update:

            # ── Read Wide ─────────────────────────────────────
            try:
                with step_log(log, "read_wide", dataset=dataset_label, year=year) as ctx:
                    def _read_wide(_year=year):
                        df = (spark.read
                              .option("mergeSchema", "true")
                              .parquet(f"{staging_path}/year={_year}"))
                        return df.filter(
                            col("date").rlike(r"^\d{4}-\d{2}$")
                            | (col("date") == "all-year-budget")
                        )
                    df_wide = with_retry(_read_wide, label=f"read wide year={year}")
                    ctx["columns"] = len(df_wide.columns)
            except Exception as e:
                log.error(f"[dataset={dataset_label}] [step=read_wide] FAILED — skipping year",
                          year=year, error=str(e))
                continue

            # ── Transform Wide -> Long ────────────────────────
            try:
                with step_log(log, "transform", dataset=dataset_label, year=year) as ctx:
                    def _transform(_df=df_wide, _id=id_cols, _ex=exclude_cols):
                        amount_cols = [c for c in _df.columns if c not in _id + _ex]
                        df_casted = _df.select(
                            *_id,
                            *[col(c).cast("double").alias(c) for c in amount_cols]
                        )
                        stack_expr = ", ".join([f"'{c}', `{c}`" for c in amount_cols])
                        id_exprs   = [f"`{c}`" for c in _id]
                        return df_casted.selectExpr(
                            *id_exprs,
                            f"stack({len(amount_cols)}, {stack_expr}) as (category, amount)"
                        ).filter("amount is not null")

                    df_long = with_retry(_transform, label=f"transform wide->long year={year}")
                    ctx["rows"] = df_long.count()
            except Exception as e:
                log.error(f"[dataset={dataset_label}] [step=transform] FAILED — skipping year",
                          year=year, error=str(e))
                continue

            # ── Write Curated ─────────────────────────────────
            try:
                with step_log(log, "write_curated", dataset=dataset_label, year=year) as ctx:
                    atomic_write_table(
                        df=df_long,
                        table_path=curated_path,
                        table_name=long_table,
                        database=database,
                        partition_col="year",
                        partition_value=year,
                    )
                    ctx["rows"] = df_long.count()
            except Exception as e:
                log.error(f"[dataset={dataset_label}] [step=write_curated] FAILED",
                          year=year, error=str(e))

    log.info(f"[dataset={dataset_label}] [step=pipeline] ETL completed")