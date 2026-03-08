# -*- coding: utf-8 -*-
# engine/pipeline.py
"""
Generic ETL Pipeline Engine
"""

import os
from datetime import datetime
from typing import Dict, List, Set

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyhive import hive as pyhive_hive

from quality_rules.registry import get_rules
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

DATASETS_DIR = os.path.join(os.path.dirname(__file__), "..", "datasets")
HIVE_HOST = os.environ.get("HIVE_HOST", "hive-server")
HIVE_PORT = int(os.environ.get("HIVE_PORT", 10000))


# ── Hive Metadata Sync ────────────────────────────────────────────────────────

def _sync_hive_metadata(spark, ds: DatasetConfig, log) -> None:
    """
    Sync Hive curated table schema → column_metadata table
    - column ใหม่ใน curated → INSERT PARTITION (ds=dataset) เข้า column_metadata
    - column หายไป → log warning เท่านั้น
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        rows = spark.sql(
            f"DESCRIBE {ds.database}.{ds.curated_table}"
        ).collect()
        hive_cols = {
            row["col_name"]: row["data_type"]
            for row in rows
            if row["col_name"]
            and not row["col_name"].startswith("#")
            and row["col_name"].strip()
        }
    except Exception as e:
        log.warning(
            f"[dataset={ds.dataset}] [step=sync_hive_metadata] DESCRIBE failed: {e}"
        )
        return

    try:
        with pyhive_hive.connect(
            host=HIVE_HOST, port=HIVE_PORT, database="default"
        ) as conn:
            cursor = conn.cursor()
            cursor.execute("SET hive.exec.dynamic.partition.mode=nonstrict")

            # อ่าน existing cols จาก partition ds=dataset
            cursor.execute(
                f"SELECT col_name FROM column_metadata WHERE ds = '{ds.dataset}'"
            )
            existing_cols = {row[0] for row in cursor.fetchall()}

            id_cols    = set(ds.id_columns)
            amount_cols = set(ds.amount_columns)
            date_col   = ds.date_column
            partition_col = ds.partition_by

            new_cols = [
                (c, dtype) for c, dtype in hive_cols.items()
                if c not in existing_cols and c != partition_col
            ]
            removed_cols = [
                c for c in existing_cols
                if c not in hive_cols and c != partition_col
            ]

            if not new_cols and not removed_cols:
                log.info(
                    f"[dataset={ds.dataset}] [step=sync_hive_metadata] "
                    "schema ตรงกัน ไม่มีการเปลี่ยนแปลง"
                )
                return

            for col_name, dtype in new_cols:
                cursor.execute(
                    "INSERT INTO column_metadata PARTITION (ds='%s') VALUES "
                    "('%s','%s','%s','(auto-synced from Hive)',%s,%s,%s,TRUE,FALSE,'[]','','%s')"
                    % (
                        ds.dataset,
                        col_name, dtype.upper(), col_name,
                        str(col_name in amount_cols).upper(),
                        str(col_name in id_cols).upper(),
                        str(col_name == date_col).upper(),
                        now,
                    )
                )
                log.info(
                    f"[dataset={ds.dataset}] [step=sync_hive_metadata] "
                    f"เพิ่ม column '{col_name}' ({dtype.upper()})"
                )

            for col_name in removed_cols:
                log.warning(
                    f"[dataset={ds.dataset}] [step=sync_hive_metadata] "
                    f"column '{col_name}' อยู่ใน metadata แต่ไม่มีใน Hive"
                )

    except Exception as e:
        log.warning(
            f"[dataset={ds.dataset}] [step=sync_hive_metadata] failed: {e}"
        )


# ── Main Pipeline ─────────────────────────────────────────────────────────────

def run_pipeline(spark: SparkSession, sc, ds: DatasetConfig) -> None:
    log = setup_logger(f"etl.{ds.dataset}")
    dataset_label = ds.dataset

    raw_path     = f"hdfs://namenode:8020{ds.paths['raw']}"
    staging_path = f"hdfs://namenode:8020{ds.paths['staging']}"
    curated_path = f"hdfs://namenode:8020{ds.paths['curated']}"
    wide_table   = ds.staging_table
    long_table   = ds.curated_table
    database     = ds.database
    id_cols      = ds.id_columns
    exclude_cols = ds.exclude_columns
    date_col     = ds.date_column

    log.info(f"[dataset={dataset_label}] [step=pipeline] START")
    if not ds.has_date:
        log.info(
            f"[dataset={dataset_label}] [step=pipeline] "
            "date_column=None — ข้าม date-dependent transforms"
        )

    # ============================================================
    # PART 1: Raw -> Staging (Wide)
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
                log.warning(
                    f"[dataset={dataset_label}] [step=scan_hdfs] Cannot extract year",
                    file=f,
                )

        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        failed_years: Set[int] = set()

        for year, files in sorted(pending_by_year.items()):

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
                log.error(
                    f"[dataset={dataset_label}] [step=read_csv] FAILED — skipping year",
                    year=year, error=str(e),
                )
                failed_years.add(year)
                continue

            dq_report = {}
            try:
                with step_log(log, "data_quality", dataset=dataset_label, year=year) as ctx:
                    rules = get_rules(ds)
                    dq_passed, dq_report = with_retry(
                        lambda: rules.run_checks(df, files[0]),
                        label=f"data quality year={year}",
                    )
                    ctx["passed"] = dq_passed
            except Exception as e:
                log.error(
                    f"[dataset={dataset_label}] [step=data_quality] FAILED — skipping year",
                    year=year, error=str(e),
                )
                for f in files:
                    hdfs_touch(sc, f + ".failed")
                send_quality_alert(files[0], dq_report)
                failed_years.add(year)
                continue

            if not dq_passed:
                log.error(
                    f"[dataset={dataset_label}] [step=data_quality] DQ failed",
                    year=year,
                )
                for f in files:
                    hdfs_touch(sc, f + ".failed")
                send_quality_alert(files[0], dq_report)
                failed_years.add(year)
                continue

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
                            label=f"checksum {f.split('/')[-1]}",
                        )
                        with_retry(
                            hdfs_write_done, sc, f, checksum,
                            label=f"write .done {f.split('/')[-1]}",
                        )

                with step_log(log, "versioning", dataset=dataset_label, year=year) as ctx:
                    version_id = create_version(sc, df, files[0], year)
                    cleanup_old_versions(sc, year)
                    ctx["version"] = version_id

            except Exception as e:
                log.error(
                    f"[dataset={dataset_label}] [step=atomic_write] FAILED",
                    year=year, error=str(e),
                )
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
            # ถ้า long table ไม่มี → ถือว่าทุก staging year ยังไม่ได้ process
            try:
                curated_years = set(
                    int(row[0].split("=")[1])
                    for row in spark.sql(
                        f"SHOW PARTITIONS {database}.{long_table}"
                    ).collect()
                    if "year=" in row[0]
                )
            except Exception:
                curated_years = set()

            incomplete = staging_years - curated_years
            if incomplete:
                years_to_update |= incomplete
            ctx["staging_years"]    = sorted(staging_years)
            ctx["incomplete_years"] = sorted(incomplete)
    except Exception as e:
        log.warning(
            f"[dataset={dataset_label}] [step=check_partitions] Cannot check",
            error=str(e),
        )

    years_to_update = sorted(years_to_update)

    if not years_to_update:
        log.info(f"[dataset={dataset_label}] [step=pipeline] No new data — skipping Part 2")
    else:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        curated_written = False

        for year in years_to_update:

            try:
                with step_log(log, "read_wide", dataset=dataset_label, year=year) as ctx:
                    _date_col = date_col

                    def _read_wide(_year=year, _dc=_date_col):
                        df = (spark.read
                              .option("mergeSchema", "true")
                              .parquet(f"{staging_path}/year={_year}"))
                        if _dc and _dc in df.columns:
                            df = df.filter(
                                col(_dc).rlike(r"^\d{4}-\d{2}")  # 2024-01 หรือ 2024-01-xx
                                | (col(_dc) == "all-year-budget")
                            )
                        return df

                    df_wide = with_retry(_read_wide, label=f"read wide year={year}")
                    ctx["columns"] = len(df_wide.columns)
            except Exception as e:
                log.error(
                    f"[dataset={dataset_label}] [step=read_wide] FAILED — skipping year",
                    year=year, error=str(e),
                )
                continue

            try:
                with step_log(log, "transform", dataset=dataset_label, year=year) as ctx:
                    def _transform(_df=df_wide, _id=id_cols, _ex=exclude_cols):
                        amount_cols_t = [c for c in _df.columns if c not in _id + _ex]
                        if not amount_cols_t:
                            raise ValueError("ไม่มี amount columns ให้ transform")
                        df_casted = _df.select(
                            *[c for c in _id if c in _df.columns],
                            *[col(c).cast("double").alias(c) for c in amount_cols_t],
                        )
                        id_in_df = [c for c in _id if c in _df.columns]
                        stack_expr = ", ".join([f"'{c}', `{c}`" for c in amount_cols_t])
                        id_exprs   = [f"`{c}`" for c in id_in_df]
                        return df_casted.selectExpr(
                            *id_exprs,
                            f"stack({len(amount_cols_t)}, {stack_expr}) as (category, amount)",
                        ).filter("amount is not null")

                    df_long = with_retry(_transform, label=f"transform wide->long year={year}")
                    ctx["rows"] = df_long.count()
            except Exception as e:
                log.error(
                    f"[dataset={dataset_label}] [step=transform] FAILED — skipping year",
                    year=year, error=str(e),
                )
                continue

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
                    curated_written = True
            except Exception as e:
                log.error(
                    f"[dataset={dataset_label}] [step=write_curated] FAILED",
                    year=year, error=str(e),
                )

        if curated_written:
            with step_log(log, "sync_hive_metadata", dataset=dataset_label):
                _sync_hive_metadata(spark, ds, log)

    log.info(f"[dataset={dataset_label}] [step=pipeline] ETL completed")