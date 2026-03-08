# jobs/utils/retry.py
"""
Retry + Atomic write utility สำหรับ Spark ETL
"""

import os
import time
from typing import Callable, Any
from pyspark.sql import DataFrame

from logger import get_logger

log = get_logger(__name__)

MAX_RETRIES = int(os.getenv("ETL_MAX_RETRIES", 3))
RETRY_DELAY = int(os.getenv("ETL_RETRY_DELAY", 5))


def with_retry(fn: Callable, *args, label: str = "", max_retries: int = MAX_RETRIES, delay: int = RETRY_DELAY, **kwargs) -> Any:
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            result = fn(*args, **kwargs)
            if attempt > 1:
                log.info("Retry succeeded", label=label, attempt=attempt)
            return result
        except Exception as e:
            last_error = e
            wait = delay * (2 ** (attempt - 1))
            if attempt < max_retries:
                log.warning(
                    "Step failed — retrying",
                    label=label, attempt=attempt,
                    max_retries=max_retries, wait_seconds=wait, error=str(e),
                )
                time.sleep(wait)
            else:
                log.error(
                    "All retries exhausted",
                    label=label, attempt=attempt,
                    max_retries=max_retries, error=str(e),
                )
    raise last_error


def _table_exists(spark, database: str, table_name: str) -> bool:
    try:
        rows = spark.sql(f"SHOW TABLES IN {database}").collect()
        return table_name in [r[1] for r in rows]
    except Exception:
        return False


def _create_table_from_parquet(
    spark,
    partition_path: str,
    table_path: str,
    table_name: str,
    database: str,
    partition_col: str,
):
    """CREATE EXTERNAL TABLE จาก schema ของ parquet"""
    sample = spark.read.parquet(partition_path)
    col_defs = ", ".join(
        f"`{f.name}` {f.dataType.simpleString()}"
        for f in sample.schema
        if f.name != partition_col
    )
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table_name} (
            {col_defs}
        )
        PARTITIONED BY ({partition_col} INT)
        STORED AS PARQUET
        LOCATION '{table_path}'
    """)
    log.info("Table created from parquet schema", table=f"{database}.{table_name}")


def _register_partition_via_pyhive(
    table_name: str,
    database: str,
    partition_col: str,
    partition_value: Any,
    partition_path: str,
):
    """Register partition ผ่าน pyhive โดยตรงเพื่อให้ Hive metastore รู้จักทันที"""
    from pyhive import hive as pyhive_hive
    hive_host = os.environ.get("HIVE_HOST", "hive-server")
    hive_port = int(os.environ.get("HIVE_PORT", 10000))

    with pyhive_hive.connect(host=hive_host, port=hive_port, database="default") as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"ALTER TABLE {database}.{table_name} "
            f"DROP IF EXISTS PARTITION ({partition_col}={partition_value})"
        )
        cursor.execute(
            f"ALTER TABLE {database}.{table_name} "
            f"ADD PARTITION ({partition_col}={partition_value}) "
            f"LOCATION '{partition_path}'"
        )


def atomic_write_table(
    df: DataFrame,
    table_path: str,
    table_name: str,
    database: str,
    partition_col: str = "year",
    partition_value: Any = None,
    max_retries: int = MAX_RETRIES,
):
    """Atomic write ด้วย swap pattern เฉพาะ partition ที่เปลี่ยน"""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    partition_path = f"{table_path}/{partition_col}={partition_value}"
    tmp_path = f"{partition_path}_tmp"

    def _do_write():
        log.info("Writing to temp partition",
                 tmp_path=tmp_path, table=table_name, partition_value=partition_value)
        df.write.mode("overwrite").parquet(tmp_path)

        log.info("Swapping partition", table=table_name, partition_value=partition_value)
        _hdfs_swap(sc, tmp_path, partition_path)

        if not _table_exists(spark, database, table_name):
            log.info("Table not found — creating", table=f"{database}.{table_name}")
            _create_table_from_parquet(
                spark, partition_path, table_path,
                table_name, database, partition_col,
            )

        # Register partition ผ่าน pyhive โดยตรง (reliable กว่า spark.sql)
        try:
            _register_partition_via_pyhive(
                table_name, database, partition_col, partition_value, partition_path
            )
            log.info("Partition registered via pyhive",
                     table=table_name, partition_value=partition_value)
        except Exception as e:
            log.warning("pyhive partition register failed — falling back to spark.sql",
                        error=str(e))
            spark.sql(
                f"ALTER TABLE {database}.{table_name} "
                f"DROP IF EXISTS PARTITION ({partition_col}={partition_value})"
            )
            spark.sql(
                f"ALTER TABLE {database}.{table_name} "
                f"ADD PARTITION ({partition_col}={partition_value}) "
                f"LOCATION '{partition_path}'"
            )

        log.info("Atomic write completed", table=table_name, partition_value=partition_value)

    def _cleanup_tmp():
        try:
            _hdfs_delete(sc, tmp_path)
        except Exception as e:
            log.warning("Could not clean up temp partition", tmp_path=tmp_path, error=str(e))

    try:
        with_retry(_do_write, label=f"write {table_name} partition={partition_value}",
                   max_retries=max_retries)
    except Exception as e:
        log.error("Atomic write failed — cleaning up",
                  table=table_name, partition_value=partition_value, error=str(e))
        _cleanup_tmp()
        raise


def _hdfs_swap(sc, src: str, dst: str):
    from utils.soft_delete import safe_delete

    fs = _get_fs(sc)
    src_path = sc._jvm.org.apache.hadoop.fs.Path(src)
    dst_path = sc._jvm.org.apache.hadoop.fs.Path(dst)
    old_path = sc._jvm.org.apache.hadoop.fs.Path(f"{dst}_old")

    if fs.exists(dst_path):
        if fs.exists(old_path):
            safe_delete(sc, f"{dst}_old")
            log.warning("Stale _old partition moved to trash before backup", path=f"{dst}_old")
        success = fs.rename(dst_path, old_path)
        if not success:
            raise RuntimeError(f"HDFS backup failed: {dst} -> {dst}_old")
        log.info("Partition backed up", src=dst, backup=f"{dst}_old")

    success = fs.rename(src_path, dst_path)
    if not success:
        if fs.exists(old_path):
            fs.rename(old_path, dst_path)
            log.warning("Rename failed — rolled back from backup", dst=dst)
        raise RuntimeError(f"HDFS rename failed: {src} -> {dst}")

    log.info("Partition swapped", new=dst)

    if fs.exists(old_path):
        trash_path = safe_delete(sc, f"{dst}_old")
        log.info("Backup moved to trash", path=f"{dst}_old", trash=trash_path)


def _hdfs_delete(sc, path: str):
    fs = _get_fs(sc)
    hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)
    if fs.exists(hadoop_path):
        fs.delete(hadoop_path, True)


def _get_fs(sc):
    uri = sc._jvm.java.net.URI.create("hdfs://namenode:8020")
    conf = sc._jsc.hadoopConfiguration()
    return sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)