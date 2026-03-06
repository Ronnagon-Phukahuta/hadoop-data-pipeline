# jobs/utils/versioning.py
"""
Data Versioning สำหรับ Finance ITSC Pipeline

ทุกครั้งที่ upload CSV ใหม่จะ snapshot ไว้ที่:
    /datalake/versions/finance_itsc/year=2024/v_20260301_120000/

พร้อม metadata:
    /datalake/versions/finance_itsc/year=2024/v_20260301_120000/_version.json
    {
        "version": "v_20260301_120000",
        "source_file": "finance_2024.csv",
        "year": 2024,
        "timestamp": "2026-03-01T12:00:00",
        "row_count": 1500,
        "checksum": "abc123...",
        "columns": [...],
    }

เก็บไว้ 90 วัน แล้ว cleanup อัตโนมัติ
"""

import json
import hashlib
from datetime import datetime
from typing import Optional, List, Dict
from pyspark.sql import DataFrame

from logger import get_logger
from utils.retry import atomic_write_table
from utils.soft_delete import safe_delete  # ← เพิ่ม

log = get_logger(__name__)


def compute_schema_hash(columns: list) -> str:
    """
    คำนวณ SHA256 hash ของ schema (sorted columns)
    ใช้เปรียบเทียบว่า schema เปลี่ยนระหว่าง version หรือไม่

    sorted() ก่อน hash เพื่อให้ column order ไม่ส่งผล
    Returns: hex string 64 ตัว เช่น "a3f2c1d8..."
    """
    schema_str = json.dumps(sorted(columns), ensure_ascii=False)
    return hashlib.sha256(schema_str.encode()).hexdigest()


VERSIONS_BASE_PATH = "hdfs://namenode:8020/datalake/versions/finance_itsc"
KEEP_VERSIONS = 5  # เก็บ 5 version ล่าสุดต่อ year


def create_version(
    sc,
    df: DataFrame,
    source_file: str,
    year: int,
) -> str:
    """
    สร้าง snapshot ของ DataFrame พร้อม metadata

    Returns:
        version_id เช่น "v_20260301_120000"
    """
    timestamp = datetime.now()
    version_id = timestamp.strftime("v_%Y%m%d_%H%M%S")
    version_path = f"{VERSIONS_BASE_PATH}/year={year}/{version_id}"

    log.info("Creating version snapshot", version=version_id, year=year, source=source_file)

    # เขียน snapshot
    df.write.mode("overwrite").parquet(version_path)

    # คำนวณ metadata
    row_count = df.count()
    checksum = _compute_checksum(sc, version_path)

    schema_hash = compute_schema_hash(df.columns)

    metadata = {
        "version": version_id,
        "source_file": source_file.split("/")[-1],
        "year": year,
        "timestamp": timestamp.isoformat(),
        "row_count": row_count,
        "checksum": checksum,
        "columns": df.columns,
        "schema_hash": schema_hash,
        "keep_versions": KEEP_VERSIONS,
    }

    _write_metadata(sc, version_path, metadata)

    log.info(
        "✅ Version created",
        version=version_id,
        year=year,
        rows=row_count,
        columns=len(df.columns),
        schema_hash=schema_hash[:12],
        checksum=checksum[:12],
        path=version_path,
    )

    return version_id


def list_versions(sc, year: int) -> List[Dict]:
    """
    ดู versions ทั้งหมดของปีนั้น เรียงจากใหม่ไปเก่า
    """
    from utils.hdfs import hdfs_ls_recursive

    year_path = f"{VERSIONS_BASE_PATH}/year={year}"
    all_files = hdfs_ls_recursive(sc, year_path)
    meta_files = [f for f in all_files if f.endswith("_version.json")]

    versions = []
    for meta_file in meta_files:
        content = _read_file(sc, meta_file)
        if content:
            try:
                versions.append(json.loads(content))
            except Exception as e:
                log.warning("Could not parse version metadata", file=meta_file, error=str(e))

    return sorted(versions, key=lambda v: v["timestamp"], reverse=True)


def restore_version(
    spark,
    version_id: str,
    year: int,
    target_table: str,
    target_path: str,
    database: str = "default",
):
    """
    Restore data จาก version ที่ต้องการ ผ่าน atomic write

    Usage:
        restore_version(
            spark, "v_20260215_090000", 2024,
            "finance_itsc_wide",
            "hdfs://namenode:8020/datalake/staging/finance_itsc_wide"
        )
    """
    version_path = f"{VERSIONS_BASE_PATH}/year={year}/{version_id}"
    log.info("Restoring version", version=version_id, year=year, table=target_table)

    df = spark.read.parquet(version_path)
    atomic_write_table(
        df=df,
        table_path=target_path,
        table_name=target_table,
        database=database,
        partition_col="year",
        partition_value=year,
    )

    log.info("Version restored", version=version_id, year=year, table=target_table)


def cleanup_old_versions(sc, year: int, keep: int = KEEP_VERSIONS):
    """
    เก็บแค่ {keep} version ล่าสุด ย้ายอันเก่าเกินไป trash (soft delete)
    versions เรียงจากใหม่ → เก่า อยู่แล้ว
    """
    versions = list_versions(sc, year)

    to_keep = versions[:keep]
    to_delete = versions[keep:]

    for v in to_delete:
        version_path = f"{VERSIONS_BASE_PATH}/year={year}/{v['version']}"
        trash_path = safe_delete(sc, version_path)  # ← เปลี่ยนจาก _hdfs_delete
        log.info(
            "Old version moved to trash",
            version=v["version"],
            year=year,
            trash=trash_path,
        )

    log.info(
        "Cleanup complete",
        year=year,
        kept=len(to_keep),
        trashed=len(to_delete),
        latest=to_keep[0]["version"] if to_keep else None,
    )


def diff_versions(sc, version_a: str, version_b: str, year: int) -> Dict:
    """
    เปรียบเทียบ 2 versions ว่าต่างกันอะไรบ้าง

    Returns dict:
        {
            "version_a": "v_20260301_120000",
            "version_b": "v_20260306_095749",
            "schema_changed": True,
            "added_columns":   ["new_fund_column"],
            "removed_columns": ["old_column"],
            "row_count_a": 1500,
            "row_count_b": 1520,
            "row_diff": +20,
            "source_a": "finance_2024_v1.csv",
            "source_b": "finance_2024_v2.csv",
            "same_source": False,
        }
    """
    path_a = f"{VERSIONS_BASE_PATH}/year={year}/{version_a}/_version.json"
    path_b = f"{VERSIONS_BASE_PATH}/year={year}/{version_b}/_version.json"

    raw_a = _read_file(sc, path_a)
    raw_b = _read_file(sc, path_b)

    if not raw_a:
        raise FileNotFoundError(f"diff_versions: metadata not found for {version_a}")
    if not raw_b:
        raise FileNotFoundError(f"diff_versions: metadata not found for {version_b}")

    meta_a = json.loads(raw_a)
    meta_b = json.loads(raw_b)

    cols_a = set(meta_a.get("columns", []))
    cols_b = set(meta_b.get("columns", []))
    added   = sorted(cols_b - cols_a)
    removed = sorted(cols_a - cols_b)

    hash_a = meta_a.get("schema_hash") or compute_schema_hash(list(cols_a))
    hash_b = meta_b.get("schema_hash") or compute_schema_hash(list(cols_b))
    schema_changed = hash_a != hash_b

    row_a = meta_a.get("row_count", 0)
    row_b = meta_b.get("row_count", 0)

    result = {
        "version_a":       version_a,
        "version_b":       version_b,
        "year":            year,
        "schema_changed":  schema_changed,
        "added_columns":   added,
        "removed_columns": removed,
        "row_count_a":     row_a,
        "row_count_b":     row_b,
        "row_diff":        row_b - row_a,
        "source_a":        meta_a.get("source_file"),
        "source_b":        meta_b.get("source_file"),
        "same_source":     meta_a.get("source_file") == meta_b.get("source_file"),
        "timestamp_a":     meta_a.get("timestamp"),
        "timestamp_b":     meta_b.get("timestamp"),
    }

    log.info(
        "Version diff",
        version_a=version_a,
        version_b=version_b,
        year=year,
        schema_changed=schema_changed,
        added=len(added),
        removed=len(removed),
        row_diff=row_b - row_a,
    )

    return result


def _compute_checksum(sc, path: str) -> str:
    """คำนวณ checksum ของไฟล์ใน HDFS path"""
    try:
        fs = _get_fs(sc)
        hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)
        checksum = fs.getFileChecksum(hadoop_path)
        if checksum:
            return checksum.toString()
    except Exception:
        pass
    # fallback: ใช้ path + timestamp เป็น pseudo checksum
    return hashlib.md5(f"{path}{datetime.now().isoformat()}".encode()).hexdigest()[:12]


def _write_metadata(sc, version_path: str, metadata: dict):
    """เขียน _version.json ลง HDFS"""
    meta_path = f"{version_path}/_version.json"
    content = json.dumps(metadata, ensure_ascii=False, indent=2)
    fs = _get_fs(sc)
    out = fs.create(sc._jvm.org.apache.hadoop.fs.Path(meta_path))
    out.write(content.encode("utf-8"))
    out.close()


def _read_file(sc, path: str) -> Optional[str]:
    """อ่านไฟล์จาก HDFS"""
    try:
        fs = _get_fs(sc)
        stream = fs.open(sc._jvm.org.apache.hadoop.fs.Path(path))
        reader = sc._jvm.java.io.BufferedReader(
            sc._jvm.java.io.InputStreamReader(stream, "UTF-8")
        )
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        reader.close()
        return "\n".join(lines)
    except Exception as e:
        log.warning("Could not read file", path=path, error=str(e))
        return None


def _get_fs(sc):
    uri = sc._jvm.java.net.URI.create("hdfs://namenode:8020")
    conf = sc._jsc.hadoopConfiguration()
    return sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)