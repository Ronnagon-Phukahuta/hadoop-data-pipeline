# jobs/utils/soft_delete.py
"""
Soft Delete Wrapper สำหรับ HDFS

แทนที่จะลบตรง จะ move ไปที่ trash ก่อน:
    /datalake/trash/YYYYMMDD/{original_path_escaped}/

เก็บไว้ 30 วัน แล้ว cleanup อัตโนมัติ (เรียก purge_old_trash)

Usage:
    safe_delete(sc, "/datalake/versions/finance_itsc/year=2024/v_old")
    list_trash(sc)
    restore_from_trash(sc, "20260304", "v_20260301_120000")
    purge_old_trash(sc, keep_days=30)
"""

from datetime import datetime, timedelta as _timedelta
from typing import List, Dict, Optional

from logger import get_logger

log = get_logger(__name__)

TRASH_BASE_PATH = "hdfs://namenode:8020/datalake/trash"
TRASH_KEEP_DAYS = 30


def safe_delete(sc, path: str) -> str:
    """
    Move path ไป trash แทนลบตรง

    Returns:
        trash_path ที่ย้ายไป
    """
    fs = _get_fs(sc)
    src = sc._jvm.org.apache.hadoop.fs.Path(path)

    if not fs.exists(src):
        log.warning("safe_delete: path not found, skipping", path=path)
        return ""

    date_str = datetime.now().strftime("%Y%m%d")
    # escape path เพื่อใช้เป็นชื่อ folder ใน trash
    escaped = path.replace("hdfs://namenode:8020", "").replace("/", "__").strip("_")
    trash_dir = f"{TRASH_BASE_PATH}/{date_str}/{escaped}"
    trash_path = sc._jvm.org.apache.hadoop.fs.Path(trash_dir)

    # สร้าง parent directory โดยใช้ string แทน getParent()
    parent_dir = "/".join(trash_dir.split("/")[:-1])
    parent_path = sc._jvm.org.apache.hadoop.fs.Path(parent_dir)
    fs.mkdirs(parent_path)

    # ถ้า trash_dir ซ้ำ ให้ต่อ timestamp
    if fs.exists(trash_path):
        ts = datetime.now().strftime("%H%M%S")
        trash_dir = f"{trash_dir}_{ts}"
        trash_path = sc._jvm.org.apache.hadoop.fs.Path(trash_dir)

    success = fs.rename(src, trash_path)
    if not success:
        raise RuntimeError(f"safe_delete failed: could not move {path} -> {trash_dir}")

    log.info("Moved to trash", original=path, trash=trash_dir)
    return trash_dir


def list_trash(sc, date_str: Optional[str] = None) -> List[Dict]:
    """
    ดู items ใน trash

    Args:
        date_str: "20260304" ถ้า None จะดูทุกวัน

    Returns:
        list of {"date": "20260304", "name": "...", "trash_path": "..."}
    """
    fs = _get_fs(sc)
    results = []

    if date_str:
        date_dirs = [f"{TRASH_BASE_PATH}/{date_str}"]
    else:
        trash_root = sc._jvm.org.apache.hadoop.fs.Path(TRASH_BASE_PATH)
        if not fs.exists(trash_root):
            return []
        statuses = fs.listStatus(trash_root)
        date_dirs = [s.getPath().toString() for s in statuses]

    for date_dir in date_dirs:
        date_path = sc._jvm.org.apache.hadoop.fs.Path(date_dir)
        if not fs.exists(date_path):
            continue
        try:
            statuses = fs.listStatus(date_path)
            for s in statuses:
                p = s.getPath()
                results.append({
                    "date": date_dir.split("/")[-1],
                    "name": p.getName(),
                    "trash_path": p.toString(),
                })
        except Exception as e:
            log.warning("Could not list trash dir", dir=date_dir, error=str(e))

    return results


def restore_from_trash(sc, date_str: str, name: str, restore_to: Optional[str] = None) -> str:
    """
    Restore item จาก trash กลับไปยัง path เดิม (หรือ path ที่กำหนด)

    Args:
        date_str: "20260304"
        name: ชื่อ folder ใน trash (จาก list_trash)
        restore_to: path ปลายทาง ถ้า None จะ decode จากชื่อ folder อัตโนมัติ

    Returns:
        path ที่ restore ไป
    """
    fs = _get_fs(sc)
    trash_path = sc._jvm.org.apache.hadoop.fs.Path(
        f"{TRASH_BASE_PATH}/{date_str}/{name}"
    )

    if not fs.exists(trash_path):
        raise FileNotFoundError(f"Trash item not found: {date_str}/{name}")

    if restore_to is None:
        # decode กลับจากชื่อ folder
        restore_to = "hdfs://namenode:8020/" + name.replace("__", "/").strip("/")

    dst = sc._jvm.org.apache.hadoop.fs.Path(restore_to)
    if fs.exists(dst):
        raise RuntimeError(f"restore_from_trash: destination already exists: {restore_to}")

    parent_dir = "/".join(restore_to.split("/")[:-1])
    parent_path = sc._jvm.org.apache.hadoop.fs.Path(parent_dir)
    fs.mkdirs(parent_path)
    success = fs.rename(trash_path, dst)
    if not success:
        raise RuntimeError(f"restore_from_trash failed: {trash_path} -> {restore_to}")

    log.info("Restored from trash", name=name, date=date_str, restored_to=restore_to)
    return restore_to


def purge_old_trash(sc, keep_days: int = TRASH_KEEP_DAYS):
    """
    ลบ trash ที่เก่ากว่า keep_days วัน (ลบจริง — ไม่สามารถ restore ได้แล้ว)
    เรียกใน cleanup job หรือ Airflow DAG
    """
    fs = _get_fs(sc)
    trash_root = sc._jvm.org.apache.hadoop.fs.Path(TRASH_BASE_PATH)

    if not fs.exists(trash_root):
        return

    cutoff = datetime.now() - _timedelta(days=keep_days)
    cutoff_str = cutoff.strftime("%Y%m%d")

    statuses = fs.listStatus(trash_root)
    deleted = 0
    for s in statuses:
        date_folder = s.getPath().getName()
        if date_folder <= cutoff_str:  # string compare ได้เพราะ format YYYYMMDD
            fs.delete(s.getPath(), True)
            log.info("Purged old trash", date=date_folder)
            deleted += 1

    log.info("Trash purge complete", deleted_days=deleted, cutoff=cutoff_str)


def _get_fs(sc):
    uri = sc._jvm.java.net.URI.create("hdfs://namenode:8020")
    conf = sc._jsc.hadoopConfiguration()
    return sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)