# jobs/utils/hdfs.py
import json
import hashlib
from typing import List, Optional

from logger import get_logger

log = get_logger(__name__)


def get_fs(sc):
    uri = sc._jvm.java.net.URI.create("hdfs://namenode:8020")
    conf = sc._jsc.hadoopConfiguration()
    return sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)


def hdfs_ls_recursive(sc, path: str) -> List[str]:
    fs = get_fs(sc)
    hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(hadoop_path):
        log.warning("HDFS path not found", path=path)
        return []
    files = []
    iterator = fs.listFiles(hadoop_path, True)
    while iterator.hasNext():
        files.append(iterator.next().getPath().toString())
    log.debug("HDFS listing complete", path=path, total_files=len(files))
    return files


def hdfs_touch(sc, path: str):
    """สร้าง marker file เปล่า (legacy — ใช้ hdfs_write_done สำหรับ .done markers)"""
    fs = get_fs(sc)
    fs.create(sc._jvm.org.apache.hadoop.fs.Path(path)).close()
    log.info("Marker created", path=path)


def compute_file_checksum(sc, path: str) -> str:
    """
    คำนวณ SHA256 checksum ของ file content ใน HDFS

    อ่าน bytes ทีละ chunk เพื่อรองรับไฟล์ใหญ่
    Returns: hex string เช่น "a3f2c1..."
    """
    fs = get_fs(sc)
    hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)

    if not fs.exists(hadoop_path):
        raise FileNotFoundError(f"compute_file_checksum: file not found: {path}")

    hasher = hashlib.sha256()
    stream = fs.open(hadoop_path)
    buf = sc._jvm.java.lang.reflect.Array.newInstance(
        sc._jvm.java.lang.Byte.TYPE, 65536  # 64KB chunks
    )
    try:
        while True:
            n = stream.read(buf)
            if n == -1:
                break
            # แปลง Java byte array → Python bytes
            chunk = bytes(bytearray([b & 0xFF for b in buf[:n]]))
            hasher.update(chunk)
    finally:
        stream.close()

    checksum = hasher.hexdigest()
    log.debug("Checksum computed", path=path, checksum=checksum[:12])
    return checksum


def hdfs_write_done(sc, csv_path: str, checksum: str):
    """
    เขียน .done marker พร้อม checksum ของ file content

    Format: {"checksum": "sha256hex", "source": "filename.csv"}

    Usage:
        checksum = compute_file_checksum(sc, csv_path)
        hdfs_write_done(sc, csv_path, checksum)
    """
    done_path = csv_path + ".done"
    content = json.dumps({
        "checksum": checksum,
        "source": csv_path.split("/")[-1],
    }, ensure_ascii=False)

    fs = get_fs(sc)
    out = fs.create(sc._jvm.org.apache.hadoop.fs.Path(done_path))
    out.write(content.encode("utf-8"))
    out.close()
    log.info("Done marker written", path=done_path, checksum=checksum[:12])


def hdfs_read_done(sc, csv_path: str) -> Optional[str]:
    """
    อ่าน checksum จาก .done marker

    Returns:
        checksum string ถ้าอ่านได้
        None ถ้าไม่มีไฟล์หรืออ่านไม่ได้ (รองรับ .done แบบเก่าที่เป็นไฟล์เปล่า)
    """
    done_path = csv_path + ".done"
    fs = get_fs(sc)
    hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(done_path)

    if not fs.exists(hadoop_path):
        return None

    try:
        stream = fs.open(hadoop_path)
        reader = sc._jvm.java.io.BufferedReader(
            sc._jvm.java.io.InputStreamReader(stream, "UTF-8")
        )
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        reader.close()

        content = "\n".join(lines).strip()
        if not content:
            # .done แบบเก่า (ไฟล์เปล่า) — ถือว่า processed แต่ไม่มี checksum
            return None

        data = json.loads(content)
        return data.get("checksum")
    except Exception as e:
        log.warning("Could not read done marker", path=done_path, error=str(e))
        return None


def is_already_processed(sc, csv_path: str) -> bool:
    """
    เช็คว่าไฟล์นี้ถูก process ไปแล้วหรือยัง โดยเปรียบเทียบ checksum

    Logic:
        - ไม่มี .done → ยังไม่ได้ process
        - มี .done แบบเก่า (เปล่า) → ถือว่า processed แล้ว (backward compat)
        - มี .done พร้อม checksum → เปรียบเทียบกับ file จริง
            - ตรงกัน → processed แล้ว skip ได้
            - ไม่ตรง → file เปลี่ยน reprocess

    Returns:
        True = skip ได้, False = ต้อง process
    """
    done_path = csv_path + ".done"
    fs = get_fs(sc)
    done_hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(done_path)

    # ไม่มี .done → ยังไม่ได้ process
    if not fs.exists(done_hadoop_path):
        return False

    stored_checksum = hdfs_read_done(sc, csv_path)

    # .done แบบเก่า (เปล่า ไม่มี checksum) → backward compat ถือว่า processed
    if stored_checksum is None:
        log.info(
            "Legacy .done marker (no checksum) — treating as processed",
            file=csv_path.split("/")[-1],
        )
        return True

    # เปรียบเทียบ checksum
    try:
        current_checksum = compute_file_checksum(sc, csv_path)
    except FileNotFoundError:
        log.warning("CSV file not found when checking idempotency", path=csv_path)
        return True  # ไม่มีไฟล์ต้นทางแล้ว → skip

    if current_checksum == stored_checksum:
        log.info(
            "File already processed (checksum match) — skipping",
            file=csv_path.split("/")[-1],
            checksum=current_checksum[:12],
        )
        return True
    else:
        log.info(
            "File content changed (checksum mismatch) — reprocessing",
            file=csv_path.split("/")[-1],
            stored=stored_checksum[:12],
            current=current_checksum[:12],
        )
        return False


def extract_year_from_path(path: str) -> Optional[int]:
    import re
    m = re.search(r"year=(\d{4})", path)
    if m:
        return int(m.group(1))
    log.warning("Could not extract year from path", path=path)
    return None