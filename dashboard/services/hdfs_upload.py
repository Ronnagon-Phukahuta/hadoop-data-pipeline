# dashboard/services/hdfs_upload.py
"""
HDFS Upload Service — push CSV เข้า HDFS raw folder
และ ALTER TABLE เพิ่ม column ใหม่ถ้าจำเป็น
"""

import os
import requests
from pyhive import hive

WEBHDFS_URL = os.environ.get("WEBHDFS_URL", "http://namenode:50070")
HIVE_HOST = os.environ.get("HIVE_HOST", "hive-server")
HIVE_PORT = int(os.environ.get("HIVE_PORT", 10000))


def upload_csv_to_hdfs(csv_bytes: bytes, hdfs_path: str) -> None:
    """
    Push CSV bytes เข้า HDFS ผ่าน WebHDFS REST API
    hdfs_path เช่น /datalake/raw/finance_itsc/finance_itsc_2024.csv
    """
    # Step 1: CREATE (redirect)
    create_url = f"{WEBHDFS_URL}/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true"
    resp = requests.put(create_url, allow_redirects=False, timeout=10)
    if resp.status_code != 307:
        raise RuntimeError(f"WebHDFS CREATE failed: {resp.status_code} {resp.text}")

    # Step 2: Follow redirect → actual upload
    upload_url = resp.headers["Location"]
    resp2 = requests.put(
        upload_url,
        data=csv_bytes,
        headers={"Content-Type": "application/octet-stream"},
        timeout=60,
    )
    if resp2.status_code != 201:
        raise RuntimeError(f"WebHDFS upload failed: {resp2.status_code} {resp2.text}")


def alter_table_add_columns(table_name: str, new_columns: list[str]) -> None:
    """
    ALTER TABLE เพิ่ม column ใหม่ทั้งหมดเป็น STRING
    (type default STRING เพราะข้อมูลมาจาก CSV)
    """
    if not new_columns:
        return

    col_defs = ", ".join(f"`{col}` STRING" for col in new_columns)
    sql = f"ALTER TABLE `{table_name}` ADD COLUMNS ({col_defs})"

    conn = hive.connect(host=HIVE_HOST, port=HIVE_PORT, database="default")
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
    finally:
        conn.close()