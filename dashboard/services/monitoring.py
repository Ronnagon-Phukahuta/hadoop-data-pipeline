# dashboard/services/monitoring.py
"""
Parse etl.log สำหรับ Monitoring Dashboard

Improvements:
- st.cache_data(ttl=60) ป้องกัน re-parse log ทุก interaction
- step_durations: parse duration_ms ของแต่ละ step ต่อ run
"""

import json
import os
from datetime import datetime
from typing import List, Dict, Any

import streamlit as st

ETL_LOG = os.environ.get("ETL_LOG_PATH", "/jobs/logs/etl.log")

PIPELINE_STEPS = [
    "scan_hdfs",
    "read_csv",
    "data_quality",
    "atomic_write",
    "write_done",
    "versioning",
    "check_partitions",
    "read_wide",
    "transform",
    "write_curated",
]

STEP_LABELS = {
    "scan_hdfs":       "Scan HDFS",
    "read_csv":        "Read CSV",
    "data_quality":    "Data Quality",
    "atomic_write":    "Atomic Write",
    "write_done":      "Write Done",
    "versioning":      "Versioning",
    "check_partitions":"Check Partitions",
    "read_wide":       "Read Wide",
    "transform":       "Transform",
    "write_curated":   "Write Curated",
}


def _parse_log_lines(path: str) -> List[Dict]:
    records = []
    if not os.path.exists(path):
        return records
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                continue
    return records


@st.cache_data(ttl=60)
def get_pipeline_runs() -> List[Dict]:
    """
    ดึง pipeline run แต่ละครั้ง โดยดูจาก process_id
    คืน list เรียงจากล่าสุด — cache 60 วินาที
    """
    records = _parse_log_lines(ETL_LOG)
    runs = {}

    for record in records:
        r = record.get("record", {})
        pid = r.get("process", {}).get("id")
        if not pid:
            continue

        msg = r.get("message", "")
        extra = r.get("extra", {})
        ts = r.get("time", {}).get("repr", "")
        level = r.get("level", {}).get("name", "INFO")

        if pid not in runs:
            runs[pid] = {
                "pid": pid,
                "start_time": ts,
                "end_time": ts,
                "status": "running",
                "years_processed": [],
                "years_failed": [],
                "dq_checks": {"pass": 0, "fail": 0},
                "files": {"found": 0, "processed": 0, "failed": 0, "pending": 0},
                "errors": [],
                "step_durations": {},  # {step: total_ms}
            }

        run = runs[pid]
        run["end_time"] = ts

        step = extra.get("step", "")
        duration_ms = extra.get("duration_ms")

        # Step duration — sum ถ้า step เดียวกันเกิดหลายปี
        if step and duration_ms is not None and step in PIPELINE_STEPS:
            run["step_durations"][step] = run["step_durations"].get(step, 0) + duration_ms

        # File scan
        if step == "scan_hdfs" and extra.get("csv_found") is not None:
            run["files"]["found"] = extra.get("csv_found", 0)
            run["files"]["processed"] = extra.get("already_done", 0)
            run["files"]["failed"] = extra.get("dq_failed", 0)
            run["files"]["pending"] = extra.get("pending", 0)

        # DQ pass/fail
        elif msg.startswith("✅ ") and extra.get("check"):
            run["dq_checks"]["pass"] += 1
        elif msg.startswith("❌ ") and extra.get("check"):
            run["dq_checks"]["fail"] += 1

        # Year success
        elif step == "write_curated" and extra.get("rows") is not None:
            year = extra.get("year")
            if year and year not in run["years_processed"]:
                run["years_processed"].append(year)

        # Year DQ fail
        elif msg == "Data quality check error — skipping year":
            year = extra.get("year")
            if year and year not in run["years_failed"]:
                run["years_failed"].append(year)

        # Pipeline complete
        elif "ETL completed" in msg:
            run["status"] = "success" if not run["years_failed"] else "partial"

        # Errors
        elif level == "ERROR":
            run["errors"].append({
                "timestamp": ts,
                "message": msg,
                "error": extra.get("error", ""),
            })

    result = []
    for run in runs.values():
        try:
            start = datetime.fromisoformat(run["start_time"].split("+")[0])
            end = datetime.fromisoformat(run["end_time"].split("+")[0])
            run["duration_sec"] = int((end - start).total_seconds())
        except Exception:
            run["duration_sec"] = 0

        if run["status"] == "running" and run.get("errors"):
            run["status"] = "failed"

        result.append(run)

    return sorted(result, key=lambda x: x["start_time"], reverse=True)


@st.cache_data(ttl=60)
def get_dq_stats() -> Dict[str, Any]:
    """สถิติ DQ pass/fail rate รวมทุก run — cache 60 วินาที"""
    records = _parse_log_lines(ETL_LOG)
    stats = {
        "Schema": {"pass": 0, "fail": 0},
        "Null Values": {"pass": 0, "fail": 0},
        "Date Format": {"pass": 0, "fail": 0},
        "Total Amount": {"pass": 0, "fail": 0},
        "Remaining Decreasing": {"pass": 0, "fail": 0},
    }

    for record in records:
        r = record.get("record", {})
        extra = r.get("extra", {})
        msg = r.get("message", "")
        check = extra.get("check", "")

        if not check or check not in stats:
            continue

        if msg.startswith("✅ "):
            stats[check]["pass"] += 1
        elif msg.startswith("❌ "):
            stats[check]["fail"] += 1

    return stats


WEBHDFS_URL = os.environ.get("WEBHDFS_URL", "http://namenode:50070")
VERSIONS_BASE = "/datalake/versions"


def _webhdfs_ls(path: str) -> List[Dict]:
    import requests
    url = f"{WEBHDFS_URL}/webhdfs/v1{path}?op=LISTSTATUS"
    try:
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            return r.json().get("FileStatuses", {}).get("FileStatus", [])
    except Exception:
        pass
    return []


def _webhdfs_read(path: str) -> str:
    import requests
    url = f"{WEBHDFS_URL}/webhdfs/v1{path}?op=OPEN"
    try:
        r = requests.get(url, timeout=5, allow_redirects=True)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return ""


@st.cache_data(ttl=60)
def get_version_history(dataset_name: str = None) -> List[Dict]:
    """
    ดึง version snapshot จาก HDFS — cache 60 วินาที
    - dataset_name=None → scan ทุก dataset ใน /datalake/versions/
    - dataset_name="finance_itsc" → scan เฉพาะ dataset นั้น
    """
    versions = []

    # รายชื่อ dataset ที่จะ scan
    if dataset_name:
        dataset_dirs = [{"pathSuffix": dataset_name, "type": "DIRECTORY"}]
    else:
        dataset_dirs = [d for d in _webhdfs_ls(VERSIONS_BASE) if d["type"] == "DIRECTORY"]

    for ds_dir in dataset_dirs:
        ds_name = ds_dir["pathSuffix"]
        versions_path = f"{VERSIONS_BASE}/{ds_name}"

        year_dirs = _webhdfs_ls(versions_path)
        for year_dir in year_dirs:
            if year_dir["type"] != "DIRECTORY":
                continue
            year_path = f"{versions_path}/{year_dir['pathSuffix']}"
            year = year_dir["pathSuffix"].replace("year=", "")

            version_dirs = _webhdfs_ls(year_path)
            for v_dir in version_dirs:
                if v_dir["type"] != "DIRECTORY" or not v_dir["pathSuffix"].startswith("v_"):
                    continue
                version_id = v_dir["pathSuffix"]
                meta_path = f"{year_path}/{version_id}/_version.json"

                meta_text = _webhdfs_read(meta_path)
                if meta_text:
                    try:
                        meta = json.loads(meta_text)
                        raw_checksum = meta.get("checksum", "")
                        checksum = raw_checksum.split(":")[-1][:12] if ":" in raw_checksum else raw_checksum[:12]
                        versions.append({
                            "dataset": ds_name,
                            "version": meta.get("version", version_id),
                            "year": meta.get("year", year),
                            "rows": meta.get("row_count", 0),
                            "checksum": checksum,
                            "timestamp": meta.get("timestamp", ""),
                            "path": f"hdfs://namenode:8020{year_path}/{version_id}",
                        })
                    except Exception:
                        pass

    return sorted(versions, key=lambda x: x["timestamp"], reverse=True)