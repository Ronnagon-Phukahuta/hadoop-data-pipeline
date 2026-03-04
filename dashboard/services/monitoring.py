# dashboard/services/monitoring.py
"""
Parse etl.log และ etl.error.log สำหรับ Monitoring Dashboard
"""

import json
import os
from datetime import datetime
from typing import List, Dict, Any

ETL_LOG = os.environ.get("ETL_LOG_PATH", "/jobs/logs/etl.log")


def _parse_log_lines(path: str) -> List[Dict]:
    """อ่านและ parse JSON log ทีละบรรทัด ข้ามบรรทัดที่ invalid"""
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


def _extract(record: Dict) -> Dict:
    """ดึง fields ที่ใช้บ่อยออกมา"""
    r = record.get("record", {})
    extra = r.get("extra", {})
    return {
        "timestamp": r.get("time", {}).get("repr", ""),
        "level": r.get("level", {}).get("name", ""),
        "message": r.get("message", record.get("text", "")),
        "module": extra.get("module", r.get("name", "")),
        "extra": extra,
    }


def get_pipeline_runs() -> List[Dict]:
    """
    ดึง pipeline run แต่ละครั้ง โดยดูจาก process_id
    คืน list เรียงจากล่าสุด
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
            }

        run = runs[pid]
        run["end_time"] = ts

        # File scan
        if msg == "File scan complete":
            run["files"]["found"] = extra.get("csv_found", 0)
            run["files"]["processed"] = extra.get("already_processed", 0)
            run["files"]["failed"] = extra.get("dq_failed", 0)
            run["files"]["pending"] = extra.get("pending", 0)

        # DQ pass/fail
        elif msg == "Check passed":
            run["dq_checks"]["pass"] += 1
        elif msg == "All quality checks passed":
            pass
        elif level == "ERROR" and "quality" in msg.lower():
            run["dq_checks"]["fail"] += 1

        # Year success/fail
        elif msg == "Year written to staging":
            year = extra.get("year")
            if year and year not in run["years_processed"]:
                run["years_processed"].append(year)
        elif msg == "Data quality check error — skipping year":
            year = extra.get("year")
            if year and year not in run["years_failed"]:
                run["years_failed"].append(year)

        # Pipeline complete
        elif "ETL Pipeline completed" in msg:
            run["status"] = "success" if not run["years_failed"] else "partial"

        # Errors
        elif level == "ERROR":
            run["errors"].append({
                "timestamp": ts,
                "message": msg,
                "error": extra.get("error", ""),
            })

    # คำนวณ duration
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


def get_dq_stats() -> Dict[str, Any]:
    """สถิติ DQ pass/fail rate รวมทุก run"""
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

        if msg == "Check passed" and check in stats:
            stats[check]["pass"] += 1
        elif msg == "Check failed" and check in stats:
            stats[check]["fail"] += 1

    return stats


WEBHDFS_URL = os.environ.get("WEBHDFS_URL", "http://namenode:50070")
VERSIONS_PATH = "/datalake/versions/finance_itsc"


def _webhdfs_ls(path: str) -> List[Dict]:
    """List directory ผ่าน WebHDFS REST API"""
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
    """อ่านไฟล์ผ่าน WebHDFS"""
    import requests
    url = f"{WEBHDFS_URL}/webhdfs/v1{path}?op=OPEN"
    try:
        r = requests.get(url, timeout=5, allow_redirects=True)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return ""


def get_version_history() -> List[Dict]:
    """ดึง version snapshot จาก HDFS โดยตรง (ข้อมูล accurate 100%)"""
    versions = []

    # list ปีทั้งหมด
    year_dirs = _webhdfs_ls(VERSIONS_PATH)
    for year_dir in year_dirs:
        if year_dir["type"] != "DIRECTORY":
            continue
        year_path = f"{VERSIONS_PATH}/{year_dir['pathSuffix']}"
        year = year_dir["pathSuffix"].replace("year=", "")

        # list versions ในแต่ละปี
        version_dirs = _webhdfs_ls(year_path)
        for v_dir in version_dirs:
            if v_dir["type"] != "DIRECTORY" or not v_dir["pathSuffix"].startswith("v_"):
                continue
            version_id = v_dir["pathSuffix"]
            meta_path = f"{year_path}/{version_id}/_version.json"

            # อ่าน metadata
            meta_text = _webhdfs_read(meta_path)
            if meta_text:
                try:
                    meta = json.loads(meta_text)
                    versions.append({
                        "version": meta.get("version", version_id),
                        "year": meta.get("year", year),
                        "rows": meta.get("row_count", 0),
                        "checksum": meta.get("checksum", ""),
                        "timestamp": meta.get("timestamp", ""),
                        "path": f"hdfs://namenode:8020{year_path}/{version_id}",
                    })
                except Exception:
                    pass

    return sorted(versions, key=lambda x: x["timestamp"], reverse=True)