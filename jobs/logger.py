"""
Structured logger สำหรับ Finance ITSC Pipeline
ใช้ loguru — output ไป console (plain) และ file (JSON)
"""

import os
import sys
import time
from pathlib import Path
from contextlib import contextmanager
from loguru import logger

# ใน container ใช้ /jobs/logs, รันใน local/test ใช้ logs/ ใน cwd
_default_log_dir = "/jobs/logs" if os.path.exists("/jobs") else "logs"
LOG_DIR = Path(os.getenv("LOG_DIR", _default_log_dir))
LOG_DIR.mkdir(parents=True, exist_ok=True)


def get_logger(name: str):
    """
    สร้าง logger สำหรับแต่ละ module

    Usage:
        from logger import get_logger
        log = get_logger(__name__)
        log.info("ETL started", rows=1000, file="finance_2024.csv")
    """
    return logger.bind(module=name)


def setup_logger(name: str = "app"):
    """
    ตั้งค่า logger ทั้งระบบ — เรียกครั้งเดียวตอน startup
    """
    logger.remove()  # ลบ default handler

    # ── Console: plain text อ่านง่าย + แสดง extra fields ──────────
    def _fmt(record):
        skip = {"module", "_extra_str"}
        extra = {k: v for k, v in record["extra"].items() if k not in skip}
        extra_str = " | " + " ".join(f"{k}={v}" for k, v in extra.items()) if extra else ""
        record["extra"]["_extra_str"] = extra_str
        return (
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{extra[module]}</cyan> | "
            "{message}{extra[_extra_str]}\n"
        )

    logger.add(
        sys.stdout,
        format=_fmt,
        level="DEBUG",
        colorize=True,
    )

    # ── File: JSON สำหรับ query / filter ──────────────────────────
    logger.add(
        LOG_DIR / f"{name}.log",
        format="{time} | {level} | {extra[module]} | {message} | {extra}",
        level="INFO",
        rotation="10 MB",
        retention="30 days",
        compression="zip",
        serialize=True,
        encoding="utf-8",
    )

    # ── Error file: เก็บเฉพาะ ERROR ขึ้นไป ───────────────────────
    logger.add(
        LOG_DIR / f"{name}.error.log",
        level="ERROR",
        rotation="5 MB",
        retention="60 days",
        compression="zip",
        serialize=True,
        encoding="utf-8",
    )

    return get_logger(name)


@contextmanager
def step_log(log, step: str, dataset: str = "finance", **extra):
    """
    Context manager สำหรับ log แต่ละ step พร้อม duration อัตโนมัติ
    และ parse Spark/Java error ให้อ่านง่ายแทน stacktrace ยาวๆ

    Log format:
        [dataset=finance] [step=transform] START
        [dataset=finance] [step=transform] SUCCESS (2341ms)
        [dataset=finance] [step=transform] FAILED (810ms) — error message

    Usage:
        with step_log(log, "transform", dataset="finance", year=2024):
            df_long = do_transform(df_wide)

        # รับ context object เพื่อ attach ข้อมูลระหว่างรัน
        with step_log(log, "read_csv", year=2024) as ctx:
            df = spark.read.csv(...)
            ctx["rows"] = df.count()  # จะโผล่ใน SUCCESS log
    """
    ctx = {}
    tag = f"[dataset={dataset}] [step={step}]"
    fields = dict(dataset=dataset, step=step, **extra)

    log.info(f"{tag} START", **fields)
    t0 = time.time()
    try:
        yield ctx
        duration_ms = int((time.time() - t0) * 1000)
        merged = {**fields, **ctx, "duration_ms": duration_ms}
        log.info(f"{tag} SUCCESS ({duration_ms}ms)", **merged)
    except Exception as e:
        duration_ms = int((time.time() - t0) * 1000)
        error_msg = _parse_error(e)
        merged = {**fields, **ctx, "duration_ms": duration_ms, "error": error_msg}
        log.error(f"{tag} FAILED ({duration_ms}ms) — {error_msg}", **merged)
        raise


def _parse_error(e: Exception) -> str:
    """
    แปลง exception ให้อ่านง่าย โดยเฉพาะ Spark/Java errors

    - Py4JJavaError: ดึงแค่ root cause บรรทัดแรก ตัด stacktrace ทิ้ง
    - Exception ทั่วไป: แสดงตามปกติ

    ตัวอย่าง:
        Py4JJavaError ยาว 100 บรรทัด →
        "QueryExecutionException: Parquet column cannot be converted...
         Expected: string, Found: DOUBLE"
    """
    try:
        from py4j.protocol import Py4JJavaError
        if isinstance(e, Py4JJavaError):
            java_str = str(e.java_exception)
            lines = [l.strip() for l in java_str.splitlines() if l.strip()]

            # หา root cause — บรรทัดหลัง "Caused by:" สุดท้าย
            root_cause_idx = None
            for i, line in enumerate(lines):
                if line.startswith("Caused by:"):
                    root_cause_idx = i

            if root_cause_idx is not None:
                # เอา Caused by: + บรรทัดถัดไปที่มีข้อความ (ถ้ามี)
                cause_line = lines[root_cause_idx].replace("Caused by: ", "")
                # บางครั้งมี detail ในบรรทัดถัดไป
                if root_cause_idx + 1 < len(lines) and not lines[root_cause_idx + 1].startswith("at "):
                    cause_line += " — " + lines[root_cause_idx + 1]
                return cause_line
            else:
                # ไม่มี Caused by → เอาบรรทัดแรก
                return lines[0] if lines else str(e)
    except ImportError:
        pass

    return str(e)