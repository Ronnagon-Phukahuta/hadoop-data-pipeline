# -*- coding: utf-8 -*-
# engine/run_pipeline.py
"""
Entry point สำหรับรัน ETL pipeline

รองรับ 2 วิธี:

1) spark-submit argument:
   spark-submit /jobs/engine/run_pipeline.py finance_itsc

2) Airflow Variable (set ผ่าน Airflow UI หรือ dag_run.conf):
   Variable key: "dataset_name"
   dag_run.conf: {"dataset_name": "finance_itsc"}
   (ใช้ใน Item 4 — Airflow Parameterization)

Priority: CLI arg > dag_run.conf > Airflow Variable > default
"""

import sys
from pathlib import Path
from pyspark.sql import SparkSession
from datasets.registry import load_dataset

# ── เพิ่ม project root เข้า sys.path สำหรับ spark-submit ───
PROJECT_ROOT = str(Path(__file__).parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def _resolve_dataset_name() -> str:
    """
    หา dataset name จากหลาย source ตาม priority:
    CLI arg > dag_run.conf > Airflow Variable > default
    """
    # 1) CLI argument: spark-submit run_pipeline.py <dataset_name>
    if len(sys.argv) > 1:
        return sys.argv[1]

    # 2) Airflow dag_run.conf หรือ Variable (import เฉพาะเมื่ออยู่ใน Airflow)
    try:
        from airflow.models import Variable
        from airflow.operators.python import get_current_context
        try:
            ctx = get_current_context()
            conf_name = ctx["dag_run"].conf.get("dataset_name")
            if conf_name:
                return conf_name
        except Exception:
            pass
        var_name = Variable.get("dataset_name", default_var=None)
        if var_name:
            return var_name
    except ImportError:
        pass  # ไม่ได้รันใน Airflow

    # 3) Default
    return "finance_itsc"


def main():
    dataset_name = _resolve_dataset_name()

    # โหลด config จาก registry
    ds = load_dataset(dataset_name)

    spark = (SparkSession.builder
             .appName(f"ETL Pipeline — {ds.dataset}")
             .enableHiveSupport()
             .getOrCreate())
    sc = spark.sparkContext

    from engine.pipeline import run_pipeline
    run_pipeline(spark, sc, ds)

    spark.stop()


if __name__ == "__main__":
    main()