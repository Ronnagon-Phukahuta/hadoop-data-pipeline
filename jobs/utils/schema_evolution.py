# jobs/utils/schema_evolution.py
"""
Schema Evolution — sync Hive table schema กับ DataFrame ก่อน write
ทำงาน 2 อย่าง:
  1. column ใหม่ใน df → ALTER TABLE ADD COLUMNS
  2. column ขาดใน df → เพิ่ม null column ให้ df ก่อน write
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from typing import List


def sync_schema(
    spark,
    df: DataFrame,
    table_name: str,
    database: str = "default",
    skip_cols: List[str] = None,
) -> DataFrame:
    """
    Sync schema ระหว่าง df กับ Hive table แล้วคืน df ที่พร้อม write

    Args:
        spark: SparkSession
        df: DataFrame ที่ได้จาก CSV
        table_name: ชื่อ Hive table เช่น "finance_itsc_wide"
        database: ชื่อ database ใน Hive
        skip_cols: columns ที่ไม่ต้อง sync เช่น partition col ["year"]

    Returns:
        DataFrame ที่ columns ตรงกับ Hive table แล้ว
    """
    skip_cols = set(skip_cols or [])

    # ── ดึง schema ปัจจุบันจาก Hive ──────────────────────────────────────────
    try:
        hive_schema = {
            row["col_name"]: row["data_type"]
            for row in spark.sql(f"DESCRIBE {database}.{table_name}").collect()
            if row["col_name"] and not row["col_name"].startswith("#")
        }
    except Exception:
        # table ยังไม่มี → ไม่ต้อง sync
        return df

    df_cols = set(df.columns) - skip_cols
    hive_cols = set(hive_schema.keys()) - skip_cols

    # ── 1. column ใหม่ใน df → ALTER TABLE ────────────────────────────────────
    new_cols = df_cols - hive_cols
    if new_cols:
        # infer type จาก df แทน hardcode STRING
        col_defs = ", ".join(
            f"`{c}` {dict(df.dtypes).get(c, 'DOUBLE').upper()}"
            for c in sorted(new_cols)
        )
        alter_sql = f"ALTER TABLE {database}.{table_name} ADD COLUMNS ({col_defs})"
        spark.sql(alter_sql)

    # ── 2. column ขาดใน df → เพิ่ม null column ──────────────────────────────
    missing_cols = hive_cols - df_cols
    for col_name in missing_cols:
        col_type = hive_schema.get(col_name, "string")
        df = df.withColumn(col_name, lit(None).cast(col_type))

    return df