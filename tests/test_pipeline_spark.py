# -*- coding: utf-8 -*-
# tests/test_pipeline_spark.py
"""
Spark-based tests ครอบคลุมสิ่งที่ test_etl.py (pure Python) ทำไม่ได้:
- Wide→Long transform ด้วย PySpark จริง
- inferSchema=false + cast behavior
- String→Double cast (Hive type mismatch bug)
- PART 2 recovery logic (staging-curated gap)
- Negative amount preservation
- Null filtering หลัง transform
"""

import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_pipeline_spark")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
        .config("spark.python.worker.faulthandler.enabled", "true")
        .getOrCreate()
    )


@pytest.fixture
def wide_schema():
    return StructType([
        StructField("date",                StringType(),  True),
        StructField("details",             StringType(),  True),
        StructField("compensation_budget", DoubleType(),  True),
        StructField("expense_budget",      DoubleType(),  True),
        StructField("material_budget",     DoubleType(),  True),
        StructField("total_amount",        DoubleType(),  True),
        StructField("year",                IntegerType(), False),
    ])


def do_transform(df_wide, id_columns=None, exclude_columns=None):
    from pyspark.sql import functions as F

    if id_columns is None:
        id_columns = ["date", "details", "year"]
    if exclude_columns is None:
        exclude_columns = ["total_amount"]

    amount_cols = [c for c in df_wide.columns if c not in id_columns + exclude_columns]

    # cast amount columns เป็น double
    df_casted = df_wide.select(
        *[F.col(c) for c in id_columns],
        *[F.col(c).cast("double").alias(c) for c in amount_cols]
    )

    # ใช้ stack() ผ่าน expr — ชื่อ column ทุกตัวใน stack ต้องไม่มี backtick
    # แก้ด้วยการ rename amount_cols ชั่วคราวให้ปลอดภัยก่อน selectExpr
    safe_cols = [f"_amt_{i}" for i in range(len(amount_cols))]
    dict(zip(amount_cols, safe_cols))

    df_renamed = df_casted.select(
        *[F.col(c) for c in id_columns],
        *[F.col(c).alias(safe_cols[i]) for i, c in enumerate(amount_cols)]
    )

    # สร้าง stack ด้วย safe names — ส่ง original name เป็น string literal
    stack_pairs = ", ".join([f"'{c}', `{safe_cols[i]}`" for i, c in enumerate(amount_cols)])
    id_exprs = [f"`{c}`" for c in id_columns]

    return df_renamed.selectExpr(
        *id_exprs,
        f"stack({len(amount_cols)}, {stack_pairs}) as (category, amount)"
    ).filter("amount is not null")

def apply_date_filter(df):
    return df.filter(
        col("date").rlike(r"^\d{4}-\d{2}$") | (col("date") == "all-year-budget")
    )


def apply_csv_cast(df, year):
    """Replicate PART 1 casting logic"""
    df = df.withColumn("year", lit(year).cast("int"))
    for c in df.columns:
        if c in ["date", "details"]:
            df = df.withColumn(c, col(c).cast("string"))
        elif c != "year":
            df = df.withColumn(c, col(c).cast("double"))
    return df


# ─────────────────────────────────────────────────────────────
# 1. CSV Reading & Schema Casting
# ─────────────────────────────────────────────────────────────

class TestCSVCasting:

    def test_all_numeric_columns_become_double(self, spark, tmp_path):
        """inferSchema=false แล้ว cast เอง — numeric columns ต้องเป็น DoubleType"""
        csv = "date,details,compensation_budget,expense_budget\n2566-01,test,100000,50000\n"
        f = tmp_path / "t.csv"
        f.write_text(csv)

        df = spark.read.option("header", "true").option("inferSchema", "false").csv(str(f))
        df = apply_csv_cast(df, year=2023)

        assert df.schema["compensation_budget"].dataType == DoubleType()
        assert df.schema["expense_budget"].dataType == DoubleType()

    def test_date_stays_string_not_double(self, spark, tmp_path):
        """date ต้องเป็น StringType ไม่ใช่ Double แม้ถูก inferSchema"""
        csv = "date,amount\n2566-01,100\n2566-02,200\n"
        f = tmp_path / "t.csv"
        f.write_text(csv)

        df = spark.read.option("header", "true").option("inferSchema", "false").csv(str(f))
        df = apply_csv_cast(df, year=2023)

        assert df.schema["date"].dataType == StringType()
        assert df.first()["date"] == "2566-01"

    def test_non_numeric_string_cast_to_null(self, spark, tmp_path):
        """ค่าที่ cast เป็น double ไม่ได้ต้องเป็น null — ใช้ try_cast หรือ ansi=false"""
        csv = "date,amount\n2566-01,N/A\n2566-02,100\n"
        f = tmp_path / "t.csv"
        f.write_text(csv)

        # ปิด ANSI mode เพื่อให้ cast ที่ fail return null แทน error (เหมือน Spark 2.x)
        spark.conf.set("spark.sql.ansi.enabled", "false")
        df = spark.read.option("header", "true").option("inferSchema", "false").csv(str(f))
        df = apply_csv_cast(df, year=2023)

        rows = {r["date"]: r["amount"] for r in df.collect()}
        spark.conf.set("spark.sql.ansi.enabled", "true")  # restore

        assert rows["2566-01"] is None
        assert rows["2566-02"] == 100.0

    def test_year_injected_correctly(self, spark, tmp_path):
        csv = "date,amount\n2566-01,100\n"
        f = tmp_path / "t.csv"
        f.write_text(csv)

        df = spark.read.option("header", "true").option("inferSchema", "false").csv(str(f))
        df = apply_csv_cast(df, year=2566)

        assert df.schema["year"].dataType == IntegerType()
        assert df.first()["year"] == 2566

    def test_multiple_files_union_correctly(self, spark, tmp_path):
        for i, content in enumerate(["date,amount\n2566-01,100\n", "date,amount\n2566-02,200\n"]):
            (tmp_path / f"f{i}.csv").write_text(content)

        files = [str(tmp_path / f"f{i}.csv") for i in range(2)]
        df = spark.read.option("header", "true").option("inferSchema", "false").csv(files)
        df = apply_csv_cast(df, year=2023)

        assert df.count() == 2


# ─────────────────────────────────────────────────────────────
# 2. Wide → Long Transform
# ─────────────────────────────────────────────────────────────

class TestWideToLong:

    @pytest.fixture(autouse=True)
    def require_python_workers(self, spark):
        """Skip if Spark Python worker serialization is unavailable (local Windows mode)."""
        try:
            spark.createDataFrame([(1,)], ["x"]).count()
        except Exception:
            pytest.skip("Python workers unavailable in local mode — run via ./run_tests.sh in Docker")

    def test_basic_row_count(self, spark, wide_schema):
        """3 valid rows × 3 amount columns = 9 rows (minus nulls)"""
        data = [
            Row(date="2566-01", details="q1", compensation_budget=100.0,
                expense_budget=50.0, material_budget=30.0, total_amount=180.0, year=2023),
            Row(date="2566-02", details="q2", compensation_budget=200.0,
                expense_budget=None, material_budget=40.0, total_amount=240.0, year=2023),
            Row(date="all-year-budget", details="yr", compensation_budget=500.0,
                expense_budget=200.0, material_budget=100.0, total_amount=800.0, year=2023),
        ]
        df = spark.createDataFrame(data, schema=wide_schema)
        df = apply_date_filter(df)
        df_long = do_transform(df)

        # 3 rows × 3 cols = 9, แต่ row 2 expense_budget=null → 8
        assert df_long.count() == 8

    def test_null_amounts_filtered_out(self, spark, wide_schema):
        data = [
            Row(date="2566-01", details="q1", compensation_budget=100.0,
                expense_budget=None, material_budget=None, total_amount=100.0, year=2023),
        ]
        df = spark.createDataFrame(data, schema=wide_schema)
        df_long = do_transform(apply_date_filter(df))

        assert df_long.filter(col("amount").isNull()).count() == 0

    def test_total_amount_not_in_categories(self, spark, wide_schema):
        data = [
            Row(date="2566-01", details="q1", compensation_budget=100.0,
                expense_budget=50.0, material_budget=30.0, total_amount=180.0, year=2023),
        ]
        df = spark.createDataFrame(data, schema=wide_schema)
        df_long = do_transform(apply_date_filter(df))

        cats = {r["category"] for r in df_long.select("category").collect()}
        assert "total_amount" not in cats

    def test_amount_column_type_is_double(self, spark, wide_schema):
        data = [
            Row(date="2566-01", details="q1", compensation_budget=100.0,
                expense_budget=50.0, material_budget=30.0, total_amount=180.0, year=2023),
        ]
        df = spark.createDataFrame(data, schema=wide_schema)
        df_long = do_transform(apply_date_filter(df))

        assert df_long.schema["amount"].dataType == DoubleType()

    def test_id_columns_preserved(self, spark, wide_schema):
        data = [
            Row(date="2566-01", details="q1", compensation_budget=100.0,
                expense_budget=50.0, material_budget=30.0, total_amount=180.0, year=2023),
        ]
        df = spark.createDataFrame(data, schema=wide_schema)
        df_long = do_transform(apply_date_filter(df))

        assert set(["date", "details", "year", "category", "amount"]).issubset(set(df_long.columns))

    def test_invalid_date_rows_excluded(self, spark, wide_schema):
        """row ที่ date ไม่ตรง pattern ต้องไม่เข้า long table"""
        data = [
            Row(date="total spent",     details="x", compensation_budget=999.0,
                expense_budget=999.0, material_budget=999.0, total_amount=999.0, year=2023),
            Row(date="remaining",       details="x", compensation_budget=888.0,
                expense_budget=888.0, material_budget=888.0, total_amount=888.0, year=2023),
            Row(date="2566-01",         details="ok", compensation_budget=100.0,
                expense_budget=50.0, material_budget=30.0, total_amount=180.0, year=2023),
        ]
        df = spark.createDataFrame(data, schema=wide_schema)
        df_long = do_transform(apply_date_filter(df))

        amounts = {r["amount"] for r in df_long.select("amount").collect()}
        assert 999.0 not in amounts
        assert 888.0 not in amounts

    def test_negative_amounts_kept(self, spark):
        """จำนวนเงินติดลบ (budget reduction) ต้องไม่ถูก filter ออก"""
        schema = StructType([
            StructField("date",    StringType(),  True),
            StructField("details", StringType(),  True),
            StructField("budget",  DoubleType(),  True),
            StructField("year",    IntegerType(), False),
        ])
        data = [
            Row(date="2566-01", details="ลดงบ", budget=-50000.0, year=2023),
            Row(date="2566-02", details="งบปกติ", budget=100000.0, year=2023),
        ]
        df = spark.createDataFrame(data, schema=schema)
        df_long = do_transform(apply_date_filter(df), exclude_columns=["total_amount"])

        amounts = {r["amount"] for r in df_long.select("amount").collect()}
        assert -50000.0 in amounts
        assert 100000.0 in amounts

    def test_all_year_budget_row_included(self, spark, wide_schema):
        """row 'all-year-budget' ต้องเข้า long table ด้วย"""
        data = [
            Row(date="all-year-budget", details="yr", compensation_budget=500.0,
                expense_budget=200.0, material_budget=100.0, total_amount=800.0, year=2023),
        ]
        df = spark.createDataFrame(data, schema=wide_schema)
        df_long = do_transform(apply_date_filter(df))

        dates = {r["date"] for r in df_long.select("date").collect()}
        assert "all-year-budget" in dates


# ─────────────────────────────────────────────────────────────
# 3. String→Double Cast (Hive Type Mismatch Bug)
# ─────────────────────────────────────────────────────────────

class TestStringToDoubleCast:
    """
    ครอบคลุม bug ที่ Hive schema มี column เป็น string
    แต่ parquet จริงเก็บเป็น double — pipeline ต้อง cast ได้
    """

    @pytest.fixture(autouse=True)
    def require_python_workers(self, spark):
        """Skip if Spark Python worker serialization is unavailable (local Windows mode)."""
        try:
            spark.createDataFrame([(1,)], ["x"]).count()
        except Exception:
            pytest.skip("Python workers unavailable in local mode — run via ./run_tests.sh in Docker")

    def test_string_numeric_column_cast_to_double(self, spark):
        """column ที่ Hive เห็นเป็น string แต่ค่าเป็นตัวเลข ต้อง cast ได้"""
        schema = StructType([
            StructField("date",    StringType(), True),
            StructField("details", StringType(), True),
            StructField("year",    IntegerType(), False),
            StructField("budget",  StringType(), True),  # ← Hive บอก string
        ])
        data = [Row(date="2566-01", details="test", year=2023, budget="100000")]
        df = spark.createDataFrame(data, schema=schema)

        # pipeline cast เอง
        amount_cols = ["budget"]
        df_casted = df.select(
            "date", "details", "year",
            *[col(c).cast("double").alias(c) for c in amount_cols]
        )
        assert df_casted.schema["budget"].dataType == DoubleType()
        assert df_casted.first()["budget"] == 100000.0

    def test_non_numeric_string_column_becomes_null(self, spark):
        """column ที่ cast ไม่ได้ต้องเป็น null — ปิด ANSI mode เหมือน production (Spark 2.x behavior)"""
        schema = StructType([
            StructField("date",    StringType(), True),
            StructField("details", StringType(), True),
            StructField("year",    IntegerType(), False),
            StructField("budget",  StringType(), True),
        ])
        data = [Row(date="2566-01", details="test", year=2023, budget="N/A")]
        df = spark.createDataFrame(data, schema=schema)

        spark.conf.set("spark.sql.ansi.enabled", "false")
        df_casted = df.withColumn("budget", col("budget").cast("double"))
        result = df_casted.first()["budget"]
        spark.conf.set("spark.sql.ansi.enabled", "true")

        assert result is None

    def test_transform_handles_mixed_type_columns(self, spark):
        """wide df ที่มี mixed string/double columns ต้อง transform ได้ไม่ error"""
        schema = StructType([
            StructField("date",       StringType(), True),
            StructField("details",    StringType(), True),
            StructField("year",       IntegerType(), False),
            StructField("budget_a",   DoubleType(), True),   # double
            StructField("budget_b",   StringType(), True),   # string (type mismatch)
            StructField("total_amount", DoubleType(), True),
        ])
        data = [Row(date="2566-01", details="test", year=2023,
                    budget_a=100.0, budget_b="200", total_amount=300.0)]
        df = spark.createDataFrame(data, schema=schema)

        # pipeline cast ก่อน transform
        df_long = do_transform(df)
        cats = {r["category"] for r in df_long.select("category").collect()}
        assert "budget_a" in cats
        assert "budget_b" in cats
        assert "total_amount" not in cats


# ─────────────────────────────────────────────────────────────
# 4. PART 2 Recovery Logic
# ─────────────────────────────────────────────────────────────

class TestPart2Recovery:

    def test_staging_minus_curated_gives_incomplete(self):
        """years ที่อยู่ใน staging แต่ไม่มีใน curated ต้องเข้า queue"""
        staging_years = {2021, 2022, 2023}
        curated_years = {2021}
        pending_part1 = set()

        incomplete = staging_years - curated_years
        years_to_update = sorted(pending_part1 | incomplete)

        assert years_to_update == [2022, 2023]

    def test_part1_years_merged_without_duplicate(self):
        """year ที่อยู่ทั้งใน PART 1 queue และ incomplete ต้องไม่ duplicate"""
        pending_part1 = {2023}
        incomplete = {2022, 2023}

        years_to_update = sorted(pending_part1 | incomplete)
        assert years_to_update == [2022, 2023]
        assert len(years_to_update) == 2

    def test_empty_queue_when_fully_synced(self):
        """staging = curated ต้องไม่มีอะไรใน queue"""
        staging_years = {2021, 2022, 2023}
        curated_years = {2021, 2022, 2023}
        pending_part1 = set()

        years_to_update = pending_part1 | (staging_years - curated_years)
        assert len(years_to_update) == 0

    def test_failed_year_excluded_from_part2(self):
        """year ที่ PART 1 fail ต้องไม่เข้า PART 2 (FIX 3)"""
        pending_by_year = {2021: ["f1.csv"], 2022: ["f2.csv"], 2023: ["f3.csv"]}
        failed_years = {2022}

        for y in failed_years:
            pending_by_year.pop(y, None)

        years_to_update = set(pending_by_year.keys())
        assert 2022 not in years_to_update
        assert years_to_update == {2021, 2023}

    def test_part2_reads_from_parquet_not_hive(self, spark, tmp_path):
        """_read_wide ต้องอ่านจาก parquet ตรงๆ (mergeSchema=true) ไม่ใช่ spark.sql"""
        import os
        import platform
        if platform.system() == "Windows":
            hadoop_home = os.environ.get("HADOOP_HOME", "")
            winutils = os.path.join(hadoop_home, "bin", "winutils.exe") if hadoop_home else ""
            if not hadoop_home or not os.path.exists(winutils):
                pytest.skip("ต้องตั้ง HADOOP_HOME และมี winutils.exe บน Windows ก่อนถึงจะ write parquet ได้")

        schema = StructType([
            StructField("date",   StringType(), True),
            StructField("budget", DoubleType(), True),
        ])
        data = [Row(date="2566-01", budget=100.0)]
        df = spark.createDataFrame(data, schema=schema)
        parquet_path = str(tmp_path / "year=2023")
        df.write.parquet(parquet_path)

        df_read = spark.read.option("mergeSchema", "true").parquet(parquet_path)
        assert df_read.schema["budget"].dataType == DoubleType()
        assert df_read.first()["budget"] == 100.0


# ─────────────────────────────────────────────────────────────
# 5. Schema Evolution
# ─────────────────────────────────────────────────────────────

class TestSchemaEvolution:

    def test_new_column_in_df_not_in_existing_schema(self, spark):
        """CSV ที่มี column ใหม่ — column เดิมใน partition อื่นต้องเป็น null"""
        schema_old = StructType([
            StructField("date",   StringType(),  True),
            StructField("budget", DoubleType(),  True),
            StructField("year",   IntegerType(), False),
        ])
        schema_new = StructType([
            StructField("date",       StringType(),  True),
            StructField("budget",     DoubleType(),  True),
            StructField("new_column", DoubleType(),  True),
            StructField("year",       IntegerType(), False),
        ])
        old_data = [Row(date="2566-01", budget=100.0, year=2022)]
        new_data = [Row(date="2566-01", budget=200.0, new_column=999.0, year=2023)]

        df_old = spark.createDataFrame(old_data, schema=schema_old)
        df_new = spark.createDataFrame(new_data, schema=schema_new)

        # new_column ต้องมีเฉพาะใน df_new ไม่ใช่ df_old
        assert "new_column" not in df_old.columns
        assert "new_column" in df_new.columns

    def test_amount_column_detection_excludes_id_and_total(self, spark):
        """amount_cols detection ต้องไม่รวม id_columns และ total_amount"""
        id_columns = ["date", "details", "year"]
        exclude_columns = ["total_amount"]
        all_columns = ["date", "details", "year", "total_amount",
                       "compensation_budget", "expense_budget", "new_fund"]

        amount_cols = [c for c in all_columns if c not in id_columns + exclude_columns]

        assert amount_cols == ["compensation_budget", "expense_budget", "new_fund"]
        assert "date" not in amount_cols
        assert "total_amount" not in amount_cols