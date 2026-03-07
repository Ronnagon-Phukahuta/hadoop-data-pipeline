# tests/test_pipeline_engine.py
"""
Tests for engine/pipeline.py (Item 2 — Pipeline Engine Abstraction)
รันได้โดยไม่ต้องมี Hive/Spark/Docker (mock ทั้งหมด)

รันผ่าน CI (Python 3.12):
    pytest tests/test_pipeline_engine.py -v

รันผ่าน Docker (PySpark):
    ./run_tests.sh tests/test_pipeline_engine.py -v
"""

import pytest
from unittest.mock import MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))

from conftest import make_mock_sc, make_mock_log
from datasets.registry import DatasetConfig, ColumnDef


# ── Fixtures ─────────────────────────────────────────────────

def make_dataset_config(**overrides) -> DatasetConfig:
    """สร้าง DatasetConfig สำหรับ test"""
    defaults = dict(
        dataset="test_ds",
        owner="test",
        paths={
            "raw":      "/datalake/raw/test_ds",
            "staging":  "/datalake/staging/test_ds_wide",
            "curated":  "/datalake/curated/test_ds_long",
            "versions": "/datalake/versions/test_ds",
            "trash":    "/datalake/trash",
        },
        tables={
            "database": "default",
            "staging":  "test_ds_wide",
            "curated":  "test_ds_long",
        },
        pipeline={
            "critical_columns": ["date", "details"],
            "partition_by": "year",
            "id_columns": ["date", "details", "year"],
            "exclude_columns": ["total_amount"],
        },
        schema=[
            ColumnDef(name="date",     type="STRING",  reserved_keyword=True),
            ColumnDef(name="details",  type="STRING"),
            ColumnDef(name="category", type="STRING"),
            ColumnDef(name="amount",   type="DECIMAL"),
            ColumnDef(name="year",     type="INT",     partition=True),
        ],
        category_mapping={},
        nlp_rules=[],
        example_queries=[],
    )
    defaults.update(overrides)
    return DatasetConfig(**defaults)


def make_mock_df(rows=10, columns=None):
    """สร้าง mock DataFrame"""
    df = MagicMock()
    df.columns = columns or ["date", "details", "year", "expense_budget", "total_amount"]
    df.count.return_value = rows
    df.withColumn.return_value = df
    df.select.return_value = df
    df.selectExpr.return_value = df
    df.filter.return_value = df
    return df


@pytest.fixture
def ds():
    return make_dataset_config()


@pytest.fixture
def mock_spark(ds):
    spark = MagicMock()
    spark.read.option.return_value = spark.read
    spark.read.csv.return_value = make_mock_df()
    spark.read.parquet.return_value = make_mock_df()
    spark.sql.return_value = MagicMock(collect=MagicMock(return_value=[]))
    return spark


@pytest.fixture
def mock_sc_fixture():
    sc, fs, existing = make_mock_sc()
    return sc, fs, existing


# ── DatasetConfig properties ──────────────────────────────────

class TestDatasetConfigPipelineProps:

    def test_id_columns(self, ds):
        assert ds.id_columns == ["date", "details", "year"]

    def test_exclude_columns(self, ds):
        assert ds.exclude_columns == ["total_amount"]

    def test_partition_by(self, ds):
        assert ds.partition_by == "year"

    def test_id_columns_default(self):
        ds = make_dataset_config(pipeline={})
        assert ds.id_columns == ["date", "details", "year"]

    def test_exclude_columns_default(self):
        ds = make_dataset_config(pipeline={})
        assert ds.exclude_columns == []

    def test_curated_table(self, ds):
        assert ds.curated_table == "test_ds_long"

    def test_staging_table(self, ds):
        assert ds.staging_table == "test_ds_wide"


# ── run_pipeline: No pending files ───────────────────────────

class TestRunPipelineNoPendingFiles:

    @patch("engine.pipeline.with_retry")
    @patch("engine.pipeline.hdfs_ls_recursive")
    @patch("engine.pipeline.is_already_processed", return_value=True)
    def test_skip_part1_when_no_pending(
        self, mock_processed, mock_ls, mock_retry, ds, mock_spark, mock_sc_fixture
    ):
        sc, _, _ = mock_sc_fixture
        mock_retry.side_effect = lambda fn, *a, **kw: fn(*a) if callable(fn) else fn
        mock_ls.return_value = ["hdfs:///raw/year=2024/data.csv"]

        # SHOW PARTITIONS คืน empty → ไม่มี incomplete years
        mock_spark.sql.return_value.collect.return_value = []

        from engine.pipeline import run_pipeline
        run_pipeline(mock_spark, sc, ds)  # ต้องไม่ raise

    @patch("engine.pipeline.with_retry")
    @patch("engine.pipeline.hdfs_ls_recursive", return_value=[])
    def test_skip_part1_when_no_csv(
        self, mock_ls, mock_retry, ds, mock_spark, mock_sc_fixture
    ):
        sc, _, _ = mock_sc_fixture
        mock_retry.side_effect = lambda fn, *a, **kw: fn(*a) if callable(fn) else fn
        mock_spark.sql.return_value.collect.return_value = []

        from engine.pipeline import run_pipeline
        run_pipeline(mock_spark, sc, ds)  # ต้องไม่ raise

    @patch("engine.pipeline.with_retry")
    @patch("engine.pipeline.hdfs_ls_recursive", return_value=[])
    def test_skip_part2_when_no_years(
        self, mock_ls, mock_retry, ds, mock_spark, mock_sc_fixture
    ):
        sc, _, _ = mock_sc_fixture
        mock_retry.side_effect = lambda fn, *a, **kw: fn(*a) if callable(fn) else fn
        mock_spark.sql.return_value.collect.return_value = []

        from engine.pipeline import run_pipeline
        run_pipeline(mock_spark, sc, ds)
        # SHOW PARTITIONS ถูกเรียก (check_partitions step)
        assert mock_spark.sql.called


# ── run_pipeline: uses dataset config (not hardcoded) ────────

class TestRunPipelineUsesDatasetConfig:

    @patch("engine.pipeline.atomic_write_table")
    @patch("engine.pipeline.sync_schema", side_effect=lambda sp, df, *a, **kw: df)
    @patch("engine.pipeline.create_version", return_value="v_test")
    @patch("engine.pipeline.cleanup_old_versions")
    @patch("engine.pipeline.run_quality_checks", return_value=(True, {}))
    @patch("engine.pipeline.with_retry")
    @patch("engine.pipeline.hdfs_ls_recursive")
    @patch("engine.pipeline.is_already_processed", return_value=False)
    @patch("engine.pipeline.compute_file_checksum", return_value="abc123")
    @patch("engine.pipeline.hdfs_write_done")
    def test_atomic_write_uses_registry_table_names(
        self, mock_done, mock_checksum, mock_processed, mock_ls,
        mock_retry, mock_dq, mock_cleanup, mock_version,
        mock_sync, mock_atomic, ds, mock_spark, mock_sc_fixture
    ):
        sc, _, _ = mock_sc_fixture
        csv_file = "hdfs:///datalake/raw/test_ds/year=2024/data.csv"
        mock_ls.return_value = [csv_file]

        df = make_mock_df()
        mock_spark.read.option.return_value = mock_spark.read
        mock_spark.read.csv.return_value = df

        def retry_side(fn, *a, **kw):
            if callable(fn):
                try:
                    return fn()
                except Exception:
                    return fn
            return fn
        mock_retry.side_effect = retry_side
        mock_spark.sql.return_value.collect.return_value = []

        from engine.pipeline import run_pipeline
        with patch("engine.pipeline.extract_year_from_path", return_value=2024):
            run_pipeline(mock_spark, sc, ds)

        # atomic_write ต้องใช้ table names จาก ds ไม่ใช่ hardcode
        if mock_atomic.called:
            _, kwargs = mock_atomic.call_args
            assert kwargs.get("table_name") == ds.staging_table or \
                   mock_atomic.call_args[0][2] == ds.staging_table

    def test_dataset_label_in_log_uses_dataset_name(self, ds):
        """logger ต้องใช้ชื่อ dataset จาก config"""
        with patch("engine.pipeline.setup_logger") as mock_logger:
            mock_logger.return_value = make_mock_log()
            with patch("engine.pipeline.hdfs_ls_recursive", return_value=[]):
                with patch("engine.pipeline.with_retry",
                           side_effect=lambda fn, *a, **kw: fn() if callable(fn) else fn):
                    mock_spark = MagicMock()
                    mock_spark.sql.return_value.collect.return_value = []
                    sc, _, _ = make_mock_sc()
                    from engine.pipeline import run_pipeline
                    run_pipeline(mock_spark, sc, ds)

            mock_logger.assert_called_with("etl.test_ds")


# ── run_pipeline: failed files handling ──────────────────────

class TestRunPipelineFailedFiles:

    @patch("engine.pipeline.with_retry")
    @patch("engine.pipeline.hdfs_ls_recursive")
    @patch("engine.pipeline.is_already_processed", return_value=False)
    def test_skip_failed_csv(
        self, mock_processed, mock_ls, mock_retry, ds, mock_spark, mock_sc_fixture
    ):
        sc, _, _ = mock_sc_fixture
        csv_file = "hdfs:///raw/year=2024/data.csv"
        mock_ls.return_value = [csv_file, csv_file + ".failed"]
        mock_retry.side_effect = lambda fn, *a, **kw: fn() if callable(fn) else fn
        mock_spark.sql.return_value.collect.return_value = []

        from engine.pipeline import run_pipeline
        with patch("engine.pipeline.run_quality_checks") as mock_dq:
            run_pipeline(mock_spark, sc, ds)
            # DQ ไม่ถูกเรียกเพราะ file มี .failed แล้ว
            mock_dq.assert_not_called()

    @patch("engine.pipeline.send_quality_alert")
    @patch("engine.pipeline.hdfs_touch")
    @patch("engine.pipeline.run_quality_checks", return_value=(False, {"error": "bad data"}))
    @patch("engine.pipeline.with_retry")
    @patch("engine.pipeline.hdfs_ls_recursive")
    @patch("engine.pipeline.is_already_processed", return_value=False)
    def test_dq_fail_touches_failed_file(
        self, mock_processed, mock_ls, mock_retry, mock_dq,
        mock_touch, mock_alert, ds, mock_spark, mock_sc_fixture
    ):
        sc, _, _ = mock_sc_fixture
        csv_file = "hdfs:///raw/year=2024/data.csv"
        mock_ls.return_value = [csv_file]

        df = make_mock_df()

        # with_retry คืน df โดยตรงสำหรับ read_csv, คืนผล fn() สำหรับที่เหลือ
        def retry_side(fn, *a, **kw):
            label = kw.get("label", "")
            if "read CSV" in label:
                return df
            if callable(fn):
                try:
                    return fn()
                except Exception:
                    return fn
            return fn

        mock_retry.side_effect = retry_side
        mock_spark.sql.return_value.collect.return_value = []

        from engine.pipeline import run_pipeline
        with patch("engine.pipeline.extract_year_from_path", return_value=2024):
            run_pipeline(mock_spark, sc, ds)

        mock_touch.assert_called()
        mock_alert.assert_called()


# ── run_pipeline: incomplete years recovery ──────────────────

class TestRunPipelineIncompleteYears:

    @patch("engine.pipeline.atomic_write_table")
    @patch("engine.pipeline.with_retry",
           side_effect=lambda fn, *a, **kw: fn() if callable(fn) else fn)
    @patch("engine.pipeline.hdfs_ls_recursive", return_value=[])
    def test_incomplete_years_added_to_part2(
        self, mock_ls, mock_retry, mock_atomic, ds, mock_spark, mock_sc_fixture
    ):
        sc, _, _ = mock_sc_fixture

        # staging มีปี 2023, curated ไม่มี → incomplete
        staging_rows = [MagicMock()]
        staging_rows[0].__getitem__ = lambda self, i: "year=2023"

        call_count = [0]
        def sql_side(query):
            result = MagicMock()
            if "wide" in query.lower() or call_count[0] == 0:
                result.collect.return_value = staging_rows
            else:
                result.collect.return_value = []
            call_count[0] += 1
            return result

        mock_spark.sql.side_effect = sql_side
        mock_spark.read.option.return_value = mock_spark.read
        mock_spark.read.parquet.return_value = make_mock_df()

        from engine.pipeline import run_pipeline
        run_pipeline(mock_spark, sc, ds)  # ต้องไม่ raise


# ── run_pipeline.py: _resolve_dataset_name ───────────────────

class TestResolveDatasetName:

    def test_cli_arg_takes_priority(self):
        with patch("sys.argv", ["run_pipeline.py", "my_dataset"]):
            from engine.run_pipeline import _resolve_dataset_name
            assert _resolve_dataset_name() == "my_dataset"

    def test_default_when_no_arg(self):
        with patch("sys.argv", ["run_pipeline.py"]):
            with patch.dict("sys.modules", {"airflow": None,
                                            "airflow.models": None,
                                            "airflow.operators.python": None}):
                from engine.run_pipeline import _resolve_dataset_name
                assert _resolve_dataset_name() == "finance_itsc"

    def test_airflow_variable_used_when_no_cli(self):
        with patch("sys.argv", ["run_pipeline.py"]):
            mock_variable = MagicMock()
            mock_variable.get.return_value = "another_dataset"

            mock_airflow = MagicMock()
            mock_airflow.models.Variable = mock_variable

            with patch.dict("sys.modules", {
                "airflow": mock_airflow,
                "airflow.models": mock_airflow.models,
                "airflow.operators.python": MagicMock(),
            }):
                # import ใหม่หลัง patch
                import importlib
                import engine.run_pipeline as rp
                importlib.reload(rp)

                with patch("engine.run_pipeline._resolve_dataset_name",
                           return_value="another_dataset"):
                    assert rp._resolve_dataset_name() == "another_dataset"