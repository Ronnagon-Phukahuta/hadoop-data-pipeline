# tests/test_quality_rules.py
"""
Tests for quality_rules/ (Item 3 — Quality Rule Layer)
รันได้โดยไม่ต้องมี Hive/Spark/Docker (mock DataFrame)

รันผ่าน CI (Python 3.12):
    pytest tests/test_quality_rules.py -v

รันผ่าน Docker (PySpark):
    ./run_tests.sh tests/test_quality_rules.py -v
"""

import pytest
from pathlib import Path
from typing import List

import sys
import os

import pyspark.sql.functions as _psf
import quality_rules.finance_rules as _fr
from datasets.registry import DatasetConfig

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))

from unittest.mock import MagicMock

# ── Mock pyspark ทั้งหมดก่อน import ─────────────────────────
# col(), abs() ต้องการ SparkContext active ซึ่งไม่มีใน unit test
# patch ที่ sys.modules เพื่อให้ finance_rules import mock แทนของจริง
def _make_col_mock(name=None):
    m = MagicMock()
    m.isNull.return_value = m
    m.rlike.return_value = m
    m.__eq__ = MagicMock(return_value=m)
    m.__and__ = MagicMock(return_value=m)
    m.__gt__ = MagicMock(return_value=m)
    m.__lt__ = MagicMock(return_value=m)
    m.__mul__ = MagicMock(return_value=m)
    m.__sub__ = MagicMock(return_value=m)
    m.__or__ = MagicMock(return_value=m)
    return m

_mock_col = MagicMock(side_effect=lambda name: _make_col_mock(name))
_mock_spark_abs = MagicMock(side_effect=lambda x: _make_col_mock())

_psf.col = _mock_col
_psf.abs = _mock_spark_abs

# patch col และ spark_abs ที่ finance_rules module โดยตรง
# เพราะ `from pyspark.sql.functions import col` bind แล้วตอน import
_fr.col = _mock_col
_fr.spark_abs = _mock_spark_abs


# ── Helpers ───────────────────────────────────────────────────

def make_ds(**overrides) -> DatasetConfig:
    defaults = dict(
        dataset="finance_itsc",
        owner="itsc-cmu",
        paths={
            "raw": "/datalake/raw/finance_itsc",
            "staging": "/datalake/staging/finance_itsc_wide",
            "curated": "/datalake/curated/finance_itsc_long",
            "versions": "/datalake/versions/finance_itsc",
            "trash": "/datalake/trash",
        },
        tables={"database": "default", "staging": "finance_itsc_wide", "curated": "finance_itsc_long"},
        pipeline={
            "critical_columns": ["date", "details"],
            "required_columns": ["date", "details", "total_amount"],
            "partition_by": "year",
            "id_columns": ["date", "details", "year"],
            "exclude_columns": ["total_amount"],
            "amount_columns": ["expense_budget", "utilities", "compensation_budget"],
        },
        schema=[],
        category_mapping={},
        nlp_rules=[],
        example_queries=[],
    )
    defaults.update(overrides)
    return DatasetConfig(**defaults)


def make_df(columns: List[str], rows: int = 5) -> MagicMock:
    """สร้าง mock DataFrame"""
    df = MagicMock()
    df.columns = columns
    df.count.return_value = rows
    df.filter.return_value = df
    df.select.return_value = df
    df.selectExpr.return_value = df
    df.distinct.return_value = df
    df.orderBy.return_value = df
    df.limit.return_value = df
    df.collect.return_value = []
    return df


FULL_COLUMNS = ["date", "details", "total_amount", "year",
                "expense_budget", "utilities", "compensation_budget"]

FILEPATH = "hdfs:///raw/year=2024/finance_itsc_wide_20260304.csv"


@pytest.fixture
def ds():
    return make_ds()


# ── QualityRulesBase ──────────────────────────────────────────

class TestQualityRulesBase:

    def test_base_all_checks_pass_by_default(self, ds):
        from quality_rules.base import QualityRulesBase
        rules = QualityRulesBase(ds)
        df = make_df(FULL_COLUMNS)
        passed, report = rules.run_checks(df, FILEPATH)
        assert passed is True

    def test_base_stores_dataset_config(self, ds):
        from quality_rules.base import QualityRulesBase
        rules = QualityRulesBase(ds)
        assert rules.ds is ds

    def test_base_report_contains_filepath(self, ds):
        from quality_rules.base import QualityRulesBase
        rules = QualityRulesBase(ds)
        df = make_df(FULL_COLUMNS)
        _, report = rules.run_checks(df, FILEPATH)
        assert FILEPATH in report

    def test_extra_checks_empty_by_default(self, ds):
        from quality_rules.base import QualityRulesBase
        rules = QualityRulesBase(ds)
        df = make_df(FULL_COLUMNS)
        assert rules.extra_checks(df, FILEPATH) == []

    def test_fatal_error_sets_passed_false(self, ds):
        from quality_rules.base import QualityRulesBase

        class FailingRules(QualityRulesBase):
            def check_schema(self, df, filepath):
                return False, ["❌ Missing required columns: ['date']"]

        rules = FailingRules(ds)
        df = make_df(["details", "total_amount"])
        passed, report = rules.run_checks(df, FILEPATH)
        assert passed is False
        assert "ERRORS" in report

    def test_warning_only_passes(self, ds):
        from quality_rules.base import QualityRulesBase

        class WarnRules(QualityRulesBase):
            def check_total_amount(self, df):
                return False, ["⚠️  total_amount mismatch at 2024-01/budget"]

        rules = WarnRules(ds)
        df = make_df(FULL_COLUMNS)
        passed, report = rules.run_checks(df, FILEPATH)
        assert passed is True
        assert "WARNINGS" in report

    def test_extra_checks_included_in_run(self, ds):
        from quality_rules.base import QualityRulesBase

        class ExtraRules(QualityRulesBase):
            def extra_checks(self, df, filepath):
                return [("Custom Check", (False, ["❌ custom fatal error"]))]

        rules = ExtraRules(ds)
        df = make_df(FULL_COLUMNS)
        passed, report = rules.run_checks(df, FILEPATH)
        assert passed is False


# ── FinanceQualityRules: check_schema ────────────────────────

class TestFinanceCheckSchema:

    @pytest.fixture(autouse=True)
    def setup(self, ds):
        from quality_rules.finance_rules import FinanceQualityRules
        self.rules = FinanceQualityRules(ds)

    def test_schema_pass_with_all_required(self):
        df = make_df(FULL_COLUMNS)
        passed, errors = self.rules.check_schema(df, FILEPATH)
        assert passed is True
        assert errors == []

    def test_schema_fail_missing_date(self):
        df = make_df(["details", "total_amount"])
        passed, errors = self.rules.check_schema(df, FILEPATH)
        assert passed is False
        assert any("❌" in e for e in errors)
        assert any("date" in e for e in errors)

    def test_schema_fail_missing_total_amount(self):
        df = make_df(["date", "details"])
        passed, errors = self.rules.check_schema(df, FILEPATH)
        assert passed is False
        assert any("total_amount" in e for e in errors)

    def test_schema_pass_with_extra_columns(self):
        """extra columns ไม่ควร fail — schema evolution"""
        df = make_df(FULL_COLUMNS + ["new_fund_column"])
        passed, errors = self.rules.check_schema(df, FILEPATH)
        assert passed is True

    def test_schema_reads_required_from_registry(self, ds):
        """required_columns มาจาก ds ไม่ใช่ hardcode"""
        from quality_rules.finance_rules import FinanceQualityRules
        custom_ds = make_ds(pipeline={
            **ds.pipeline,
            "required_columns": ["date"],  # แค่ date พอ
        })
        rules = FinanceQualityRules(custom_ds)
        df = make_df(["date"])  # ไม่มี details, total_amount
        passed, errors = rules.check_schema(df, FILEPATH)
        assert passed is True


# ── FinanceQualityRules: check_null_values ────────────────────

class TestFinanceCheckNullValues:

    @pytest.fixture(autouse=True)
    def setup(self, ds):
        from quality_rules.finance_rules import FinanceQualityRules
        self.rules = FinanceQualityRules(ds)

    def test_no_nulls_passes(self):
        df = make_df(FULL_COLUMNS)
        df.filter.return_value.count.return_value = 0
        passed, errors = self.rules.check_null_values(df)
        assert passed is True

    def test_null_date_fails(self):
        df = make_df(FULL_COLUMNS)
        # filter คืน df ที่มี count > 0 สำหรับ date
        null_df = MagicMock()
        null_df.count.return_value = 3
        df.filter.return_value = null_df
        passed, errors = self.rules.check_null_values(df)
        assert passed is False
        assert any("❌" in e for e in errors)

    def test_uses_critical_columns_from_registry(self):
        """null check ใช้ critical_columns จาก ds ไม่ใช่ hardcode"""
        from quality_rules.finance_rules import FinanceQualityRules
        custom_ds = make_ds(pipeline={
            **make_ds().pipeline,
            "critical_columns": ["date"],  # แค่ date
        })
        rules = FinanceQualityRules(custom_ds)
        df = make_df(["date", "details"])
        df.filter.return_value.count.return_value = 0
        passed, errors = rules.check_null_values(df)
        assert passed is True

    def test_skip_column_not_in_df(self):
        """column ที่ไม่มีใน df ให้ข้ามไป ไม่ crash"""
        df = make_df(["date"])  # ไม่มี details
        df.filter.return_value.count.return_value = 0
        passed, errors = self.rules.check_null_values(df)
        assert passed is True


# ── FinanceQualityRules: check_date_format ────────────────────

class TestFinanceCheckDateFormat:

    @pytest.fixture(autouse=True)
    def setup(self, ds):
        from quality_rules.finance_rules import FinanceQualityRules
        self.rules = FinanceQualityRules(ds)

    def _mock_df_with_dates(self, dates: List[str]) -> MagicMock:
        df = make_df(FULL_COLUMNS)
        rows = [MagicMock(date=d) for d in dates]
        df.select.return_value.distinct.return_value.collect.return_value = rows
        return df

    def test_valid_dates_pass(self):
        df = self._mock_df_with_dates(["all-year-budget", "2024-01", "2024-02"])
        passed, errors = self.rules.check_date_format(df)
        assert passed is True

    def test_missing_all_year_budget_fails(self):
        df = self._mock_df_with_dates(["2024-01", "2024-02"])
        passed, errors = self.rules.check_date_format(df)
        assert passed is False
        assert any("all-year-budget" in e for e in errors)

    def test_invalid_date_format_warns(self):
        df = self._mock_df_with_dates(["all-year-budget", "2024-01", "invalid-date"])
        passed, errors = self.rules.check_date_format(df)
        # passed=True เพราะแค่ warning
        assert passed is True
        assert any("⚠️" in e for e in errors)

    def test_no_date_column_passes(self):
        df = make_df(["details", "total_amount"])
        passed, errors = self.rules.check_date_format(df)
        assert passed is True
        assert errors == []


# ── FinanceQualityRules: check_total_amount ───────────────────

class TestFinanceCheckTotalAmount:

    @pytest.fixture(autouse=True)
    def setup(self, ds):
        from quality_rules.finance_rules import FinanceQualityRules
        self.rules = FinanceQualityRules(ds)

    def test_no_total_amount_column_passes(self):
        df = make_df(["date", "details"])
        passed, errors = self.rules.check_total_amount(df)
        assert passed is True

    def test_no_amount_cols_in_df_passes(self):
        """amount_columns ไม่มีใน df เลย → skip"""
        df = make_df(["date", "details", "total_amount"])
        passed, errors = self.rules.check_total_amount(df)
        assert passed is True

    def test_mismatch_returns_warning(self):
        df = make_df(FULL_COLUMNS)
        row = MagicMock()
        row.date = "2024-01"
        row.details = "budget"
        row.total_amount = 100000
        row.computed_sum = 150000
        df.filter.return_value.selectExpr.return_value.filter.return_value.limit.return_value.collect.return_value = [row]
        passed, errors = self.rules.check_total_amount(df)
        # warning ไม่ fail
        assert passed is False  # errors list ไม่ว่าง
        assert all("⚠️" in e for e in errors)

    def test_reads_amount_columns_from_registry(self):
        """amount_columns มาจาก ds ไม่ใช่ hardcode"""
        from quality_rules.finance_rules import FinanceQualityRules
        custom_ds = make_ds(pipeline={
            **make_ds().pipeline,
            "amount_columns": [],  # ว่าง → skip check
        })
        rules = FinanceQualityRules(custom_ds)
        df = make_df(FULL_COLUMNS)
        passed, errors = rules.check_total_amount(df)
        assert passed is True
        assert errors == []


# ── FinanceQualityRules: check_remaining_decreasing ──────────

class TestFinanceCheckRemainingDecreasing:

    @pytest.fixture(autouse=True)
    def setup(self, ds):
        from quality_rules.finance_rules import FinanceQualityRules
        self.rules = FinanceQualityRules(ds)

    def _make_row(self, date, total_amount):
        r = MagicMock()
        r.date = date
        r.total_amount = total_amount
        return r

    def test_decreasing_remaining_passes(self):
        df = make_df(FULL_COLUMNS)
        rows = [
            self._make_row("2024-01", 1000000),
            self._make_row("2024-02", 900000),
            self._make_row("2024-03", 800000),
        ]
        df.filter.return_value.select.return_value.orderBy.return_value.collect.return_value = rows
        passed, errors = self.rules.check_remaining_decreasing(df)
        assert passed is True

    def test_increasing_remaining_warns(self):
        df = make_df(FULL_COLUMNS)
        rows = [
            self._make_row("2024-01", 800000),
            self._make_row("2024-02", 900000),  # เพิ่มขึ้น → warning
        ]
        df.filter.return_value.select.return_value.orderBy.return_value.collect.return_value = rows
        passed, errors = self.rules.check_remaining_decreasing(df)
        assert passed is False
        assert any("⚠️" in e for e in errors)

    def test_no_details_column_passes(self):
        df = make_df(["date", "total_amount"])
        passed, errors = self.rules.check_remaining_decreasing(df)
        assert passed is True

    def test_single_row_passes(self):
        df = make_df(FULL_COLUMNS)
        df.filter.return_value.select.return_value.orderBy.return_value.collect.return_value = [
            self._make_row("2024-01", 1000000)
        ]
        passed, errors = self.rules.check_remaining_decreasing(df)
        assert passed is True


# ── FinanceQualityRules: run_checks integration ───────────────

class TestFinanceRunChecks:

    @pytest.fixture(autouse=True)
    def setup(self, ds):
        from quality_rules.finance_rules import FinanceQualityRules
        self.rules = FinanceQualityRules(ds)

    def test_run_checks_returns_tuple(self):
        df = make_df(FULL_COLUMNS)
        result = self.rules.run_checks(df, FILEPATH)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_run_checks_fatal_sets_passed_false(self):
        """ขาด required column → fatal → passed=False"""
        df = make_df(["details", "total_amount"])  # ขาด date
        passed, report = self.rules.run_checks(df, FILEPATH)
        assert passed is False
        assert "ERRORS" in report

    def test_run_checks_uses_dataset_name_in_log(self, ds):
        from quality_rules.finance_rules import FinanceQualityRules
        rules = FinanceQualityRules(ds)
        df = make_df(FULL_COLUMNS)
        # ต้องไม่ raise
        rules.run_checks(df, FILEPATH)

    def test_run_checks_compatible_with_pipeline_signature(self, ds):
        """
        pipeline.py เรียก: rules.run_checks(df, files[0])
        ต้อง return (bool, str) เหมือนเดิม
        """
        from quality_rules.finance_rules import FinanceQualityRules
        rules = FinanceQualityRules(ds)
        df = make_df(FULL_COLUMNS)
        passed, report = rules.run_checks(df, FILEPATH)
        assert isinstance(passed, bool)
        assert isinstance(report, str)


# ── Registry: new properties ──────────────────────────────────

class TestRegistryNewProperties:

    def test_required_columns_from_yaml(self):
        ds = make_ds()
        assert "date" in ds.required_columns
        assert "details" in ds.required_columns
        assert "total_amount" in ds.required_columns

    def test_amount_columns_from_yaml(self):
        ds = make_ds()
        assert "expense_budget" in ds.amount_columns
        assert "utilities" in ds.amount_columns

    def test_required_columns_default_empty(self):
        ds = make_ds(pipeline={})
        assert ds.required_columns == []

    def test_amount_columns_default_empty(self):
        ds = make_ds(pipeline={})
        assert ds.amount_columns == []

    def test_finance_yaml_has_required_columns(self):
        finance_dir = Path(__file__).parent.parent / "jobs" / "datasets"
        if not (finance_dir / "finance.yaml").exists():
            pytest.skip("finance.yaml not found")
        from datasets.registry import load_dataset
        ds = load_dataset("finance_itsc", datasets_dir=finance_dir)
        assert len(ds.required_columns) >= 3
        assert len(ds.amount_columns) >= 30