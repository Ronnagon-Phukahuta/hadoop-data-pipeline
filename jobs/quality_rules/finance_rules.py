# jobs/quality_rules/finance_rules.py
"""
Data Quality Rules สำหรับ finance_itsc dataset

Rules ทั้งหมดอ่าน config จาก DatasetConfig (finance.yaml)
ไม่มี hardcode ใดๆ
"""

import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, abs as spark_abs

from quality_rules.base import QualityRulesBase, CheckResult
from datasets.registry import DatasetConfig


class FinanceQualityRules(QualityRulesBase):

    def __init__(self, ds: DatasetConfig):
        super().__init__(ds)
        self._required_columns = ds.required_columns
        self._amount_columns   = ds.amount_columns

    def check_schema(self, df: DataFrame, filepath: str) -> CheckResult:
        """
        Fatal: ขาด required columns (date, details, total_amount)
        Warning: extra columns → schema evolution ปกติ ไม่ต้อง fail
        """
        errors = []
        missing = set(self._required_columns) - set(df.columns)
        if missing:
            errors.append(f"❌ Missing required columns: {sorted(missing)}")
        return len(errors) == 0, errors

    def check_null_values(self, df: DataFrame) -> CheckResult:
        """Fatal: critical_columns (date, details) เป็น null"""
        errors = []
        for c in self.ds.critical_columns:
            if c not in df.columns:
                continue
            null_count = df.filter(col(c).isNull()).count()
            if null_count > 0:
                errors.append(f"❌ Column '{c}' มี null {null_count} rows")
        return len(errors) == 0, errors

    def check_date_format(self, df: DataFrame) -> CheckResult:
        """
        Fatal: ไม่มี all-year-budget เลย
        Warning: date format แปลก
        """
        errors = []
        if "date" not in df.columns:
            return True, []

        dates = {
            str(r.date)
            for r in df.select("date").distinct().collect()
            if r.date
        }

        if "all-year-budget" not in dates:
            errors.append("❌ ไม่พบ row 'all-year-budget' ใน column date")

        special = {"all-year-budget", "total spent", "remaining"}
        pattern = re.compile(r"^\d{4}-\d{2}$")
        invalid = [d for d in dates if d not in special and not pattern.match(d)]
        if invalid:
            errors.append(f"⚠️  date format ไม่ตรง: {invalid[:5]}")

        has_fatal = any("❌" in e for e in errors)
        return not has_fatal, errors

    def check_total_amount(self, df: DataFrame) -> CheckResult:
        """Warning: total_amount ไม่ตรงกับ sum ของ amount_columns (±1%)"""
        errors = []
        if "total_amount" not in df.columns:
            return True, []

        amount_cols = [c for c in self._amount_columns if c in df.columns]
        if not amount_cols:
            return True, []

        sum_expr = " + ".join([f"COALESCE(`{c}`, 0)" for c in amount_cols])
        df_check = df.filter(
            col("date").rlike(r"^\d{4}-\d{2}$") | (col("date") == "all-year-budget")
        ).selectExpr(
            "date", "details", "total_amount", f"({sum_expr}) AS computed_sum"
        ).filter(
            (col("total_amount") > 0) &
            (spark_abs(col("total_amount") - col("computed_sum")) > col("total_amount") * 0.01)
        )

        for row in df_check.limit(3).collect():
            errors.append(
                f"⚠️  total_amount mismatch at {row.date}/{row.details}: "
                f"total={row.total_amount:.0f}, computed={row.computed_sum:.0f}"
            )
        return len(errors) == 0, errors

    def check_remaining_decreasing(self, df: DataFrame) -> CheckResult:
        """Warning: ยอด remaining เพิ่มขึ้นผิดปกติ"""
        errors = []
        if "details" not in df.columns or "date" not in df.columns:
            return True, []

        rows = df.filter(
            (col("details") == "remaining") & col("date").rlike(r"^\d{4}-\d{2}$")
        ).select("date", "total_amount").orderBy("date").collect()

        for i in range(1, len(rows)):
            prev, curr = rows[i-1], rows[i]
            if curr.total_amount is not None and prev.total_amount is not None:
                if curr.total_amount > prev.total_amount:
                    errors.append(
                        f"⚠️  Remaining เพิ่มขึ้นที่ {curr.date}: "
                        f"{prev.total_amount:.0f} → {curr.total_amount:.0f}"
                    )
        return len(errors) == 0, errors