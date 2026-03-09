# jobs/quality_rules/finance_rules.py
"""
Data Quality Rules สำหรับ finance_itsc dataset
Rules ทั้งหมดอ่าน config จาก DatasetConfig
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
        Fatal: ขาด required columns
        ถ้า dataset ไม่มี date_column → ตัด date ออกจาก required อัตโนมัติ
        """
        required = set(self._required_columns)
        if not self.ds.has_date:
            required.discard("date")
        missing = required - set(df.columns)
        errors = ["❌ Missing required columns: %s" % sorted(missing)] if missing else []
        return len(errors) == 0, errors

    def check_null_values(self, df: DataFrame) -> CheckResult:
        """Fatal: critical_columns เป็น null"""
        errors = []
        for c in self.ds.critical_columns:
            if c not in df.columns:
                continue
            null_count = df.filter(col(c).isNull()).count()
            if null_count > 0:
                errors.append("❌ Column '%s' มี null %d rows" % (c, null_count))
        return len(errors) == 0, errors

    def check_date_format(self, df: DataFrame) -> CheckResult:
        """
        Fatal: ไม่มี all-year-budget
        Warning: date format แปลก
        ข้าม: ถ้าไม่มี date column
        """
        date_col = self.ds.date_column
        if not date_col or date_col not in df.columns:
            return True, []

        errors = []
        dates = {
            str(r[0])
            for r in df.select(date_col).distinct().collect()
            if r[0]
        }
        # support test mock: df.select().distinct().collect() หรือ df.select(col).distinct().collect()

        if "all-year-budget" not in dates:
            errors.append("❌ ไม่พบ row 'all-year-budget' ใน column %s" % date_col)

        special = {"all-year-budget", "total spent", "remaining"}
        pattern = re.compile(r"^\d{4}-\d{2}$")
        invalid = [d for d in dates if d not in special and not pattern.match(d)]
        if invalid:
            errors.append("⚠️ date format ไม่ตรง: %s" % invalid[:5])

        has_fatal = any("all-year-budget" in e for e in errors)
        return not has_fatal, errors

    def check_total_amount(self, df: DataFrame) -> CheckResult:
        """Warning: total_amount ไม่ตรงกับ sum ของ amount_columns (1%)"""
        if "total_amount" not in df.columns:
            return True, []
        amount_cols = [c for c in self._amount_columns if c in df.columns]
        if not amount_cols:
            return True, []

        sum_expr = " + ".join(["COALESCE(`%s`, 0)" % c for c in amount_cols])
        date_col = self.ds.date_column

        # filter date ถ้ามี
        if date_col and date_col in df.columns:
            df_base = df.filter(
                col(date_col).rlike(r"^\d{4}-\d{2}$") | (col(date_col) == "all-year-budget")
            )
            df_check = df_base.selectExpr(
                date_col, "details", "total_amount", "(%s) AS computed_sum" % sum_expr
            )
        else:
            df_check = df.selectExpr(
                "total_amount", "(%s) AS computed_sum" % sum_expr
            )

        df_check = df_check.filter(
            (col("total_amount") > 0) &
            (spark_abs(col("total_amount") - col("computed_sum")) > col("total_amount") * 0.01)
        )

        errors = []
        for row in df_check.limit(3).collect():
            date_val = getattr(row, date_col, "N/A") if date_col else "N/A"
            details_val = getattr(row, "details", "N/A")
            errors.append(
                "⚠️ total_amount mismatch at %s/%s: total=%s, computed=%s"
                % (date_val, details_val, row.total_amount, row.computed_sum)
            )
        return len(errors) == 0, errors

    def check_remaining_decreasing(self, df: DataFrame) -> CheckResult:
        """Warning: ยอด remaining เพิ่มขึ้นผิดปกติ — ข้ามถ้าไม่มี date/details"""
        date_col = self.ds.date_column
        if not date_col or date_col not in df.columns or "details" not in df.columns:
            return True, []

        rows = df.filter(
            (col("details") == "remaining") & col(date_col).rlike(r"^\d{4}-\d{2}$")
        ).select(date_col, "total_amount").orderBy(date_col).collect()

        errors = []
        for i in range(1, len(rows)):
            prev, curr = rows[i-1], rows[i]
            if curr.total_amount is not None and prev.total_amount is not None:
                if curr.total_amount > prev.total_amount:
                    errors.append(
                        "⚠️ Remaining เพิ่มขึ้นที่ %s: %s -> %s"
                        % (curr[0], prev.total_amount, curr.total_amount)
                    )
        return len(errors) == 0, errors