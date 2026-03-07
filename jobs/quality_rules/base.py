# jobs/quality_rules/base.py
"""
Base interface สำหรับ Data Quality Rules

แต่ละ dataset สร้าง rules class ของตัวเองที่ inherit QualityRulesBase
แล้ว override check methods ที่ต้องการ

Usage:
    from quality_rules.finance_rules import FinanceQualityRules
    from datasets.registry import load_dataset

    ds = load_dataset("finance_itsc")
    rules = FinanceQualityRules(ds)
    passed, report = rules.run_checks(df, filepath)
"""

from __future__ import annotations

from typing import List, Tuple
from pyspark.sql import DataFrame

from datasets.registry import DatasetConfig
from logger import get_logger

log = get_logger(__name__)

# type alias
CheckResult = Tuple[bool, List[str]]


class QualityRulesBase:
    """
    Base class สำหรับ dataset-specific quality rules

    Subclass ต้อง override เมธอดที่ต้องการ
    เมธอดที่ไม่ได้ override จะ return (True, []) — ผ่านเสมอ
    """

    def __init__(self, ds: DatasetConfig):
        self.ds = ds

    # ── Override เหล่านี้ใน subclass ────────────────────────

    def check_schema(self, df: DataFrame, filepath: str) -> CheckResult:
        return True, []

    def check_null_values(self, df: DataFrame) -> CheckResult:
        return True, []

    def check_date_format(self, df: DataFrame) -> CheckResult:
        return True, []

    def check_total_amount(self, df: DataFrame) -> CheckResult:
        return True, []

    def check_remaining_decreasing(self, df: DataFrame) -> CheckResult:
        return True, []

    def extra_checks(self, df: DataFrame, filepath: str) -> List[Tuple[str, CheckResult]]:
        """
        Override เพื่อเพิ่ม checks เฉพาะ dataset
        Returns: list of (check_name, (passed, errors))
        """
        return []

    # ── run_checks (ไม่ต้อง override) ───────────────────────

    def run_checks(self, df: DataFrame, filepath: str) -> Tuple[bool, str]:
        """
        รัน quality checks ทั้งหมด

        Returns:
            (passed, report_string)
            passed=False เมื่อมี fatal error (❌)
            passed=True เมื่อมีแค่ warning (⚠️) หรือผ่านหมด
        """
        filename = filepath.split("/")[-1]
        standard_checks = [
            ("Schema",               self.check_schema(df, filepath)),
            ("Null Values",          self.check_null_values(df)),
            ("Date Format",          self.check_date_format(df)),
            ("Total Amount",         self.check_total_amount(df)),
            ("Remaining Decreasing", self.check_remaining_decreasing(df)),
        ]
        all_checks = standard_checks + self.extra_checks(df, filepath)

        log.info("Data quality check started", file=filename,
                 checks=len(all_checks), dataset=self.ds.dataset)

        all_errors, all_warnings = [], []
        passed = True

        for name, (ok, errors) in all_checks:
            if ok and not errors:
                log.info(f"✅ {name}", file=filename, check=name, result="passed")
            else:
                has_fatal = any("❌" in e for e in errors)
                if has_fatal:
                    passed = False
                    for e in errors:
                        log.error(f"❌ {name} — {e}", file=filename,
                                  check=name, result="failed")
                    all_errors.extend(errors)
                else:
                    for e in errors:
                        log.warning(f"⚠️  {name} — {e}", file=filename,
                                    check=name, result="warning")
                    all_warnings.extend(errors)

        if passed:
            log.info("All quality checks passed", file=filename,
                     warnings=len(all_warnings))
        else:
            log.error("Quality checks FAILED", file=filename,
                      fatal=len(all_errors), warnings=len(all_warnings))

        report = f"File: {filepath}\n\n"
        if all_errors:
            report += "ERRORS (Fatal):\n" + "\n".join(all_errors) + "\n\n"
        if all_warnings:
            report += "WARNINGS:\n" + "\n".join(all_warnings)

        return passed, report