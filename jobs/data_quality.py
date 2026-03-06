# jobs/data_quality.py
import re
from typing import List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, abs as spark_abs

from logger import get_logger

log = get_logger(__name__)

REQUIRED_COLUMNS = ["date", "details", "total_amount"]

AMOUNT_COLUMNS = [
    "general_fund_admin_wifi_grant", "compensation_budget", "expense_budget",
    "material_budget", "utilities", "grant_welfare_health", "grant_ms_365",
    "education_fund_academic_computer_service_salary_staff", "government_staff",
    "asset_fund_academic_computer_service_equipment_budget", "equipment_budget_over_1m",
    "permanent_asset_fund_land_construction", "equipment_firewall", "grant_siem",
    "grant_data_center", "grant_wifi_satit",
    "research_fund_research_admin_personnel_research_grant",
    "reserve_fund_general_admin_other_expenses_reserve",
    "contribute_development_fund", "contribute_personnel_development_fund_cmu",
    "personnel_development_fund_education_management_support_special_grant",
    "art_preservation_fund_general_grant", "wifi_jumboplus", "firewall",
    "cmu_cloud", "siem", "digital_health", "benefit_access_request_system",
    "ups", "ups_rent_wifi_care", "uplift", "open_data",
]


def check_schema(df: DataFrame, filepath: str) -> Tuple[bool, List[str]]:
    """
    Fatal: ขาด required columns (date, details, total_amount)
    Warning: extra columns → schema evolution ปกติ ไม่ต้อง fail
    """
    errors = []
    missing_required = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_required:
        errors.append(f"❌ Missing required columns: {sorted(missing_required)}")
    return len(errors) == 0, errors


def check_null_values(df: DataFrame) -> Tuple[bool, List[str]]:
    """Fatal: date หรือ details เป็น null"""
    errors = []
    for c in ["date", "details"]:
        if c not in df.columns:
            continue
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            errors.append(f"❌ Column '{c}' มี null {null_count} rows")
    return len(errors) == 0, errors


def check_date_format(df: DataFrame) -> Tuple[bool, List[str]]:
    """
    Fatal: ไม่มี all-year-budget เลย
    Warning: date format แปลก
    """
    errors = []
    if "date" not in df.columns:
        return True, []

    dates = {str(r.date) for r in df.select("date").distinct().collect() if r.date}

    if "all-year-budget" not in dates:
        errors.append("❌ ไม่พบ row 'all-year-budget' ใน column date")

    special = {"all-year-budget", "total spent", "remaining"}
    pattern = re.compile(r"^\d{4}-\d{2}$")
    invalid = [d for d in dates if d not in special and not pattern.match(d)]
    if invalid:
        errors.append(f"⚠️  date format ไม่ตรง: {invalid[:5]}")

    has_fatal = any("❌" in e for e in errors)
    return not has_fatal, errors


def check_total_amount(df: DataFrame) -> Tuple[bool, List[str]]:
    """Warning: total_amount ไม่ตรงกับ sum ของ amount columns (±1%)"""
    errors = []
    if "total_amount" not in df.columns:
        return True, []

    amount_cols = [c for c in AMOUNT_COLUMNS if c in df.columns]
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


def check_remaining_decreasing(df: DataFrame) -> Tuple[bool, List[str]]:
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


def run_quality_checks(df: DataFrame, filepath: str) -> Tuple[bool, str]:
    filename = filepath.split("/")[-1]
    log.info("Data quality check started", file=filename, checks=5)

    all_errors, all_warnings = [], []
    passed = True

    checks = [
        ("Schema",               check_schema(df, filepath)),
        ("Null Values",          check_null_values(df)),
        ("Date Format",          check_date_format(df)),
        ("Total Amount",         check_total_amount(df)),
        ("Remaining Decreasing", check_remaining_decreasing(df)),
    ]

    for name, (ok, errors) in checks:
        if ok and not errors:
            log.info(f"✅ {name}", file=filename, check=name, result="passed")
        else:
            has_fatal = any("❌" in e for e in errors)
            if has_fatal:
                passed = False
                for e in errors:
                    log.error(f"❌ {name} — {e}", file=filename, check=name, result="failed")
                all_errors.extend(errors)
            else:
                for e in errors:
                    log.warning(f"⚠️  {name} — {e}", file=filename, check=name, result="warning")
                all_warnings.extend(errors)

    if passed:
        log.info(
            "All quality checks passed",
            file=filename,
            warnings=len(all_warnings),
        )
    else:
        log.error(
            "Quality checks FAILED",
            file=filename,
            fatal=len(all_errors),
            warnings=len(all_warnings),
        )

    report = f"File: {filepath}\n\n"
    if all_errors:
        report += "ERRORS (Fatal):\n" + "\n".join(all_errors) + "\n\n"
    if all_warnings:
        report += "WARNINGS:\n" + "\n".join(all_warnings)

    return passed, report