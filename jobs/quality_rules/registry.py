# jobs/quality_rules/registry.py
"""
Quality Rules Registry — map dataset name → rules class

Usage:
    from quality_rules.registry import get_rules
    rules = get_rules(ds)
    passed, report = rules.run_checks(df, filepath)

เพิ่ม dataset ใหม่:
    1. สร้าง class ใน quality_rules/{dataset_name}_rules.py
    2. เพิ่ม entry ใน _REGISTRY ด้านล่าง

ถ้า dataset ยังไม่มี rules class → ใช้ QualityRulesBase (ผ่านทุก check)
"""

from __future__ import annotations

from typing import Type

from quality_rules.base import QualityRulesBase
from quality_rules.finance_rules import FinanceQualityRules
from datasets.registry import DatasetConfig

# ── Registry ──────────────────────────────────────────────────
# dataset_name → rules class
_REGISTRY: dict[str, Type[QualityRulesBase]] = {
    "finance_itsc": FinanceQualityRules,
}


def get_rules(ds: DatasetConfig) -> QualityRulesBase:
    """
    คืน rules instance สำหรับ dataset นั้น
    ถ้าไม่มีใน registry → fallback เป็น QualityRulesBase (ผ่านทุก check)
    """
    cls = _REGISTRY.get(ds.dataset, QualityRulesBase)
    if cls is QualityRulesBase:
        import logging
        logging.getLogger(__name__).warning(
            f"[dataset={ds.dataset}] ไม่พบ rules class — ใช้ QualityRulesBase (skip all checks)"
        )
    return cls(ds)


def register(dataset_name: str, cls: Type[QualityRulesBase]) -> None:
    """
    Register rules class สำหรับ dataset (ใช้สำหรับ plugin หรือ test)

    Example:
        from quality_rules.registry import register
        register("budget_2025", BudgetQualityRules)
    """
    _REGISTRY[dataset_name] = cls