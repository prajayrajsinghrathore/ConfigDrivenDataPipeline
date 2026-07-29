# File: rlam_airflow_framework/data_quality/duplicate_count.py
"""duplicate_count check: duplicates in ``column`` must not exceed ``max``."""

from typing import Any, cast

from rlam_airflow_framework.data_quality.base import CheckOutcome, QualityCheck


class DuplicateCountCheck(QualityCheck):
    check_type = "duplicate_count"

    def evaluate(self, df, check):
        column = check["column"]
        max_duplicates = check.get("max", 0)
        duplicate_count = cast(Any, df[column].duplicated().sum())
        diagnostics = {
            "duplicate_count": int(duplicate_count),
            "max_allowed": max_duplicates,
        }
        if duplicate_count > max_duplicates:
            # Mark duplicate rows as invalid (keep first)
            return CheckOutcome("fail", diagnostics, df[column].duplicated(keep="first"))
        return CheckOutcome("pass", diagnostics)

    def to_sodacl(self, check):
        column = check.get("column")
        if not column:
            return None
        return f"  - duplicate_count({column}) = {check.get('max', 0)}"
