# File: rlam_airflow_framework/data_quality/missing_count.py
"""missing_count check: nulls in ``column`` must not exceed ``max``."""

from typing import Any, cast

import pandas as pd

from rlam_airflow_framework.data_quality.base import CheckOutcome, QualityCheck


class MissingCountCheck(QualityCheck):
    check_type = "missing_count"

    def evaluate(self, df, check):
        column = check["column"]
        max_missing = check.get("max", 0)
        missing = cast(pd.Series, df[column].isna())
        missing_count = cast(Any, missing.sum())
        diagnostics = {
            "missing_count": int(missing_count),
            "max_allowed": max_missing,
        }
        if missing_count > max_missing:
            return CheckOutcome("fail", diagnostics, missing)
        return CheckOutcome("pass", diagnostics)

    def to_sodacl(self, check):
        column = check.get("column")
        if not column:
            return None
        return f"  - missing_count({column}) = {check.get('max', 0)}"
