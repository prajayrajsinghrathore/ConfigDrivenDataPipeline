# File: rlam_airflow_framework/data_quality/max_value.py
"""max check: ``column`` values must stay below ``threshold``."""

from typing import Any, cast

from rlam_airflow_framework.data_quality.base import CheckOutcome, QualityCheck


class MaxCheck(QualityCheck):
    check_type = "max"

    def evaluate(self, df, check):
        column = check["column"]
        threshold = check.get("threshold", 0)
        actual_max = cast(Any, df[column].max())
        diagnostics = {"actual_max": float(actual_max), "threshold": threshold}
        if actual_max >= threshold:
            # Mark rows that meet/exceed the threshold as invalid
            return CheckOutcome("fail", diagnostics, df[column] >= threshold)
        return CheckOutcome("pass", diagnostics)
