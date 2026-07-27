# File: rlam_airflow_framework/data_quality/range_check.py
"""range check: ``column`` values must fall within [``min``, ``max``]."""

import pandas as pd

from rlam_airflow_framework.data_quality.base import CheckOutcome, QualityCheck


class RangeCheck(QualityCheck):
    check_type = "range"

    def evaluate(self, df, check):
        column = check["column"]
        min_val = check.get("min")
        max_val = check.get("max")

        invalid_rows = pd.Series([False] * len(df), index=df.index)
        if min_val is not None:
            invalid_rows |= df[column] < min_val
        if max_val is not None:
            invalid_rows |= df[column] > max_val

        invalid_count = invalid_rows.sum()
        diagnostics = {
            "invalid_count": int(invalid_count),
            "min": min_val,
            "max": max_val,
        }
        if invalid_count > 0:
            return CheckOutcome("fail", diagnostics, invalid_rows)
        return CheckOutcome("pass", diagnostics)
