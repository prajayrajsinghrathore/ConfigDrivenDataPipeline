# File: rlam_airflow_framework/data_quality/values_in_set.py
"""values_in_set check: every value in ``column`` must be in ``valid_values``."""

from typing import Any, cast

import pandas as pd

from rlam_airflow_framework.data_quality.base import CheckOutcome, QualityCheck


class ValuesInSetCheck(QualityCheck):
    check_type = "values_in_set"

    def evaluate(self, df, check):
        column = check["column"]
        allowed_values = check.get("valid_values", [])
        invalid_rows = cast(pd.Series, ~df[column].isin(allowed_values))
        invalid_count = cast(Any, invalid_rows.sum())
        diagnostics = {"invalid_count": int(invalid_count)}
        if invalid_count > 0:
            return CheckOutcome("fail", diagnostics, invalid_rows)
        return CheckOutcome("pass", diagnostics)
