# File: rlam_airflow_framework/data_quality/freshness.py
"""freshness check: the latest ``column`` timestamp must be within ``max_hours``."""

from datetime import datetime, timezone

import pandas as pd

from rlam_airflow_framework.data_quality.base import CheckOutcome, QualityCheck


class FreshnessCheck(QualityCheck):
    check_type = "freshness"

    def evaluate(self, df, check):
        column = check["column"]
        max_hours = check.get("max_hours", 24)
        latest_value = pd.to_datetime(df[column]).max()
        age_hours = (
            datetime.now(timezone.utc) - latest_value.replace(tzinfo=timezone.utc)
        ).total_seconds() / 3600

        diagnostics = {
            "latest_timestamp": str(latest_value),
            "age_hours": round(age_hours, 2),
            "max_hours": max_hours,
        }
        outcome = "fail" if age_hours > max_hours else "pass"
        return CheckOutcome(outcome, diagnostics)
