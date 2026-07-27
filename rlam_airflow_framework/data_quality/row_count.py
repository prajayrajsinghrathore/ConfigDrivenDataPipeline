# File: rlam_airflow_framework/data_quality/row_count.py
"""row_count check: the DataFrame must have more than ``min`` rows."""

from rlam_airflow_framework.data_quality.base import CheckOutcome, QualityCheck


class RowCountCheck(QualityCheck):
    check_type = "row_count"
    requires_column = False

    def evaluate(self, df, check):
        min_count = check.get("min", 0)
        actual_count = len(df)
        diagnostics = {"actual": actual_count, "expected_min": min_count}
        outcome = "fail" if actual_count <= min_count else "pass"
        return CheckOutcome(outcome, diagnostics)

    def to_sodacl(self, check):
        return f"  - row_count > {check.get('min', 0)}"
