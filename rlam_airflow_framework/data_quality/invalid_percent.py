# File: rlam_airflow_framework/data_quality/invalid_percent.py
"""invalid_percent check: rows failing ``valid_regex`` must stay under ``max_percent``."""

from rlam_airflow_framework.data_quality.base import CheckOutcome, QualityCheck


class InvalidPercentCheck(QualityCheck):
    check_type = "invalid_percent"

    def evaluate(self, df, check):
        column = check["column"]
        max_percent = check.get("max_percent", 5)
        regex_pattern = check.get("valid_regex")

        # No regex configured -> nothing to validate (legacy behavior: pass)
        if not regex_pattern:
            return CheckOutcome("pass")

        invalid_rows = ~df[column].astype(str).str.match(regex_pattern, na=False)
        invalid_percent = (invalid_rows.sum() / len(df)) * 100
        diagnostics = {
            "invalid_percent": round(invalid_percent, 2),
            "max_allowed_percent": max_percent,
        }
        if invalid_percent > max_percent:
            return CheckOutcome("fail", diagnostics, invalid_rows)
        return CheckOutcome("pass", diagnostics)

    def to_sodacl(self, check):
        column = check.get("column")
        if not column:
            return None
        max_percent = check.get("max_percent", 5)
        return f"  - invalid_percent({column}) < {max_percent}%"
