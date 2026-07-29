# File: rlam_airflow_framework/formula_engine/errors.py
"""Exceptions for the formula engine."""

class FormulaError(Exception):
    """Exception raised when formula evaluation fails."""
    pass

class FormulaTimeoutError(Exception):
    """Raised when formula evaluation times out."""
    pass
