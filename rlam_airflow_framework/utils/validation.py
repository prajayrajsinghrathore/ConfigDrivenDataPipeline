# File: rlam_airflow_framework/utils/validation.py
"""
Generic validation helpers shared across the framework.

These are deliberately domain-neutral (no Snowflake/loader-specific knowledge)
so any module - transformers, destination loaders, future sources - can depend
on them without pulling in unrelated concerns.
"""

import re
from typing import Any

import pandas as pd


def validate_identifier(identifier: str, identifier_type: str = "identifier") -> str:
    """
    Validate and sanitize SQL identifiers to prevent SQL injection.

    Args:
        identifier: The identifier to validate (table name, schema, column)
        identifier_type: Description of what's being validated for error messages

    Returns:
        The validated identifier

    Raises:
        ValueError: If the identifier contains invalid characters
    """
    if not identifier:
        raise ValueError(f"{identifier_type} cannot be empty")

    # Allow alphanumeric, underscores, and dots (for qualified names)
    # Snowflake also allows $, but we'll be conservative
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_$.]*$", identifier):
        raise ValueError(
            f"Invalid {identifier_type}: '{identifier}'. "
            f"Must start with letter/underscore and contain only alphanumeric, underscore, or dot."
        )

    return identifier


def validate_dataframe(df: Any, operation: str) -> pd.DataFrame:
    """
    Validate that input is a non-None DataFrame.

    Args:
        df: The input to validate
        operation: Description of the operation for error messages

    Returns:
        The validated DataFrame

    Raises:
        ValueError: If df is None
        TypeError: If df is not a DataFrame
    """
    if df is None:
        raise ValueError(f"DataFrame cannot be None for {operation}")

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected DataFrame for {operation}, got {type(df).__name__}")

    return df
