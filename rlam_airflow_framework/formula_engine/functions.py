# File: rlam_airflow_framework/formula_engine/functions.py
"""Registry of safe whitelisted functions available in formulas."""

import pandas as pd
from datetime import datetime, date
from typing import Any, Dict, Callable, Optional
import structlog

logger = structlog.get_logger(__name__)

def _safe_divide(a: Any, b: Any) -> Optional[float]:
    """Safe division that handles division by zero."""
    try:
        if b == 0 or b is None:
            return None
        return float(a) / float(b)
    except (TypeError, ValueError):
        return None

def _date_diff(d1, d2, unit="days") -> Optional[int]:
    """Calculate difference between two dates."""
    if d1 is None or d2 is None:
        return None
    try:
        dt1 = pd.to_datetime(d1)
        dt2 = pd.to_datetime(d2)
        delta = dt1 - dt2

        if unit == "days":
            return delta.days
        elif unit == "hours":
            return int(delta.total_seconds() / 3600)
        elif unit == "minutes":
            return int(delta.total_seconds() / 60)
        elif unit == "seconds":
            return int(delta.total_seconds())
        else:
            return delta.days
    except Exception as e:
        logger.debug(f"date_diff failed: {e}")
        return None

def _case_when(*args) -> Any:
    """
    SQL-style CASE WHEN expression.
    Usage: case_when(cond1, val1, cond2, val2, ..., default)
    """
    if len(args) < 2:
        return None

    # Process condition-value pairs
    for i in range(0, len(args) - 1, 2):
        if i + 1 < len(args):
            condition = args[i]
            value = args[i + 1]
            if condition:
                return value

    # Return last argument as default if odd number of args
    if len(args) % 2 == 1:
        return args[-1]

    return None

def _to_date(val, fmt=None) -> Optional[datetime]:
    """Convert value to datetime."""
    if val is None:
        return None
    try:
        if fmt:
            return datetime.strptime(str(val), fmt)
        return pd.to_datetime(val).to_pydatetime()
    except Exception as e:
        logger.debug(f"to_date failed for value '{val}': {e}")
        return None

def _safe_to_int(val) -> Optional[int]:
    """Safely convert value to integer."""
    if val is None:
        return None
    try:
        str_val = str(val).strip()
        if not str_val:
            return None
        return int(float(str_val))
    except (ValueError, TypeError) as e:
        logger.debug(f"to_int failed for value '{val}': {e}")
        return None

def _safe_to_float(val) -> Optional[float]:
    """Safely convert value to float."""
    if val is None:
        return None
    try:
        str_val = str(val).strip()
        if not str_val:
            return None
        return float(str_val)
    except (ValueError, TypeError) as e:
        logger.debug(f"to_float failed for value '{val}': {e}")
        return None

# Whitelisted safe functions available in formulas
SAFE_FUNCTIONS: Dict[str, Callable] = {
    # Math functions
    "abs": abs,
    "round": round,
    "min": min,
    "max": max,
    "sum": sum,
    "len": len,
    "int": int,
    "float": float,
    "pow": pow,
    "safe_divide": _safe_divide,
    # String functions
    "upper": lambda s: str(s).upper() if s is not None else None,
    "lower": lambda s: str(s).lower() if s is not None else None,
    "strip": lambda s: str(s).strip() if s is not None else None,
    "trim": lambda s: str(s).strip() if s is not None else None,
    "left": lambda s, n: str(s)[:n] if s is not None else None,
    "right": lambda s, n: str(s)[-n:] if s is not None else None,
    "substring": lambda s, start, end=None: str(s)[start:end] if s is not None else None,
    "concat": lambda *args: "".join(str(a) for a in args if a is not None),
    "replace": lambda s, old, new: str(s).replace(old, new) if s is not None else None,
    "split": lambda s, sep: str(s).split(sep) if s is not None else [],
    "contains": lambda s, sub: sub in str(s) if s is not None else False,
    "starts_with": lambda s, prefix: str(s).startswith(prefix) if s is not None else False,
    "ends_with": lambda s, suffix: str(s).endswith(suffix) if s is not None else False,
    "length": lambda s: len(str(s)) if s is not None else 0,
    # Date/Time functions
    "now": lambda: datetime.now(),
    "today": lambda: date.today(),
    "year": lambda d: pd.to_datetime(d).year if d is not None else None,
    "month": lambda d: pd.to_datetime(d).month if d is not None else None,
    "day": lambda d: pd.to_datetime(d).day if d is not None else None,
    "hour": lambda d: pd.to_datetime(d).hour if d is not None else None,
    "minute": lambda d: pd.to_datetime(d).minute if d is not None else None,
    "date_diff": lambda d1, d2, unit="days": _date_diff(d1, d2, unit),
    "format_date": lambda d, fmt="%Y-%m-%d": pd.to_datetime(d).strftime(fmt) if d is not None else None,
    "parse_date": lambda s, fmt="%Y-%m-%d": datetime.strptime(s, fmt) if s is not None else None,
    # Null handling functions
    "coalesce": lambda *args: next((a for a in args if a is not None and a != ""), None),
    "ifnull": lambda val, default: default if val is None else val,
    "nullif": lambda val, compare: None if val == compare else val,
    "is_null": lambda val: val is None,
    "is_not_null": lambda val: val is not None,
    # Conditional functions
    "if_else": lambda condition, true_val, false_val: true_val if condition else false_val,
    "case_when": _case_when,
    # Type conversion functions
    "to_string": lambda val: str(val) if val is not None else None,
    "to_int": _safe_to_int,
    "to_float": _safe_to_float,
    "to_bool": lambda val: bool(val) if val is not None else None,
    "to_date": lambda val, fmt=None: _to_date(val, fmt),
    # Comparison helpers
    "between": lambda val, low, high: low <= val <= high if val is not None else False,
    "in_list": lambda val, lst: val in lst if val is not None else False,
}
