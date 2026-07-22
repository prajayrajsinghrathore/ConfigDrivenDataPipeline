# File: dags/utils/formula_engine.py
"""
Safe formula evaluation engine for data transformations.

This module provides a secure, sandboxed environment for evaluating
formulas defined in YAML configuration files. It uses:
- simpleeval: For safe expression evaluation with whitelisted functions
- numexpr: For high-performance vectorized numeric operations

Security Features:
- All functions are whitelisted - no arbitrary code execution
- Formula length limits to prevent DoS attacks
- Execution timeout protection
- Nesting depth limits
- Division by zero protection
- Centralized timeout configuration support
"""

import pandas as pd
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, date
import logging
import re
import os
import signal
from contextlib import contextmanager

try:
    from simpleeval import EvalWithCompoundTypes

    SIMPLEEVAL_AVAILABLE = True
except ImportError:
    SIMPLEEVAL_AVAILABLE = False

try:
    import numexpr as ne

    NUMEXPR_AVAILABLE = True
except ImportError:
    NUMEXPR_AVAILABLE = False

logger = logging.getLogger(__name__)


# =============================================================================
# CENTRALIZED TIMEOUT CONFIGURATION (DoS protection)
# =============================================================================
# Environment variables take precedence, then fall back to defaults.
# These align with config/global_settings.yaml timeouts.formula section.

MAX_FORMULA_LENGTH = int(os.getenv("FORMULA_MAX_LENGTH", "10240"))  # 10KB default
MAX_NESTING_DEPTH = int(os.getenv("FORMULA_MAX_NESTING", "10"))
FORMULA_TIMEOUT_SECONDS = float(
    os.getenv("TIMEOUT_FORMULA_EVALUATION", os.getenv("FORMULA_TIMEOUT", "5.0"))
)


class FormulaTimeoutError(Exception):
    """Raised when formula evaluation times out."""

    pass


@contextmanager
def timeout_context(seconds: float, formula: str):
    """
    Context manager for timeout protection (Unix only).
    On Windows, this is a no-op due to signal limitations.
    """
    if os.name == "nt":  # Windows doesn't support SIGALRM
        yield
        return

    def timeout_handler(signum, frame):
        raise FormulaTimeoutError(
            f"Formula evaluation timed out after {seconds} seconds: {formula[:100]}..."
        )

    # Set the signal handler
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.setitimer(signal.ITIMER_REAL, seconds)

    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)


def _safe_divide(a: Any, b: Any) -> Optional[float]:
    """Safe division that handles division by zero."""
    try:
        if b == 0 or b is None:
            return None
        return float(a) / float(b)
    except (TypeError, ValueError):
        return None


# Helper functions (defined before class for use in SAFE_FUNCTIONS dict)


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


class FormulaEngine:
    """
    Safe formula evaluation engine with whitelisted functions and DoS protection.

    Security Features:
    - Formula length limits (configurable via FORMULA_MAX_LENGTH env var)
    - Execution timeout (configurable via FORMULA_TIMEOUT env var)
    - Nesting depth limits (configurable via FORMULA_MAX_NESTING env var)
    - Division by zero protection
    - All functions whitelisted

    Supports two evaluation modes:
    1. Row-by-row evaluation using simpleeval (safe sandbox)
    2. Vectorized evaluation using numexpr (high performance for numeric)

    Usage:
        engine = FormulaEngine()

        # Single record evaluation
        result = engine.evaluate("price * quantity", {"price": 100, "quantity": 5})

        # DataFrame evaluation
        df["total"] = engine.evaluate_column(df, "price * quantity")
    """

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
        "safe_divide": _safe_divide,  # Division by zero protection
        # String functions
        "upper": lambda s: str(s).upper() if s is not None else None,
        "lower": lambda s: str(s).lower() if s is not None else None,
        "strip": lambda s: str(s).strip() if s is not None else None,
        "trim": lambda s: str(s).strip() if s is not None else None,
        "left": lambda s, n: str(s)[:n] if s is not None else None,
        "right": lambda s, n: str(s)[-n:] if s is not None else None,
        "substring": lambda s, start, end=None: str(s)[start:end]
        if s is not None
        else None,
        "concat": lambda *args: "".join(str(a) for a in args if a is not None),
        "replace": lambda s, old, new: str(s).replace(old, new)
        if s is not None
        else None,
        "split": lambda s, sep: str(s).split(sep) if s is not None else [],
        "contains": lambda s, sub: sub in str(s) if s is not None else False,
        "starts_with": lambda s, prefix: str(s).startswith(prefix)
        if s is not None
        else False,
        "ends_with": lambda s, suffix: str(s).endswith(suffix)
        if s is not None
        else False,
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
        "format_date": lambda d, fmt="%Y-%m-%d": pd.to_datetime(d).strftime(fmt)
        if d is not None
        else None,
        "parse_date": lambda s, fmt="%Y-%m-%d": datetime.strptime(s, fmt)
        if s is not None
        else None,
        # Null handling functions
        "coalesce": lambda *args: next(
            (a for a in args if a is not None and a != ""), None
        ),
        "ifnull": lambda val, default: default if val is None else val,
        "nullif": lambda val, compare: None if val == compare else val,
        "is_null": lambda val: val is None,
        "is_not_null": lambda val: val is not None,
        # Conditional functions
        "if_else": lambda condition, true_val, false_val: true_val
        if condition
        else false_val,
        "case_when": _case_when,
        # Type conversion functions (with safe versions)
        "to_string": lambda val: str(val) if val is not None else None,
        "to_int": _safe_to_int,
        "to_float": _safe_to_float,
        "to_bool": lambda val: bool(val) if val is not None else None,
        "to_date": lambda val, fmt=None: _to_date(val, fmt),
        # Comparison helpers (for use in conditions)
        "between": lambda val, low, high: low <= val <= high
        if val is not None
        else False,
        "in_list": lambda val, lst: val in lst if val is not None else False,
    }

    # Operators allowed in expressions
    SAFE_OPERATORS = {
        "+",
        "-",
        "*",
        "/",
        "//",
        "%",
        "**",  # Arithmetic
        "==",
        "!=",
        "<",
        ">",
        "<=",
        ">=",  # Comparison
        "and",
        "or",
        "not",  # Logical
        "&",
        "|",
        "^",
        "~",  # Bitwise
    }

    def __init__(
        self,
        custom_functions: Optional[Dict[str, Callable]] = None,
        max_formula_length: int = MAX_FORMULA_LENGTH,
        max_nesting_depth: int = MAX_NESTING_DEPTH,
        timeout_seconds: float = FORMULA_TIMEOUT_SECONDS,
    ):
        """
        Initialize the formula engine with DoS protection settings.

        Args:
            custom_functions: Additional custom functions to make available in formulas.
                              These must be safe functions with no side effects.
            max_formula_length: Maximum allowed formula length in characters
            max_nesting_depth: Maximum allowed parenthesis nesting depth
            timeout_seconds: Maximum execution time per formula evaluation
        """
        self._max_formula_length = max_formula_length
        self._max_nesting_depth = max_nesting_depth
        self._timeout_seconds = timeout_seconds

        self._functions = self.SAFE_FUNCTIONS.copy()
        if custom_functions:
            self._functions.update(custom_functions)

        if SIMPLEEVAL_AVAILABLE:
            self._evaluator = EvalWithCompoundTypes()
            self._evaluator.functions.update(self._functions)
            # Add safe names
            self._evaluator.names["True"] = True
            self._evaluator.names["False"] = False
            self._evaluator.names["None"] = None
        else:
            self._evaluator = None
            logger.warning(
                "simpleeval not available. Formula evaluation will use fallback parser."
            )

    def _validate_formula(self, formula: str) -> None:
        """
        Validate formula for security constraints (DoS protection).

        Args:
            formula: The formula string to validate

        Raises:
            FormulaError: If the formula violates security constraints
        """
        # Check length
        if len(formula) > self._max_formula_length:
            raise FormulaError(
                f"Formula exceeds maximum length of {self._max_formula_length} characters "
                f"(got {len(formula)})"
            )

        # Check nesting depth
        depth = 0
        max_depth = 0
        for char in formula:
            if char == "(":
                depth += 1
                max_depth = max(max_depth, depth)
            elif char == ")":
                depth -= 1

        if max_depth > self._max_nesting_depth:
            raise FormulaError(
                f"Formula exceeds maximum nesting depth of {self._max_nesting_depth} "
                f"(got {max_depth})"
            )

        # Check for balanced parentheses
        if depth != 0:
            raise FormulaError("Unbalanced parentheses in formula")

    def evaluate(self, formula: str, record: Dict[str, Any]) -> Any:
        """
        Safely evaluate a formula against a single record with DoS protection.

        Args:
            formula: The formula string to evaluate (e.g., "price * quantity")
            record: Dictionary of field names to values

        Returns:
            The result of the formula evaluation

        Raises:
            FormulaError: If the formula cannot be evaluated or violates security constraints
            FormulaTimeoutError: If the formula evaluation times out
        """
        if not formula or not isinstance(formula, str):
            return formula

        formula = formula.strip()

        # Validate formula for DoS protection
        self._validate_formula(formula)

        # Check for legacy pandas-style formulas and convert them
        formula = self._convert_legacy_formula(formula)

        try:
            logger.debug("::group::Formula Evaluation Details")
            with timeout_context(self._timeout_seconds, formula):
                if self._evaluator is not None:
                    # Use simpleeval for safe evaluation
                    # Reset names to base values, then add record (record values take precedence)
                    self._evaluator.names = {
                        "True": True,
                        "False": False,
                        "None": None,
                        **record,
                    }
                    result = self._evaluator.eval(formula)
                    logger.debug("::endgroup::")
                    return result
                else:
                    # Fallback to basic parser
                    result = self._fallback_evaluate(formula, record)
                    logger.debug("::endgroup::")
                    return result

        except FormulaTimeoutError:
            logger.debug("::endgroup::")
            raise
        except FormulaError:
            logger.debug("::endgroup::")
            raise
        except Exception as e:
            logger.debug("::endgroup::")
            logger.warning(f"Formula evaluation failed for '{formula}': {e}")
            raise FormulaError(f"Failed to evaluate formula '{formula}': {e}") from e

        except FormulaTimeoutError:
            raise
        except FormulaError:
            raise
        except Exception as e:
            logger.warning(f"Formula evaluation failed for '{formula}': {e}")
            raise FormulaError(f"Failed to evaluate formula '{formula}': {e}") from e

    def evaluate_column(self, df: pd.DataFrame, formula: str) -> pd.Series:
        """
        Evaluate a formula to create a new column in a DataFrame.

        Uses vectorized operations when possible for better performance.

        Args:
            df: Input DataFrame
            formula: Formula string

        Returns:
            pd.Series with the calculated values
        """
        if df.empty:
            return pd.Series(dtype=object)

        formula = formula.strip()
        formula = self._convert_legacy_formula(formula)

        # Try vectorized evaluation for simple numeric formulas
        if NUMEXPR_AVAILABLE and self._is_simple_numeric_formula(formula, df):
            try:
                return self._evaluate_vectorized(df, formula)
            except Exception as e:
                logger.debug(
                    f"Vectorized evaluation failed, falling back to row-by-row: {e}"
                )

        # Fall back to row-by-row evaluation
        return df.apply(lambda row: self.evaluate(formula, row.to_dict()), axis=1)

    def _is_simple_numeric_formula(self, formula: str, df: pd.DataFrame) -> bool:
        """
        Check if formula can be evaluated using numexpr (simple numeric operations).
        """
        # Check for function calls - numexpr doesn't support these
        if "(" in formula and ")" in formula:
            return False

        # Check if all referenced columns are numeric
        tokens = re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", formula)
        for token in tokens:
            if token in df.columns:
                if not pd.api.types.is_numeric_dtype(df[token]):
                    return False
            elif token not in ["True", "False", "and", "or", "not"]:
                return False

        return True

    def _evaluate_vectorized(self, df: pd.DataFrame, formula: str) -> pd.Series:
        """
        Evaluate formula using numexpr for vectorized performance.
        """
        local_dict = {col: df[col].values for col in df.columns if col in formula}
        result = ne.evaluate(formula, local_dict=local_dict)
        return pd.Series(result, index=df.index)

    def _convert_legacy_formula(self, formula: str) -> str:
        """
        Convert legacy pandas-style formulas to new function-based syntax.

        Conversions:
            - "field.str.upper()" -> "upper(field)"
            - "field.str.lower()" -> "lower(field)"
            - "pd.Timestamp.now()" -> "now()"
            - "field.dt.year" -> "year(field)"
        """
        # pd.Timestamp.now() -> now()
        if "pd.Timestamp.now()" in formula or "pd.Timestamp.today()" in formula:
            formula = formula.replace("pd.Timestamp.now()", "now()")
            formula = formula.replace("pd.Timestamp.today()", "today()")

        # datetime.now() -> now()
        if "datetime.now()" in formula:
            formula = formula.replace("datetime.now()", "now()")

        # field.str.upper() -> upper(field)
        upper_match = re.match(r"^(\w+)\.str\.upper\(\)$", formula)
        if upper_match:
            return f"upper({upper_match.group(1)})"

        # field.str.lower() -> lower(field)
        lower_match = re.match(r"^(\w+)\.str\.lower\(\)$", formula)
        if lower_match:
            return f"lower({lower_match.group(1)})"

        # field.str.strip() -> strip(field)
        strip_match = re.match(r"^(\w+)\.str\.strip\(\)$", formula)
        if strip_match:
            return f"strip({strip_match.group(1)})"

        # field.dt.year -> year(field)
        dt_year_match = re.match(r"^(\w+)\.dt\.year$", formula)
        if dt_year_match:
            return f"year({dt_year_match.group(1)})"

        # field.dt.month -> month(field)
        dt_month_match = re.match(r"^(\w+)\.dt\.month$", formula)
        if dt_month_match:
            return f"month({dt_month_match.group(1)})"

        # field.dt.day -> day(field)
        dt_day_match = re.match(r"^(\w+)\.dt\.day$", formula)
        if dt_day_match:
            return f"day({dt_day_match.group(1)})"

        # np.where(condition, true_val, false_val) -> if_else(condition, true_val, false_val)
        np_where_match = re.match(r"^np\.where\((.+)\)$", formula)
        if np_where_match:
            return f"if_else({np_where_match.group(1)})"

        return formula

    def _fallback_evaluate(self, formula: str, record: Dict[str, Any]) -> Any:
        """
        Basic fallback evaluation when simpleeval is not available.
        Handles common patterns only.
        """
        # Handle simple function calls
        func_match = re.match(r"^(\w+)\((.+)\)$", formula)
        if func_match:
            func_name = func_match.group(1)
            args_str = func_match.group(2)

            if func_name in self._functions:
                # Parse arguments
                args = self._parse_args(args_str, record)
                return self._functions[func_name](*args)

        # Handle basic arithmetic
        for op in [" * ", " + ", " - ", " / "]:
            if op in formula:
                parts = formula.split(op)
                if len(parts) == 2:
                    val1 = self._resolve_value(parts[0].strip(), record)
                    val2 = self._resolve_value(parts[1].strip(), record)
                    if op == " * ":
                        return float(val1 or 0) * float(val2 or 0)
                    elif op == " + ":
                        return float(val1 or 0) + float(val2 or 0)
                    elif op == " - ":
                        return float(val1 or 0) - float(val2 or 0)
                    elif op == " / ":
                        return float(val1 or 0) / float(val2 or 1) if val2 else None

        # Return as literal if nothing matched
        return self._resolve_value(formula, record)

    def _parse_args(self, args_str: str, record: Dict[str, Any]) -> List[Any]:
        """Parse comma-separated arguments."""
        args = []
        depth = 0
        current = ""

        for char in args_str:
            if char == "(":
                depth += 1
                current += char
            elif char == ")":
                depth -= 1
                current += char
            elif char == "," and depth == 0:
                args.append(self._resolve_value(current.strip(), record))
                current = ""
            else:
                current += char

        if current.strip():
            args.append(self._resolve_value(current.strip(), record))

        return args

    def _resolve_value(self, token: str, record: Dict[str, Any]) -> Any:
        """Resolve a token to its value."""
        token = token.strip()

        # Check if it's a string literal
        if (token.startswith("'") and token.endswith("'")) or (
            token.startswith('"') and token.endswith('"')
        ):
            return token[1:-1]

        # Check if it's a number
        try:
            if "." in token:
                return float(token)
            return int(token)
        except ValueError:
            pass

        # Check if it's a field name
        if token in record:
            return record[token]

        # Check for boolean/None
        if token == "True":
            return True
        if token == "False":
            return False
        if token == "None":
            return None

        return token

    def get_available_functions(self) -> Dict[str, str]:
        """
        Get a dictionary of available functions with their descriptions.
        Useful for documentation and validation.
        """
        return {
            # Math
            "abs(value)": "Absolute value",
            "round(value, decimals)": "Round to specified decimals",
            "min(a, b, ...)": "Minimum value",
            "max(a, b, ...)": "Maximum value",
            "sum(values)": "Sum of values",
            "pow(base, exp)": "Power/exponent",
            # String
            "upper(string)": "Convert to uppercase",
            "lower(string)": "Convert to lowercase",
            "strip(string)": "Remove leading/trailing whitespace",
            "trim(string)": "Remove leading/trailing whitespace",
            "left(string, n)": "Get first n characters",
            "right(string, n)": "Get last n characters",
            "substring(string, start, end)": "Extract substring",
            "concat(str1, str2, ...)": "Concatenate strings",
            "replace(string, old, new)": "Replace substring",
            "contains(string, substring)": "Check if string contains substring",
            "length(string)": "Length of string",
            # Date/Time
            "now()": "Current datetime",
            "today()": "Current date",
            "year(date)": "Extract year",
            "month(date)": "Extract month",
            "day(date)": "Extract day",
            "format_date(date, format)": "Format date as string",
            "parse_date(string, format)": "Parse string to date",
            "date_diff(date1, date2, unit)": "Difference between dates",
            # Null handling
            "coalesce(val1, val2, ...)": "First non-null value",
            "ifnull(value, default)": "Return default if null",
            "nullif(value, compare)": "Return null if equal",
            "is_null(value)": "Check if null",
            "is_not_null(value)": "Check if not null",
            # Conditional
            "if_else(condition, true_val, false_val)": "Conditional expression",
            "case_when(cond1, val1, cond2, val2, ..., default)": "Multiple conditions",
            # Type conversion
            "to_string(value)": "Convert to string",
            "to_int(value)": "Convert to integer",
            "to_float(value)": "Convert to float",
            "to_date(value, format)": "Convert to date",
            # Comparison helpers
            "between(value, low, high)": "Check if value in range",
            "in_list(value, list)": "Check if value in list",
        }


class FormulaError(Exception):
    """Exception raised when formula evaluation fails."""

    pass


# Singleton instance for convenience
_default_engine: Optional[FormulaEngine] = None


def get_formula_engine(
    custom_functions: Optional[Dict[str, Callable]] = None,
) -> FormulaEngine:
    """
    Get the default formula engine instance (singleton pattern).

    Args:
        custom_functions: Optional additional functions to register

    Returns:
        FormulaEngine instance
    """
    global _default_engine
    if _default_engine is None or custom_functions:
        _default_engine = FormulaEngine(custom_functions)
    return _default_engine
