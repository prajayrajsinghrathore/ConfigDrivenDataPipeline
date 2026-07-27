# File: rlam_airflow_framework/formula_engine/engine.py
"""Facade for the Formula Engine."""

import pandas as pd
from typing import Any, Dict, Optional, Callable
import structlog

from rlam_airflow_framework.formula_engine.errors import FormulaError, FormulaTimeoutError
from rlam_airflow_framework.formula_engine.security import (
    validate_formula,
    timeout_context,
    FORMULA_TIMEOUT_SECONDS,
)
from rlam_airflow_framework.formula_engine.parser import convert_legacy_formula
from rlam_airflow_framework.formula_engine.functions import SAFE_FUNCTIONS
from rlam_airflow_framework.formula_engine.evaluators.simpleeval_engine import (
    SimpleEvalEngine,
    SIMPLEEVAL_AVAILABLE,
)
from rlam_airflow_framework.formula_engine.evaluators.numexpr_engine import (
    NumexprEngine,
    NUMEXPR_AVAILABLE,
)
from rlam_airflow_framework.formula_engine.evaluators.fallback_engine import FallbackEngine

logger = structlog.get_logger(__name__)

class FormulaEngine:
    def __init__(
        self,
        custom_functions: Optional[Dict[str, Callable]] = None,
        timeout_seconds: float = FORMULA_TIMEOUT_SECONDS,
    ):
        self._timeout_seconds = timeout_seconds
        
        self._functions = SAFE_FUNCTIONS.copy()
        if custom_functions:
            self._functions.update(custom_functions)

        # Initialize strategies
        self._fallback_engine = FallbackEngine(self._functions)
        
        self._simpleeval_engine = None
        if SIMPLEEVAL_AVAILABLE:
            self._simpleeval_engine = SimpleEvalEngine(self._functions)
        else:
            logger.warning(
                "simpleeval not available. Formula evaluation will use fallback parser."
            )

        self._numexpr_engine = None
        if NUMEXPR_AVAILABLE:
            self._numexpr_engine = NumexprEngine()

    def evaluate(self, formula: str, record: Dict[str, Any]) -> Any:
        if not formula or not isinstance(formula, str):
            return formula

        formula = formula.strip()
        validate_formula(formula)
        formula = convert_legacy_formula(formula)

        try:
            logger.debug("::group::Formula Evaluation Details")
            with timeout_context(self._timeout_seconds, formula):
                if self._simpleeval_engine is not None:
                    result = self._simpleeval_engine.evaluate(formula, record)
                    logger.debug("::endgroup::")
                    return result
                else:
                    result = self._fallback_engine.evaluate(formula, record)
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

    def evaluate_column(self, df: pd.DataFrame, formula: str) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=object)

        formula = formula.strip()
        formula = convert_legacy_formula(formula)

        if self._numexpr_engine and NumexprEngine.is_simple_numeric_formula(formula, df):
            try:
                return self._numexpr_engine.evaluate_column(df, formula)
            except Exception as e:
                logger.debug(
                    f"Vectorized evaluation failed, falling back to row-by-row: {e}"
                )

        try:
            if self._simpleeval_engine:
                return self._simpleeval_engine.evaluate_column(df, formula)
            return self._fallback_engine.evaluate_column(df, formula)
        except FormulaError:
            raise
        except Exception as e:
            logger.warning(f"Formula evaluation failed across column: {e}")
            return pd.Series([None] * len(df), index=df.index)

    def get_available_functions(self) -> Dict[str, str]:
        """
        Get a dictionary of available functions with their descriptions.
        """
        return {
            "abs(value)": "Absolute value",
            "round(value, decimals)": "Round to specified decimals",
            "min(a, b, ...)": "Minimum value",
            "max(a, b, ...)": "Maximum value",
            "sum(values)": "Sum of values",
            "pow(base, exp)": "Power/exponent",
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
            "now()": "Current datetime",
            "today()": "Current date",
            "year(date)": "Extract year",
            "month(date)": "Extract month",
            "day(date)": "Extract day",
            "format_date(date, format)": "Format date as string",
            "parse_date(string, format)": "Parse string to date",
            "date_diff(date1, date2, unit)": "Difference between dates",
            "coalesce(val1, val2, ...)": "First non-null value",
            "ifnull(value, default)": "Return default if null",
            "nullif(value, compare)": "Return null if equal",
            "is_null(value)": "Check if null",
            "is_not_null(value)": "Check if not null",
            "if_else(condition, true_val, false_val)": "Conditional expression",
            "case_when(cond1, val1, cond2, val2, ..., default)": "Multiple conditions",
            "to_string(value)": "Convert to string",
            "to_int(value)": "Convert to integer",
            "to_float(value)": "Convert to float",
            "to_date(value, format)": "Convert to date",
            "between(value, low, high)": "Check if value in range",
            "in_list(value, list)": "Check if value in list",
        }
