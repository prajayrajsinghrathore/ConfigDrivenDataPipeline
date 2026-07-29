# File: rlam_airflow_framework/formula_engine/evaluators/numexpr_engine.py
"""NumExpr evaluation engine strategy for fast vectorized math."""

import re
from typing import Any, Dict
import pandas as pd
import structlog

from rlam_airflow_framework.formula_engine.evaluators.base import EvaluatorStrategy

try:
    import numexpr as ne
    NUMEXPR_AVAILABLE = True
except ImportError:
    NUMEXPR_AVAILABLE = False

logger = structlog.get_logger(__name__)

class NumexprEngine(EvaluatorStrategy):
    def evaluate(self, formula: str, record: Dict[str, Any]) -> Any:
        raise NotImplementedError("NumexprEngine is only for vectorized column evaluation.")

    def evaluate_column(self, df: pd.DataFrame, formula: str) -> pd.Series:
        if not NUMEXPR_AVAILABLE:
            raise RuntimeError("numexpr library is not available.")
            
        local_dict = {col: df[col].values for col in df.columns if col in formula}
        result = ne.evaluate(formula, local_dict=local_dict)
        return pd.Series(result, index=df.index)

    @staticmethod
    def is_simple_numeric_formula(formula: str, df: pd.DataFrame) -> bool:
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
