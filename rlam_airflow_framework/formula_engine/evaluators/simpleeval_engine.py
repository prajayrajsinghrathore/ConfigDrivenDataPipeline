# File: rlam_airflow_framework/formula_engine/evaluators/simpleeval_engine.py
"""SimpleEval evaluation engine strategy."""

from typing import Any, Dict, Callable
import pandas as pd
from typing import cast
import structlog

from rlam_airflow_framework.formula_engine.evaluators.base import EvaluatorStrategy

try:
    from simpleeval import EvalWithCompoundTypes
    SIMPLEEVAL_AVAILABLE = True
except ImportError:
    SIMPLEEVAL_AVAILABLE = False

logger = structlog.get_logger(__name__)

class SimpleEvalEngine(EvaluatorStrategy):
    def __init__(self, functions: Dict[str, Callable]):
        self._evaluator = None
        if SIMPLEEVAL_AVAILABLE:
            self._evaluator = EvalWithCompoundTypes()
            self._evaluator.functions.update(functions)
            # Add safe names
            self._evaluator.names["True"] = True
            self._evaluator.names["False"] = False
            self._evaluator.names["None"] = None
        else:
            raise RuntimeError("simpleeval library is not available.")

    def evaluate(self, formula: str, record: Dict[str, Any]) -> Any:
        if not self._evaluator:
            raise RuntimeError("simpleeval evaluator is not initialized.")
        
        self._evaluator.names = {
            "True": True,
            "False": False,
            "None": None,
            **record,
        }
        try:
            return self._evaluator.eval(formula)
        except Exception as e:
            from rlam_airflow_framework.formula_engine.errors import FormulaError
            raise FormulaError(f"Failed to evaluate formula: {e}") from e

    def evaluate_column(self, df: pd.DataFrame, formula: str) -> pd.Series:
        return cast(
            pd.Series, df.apply(lambda row: self.evaluate(formula, row.to_dict()), axis=1)
        )
