# File: rlam_airflow_framework/formula_engine/evaluators/base.py
"""Base EvaluatorStrategy interface."""

from abc import ABC, abstractmethod
from typing import Any, Dict
import pandas as pd

class EvaluatorStrategy(ABC):
    @abstractmethod
    def evaluate(self, formula: str, record: Dict[str, Any]) -> Any:
        """Evaluate a formula against a single record."""
        pass

    @abstractmethod
    def evaluate_column(self, df: pd.DataFrame, formula: str) -> pd.Series:
        """Evaluate a formula across a dataframe column."""
        pass
