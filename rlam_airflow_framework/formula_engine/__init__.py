# File: rlam_airflow_framework/formula_engine/__init__.py
"""
Formula Engine Package.

Provides a secure, sandboxed environment for evaluating formulas.
"""

from typing import Optional, Dict, Callable
from rlam_airflow_framework.formula_engine.engine import FormulaEngine
from rlam_airflow_framework.formula_engine.errors import FormulaError, FormulaTimeoutError

_default_engine: Optional[FormulaEngine] = None

def get_formula_engine(
    custom_functions: Optional[Dict[str, Callable]] = None,
) -> FormulaEngine:
    """
    Get the default formula engine instance (singleton pattern).
    """
    global _default_engine
    if _default_engine is None or custom_functions:
        _default_engine = FormulaEngine(custom_functions)
    return _default_engine

__all__ = ["FormulaEngine", "FormulaError", "FormulaTimeoutError", "get_formula_engine"]
