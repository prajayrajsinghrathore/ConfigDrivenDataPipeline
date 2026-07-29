# File: rlam_airflow_framework/formula_engine/evaluators/fallback_engine.py
"""Fallback evaluation engine strategy."""

import re
from typing import Any, Dict, Callable, List, cast
import pandas as pd

from rlam_airflow_framework.formula_engine.evaluators.base import EvaluatorStrategy

class FallbackEngine(EvaluatorStrategy):
    def __init__(self, functions: Dict[str, Callable]):
        self._functions = functions

    def evaluate(self, formula: str, record: Dict[str, Any]) -> Any:
        """
        Basic fallback evaluation when simpleeval is not available.
        Handles common patterns only.
        """
        try:
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
        except Exception as e:
            from rlam_airflow_framework.formula_engine.errors import FormulaError
            raise FormulaError(f"Failed to evaluate formula: {e}") from e

    def evaluate_column(self, df: pd.DataFrame, formula: str) -> pd.Series:
        return cast(
            pd.Series, df.apply(lambda row: self.evaluate(formula, row.to_dict()), axis=1)
        )

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
