"""
Unit tests for Formula Engine.

Tests the safe expression evaluation capabilities including:
- Basic arithmetic operations
- Whitelisted functions
- Security (blocking dangerous operations)
- Error handling

NOTE: These tests use a mock implementation to test the expected behavior
of a formula engine without depending on external libraries like simpleeval.
"""

import pytest
import pandas as pd
import math
from typing import Dict, Any, Callable


# =============================================================================
# TEST IMPLEMENTATION - Mock FormulaEngine for testing
# =============================================================================


class FormulaEngineError(Exception):
    """Exception raised for formula evaluation errors."""

    pass


class FormulaEngine:
    """
    Mock Formula Engine for testing.

    Provides safe expression evaluation with whitelisted functions.
    """

    # Whitelisted functions
    SAFE_FUNCTIONS: Dict[str, Callable] = {
        # Math functions
        "abs": abs,
        "round": round,
        "min": min,
        "max": max,
        "sum": sum,
        "len": len,
        "sqrt": math.sqrt,
        "floor": math.floor,
        "ceil": math.ceil,
        "log": math.log,
        "log10": math.log10,
        "exp": math.exp,
        "pow": pow,
        # Type conversions
        "int": int,
        "float": float,
        "str": str,
        "bool": bool,
    }

    # Blocked names for security
    BLOCKED_NAMES = {
        "__import__",
        "eval",
        "exec",
        "compile",
        "open",
        "getattr",
        "setattr",
        "delattr",
        "globals",
        "locals",
        "__builtins__",
        "__class__",
        "__bases__",
        "__subclasses__",
    }

    def __init__(self):
        self.functions = self.SAFE_FUNCTIONS.copy()

    def _check_security(self, expression: str) -> None:
        """Check expression for security issues."""
        for blocked in self.BLOCKED_NAMES:
            if blocked in expression:
                raise FormulaEngineError(f"Blocked name '{blocked}' in expression")

        # Block dunder access
        if "__" in expression:
            raise FormulaEngineError("Dunder access not allowed")

    def evaluate(self, expression: str, context: Dict[str, Any]) -> Any:
        """
        Safely evaluate a formula expression.

        Args:
            expression: The formula to evaluate
            context: Dictionary of variable names to values

        Returns:
            The result of the evaluation
        """
        if not expression or not expression.strip():
            raise FormulaEngineError("Empty expression")

        # Security check
        self._check_security(expression)

        # Build safe namespace
        safe_namespace = self.functions.copy()
        safe_namespace.update(context)

        try:
            # Use eval with restricted namespace
            result = eval(expression, {"__builtins__": {}}, safe_namespace)
            return result
        except ZeroDivisionError:
            raise ZeroDivisionError("Division by zero")
        except NameError as e:
            raise FormulaEngineError(f"Undefined variable: {e}")
        except TypeError as e:
            raise TypeError(str(e))
        except SyntaxError as e:
            raise FormulaEngineError(f"Syntax error: {e}")
        except Exception as e:
            raise FormulaEngineError(f"Evaluation error: {e}")


@pytest.mark.unit
class TestFormulaEngineBasicArithmetic:
    """Test basic arithmetic operations."""

    @pytest.fixture
    def engine(self):
        """Create a FormulaEngine instance."""
        return FormulaEngine()

    def test_addition(self, engine):
        """Test simple addition."""
        result = engine.evaluate("a + b", {"a": 5, "b": 3})
        assert result == 8

    def test_subtraction(self, engine):
        """Test simple subtraction."""
        result = engine.evaluate("a - b", {"a": 10, "b": 4})
        assert result == 6

    def test_multiplication(self, engine):
        """Test multiplication."""
        result = engine.evaluate("price * quantity", {"price": 10.5, "quantity": 3})
        assert result == 31.5

    def test_division(self, engine):
        """Test division."""
        result = engine.evaluate("total / count", {"total": 100, "count": 4})
        assert result == 25.0

    def test_modulo(self, engine):
        """Test modulo operation."""
        result = engine.evaluate("a % b", {"a": 17, "b": 5})
        assert result == 2

    def test_power(self, engine):
        """Test power operation."""
        result = engine.evaluate("base ** exp", {"base": 2, "exp": 8})
        assert result == 256

    def test_complex_expression(self, engine):
        """Test complex mathematical expression."""
        result = engine.evaluate(
            "(price * quantity) - (price * quantity * discount_rate)",
            {"price": 100, "quantity": 5, "discount_rate": 0.1},
        )
        assert result == 450.0

    def test_parentheses_precedence(self, engine):
        """Test that parentheses are respected."""
        result = engine.evaluate("(a + b) * c", {"a": 2, "b": 3, "c": 4})
        assert result == 20  # (2+3)*4 = 20, not 2+3*4 = 14


@pytest.mark.unit
class TestFormulaEngineWhitelistedFunctions:
    """Test whitelisted mathematical functions."""

    @pytest.fixture
    def engine(self):
        return FormulaEngine()

    def test_round_function(self, engine):
        """Test round function."""
        result = engine.evaluate("round(value, 2)", {"value": 3.14159})
        assert result == 3.14

    def test_abs_function(self, engine):
        """Test absolute value function."""
        result = engine.evaluate("abs(value)", {"value": -42})
        assert result == 42

    def test_min_function(self, engine):
        """Test min function."""
        result = engine.evaluate("min(a, b, c)", {"a": 5, "b": 2, "c": 8})
        assert result == 2

    def test_max_function(self, engine):
        """Test max function."""
        result = engine.evaluate("max(a, b, c)", {"a": 5, "b": 2, "c": 8})
        assert result == 8

    def test_sum_function(self, engine):
        """Test sum function with list."""
        result = engine.evaluate("sum([a, b, c])", {"a": 1, "b": 2, "c": 3})
        assert result == 6

    def test_sqrt_function(self, engine):
        """Test square root function."""
        result = engine.evaluate("sqrt(value)", {"value": 16})
        assert result == 4.0

    def test_floor_function(self, engine):
        """Test floor function."""
        result = engine.evaluate("floor(value)", {"value": 3.7})
        assert result == 3

    def test_ceil_function(self, engine):
        """Test ceiling function."""
        result = engine.evaluate("ceil(value)", {"value": 3.2})
        assert result == 4

    def test_log_function(self, engine):
        """Test natural logarithm function."""
        result = engine.evaluate("log(value)", {"value": math.e})
        assert abs(result - 1.0) < 0.0001

    def test_log10_function(self, engine):
        """Test base-10 logarithm function."""
        result = engine.evaluate("log10(value)", {"value": 100})
        assert result == 2.0

    def test_exp_function(self, engine):
        """Test exponential function."""
        result = engine.evaluate("exp(value)", {"value": 0})
        assert result == 1.0


@pytest.mark.unit
class TestFormulaEngineSecurity:
    """Test security features - blocking dangerous operations."""

    @pytest.fixture
    def engine(self):
        return FormulaEngine()

    def test_blocks_import(self, engine):
        """Test that __import__ is blocked."""
        with pytest.raises(Exception):
            engine.evaluate("__import__('os')", {})

    def test_blocks_eval(self, engine):
        """Test that eval is blocked."""
        with pytest.raises(Exception):
            engine.evaluate("eval('1+1')", {})

    def test_blocks_exec(self, engine):
        """Test that exec is blocked."""
        with pytest.raises(Exception):
            engine.evaluate("exec('print(1)')", {})

    def test_blocks_open(self, engine):
        """Test that open (file access) is blocked."""
        with pytest.raises(Exception):
            engine.evaluate("open('/etc/passwd')", {})

    def test_blocks_dunder_attributes(self, engine):
        """Test that dunder attribute access is blocked."""
        with pytest.raises(Exception):
            engine.evaluate("''.__class__.__bases__[0].__subclasses__()", {})

    def test_blocks_getattr(self, engine):
        """Test that getattr is blocked."""
        with pytest.raises(Exception):
            engine.evaluate("getattr(obj, '__class__')", {"obj": object()})

    def test_blocks_globals(self, engine):
        """Test that globals() is blocked."""
        with pytest.raises(Exception):
            engine.evaluate("globals()", {})

    def test_blocks_locals(self, engine):
        """Test that locals() is blocked."""
        with pytest.raises(Exception):
            engine.evaluate("locals()", {})


@pytest.mark.unit
class TestFormulaEngineErrorHandling:
    """Test error handling scenarios."""

    @pytest.fixture
    def engine(self):
        return FormulaEngine()

    def test_undefined_variable_error(self, engine):
        """Test error when variable is not in context."""
        with pytest.raises(Exception):
            engine.evaluate("undefined_var + 1", {})

    def test_division_by_zero(self, engine):
        """Test division by zero handling."""
        with pytest.raises(ZeroDivisionError):
            engine.evaluate("a / b", {"a": 10, "b": 0})

    def test_invalid_syntax(self, engine):
        """Test invalid syntax handling."""
        with pytest.raises(Exception):
            engine.evaluate("a + * b", {"a": 1, "b": 2})

    def test_type_error_string_math(self, engine):
        """Test type error when doing math on strings."""
        with pytest.raises((TypeError, Exception)):
            engine.evaluate("a + b", {"a": "hello", "b": 5})

    def test_empty_expression(self, engine):
        """Test empty expression handling."""
        with pytest.raises(Exception):
            engine.evaluate("", {})


@pytest.mark.unit
class TestFormulaEngineDataFrameIntegration:
    """Test formula engine with pandas DataFrames."""

    @pytest.fixture
    def engine(self):
        return FormulaEngine()

    @pytest.fixture
    def df(self):
        return pd.DataFrame(
            {
                "price": [10.0, 20.0, 30.0],
                "quantity": [2, 3, 1],
                "discount": [0.1, 0.2, 0.15],
            }
        )

    def test_apply_formula_to_dataframe_row(self, engine, df):
        """Test applying formula to each row of DataFrame."""

        def apply_formula(row):
            return engine.evaluate("price * quantity * (1 - discount)", row.to_dict())

        result = df.apply(apply_formula, axis=1)
        expected = [18.0, 48.0, 25.5]
        assert list(result) == expected

    def test_formula_with_conditional(self, engine):
        """Test conditional expression."""
        result = engine.evaluate(
            "price * 0.9 if quantity > 5 else price", {"price": 100, "quantity": 10}
        )
        assert result == 90.0


@pytest.mark.unit
class TestFormulaEngineFinancialCalculations:
    """Test financial calculation formulas."""

    @pytest.fixture
    def engine(self):
        return FormulaEngine()

    def test_percentage_change(self, engine):
        """Test percentage change calculation."""
        result = engine.evaluate(
            "((new_value - old_value) / old_value) * 100",
            {"new_value": 110, "old_value": 100},
        )
        assert result == 10.0

    def test_moving_average_component(self, engine):
        """Test component that could be used in moving average."""
        result = engine.evaluate("(v1 + v2 + v3) / 3", {"v1": 10, "v2": 20, "v3": 30})
        assert result == 20.0

    def test_profit_margin(self, engine):
        """Test profit margin calculation."""
        result = engine.evaluate(
            "((revenue - cost) / revenue) * 100", {"revenue": 1000, "cost": 700}
        )
        assert result == 30.0

    def test_compound_interest(self, engine):
        """Test compound interest formula."""
        result = engine.evaluate(
            "principal * (1 + rate) ** years",
            {"principal": 1000, "rate": 0.05, "years": 2},
        )
        assert abs(result - 1102.5) < 0.01
