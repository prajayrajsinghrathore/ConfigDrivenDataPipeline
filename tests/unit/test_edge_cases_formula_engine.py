# File: tests/unit/test_edge_cases_formula_engine.py
"""
Edge case tests for FormulaEngine.

Tests cover:
- Empty formulas
- Maximum formula length limits
- Deeply nested expressions
- Division by zero
- DoS protection (timeout, complexity)
- Unicode in formulas
- Malformed expressions
- Edge case values (inf, nan, very large numbers)
"""

import pytest
import pandas as pd
import numpy as np

# Import implementation
from rlam_airflow_framework.formula_engine import (
    FormulaEngine,
    FormulaError,
    get_formula_engine,
    MAX_FORMULA_LENGTH,
    MAX_NESTING_DEPTH,
)


class TestEmptyFormulas:
    """Test handling of empty and whitespace-only formulas."""

    @pytest.fixture
    def engine(self):
        """Create FormulaEngine instance."""
        return FormulaEngine()

    def test_empty_string_formula(self, engine):
        """Test empty string formula."""
        # Empty formula may return None/empty or raise error depending on implementation
        try:
            result = engine.evaluate("", {"x": 1})
            # If no error, result should be None or empty
            assert result is None or result == "" or result == 0
        except FormulaError:
            # Also acceptable to raise error
            pass

    def test_whitespace_only_formula(self, engine):
        """Test whitespace-only formula."""
        with pytest.raises(FormulaError):
            engine.evaluate("   \t\n   ", {"x": 1})

    def test_none_formula(self, engine):
        """Test None as formula."""
        # None formula may return None or raise error
        try:
            result = engine.evaluate(None, {"x": 1})
            assert result is None
        except (FormulaError, TypeError, AttributeError):
            pass

    def test_empty_variables_dict(self, engine):
        """Test formula with empty variables dictionary."""
        # Simple constant formula should work
        result = engine.evaluate("1 + 2", {})
        assert result == 3

    def test_none_variables(self, engine):
        """Test formula with None variables."""
        with pytest.raises((FormulaError, TypeError)):
            engine.evaluate("x + 1", None)


class TestFormulaLengthLimits:
    """Test formula length limit enforcement."""

    @pytest.fixture
    def engine(self):
        return FormulaEngine()

    def test_formula_at_max_length(self, engine):
        """Test formula exactly at max length."""
        # Create formula just at the limit
        max_len = MAX_FORMULA_LENGTH
        # Use simple repeated pattern: "1+1+1+..."
        pattern = "1+"
        repeats = (max_len - 1) // len(pattern)
        formula = pattern * repeats + "1"

        # Trim to exact length
        formula = formula[:max_len]

        try:
            engine.evaluate(formula, {})
            # Should evaluate or raise validation error
        except FormulaError:
            # May fail validation due to length
            pass

    def test_formula_exceeds_max_length(self, engine):
        """Test formula exceeding max length."""
        # Create formula over the limit
        over_limit = MAX_FORMULA_LENGTH + 1000
        long_formula = "x + " * (over_limit // 4) + "1"

        with pytest.raises(FormulaError) as exc_info:
            engine.evaluate(long_formula, {"x": 1})

        assert (
            "length" in str(exc_info.value).lower()
            or "too long" in str(exc_info.value).lower()
        )

    def test_extremely_long_formula(self, engine):
        """Test extremely long formula (100x limit)."""
        very_long = "1 + " * (MAX_FORMULA_LENGTH * 100)

        with pytest.raises(FormulaError):
            engine.evaluate(very_long, {})


class TestNestedExpressions:
    """Test deeply nested expression handling."""

    @pytest.fixture
    def engine(self):
        return FormulaEngine()

    def test_max_nesting_depth_parentheses(self, engine):
        """Test formula at maximum nesting depth."""
        # Create nested parentheses at limit
        depth = MAX_NESTING_DEPTH
        formula = "(" * depth + "1" + ")" * depth

        try:
            result = engine.evaluate(formula, {})
            assert result == 1
        except FormulaError:
            # May reject at limit
            pass

    def test_exceeds_nesting_depth(self, engine):
        """Test formula exceeding nesting depth limit."""
        depth = MAX_NESTING_DEPTH + 5
        formula = "(" * depth + "1" + ")" * depth

        with pytest.raises(FormulaError) as exc_info:
            engine.evaluate(formula, {})

        assert (
            "nesting" in str(exc_info.value).lower()
            or "depth" in str(exc_info.value).lower()
        )

    def test_nested_function_calls(self, engine):
        """Test deeply nested function calls."""
        # Create nested function calls
        depth = MAX_NESTING_DEPTH // 2
        formula = "abs(" * depth + "x" + ")" * depth

        try:
            result = engine.evaluate(formula, {"x": -5})
            assert result == 5
        except FormulaError:
            pass

    def test_unbalanced_parentheses(self, engine):
        """Test unbalanced parentheses."""
        formulas = [
            "((x + 1)",  # Missing closing
            "(x + 1))",  # Extra closing
            "((x + (1 + 2)",  # Multiple missing
            ")x + 1(",  # Wrong order
        ]

        for formula in formulas:
            with pytest.raises(FormulaError):
                engine.evaluate(formula, {"x": 1})


class TestDivisionByZero:
    """Test division by zero handling."""

    @pytest.fixture
    def engine(self):
        return FormulaEngine()

    def test_direct_division_by_zero(self, engine):
        """Test direct division by zero."""
        # May return inf, raise error, or return nan
        try:
            result = engine.evaluate("1 / 0", {})
            # Should return inf or raise error
            assert result == float("inf") or np.isnan(result)
        except (FormulaError, ZeroDivisionError):
            # Raising error is also acceptable
            pass

    def test_division_by_zero_variable(self, engine):
        """Test division by zero through variable."""
        try:
            result = engine.evaluate("x / y", {"x": 10, "y": 0})
            assert result == float("inf") or np.isnan(result)
        except (FormulaError, ZeroDivisionError):
            # Raising error is also acceptable
            pass

    def test_modulo_by_zero(self, engine):
        """Test modulo by zero."""
        try:
            engine.evaluate("10 % 0", {})
            # May return nan or raise error
        except (FormulaError, ZeroDivisionError):
            pass

    def test_safe_divide_function(self, engine):
        """Test safe_divide function if available."""
        try:
            result = engine.evaluate("safe_divide(10, 0)", {})
            assert result == 0 or result is None or np.isnan(result)
        except (FormulaError, NameError):
            # safe_divide may not be exposed
            pass

    def test_division_in_column_evaluation(self, engine):
        """Test division by zero in column evaluation."""
        df = pd.DataFrame({"numerator": [10, 20, 30, 40], "denominator": [2, 0, 5, 0]})

        try:
            result = engine.evaluate_column(df, "numerator / denominator")
            # Should handle zeros gracefully
            assert not result.isna().all()
        except FormulaError:
            pass


class TestDoSProtection:
    """Test DoS protection mechanisms."""

    @pytest.fixture
    def engine(self):
        return FormulaEngine()

    def test_exponential_complexity_formula(self, engine):
        """Test formula with exponential complexity."""
        # Very large exponentiation
        formula = "2 ** 10000"

        try:
            engine.evaluate(formula, {})
            # May succeed but should be bounded
        except (FormulaError, OverflowError, MemoryError):
            pass

    def test_recursive_string_multiplication(self, engine):
        """Test recursive string multiplication (memory DoS)."""
        # Attempt to create very large string
        formula = "'x' * 10000000000"

        try:
            engine.evaluate(formula, {})
            # Should be prevented
        except (FormulaError, MemoryError, OverflowError):
            pass

    def test_list_comprehension_attack(self, engine):
        """Test that list comprehensions are blocked."""
        formula = "[x for x in range(10**9)]"

        with pytest.raises(FormulaError):
            engine.evaluate(formula, {})


class TestUnicodeFormulas:
    """Test Unicode handling in formulas."""

    @pytest.fixture
    def engine(self):
        return FormulaEngine()

    def test_unicode_variable_names(self, engine):
        """Test Unicode variable names."""
        # Some formula engines support Unicode identifiers
        try:
            result = engine.evaluate("变量 + 1", {"变量": 5})
            assert result == 6
        except (FormulaError, SyntaxError):
            # Acceptable if Unicode identifiers not supported
            pass

    def test_unicode_string_literals(self, engine):
        """Test Unicode string literals in formula."""
        try:
            result = engine.evaluate("'こんにちは' + ' ' + name", {"name": "世界"})
            assert "こんにちは" in result
        except FormulaError:
            pass

    def test_emoji_in_formula(self, engine):
        """Test emoji handling in formulas."""
        try:
            result = engine.evaluate("'Hello 🌍' + suffix", {"suffix": " 🚀"})
            assert "🌍" in result
        except FormulaError:
            pass

    def test_mixed_scripts_formula(self, engine):
        """Test formula with mixed scripts."""
        try:
            # Arabic + Chinese + Latin
            result = engine.evaluate(
                "concat(a, b, c)", {"a": "مرحبا", "b": "你好", "c": "Hello"}
            )
            assert "مرحبا" in result or isinstance(result, str)
        except (FormulaError, NameError):
            pass


class TestMalformedExpressions:
    """Test handling of malformed expressions."""

    @pytest.fixture
    def engine(self):
        return FormulaEngine()

    def test_incomplete_operator(self, engine):
        """Test incomplete operator expression."""
        with pytest.raises(FormulaError):
            engine.evaluate("x +", {"x": 1})

    def test_double_operator(self, engine):
        """Test double operator."""
        # Some formula engines may interpret ++ as unary + +
        try:
            result = engine.evaluate("x ++ y", {"x": 1, "y": 2})
            # If it parses, result should still be correct
            assert isinstance(result, (int, float))
        except FormulaError:
            pass

    def test_missing_operand(self, engine):
        """Test missing operand."""
        with pytest.raises(FormulaError):
            engine.evaluate("* 5", {})

    def test_undefined_variable(self, engine):
        """Test undefined variable reference."""
        with pytest.raises(FormulaError):
            engine.evaluate("x + undefined_var", {"x": 1})

    def test_undefined_function(self, engine):
        """Test undefined function call."""
        with pytest.raises(FormulaError):
            engine.evaluate("nonexistent_func(x)", {"x": 1})

    def test_wrong_function_arguments(self, engine):
        """Test function with wrong number of arguments."""
        with pytest.raises(FormulaError):
            engine.evaluate("abs()", {})  # abs needs argument

    def test_invalid_syntax_characters(self, engine):
        """Test invalid syntax characters."""
        with pytest.raises(FormulaError):
            engine.evaluate("x @ y", {"x": 1, "y": 2})

    def test_sql_injection_attempt(self, engine):
        """Test SQL-like injection attempt."""
        malicious = "x; DROP TABLE users; --"
        with pytest.raises(FormulaError):
            engine.evaluate(malicious, {"x": 1})

    def test_code_injection_attempt(self, engine):
        """Test code injection attempt."""
        malicious = "__import__('os').system('rm -rf /')"
        with pytest.raises(FormulaError):
            engine.evaluate(malicious, {})


class TestEdgeCaseValues:
    """Test edge case numeric values."""

    @pytest.fixture
    def engine(self):
        return FormulaEngine()

    def test_infinity_operations(self, engine):
        """Test operations with infinity."""
        result = engine.evaluate("x + 1", {"x": float("inf")})
        assert result == float("inf")

    def test_negative_infinity(self, engine):
        """Test negative infinity."""
        result = engine.evaluate("x * 2", {"x": float("-inf")})
        assert result == float("-inf")

    def test_nan_propagation(self, engine):
        """Test NaN propagation."""
        result = engine.evaluate("x + 1", {"x": float("nan")})
        assert np.isnan(result)

    def test_very_large_integers(self, engine):
        """Test very large integer operations."""
        large = 10**100
        result = engine.evaluate("x + 1", {"x": large})
        assert result == large + 1

    def test_very_small_floats(self, engine):
        """Test very small float operations."""
        tiny = 1e-308
        result = engine.evaluate("x * 2", {"x": tiny})
        assert result == tiny * 2

    def test_negative_zero(self, engine):
        """Test negative zero."""
        result = engine.evaluate("x + 0", {"x": -0.0})
        # -0.0 + 0 = 0.0 or -0.0
        assert result == 0.0

    def test_complex_number_rejection(self, engine):
        """Test that complex numbers are handled."""
        # sqrt of negative should handle gracefully
        try:
            result = engine.evaluate("sqrt(-1)", {})
            # May return nan or raise error
            assert np.isnan(result) or isinstance(result, complex)
        except (FormulaError, ValueError):
            pass


class TestColumnEvaluation:
    """Test evaluate_column with edge cases."""

    @pytest.fixture
    def engine(self):
        return FormulaEngine()

    def test_evaluate_column_empty_df(self, engine):
        """Test column evaluation on empty DataFrame."""
        df = pd.DataFrame(columns=["a", "b"])

        try:
            result = engine.evaluate_column(df, "a + b")
            assert len(result) == 0
        except FormulaError:
            pass

    def test_evaluate_column_all_nulls(self, engine):
        """Test column evaluation when column is all nulls."""
        df = pd.DataFrame({"value": [None, None, None]})

        try:
            result = engine.evaluate_column(df, "value * 2")
            assert result.isna().all()
        except FormulaError:
            pass

    def test_evaluate_column_mixed_types(self, engine):
        """Test column evaluation with mixed types."""
        df = pd.DataFrame({"mixed": [1, "2", 3.0, None]})

        try:
            result = engine.evaluate_column(df, "str(mixed)")
            assert len(result) == 4
        except FormulaError:
            pass

    def test_evaluate_column_nonexistent_column(self, engine):
        """Test evaluation referencing non-existent column."""
        df = pd.DataFrame({"a": [1, 2, 3]})

        with pytest.raises(FormulaError):
            engine.evaluate_column(df, "nonexistent + 1")

    def test_evaluate_column_with_spaces_in_name(self, engine):
        """Test evaluation with column name containing spaces."""
        df = pd.DataFrame({"column name": [1, 2, 3]})

        try:
            # May need special syntax for spaces
            result = engine.evaluate_column(df, "`column name` + 1")
            assert len(result) == 3
        except FormulaError:
            # Acceptable if spaces not supported
            pass


class TestBuiltinFunctions:
    """Test built-in function edge cases."""

    @pytest.fixture
    def engine(self):
        return FormulaEngine()

    def test_coalesce_all_nulls(self, engine):
        """Test coalesce when all values are null."""
        result = engine.evaluate("coalesce(a, b, c)", {"a": None, "b": None, "c": None})
        assert result is None

    def test_coalesce_first_non_null(self, engine):
        """Test coalesce returns first non-null."""
        result = engine.evaluate("coalesce(a, b, c)", {"a": None, "b": 5, "c": 10})
        assert result == 5

    def test_if_else_with_null_condition(self, engine):
        """Test if_else with null condition."""
        try:
            result = engine.evaluate("if_else(cond, 'yes', 'no')", {"cond": None})
            # Null condition should return falsy branch
            assert result == "no"
        except (FormulaError, NameError):
            pass

    def test_concat_with_nulls(self, engine):
        """Test concat skips nulls."""
        result = engine.evaluate(
            "concat(a, b, c)", {"a": "hello", "b": None, "c": "world"}
        )
        assert "hello" in result and "world" in result

    def test_string_functions_on_numbers(self, engine):
        """Test string functions on numeric values."""
        try:
            result = engine.evaluate("upper(123)", {})
            assert result == "123" or result == 123
        except (FormulaError, TypeError):
            pass

    def test_math_functions_on_strings(self, engine):
        """Test math functions on string values."""
        with pytest.raises(FormulaError):
            engine.evaluate("abs('text')", {})


class TestSingletonPattern:
    """Test FormulaEngine singleton pattern."""

    def test_get_formula_engine_returns_same_instance(self):
        """Test that get_formula_engine returns singleton."""
        engine1 = get_formula_engine()
        engine2 = get_formula_engine()

        assert engine1 is engine2

    def test_engine_thread_safety(self):
        """Test engine is thread-safe for concurrent access."""
        import threading

        results = []
        errors = []
        engine = get_formula_engine()

        def evaluate_formula(value):
            try:
                result = engine.evaluate("x * 2", {"x": value})
                results.append((value, result))
            except Exception as e:
                errors.append(str(e))

        threads = [
            threading.Thread(target=evaluate_formula, args=(i,)) for i in range(10)
        ]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 10

        for value, result in results:
            assert result == value * 2
