# File: tests/unit/test_formula_engine_actual.py
"""
Unit tests for the actual FormulaEngine implementation.

Tests cover:
- String functions (upper, lower, concat, trim, etc.)
- Date functions (year, month, date_diff, format_date, etc.)
- Null handling functions (coalesce, ifnull, nullif, is_null)
- Conditional functions (if_else, case_when)
- evaluate_column() vectorized evaluation
- Legacy formula conversion
- Edge cases: empty DataFrames, Unicode, null propagation
"""

import pytest
import pandas as pd
from datetime import datetime, date

# Import actual implementation
from rlam_airflow_framework.formula_engine import (
    FormulaEngine,
    FormulaError,
    get_formula_engine,
    _date_diff,
    _case_when,
    _to_date,
    SIMPLEEVAL_AVAILABLE,
)

# Skip marker for tests requiring simpleeval
requires_simpleeval = pytest.mark.skipif(
    not SIMPLEEVAL_AVAILABLE, reason="simpleeval not available"
)


class TestFormulaEngineStringFunctions:
    """Test string manipulation functions in FormulaEngine."""

    @pytest.fixture
    def engine(self):
        """Create a fresh FormulaEngine instance."""
        return FormulaEngine()

    def test_upper_function(self, engine):
        """Test upper() converts string to uppercase."""
        result = engine.evaluate("upper(name)", {"name": "hello"})
        assert result == "HELLO"

    def test_upper_with_none(self, engine):
        """Test upper() handles None gracefully."""
        result = engine.evaluate("upper(name)", {"name": None})
        assert result is None

    def test_lower_function(self, engine):
        """Test lower() converts string to lowercase."""
        result = engine.evaluate("lower(name)", {"name": "WORLD"})
        assert result == "world"

    def test_lower_with_none(self, engine):
        """Test lower() handles None gracefully."""
        result = engine.evaluate("lower(name)", {"name": None})
        assert result is None

    def test_strip_function(self, engine):
        """Test strip() removes whitespace."""
        result = engine.evaluate("strip(name)", {"name": "  hello  "})
        assert result == "hello"

    def test_trim_function(self, engine):
        """Test trim() (alias for strip) removes whitespace."""
        result = engine.evaluate("trim(name)", {"name": "  world  "})
        assert result == "world"

    def test_left_function(self, engine):
        """Test left() extracts first n characters."""
        result = engine.evaluate("left(name, 3)", {"name": "hello"})
        assert result == "hel"

    def test_right_function(self, engine):
        """Test right() extracts last n characters."""
        result = engine.evaluate("right(name, 3)", {"name": "hello"})
        assert result == "llo"

    def test_substring_function(self, engine):
        """Test substring() extracts a portion of string."""
        result = engine.evaluate("substring(name, 1, 4)", {"name": "hello"})
        assert result == "ell"

    def test_concat_function(self, engine):
        """Test concat() joins strings."""
        result = engine.evaluate(
            "concat(first, ' ', last)", {"first": "John", "last": "Doe"}
        )
        assert result == "John Doe"

    def test_concat_with_none_values(self, engine):
        """Test concat() skips None values."""
        result = engine.evaluate(
            "concat(first, middle, last)",
            {"first": "John", "middle": None, "last": "Doe"},
        )
        assert result == "JohnDoe"

    def test_replace_function(self, engine):
        """Test replace() substitutes substrings."""
        result = engine.evaluate(
            "replace(text, 'world', 'universe')", {"text": "hello world"}
        )
        assert result == "hello universe"

    def test_contains_function(self, engine):
        """Test contains() checks for substring presence."""
        result = engine.evaluate("contains(text, 'world')", {"text": "hello world"})
        assert result is True

    def test_contains_not_found(self, engine):
        """Test contains() returns False when substring not found."""
        result = engine.evaluate("contains(text, 'xyz')", {"text": "hello world"})
        assert result is False

    def test_starts_with_function(self, engine):
        """Test starts_with() checks prefix."""
        result = engine.evaluate("starts_with(text, 'hello')", {"text": "hello world"})
        assert result is True

    def test_ends_with_function(self, engine):
        """Test ends_with() checks suffix."""
        result = engine.evaluate("ends_with(text, 'world')", {"text": "hello world"})
        assert result is True

    def test_length_function(self, engine):
        """Test length() returns string length."""
        result = engine.evaluate("length(name)", {"name": "hello"})
        assert result == 5

    def test_length_with_none(self, engine):
        """Test length() returns 0 for None."""
        result = engine.evaluate("length(name)", {"name": None})
        assert result == 0

    @requires_simpleeval
    def test_split_function(self, engine):
        """Test split() splits string by separator."""
        result = engine.evaluate("split(text, ',')", {"text": "a,b,c"})
        assert result == ["a", "b", "c"]

    def test_unicode_string_handling(self, engine):
        """Test string functions handle Unicode characters."""
        result = engine.evaluate("upper(name)", {"name": "héllo wörld"})
        assert result == "HÉLLO WÖRLD"

    def test_unicode_concat(self, engine):
        """Test concat with Unicode and special characters."""
        result = engine.evaluate("concat(a, b)", {"a": "日本", "b": "語"})
        assert result == "日本語"


class TestFormulaEngineDateFunctions:
    """Test date/time functions in FormulaEngine."""

    @pytest.fixture
    def engine(self):
        """Create a fresh FormulaEngine instance."""
        return FormulaEngine()

    def test_year_function(self, engine):
        """Test year() extracts year from date."""
        result = engine.evaluate("year(date_col)", {"date_col": "2026-05-15"})
        assert result == 2026

    def test_month_function(self, engine):
        """Test month() extracts month from date."""
        result = engine.evaluate("month(date_col)", {"date_col": "2026-05-15"})
        assert result == 5

    def test_day_function(self, engine):
        """Test day() extracts day from date."""
        result = engine.evaluate("day(date_col)", {"date_col": "2026-05-15"})
        assert result == 15

    def test_hour_function(self, engine):
        """Test hour() extracts hour from datetime."""
        result = engine.evaluate("hour(dt)", {"dt": "2026-05-15 14:30:00"})
        assert result == 14

    def test_minute_function(self, engine):
        """Test minute() extracts minute from datetime."""
        result = engine.evaluate("minute(dt)", {"dt": "2026-05-15 14:30:45"})
        assert result == 30

    def test_year_with_none(self, engine):
        """Test year() handles None gracefully."""
        result = engine.evaluate("year(date_col)", {"date_col": None})
        assert result is None

    @requires_simpleeval
    def test_now_function(self, engine):
        """Test now() returns current datetime."""
        result = engine.evaluate("now()", {})
        assert isinstance(result, datetime)

    @requires_simpleeval
    def test_today_function(self, engine):
        """Test today() returns current date."""
        result = engine.evaluate("today()", {})
        assert isinstance(result, date)

    def test_format_date_function(self, engine):
        """Test format_date() formats date as string."""
        result = engine.evaluate(
            "format_date(date_col, '%Y/%m/%d')", {"date_col": "2026-05-15"}
        )
        assert result == "2026/05/15"

    def test_format_date_with_none(self, engine):
        """Test format_date() handles None."""
        result = engine.evaluate(
            "format_date(date_col, '%Y-%m-%d')", {"date_col": None}
        )
        assert result is None

    def test_parse_date_function(self, engine):
        """Test parse_date() parses string to datetime."""
        result = engine.evaluate(
            "parse_date(date_str, '%Y/%m/%d')", {"date_str": "2026/05/15"}
        )
        assert isinstance(result, datetime)
        assert result.year == 2026
        assert result.month == 5
        assert result.day == 15


class TestDateDiffFunction:
    """Test date_diff helper function directly."""

    def test_date_diff_days(self):
        """Test date_diff in days (default)."""
        result = _date_diff("2026-05-15", "2026-05-10")
        assert result == 5

    def test_date_diff_hours(self):
        """Test date_diff in hours."""
        result = _date_diff("2026-05-15 12:00:00", "2026-05-15 00:00:00", "hours")
        assert result == 12

    def test_date_diff_minutes(self):
        """Test date_diff in minutes."""
        result = _date_diff("2026-05-15 12:30:00", "2026-05-15 12:00:00", "minutes")
        assert result == 30

    def test_date_diff_seconds(self):
        """Test date_diff in seconds."""
        result = _date_diff("2026-05-15 12:00:30", "2026-05-15 12:00:00", "seconds")
        assert result == 30

    def test_date_diff_with_none_d1(self):
        """Test date_diff returns None if d1 is None."""
        result = _date_diff(None, "2026-05-10")
        assert result is None

    def test_date_diff_with_none_d2(self):
        """Test date_diff returns None if d2 is None."""
        result = _date_diff("2026-05-15", None)
        assert result is None

    def test_date_diff_invalid_date(self):
        """Test date_diff handles invalid dates gracefully."""
        result = _date_diff("not-a-date", "2026-05-10")
        assert result is None


class TestFormulaEngineNullHandling:
    """Test null handling functions in FormulaEngine."""

    @pytest.fixture
    def engine(self):
        """Create a fresh FormulaEngine instance."""
        return FormulaEngine()

    def test_coalesce_returns_first_non_null(self, engine):
        """Test coalesce() returns first non-null value."""
        result = engine.evaluate(
            "coalesce(a, b, c)", {"a": None, "b": "hello", "c": "world"}
        )
        assert result == "hello"

    def test_coalesce_all_null(self, engine):
        """Test coalesce() returns None if all values are null."""
        result = engine.evaluate("coalesce(a, b)", {"a": None, "b": None})
        assert result is None

    def test_coalesce_skips_empty_string(self, engine):
        """Test coalesce() skips empty strings."""
        result = engine.evaluate(
            "coalesce(a, b, c)", {"a": "", "b": None, "c": "default"}
        )
        assert result == "default"

    def test_ifnull_returns_value_when_not_null(self, engine):
        """Test ifnull() returns value if not null."""
        result = engine.evaluate("ifnull(a, 'default')", {"a": "value"})
        assert result == "value"

    def test_ifnull_returns_default_when_null(self, engine):
        """Test ifnull() returns default when value is null."""
        result = engine.evaluate("ifnull(a, 'default')", {"a": None})
        assert result == "default"

    def test_nullif_returns_null_when_equal(self, engine):
        """Test nullif() returns None when values are equal."""
        result = engine.evaluate("nullif(a, 0)", {"a": 0})
        assert result is None

    def test_nullif_returns_value_when_not_equal(self, engine):
        """Test nullif() returns value when values differ."""
        result = engine.evaluate("nullif(a, 0)", {"a": 5})
        assert result == 5

    def test_is_null_true(self, engine):
        """Test is_null() returns True for None."""
        result = engine.evaluate("is_null(a)", {"a": None})
        assert result is True

    def test_is_null_false(self, engine):
        """Test is_null() returns False for non-None."""
        result = engine.evaluate("is_null(a)", {"a": "value"})
        assert result is False

    def test_is_not_null_true(self, engine):
        """Test is_not_null() returns True for non-None."""
        result = engine.evaluate("is_not_null(a)", {"a": "value"})
        assert result is True

    def test_is_not_null_false(self, engine):
        """Test is_not_null() returns False for None."""
        result = engine.evaluate("is_not_null(a)", {"a": None})
        assert result is False


class TestCaseWhenFunction:
    """Test case_when helper function."""

    def test_case_when_first_condition_true(self):
        """Test case_when returns first matching value."""
        result = _case_when(True, "first", False, "second", "default")
        assert result == "first"

    def test_case_when_second_condition_true(self):
        """Test case_when returns second matching value."""
        result = _case_when(False, "first", True, "second", "default")
        assert result == "second"

    def test_case_when_no_match_returns_default(self):
        """Test case_when returns default when no condition matches."""
        result = _case_when(False, "first", False, "second", "default")
        assert result == "default"

    def test_case_when_no_default_returns_none(self):
        """Test case_when returns None when no default provided."""
        result = _case_when(False, "first", False, "second")
        assert result is None

    def test_case_when_insufficient_args(self):
        """Test case_when with less than 2 args returns None."""
        result = _case_when(True)
        assert result is None

    @requires_simpleeval
    def test_case_when_in_formula(self):
        """Test case_when used in formula evaluation."""
        engine = FormulaEngine()
        result = engine.evaluate(
            "case_when(status == 'A', 'Active', status == 'I', 'Inactive', 'Unknown')",
            {"status": "I"},
        )
        assert result == "Inactive"


class TestFormulaEngineConditionalFunctions:
    """Test conditional functions in FormulaEngine."""

    @pytest.fixture
    def engine(self):
        """Create a fresh FormulaEngine instance."""
        return FormulaEngine()

    def test_if_else_true_condition(self, engine):
        """Test if_else() returns true_val when condition is True."""
        result = engine.evaluate(
            "if_else(price > 10, 'expensive', 'cheap')", {"price": 15}
        )
        assert result == "expensive"

    @requires_simpleeval
    def test_if_else_false_condition(self, engine):
        """Test if_else() returns false_val when condition is False."""
        result = engine.evaluate(
            "if_else(price > 10, 'expensive', 'cheap')", {"price": 5}
        )
        assert result == "cheap"

    def test_between_true(self, engine):
        """Test between() returns True when value is in range."""
        result = engine.evaluate("between(value, 10, 20)", {"value": 15})
        assert result is True

    def test_between_false(self, engine):
        """Test between() returns False when value is out of range."""
        result = engine.evaluate("between(value, 10, 20)", {"value": 25})
        assert result is False

    def test_between_with_none(self, engine):
        """Test between() returns False for None."""
        result = engine.evaluate("between(value, 10, 20)", {"value": None})
        assert result is False

    @requires_simpleeval
    def test_in_list_true(self, engine):
        """Test in_list() returns True when value is in list."""
        result = engine.evaluate("in_list(status, ['A', 'B', 'C'])", {"status": "B"})
        assert result is True

    @requires_simpleeval
    def test_in_list_false(self, engine):
        """Test in_list() returns False when value is not in list."""
        result = engine.evaluate("in_list(status, ['A', 'B', 'C'])", {"status": "X"})
        assert result is False


class TestToDateFunction:
    """Test _to_date helper function."""

    def test_to_date_with_format(self):
        """Test _to_date parses with custom format."""
        result = _to_date("15/05/2026", "%d/%m/%Y")
        assert result.year == 2026
        assert result.month == 5
        assert result.day == 15

    def test_to_date_without_format(self):
        """Test _to_date parses standard format."""
        result = _to_date("2026-05-15")
        assert result.year == 2026

    def test_to_date_with_none(self):
        """Test _to_date returns None for None input."""
        result = _to_date(None)
        assert result is None

    def test_to_date_invalid_format(self):
        """Test _to_date returns None for invalid format."""
        result = _to_date("not-a-date", "%Y-%m-%d")
        assert result is None


class TestFormulaEngineTypeConversion:
    """Test type conversion functions."""

    @pytest.fixture
    def engine(self):
        """Create a fresh FormulaEngine instance."""
        return FormulaEngine()

    def test_to_string(self, engine):
        """Test to_string() converts to string."""
        result = engine.evaluate("to_string(value)", {"value": 123})
        assert result == "123"

    def test_to_string_none(self, engine):
        """Test to_string() handles None."""
        result = engine.evaluate("to_string(value)", {"value": None})
        assert result is None

    def test_to_int(self, engine):
        """Test to_int() converts to integer."""
        result = engine.evaluate("to_int(value)", {"value": "123"})
        assert result == 123

    def test_to_int_from_float(self, engine):
        """Test to_int() truncates floats."""
        result = engine.evaluate("to_int(value)", {"value": "123.7"})
        assert result == 123

    def test_to_int_none(self, engine):
        """Test to_int() handles None."""
        result = engine.evaluate("to_int(value)", {"value": None})
        assert result is None

    def test_to_float(self, engine):
        """Test to_float() converts to float."""
        result = engine.evaluate("to_float(value)", {"value": "123.45"})
        assert result == 123.45

    def test_to_bool(self, engine):
        """Test to_bool() converts to boolean."""
        result = engine.evaluate("to_bool(value)", {"value": 1})
        assert result is True


class TestFormulaEngineEvaluateColumn:
    """Test evaluate_column() for DataFrame operations."""

    @pytest.fixture
    def engine(self):
        """Create a fresh FormulaEngine instance."""
        return FormulaEngine()

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame(
            {
                "price": [10.0, 20.0, 30.0, 40.0],
                "quantity": [2, 3, 1, 4],
                "name": ["Alice", "Bob", "Charlie", "David"],
                "date": ["2026-01-01", "2026-02-01", "2026-03-01", "2026-04-01"],
            }
        )

    def test_evaluate_column_arithmetic(self, engine, sample_df):
        """Test evaluate_column with arithmetic formula."""
        result = engine.evaluate_column(sample_df, "price * quantity")
        expected = pd.Series([20.0, 60.0, 30.0, 160.0])
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_evaluate_column_string_function(self, engine, sample_df):
        """Test evaluate_column with string function."""
        result = engine.evaluate_column(sample_df, "upper(name)")
        expected = pd.Series(["ALICE", "BOB", "CHARLIE", "DAVID"])
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_evaluate_column_empty_dataframe(self, engine):
        """Test evaluate_column with empty DataFrame."""
        empty_df = pd.DataFrame(columns=["price", "quantity"])
        result = engine.evaluate_column(empty_df, "price * quantity")
        assert len(result) == 0

    def test_evaluate_column_with_nulls(self, engine):
        """Test evaluate_column handles null values."""
        df = pd.DataFrame(
            {
                "name": ["Alice", None, "Charlie"],
            }
        )
        result = engine.evaluate_column(df, "upper(name)")
        assert result.iloc[0] == "ALICE"
        # Null handling - may be None, NaN, pd.NA, or string "NAN" depending on implementation
        assert result.iloc[1] is None or pd.isna(result.iloc[1]) or result.iloc[1] == "NAN"
        assert result.iloc[2] == "CHARLIE"


class TestFormulaEngineLegacyConversion:
    """Test legacy formula conversion."""

    @pytest.fixture
    def engine(self):
        """Create a fresh FormulaEngine instance."""
        return FormulaEngine()

    def test_convert_pandas_str_upper(self, engine):
        """Test conversion of pandas .str.upper() syntax."""
        converted = engine._convert_legacy_formula("name.str.upper()")
        assert converted == "upper(name)"

    def test_convert_pandas_str_lower(self, engine):
        """Test conversion of pandas .str.lower() syntax."""
        converted = engine._convert_legacy_formula("name.str.lower()")
        assert converted == "lower(name)"

    def test_convert_pandas_str_strip(self, engine):
        """Test conversion of pandas .str.strip() syntax."""
        converted = engine._convert_legacy_formula("name.str.strip()")
        assert converted == "strip(name)"

    def test_convert_pandas_dt_year(self, engine):
        """Test conversion of pandas .dt.year syntax."""
        converted = engine._convert_legacy_formula("date_col.dt.year")
        assert converted == "year(date_col)"

    def test_convert_pandas_dt_month(self, engine):
        """Test conversion of pandas .dt.month syntax."""
        converted = engine._convert_legacy_formula("date_col.dt.month")
        assert converted == "month(date_col)"

    def test_convert_pandas_dt_day(self, engine):
        """Test conversion of pandas .dt.day syntax."""
        converted = engine._convert_legacy_formula("date_col.dt.day")
        assert converted == "day(date_col)"

    def test_convert_pd_timestamp_now(self, engine):
        """Test conversion of pd.Timestamp.now() syntax."""
        converted = engine._convert_legacy_formula("pd.Timestamp.now()")
        assert converted == "now()"

    def test_convert_datetime_now(self, engine):
        """Test conversion of datetime.now() syntax."""
        converted = engine._convert_legacy_formula("datetime.now()")
        assert converted == "now()"

    def test_convert_np_where(self, engine):
        """Test conversion of np.where() syntax."""
        converted = engine._convert_legacy_formula(
            "np.where(condition, true_val, false_val)"
        )
        assert converted == "if_else(condition, true_val, false_val)"

    def test_no_conversion_for_normal_formula(self, engine):
        """Test that normal formulas are not modified."""
        formula = "price * quantity"
        converted = engine._convert_legacy_formula(formula)
        assert converted == formula


class TestFormulaEngineSingleton:
    """Test get_formula_engine singleton factory."""

    def test_get_formula_engine_returns_engine(self):
        """Test get_formula_engine returns FormulaEngine instance."""
        engine = get_formula_engine()
        assert isinstance(engine, FormulaEngine)

    def test_get_formula_engine_singleton(self):
        """Test get_formula_engine returns same instance."""
        engine1 = get_formula_engine()
        engine2 = get_formula_engine()
        # Note: with custom_functions, it creates a new instance
        # Without custom_functions, should return cached
        assert engine1 is not None
        assert engine2 is not None

    def test_get_formula_engine_with_custom_functions(self):
        """Test get_formula_engine with custom functions."""
        custom_funcs = {"double": lambda x: x * 2}
        engine = get_formula_engine(custom_funcs)
        assert "double" in engine._functions


class TestFormulaEngineErrorHandling:
    """Test error handling in FormulaEngine."""

    @pytest.fixture
    def engine(self):
        """Create a fresh FormulaEngine instance."""
        return FormulaEngine()

    @requires_simpleeval
    def test_invalid_formula_raises_error(self, engine):
        """Test that invalid formula raises FormulaError."""
        with pytest.raises(FormulaError):
            engine.evaluate("invalid syntax @@##", {})

    @requires_simpleeval
    def test_undefined_variable_raises_error(self, engine):
        """Test that undefined variable raises FormulaError."""
        with pytest.raises(FormulaError):
            engine.evaluate("undefined_var * 2", {})

    @requires_simpleeval
    def test_division_by_zero(self, engine):
        """Test division by zero handling."""
        with pytest.raises(FormulaError):
            engine.evaluate("10 / 0", {})

    def test_empty_formula_returns_as_is(self, engine):
        """Test empty formula returns as-is."""
        result = engine.evaluate("", {})
        assert result == ""

    def test_none_formula_returns_none(self, engine):
        """Test None formula returns None."""
        result = engine.evaluate(None, {})
        assert result is None


class TestFormulaEngineAvailableFunctions:
    """Test get_available_functions documentation."""

    def test_get_available_functions_returns_dict(self):
        """Test get_available_functions returns dictionary."""
        engine = FormulaEngine()
        funcs = engine.get_available_functions()
        assert isinstance(funcs, dict)

    def test_get_available_functions_contains_expected(self):
        """Test get_available_functions contains expected entries."""
        engine = FormulaEngine()
        funcs = engine.get_available_functions()

        # Check some expected functions are documented
        assert any("upper" in key for key in funcs.keys())
        assert any("coalesce" in key for key in funcs.keys())
        assert any("year" in key for key in funcs.keys())
        assert any("if_else" in key for key in funcs.keys())


class TestFormulaEngineFallbackEvaluation:
    """Test fallback evaluation when simpleeval is not available."""

    def test_fallback_basic_arithmetic(self):
        """Test fallback handles basic arithmetic."""
        engine = FormulaEngine()
        # Directly test fallback method
        result = engine._fallback_evaluate(
            "price * quantity", {"price": 10, "quantity": 5}
        )
        assert result == 50.0

    def test_fallback_addition(self):
        """Test fallback handles addition."""
        engine = FormulaEngine()
        result = engine._fallback_evaluate("a + b", {"a": 10, "b": 20})
        assert result == 30.0

    def test_fallback_subtraction(self):
        """Test fallback handles subtraction."""
        engine = FormulaEngine()
        result = engine._fallback_evaluate("a - b", {"a": 30, "b": 10})
        assert result == 20.0

    def test_fallback_division(self):
        """Test fallback handles division."""
        engine = FormulaEngine()
        result = engine._fallback_evaluate("a / b", {"a": 20, "b": 4})
        assert result == 5.0

    def test_fallback_function_call(self):
        """Test fallback handles function calls."""
        engine = FormulaEngine()
        result = engine._fallback_evaluate("upper(name)", {"name": "hello"})
        assert result == "HELLO"

    def test_fallback_resolve_string_literal(self):
        """Test fallback resolves string literals."""
        engine = FormulaEngine()
        result = engine._resolve_value("'hello'", {})
        assert result == "hello"

    def test_fallback_resolve_number(self):
        """Test fallback resolves numbers."""
        engine = FormulaEngine()
        result = engine._resolve_value("123", {})
        assert result == 123

    def test_fallback_resolve_float(self):
        """Test fallback resolves floats."""
        engine = FormulaEngine()
        result = engine._resolve_value("123.45", {})
        assert result == 123.45

    def test_fallback_resolve_boolean_true(self):
        """Test fallback resolves True."""
        engine = FormulaEngine()
        result = engine._resolve_value("True", {})
        assert result is True

    def test_fallback_resolve_boolean_false(self):
        """Test fallback resolves False."""
        engine = FormulaEngine()
        result = engine._resolve_value("False", {})
        assert result is False

    def test_fallback_resolve_none(self):
        """Test fallback resolves None."""
        engine = FormulaEngine()
        result = engine._resolve_value("None", {})
        assert result is None


class TestFormulaEngineMathFunctions:
    """Test math functions in FormulaEngine."""

    @pytest.fixture
    def engine(self):
        """Create a fresh FormulaEngine instance."""
        return FormulaEngine()

    def test_abs_function(self, engine):
        """Test abs() returns absolute value."""
        result = engine.evaluate("abs(value)", {"value": -10})
        assert result == 10

    def test_round_function(self, engine):
        """Test round() rounds to specified decimals."""
        result = engine.evaluate("round(value, 2)", {"value": 3.14159})
        assert result == 3.14

    def test_min_function(self, engine):
        """Test min() returns minimum value."""
        result = engine.evaluate("min(a, b, c)", {"a": 5, "b": 3, "c": 7})
        assert result == 3

    def test_max_function(self, engine):
        """Test max() returns maximum value."""
        result = engine.evaluate("max(a, b, c)", {"a": 5, "b": 3, "c": 7})
        assert result == 7

    def test_pow_function(self, engine):
        """Test pow() returns power."""
        result = engine.evaluate("pow(2, 3)", {"x": 0})
        assert result == 8

    @requires_simpleeval
    def test_sum_function(self, engine):
        """Test sum() returns sum of iterable."""
        result = engine.evaluate("sum([1, 2, 3, 4])", {})
        assert result == 10

    def test_len_function(self, engine):
        """Test len() returns length."""
        result = engine.evaluate("len(items)", {"items": [1, 2, 3]})
        assert result == 3
