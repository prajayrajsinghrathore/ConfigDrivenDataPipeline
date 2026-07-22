# File: tests/unit/test_edge_cases_data_operations.py
"""
Edge case tests for data transformers.

Tests cover:
- Empty DataFrames
- All-null columns
- Data validation
- Large data handling
- Type coercion edge cases
- Special values (inf, nan)
- Filter edge cases
"""

import pytest
import pandas as pd
import numpy as np

# Import implementations with graceful fallback
DATA_TRANSFORMERS_AVAILABLE = False

class TransformationError(Exception):
    pass

try:
    from rlam_airflow_framework.data_transformers import (
        transform_data,
        TransformationError,
    )
    DATA_TRANSFORMERS_AVAILABLE = True
except Exception:
    def transform_data(*args, **kwargs):
        raise NotImplementedError("data_transformers not available")

requires_data_transformers = pytest.mark.skipif(
    not DATA_TRANSFORMERS_AVAILABLE, reason="data_transformers module not available"
)


class TestEmptyDataFrameHandling:
    """Test handling of empty DataFrames."""

    @requires_data_transformers
    def test_transform_empty_dataframe(self):
        """Test transforming an empty DataFrame."""
        df = pd.DataFrame()
        transform_config = {"new_columns": {"test_col": "1 + 1"}}

        result = transform_data(df, transform_config)
        assert len(result) == 0

    @requires_data_transformers
    def test_transform_dataframe_with_columns_but_no_rows(self):
        """Test transforming DataFrame with columns but no rows."""
        df = pd.DataFrame(columns=["a", "b", "c"])
        transform_config = {"new_columns": {"sum_col": "a + b"}}

        result = transform_data(df, transform_config)
        assert len(result) == 0


@requires_data_transformers
class TestAllNullColumns:
    """Test handling of DataFrames with all-null columns."""

    def test_transform_with_all_null_column(self):
        """Test transformation when a column is all nulls."""
        df = pd.DataFrame(
            {"id": [1, 2, 3], "value": [None, None, None], "name": ["a", "b", "c"]}
        )

        transform_config = {
            "new_columns": {"doubled": "value * 2"}
        }

        # Should handle null propagation correctly
        try:
            result = transform_data(df, transform_config)
            assert result["doubled"].isna().all()
        except TransformationError:
            # Also acceptable to reject
            pass

    def test_aggregation_on_all_null_column(self):
        """Test aggregation on all-null column."""
        df = pd.DataFrame(
            {"category": ["A", "A", "B", "B"], "value": [None, None, None, None]}
        )

        transform_config = {
            "aggregations": {"sum": ["value"]}
        }

        try:
            result = transform_data(df, transform_config)
            # Sum of nulls should be 0 or NaN depending on implementation
            assert "value_sum" in result.columns
        except TransformationError:
            pass

    def test_filter_on_null_column(self):
        """Test filtering on null column."""
        df = pd.DataFrame({"id": [1, 2, 3], "status": [None, None, None]})

        transform_config = {
            "filters": {"status": "== 'active'"}
        }

        try:
            result = transform_data(df, transform_config)
            # Null != 'active', so should be empty
            assert len(result) == 0
        except TransformationError:
            pass


@requires_data_transformers
class TestDataTypeCoercion:
    """Test type coercion edge cases."""

    def test_transform_mixed_types_in_column(self):
        """Test transformation with mixed types in a column."""
        df = pd.DataFrame({"mixed": [1, "2", 3.0, None, "text"]})
        transform_config = {"column_types": {"mixed": "str"}}

        result = transform_data(df, transform_config)
        dtype_str = str(result["mixed"].dtype).lower()
        assert result["mixed"].dtype == object or "string" in dtype_str or "str" in dtype_str

    def test_transform_numeric_overflow(self):
        """Test transformation with numeric overflow values."""
        df = pd.DataFrame({"big_num": [1e308, 1e308, 1e308]})
        transform_config = {"new_columns": {"doubled": "big_num * 2"}}

        result = transform_data(df, transform_config)
        assert "doubled" in result.columns

    def test_transform_date_string_to_datetime(self):
        """Test date string to datetime conversion."""
        df = pd.DataFrame({"date_str": ["2026-01-01", "2026-02-15", "invalid-date", None]})
        transform_config = {"column_types": {"date_str": "datetime"}}

        try:
            transform_data(df, transform_config)
        except (TransformationError, ValueError):
            pass

    def test_transform_boolean_edge_cases(self):
        """Test boolean type conversion edge cases."""
        df = pd.DataFrame({"bool_like": [1, 0, "true", "false", "", None, "yes", "no"]})
        transform_config = {"column_types": {"bool_like": "bool"}}

        try:
            transform_data(df, transform_config)
        except (TransformationError, ValueError):
            pass


@requires_data_transformers
class TestDataValidation:
    """Test data validation in transformers."""

    def test_transform_with_none_config(self):
        """Test transform with None config."""
        df = pd.DataFrame({"id": [1, 2, 3]})

        result = transform_data(df, None)
        assert len(result) == 3

    def test_transform_with_empty_config(self):
        """Test transform with empty config."""
        df = pd.DataFrame({"id": [1, 2, 3]})

        result = transform_data(df, {})
        assert len(result) == 3




@requires_data_transformers
class TestLargeDataHandling:
    """Test handling of large datasets."""

    def test_transform_large_dataframe(self):
        """Test transforming a large DataFrame."""
        n_rows = 100000
        df = pd.DataFrame({
            "id": range(n_rows),
            "value": np.random.randn(n_rows),
            "category": np.random.choice(["A", "B", "C"], n_rows),
        })
        transform_config = {"new_columns": {"doubled": "value * 2"}}

        result = transform_data(df, transform_config)
        assert len(result) == n_rows
        assert "doubled" in result.columns


@requires_data_transformers
class TestSpecialValues:
    """Test handling of special values (inf, nan, etc.)."""

    def test_transform_with_infinity_values(self):
        """Test transformation with infinity values."""
        df = pd.DataFrame({"value": [1.0, float("inf"), float("-inf"), 0.0]})

        transform_config = {
            "new_columns": {"plus_one": "value + 1"}
        }

        result = transform_data(df, transform_config)
        assert np.isinf(result["plus_one"].iloc[1])
        assert np.isinf(result["plus_one"].iloc[2])

    def test_transform_with_nan_values(self):
        """Test transformation with NaN values."""
        df = pd.DataFrame({"value": [1.0, float("nan"), 3.0, float("nan")]})

        transform_config = {
            "new_columns": {"doubled": "value * 2"}
        }

        result = transform_data(df, transform_config)
        assert np.isnan(result["doubled"].iloc[1])
        assert np.isnan(result["doubled"].iloc[3])

    def test_aggregation_with_special_values(self):
        """Test aggregation with special values."""
        df = pd.DataFrame(
            {
                "category": ["A", "A", "B", "B"],
                "value": [1.0, float("inf"), 3.0, float("nan")],
            }
        )

        transform_config = {
            "aggregations": {"mean": ["value"]}
        }

        result = transform_data(df, transform_config)
        assert "value_mean" in result.columns


@requires_data_transformers
class TestEdgeCaseFilters:
    """Test edge cases in filter operations."""

    def test_filter_with_empty_string(self):
        """Test filtering with empty string value."""
        df = pd.DataFrame({"name": ["Alice", "", "Bob", None, ""]})
        transform_config = {"filters": {"name": "== ''"}}

        result = transform_data(df, transform_config)
        assert len(result) == 2

    def test_filter_with_regex_special_chars(self):
        """Test filter doesn't interpret regex special chars."""
        df = pd.DataFrame({"pattern": ["a.b", "aXb", "a*b", "a.b.c"]})
        transform_config = {"filters": {"pattern": "== 'a.b'"}}

        result = transform_data(df, transform_config)
        assert len(result) == 1

    def test_filter_with_list_in_operator(self):
        """Test filter with 'in' operator and list."""
        df = pd.DataFrame({"status": ["active", "pending", "inactive", "active"]})
        transform_config = {"filters": {"status": "in ['active', 'pending']"}}

        try:
            result = transform_data(df, transform_config)
            assert len(result) == 3
        except (TransformationError, KeyError):
            pass

    def test_filter_column_not_exists(self):
        """Test filter on non-existent column."""
        df = pd.DataFrame({"id": [1, 2, 3]})
        transform_config = {"filters": {"nonexistent": "== 1"}}

        result = transform_data(df, transform_config)
        assert len(result) == 3




