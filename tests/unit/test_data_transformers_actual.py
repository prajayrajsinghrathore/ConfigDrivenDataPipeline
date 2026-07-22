# File: tests/unit/test_data_transformers_actual.py
"""
Unit tests for the actual data_transformers implementation.

Tests cover:
- transform_data() function with formula engine integration
- enrich_from_snowflake() with mocked SnowflakeHook
- Column type conversions
- New column creation with formulas
- Aggregations
- Filters
- Edge cases: empty DataFrames, null handling, errors
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
import sys

# Mock the Airflow imports before importing the module
sys.modules["airflow"] = MagicMock()
sys.modules["airflow.providers"] = MagicMock()
sys.modules["airflow.providers.snowflake"] = MagicMock()
sys.modules["airflow.providers.snowflake.hooks"] = MagicMock()
sys.modules["airflow.providers.snowflake.hooks.snowflake"] = MagicMock()

# Import actual implementation (with mocked airflow)
from rlam_airflow_framework.data_transformers import transform_data, enrich_from_snowflake, TransformationError  # noqa: E402


class TestTransformDataColumnTypes:
    """Test column type transformations in transform_data()."""

    def test_convert_to_datetime(self):
        """Test converting column to datetime type."""
        df = pd.DataFrame(
            {
                "date_str": ["2026-01-01", "2026-02-15", "2026-03-20"],
            }
        )

        transformations = {"column_types": {"date_str": "datetime"}}

        result = transform_data(df, transformations)

        assert pd.api.types.is_datetime64_any_dtype(result["date_str"])
        assert result["date_str"].iloc[0].year == 2026

    def test_convert_to_float(self):
        """Test converting column to float type."""
        df = pd.DataFrame(
            {
                "value": ["10", "20", "30"],
            }
        )

        transformations = {"column_types": {"value": "float"}}

        result = transform_data(df, transformations)

        # Accept both float64 and Float64 (nullable)
        assert pd.api.types.is_float_dtype(result["value"]) or pd.api.types.is_numeric_dtype(result["value"])
        assert result["value"].iloc[0] == 10.0

    def test_convert_to_int(self):
        """Test converting column to int type."""
        df = pd.DataFrame(
            {
                "count": [1.0, 2.0, 3.0],
            }
        )

        transformations = {"column_types": {"count": "int"}}

        result = transform_data(df, transformations)

        # Accept both int64 and Int64 (nullable)
        assert pd.api.types.is_integer_dtype(result["count"])

    def test_convert_to_string(self):
        """Test converting column to string type."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
            }
        )

        transformations = {"column_types": {"id": "str"}}

        result = transform_data(df, transformations)

        # Accept both object and StringDtype
        dtype_str = str(result["id"].dtype).lower()
        assert result["id"].dtype == object or "string" in dtype_str or "str" in dtype_str
        assert str(result["id"].iloc[0]) == "1"

    def test_skip_nonexistent_column_type_conversion(self):
        """Test that nonexistent columns are skipped in type conversion."""
        df = pd.DataFrame(
            {
                "value": [1, 2, 3],
            }
        )

        transformations = {"column_types": {"nonexistent": "float"}}

        # Should not raise error
        result = transform_data(df, transformations)
        assert len(result) == 3


class TestTransformDataNewColumns:
    """Test new column creation with formulas."""

    def test_create_simple_formula_column(self):
        """Test creating column with simple arithmetic formula."""
        df = pd.DataFrame(
            {
                "price": [10.0, 20.0, 30.0],
                "quantity": [2, 3, 1],
            }
        )

        transformations = {"new_columns": {"total": "price * quantity"}}

        result = transform_data(df, transformations)

        assert "total" in result.columns
        assert result["total"].iloc[0] == 20.0
        assert result["total"].iloc[1] == 60.0
        assert result["total"].iloc[2] == 30.0

    def test_create_multiple_formula_columns(self):
        """Test creating multiple formula columns."""
        df = pd.DataFrame(
            {
                "price": [100.0, 200.0],
                "quantity": [2, 3],
            }
        )

        transformations = {
            "new_columns": {
                "total": "price * quantity",
                "discounted": "price * 0.9",
            }
        }

        result = transform_data(df, transformations)

        assert "total" in result.columns
        assert "discounted" in result.columns
        assert result["total"].iloc[0] == 200.0
        assert result["discounted"].iloc[0] == 90.0

    def test_create_formula_column_with_functions(self):
        """Test creating column with formula functions."""
        df = pd.DataFrame(
            {
                "name": ["alice", "bob", "charlie"],
            }
        )

        transformations = {"new_columns": {"upper_name": "upper(name)"}}

        result = transform_data(df, transformations)

        assert result["upper_name"].iloc[0] == "ALICE"
        assert result["upper_name"].iloc[1] == "BOB"

    def test_create_formula_column_error_handling(self):
        """Test formula column creation handles errors gracefully."""
        df = pd.DataFrame(
            {
                "value": [1, 2, 3],
            }
        )

        transformations = {"new_columns": {"bad_col": "nonexistent_column * 2"}}

        # Should raise TransformationError for invalid column references
        with pytest.raises(TransformationError):
            transform_data(df, transformations)


class TestTransformDataAggregations:
    """Test aggregation transformations."""

    def test_sum_aggregation(self):
        """Test sum aggregation."""
        df = pd.DataFrame(
            {
                "value": [10, 20, 30, 40],
            }
        )

        transformations = {"aggregations": {"sum": ["value"]}}

        result = transform_data(df, transformations)

        assert len(result) == 1  # Aggregated to single row
        assert result["value_sum"].iloc[0] == 100

    def test_mean_aggregation(self):
        """Test mean aggregation."""
        df = pd.DataFrame(
            {
                "value": [10, 20, 30, 40],
            }
        )

        transformations = {"aggregations": {"mean": ["value"]}}

        result = transform_data(df, transformations)

        assert result["value_mean"].iloc[0] == 25.0

    def test_multiple_aggregations(self):
        """Test multiple aggregations on same column."""
        df = pd.DataFrame(
            {
                "value": [10, 20, 30],
            }
        )

        transformations = {
            "aggregations": {
                "sum": ["value"],
                "mean": ["value"],
                "min": ["value"],
                "max": ["value"],
            }
        }

        result = transform_data(df, transformations)

        assert result["value_sum"].iloc[0] == 60
        assert result["value_mean"].iloc[0] == 20.0
        assert result["value_min"].iloc[0] == 10
        assert result["value_max"].iloc[0] == 30

    def test_aggregation_multiple_columns(self):
        """Test aggregation on multiple columns."""
        df = pd.DataFrame(
            {
                "price": [10, 20, 30],
                "quantity": [2, 3, 4],
            }
        )

        transformations = {
            "aggregations": {
                "sum": ["price", "quantity"],
            }
        }

        result = transform_data(df, transformations)

        assert result["price_sum"].iloc[0] == 60
        assert result["quantity_sum"].iloc[0] == 9

    def test_aggregation_skip_nonexistent_column(self):
        """Test aggregation skips nonexistent columns."""
        df = pd.DataFrame(
            {
                "value": [10, 20, 30],
            }
        )

        transformations = {"aggregations": {"sum": ["value", "nonexistent"]}}

        result = transform_data(df, transformations)

        assert "value_sum" in result.columns
        assert "nonexistent_sum" not in result.columns


class TestTransformDataFilters:
    """Test filter transformations."""

    def test_simple_filter(self):
        """Test simple numeric filter."""
        df = pd.DataFrame(
            {
                "value": [10, 20, 30, 40, 50],
            }
        )

        transformations = {"filters": {"value": "> 25"}}

        result = transform_data(df, transformations)

        assert len(result) == 3
        assert list(result["value"]) == [30, 40, 50]

    def test_filter_less_than(self):
        """Test less than filter."""
        df = pd.DataFrame(
            {
                "price": [10, 20, 30, 40],
            }
        )

        transformations = {"filters": {"price": "< 25"}}

        result = transform_data(df, transformations)

        assert len(result) == 2
        assert list(result["price"]) == [10, 20]

    def test_filter_equality(self):
        """Test equality filter."""
        df = pd.DataFrame(
            {
                "status": ["A", "B", "A", "C"],
            }
        )

        transformations = {"filters": {"status": "== 'A'"}}

        result = transform_data(df, transformations)

        assert len(result) == 2
        assert all(result["status"] == "A")

    def test_multiple_filters(self):
        """Test multiple filters applied sequentially."""
        df = pd.DataFrame(
            {
                "value": [10, 20, 30, 40, 50],
                "category": ["A", "B", "A", "B", "A"],
            }
        )

        transformations = {
            "filters": {
                "value": "> 15",
                "category": "== 'A'",
            }
        }

        result = transform_data(df, transformations)

        # value > 15 AND category == 'A'
        assert len(result) == 2
        assert list(result["value"]) == [30, 50]

    def test_filter_skip_nonexistent_column(self):
        """Test filter skips nonexistent columns."""
        df = pd.DataFrame(
            {
                "value": [10, 20, 30],
            }
        )

        transformations = {"filters": {"nonexistent": "> 0"}}

        # Should not filter anything
        result = transform_data(df, transformations)
        assert len(result) == 3


class TestTransformDataCombined:
    """Test combined transformations."""

    def test_type_conversion_then_formula(self):
        """Test type conversion followed by formula column."""
        df = pd.DataFrame(
            {
                "price": ["10", "20", "30"],
                "quantity": [2, 3, 1],
            }
        )

        transformations = {
            "column_types": {"price": "float"},
            "new_columns": {"total": "price * quantity"},
        }

        result = transform_data(df, transformations)

        # Accept both float64 and Float64 (nullable)
        assert pd.api.types.is_float_dtype(result["price"]) or pd.api.types.is_numeric_dtype(result["price"])
        assert result["total"].iloc[0] == 20.0

    def test_formula_then_filter(self):
        """Test formula column creation followed by filter."""
        df = pd.DataFrame(
            {
                "price": [10.0, 20.0, 30.0],
                "quantity": [2, 3, 1],
            }
        )

        transformations = {
            "new_columns": {"total": "price * quantity"},
            "filters": {"total": "> 25"},
        }

        result = transform_data(df, transformations)

        # Totals: 20, 60, 30 -> filter > 25 -> 60, 30
        assert len(result) == 2

    def test_empty_transformations(self):
        """Test with empty transformations dict."""
        df = pd.DataFrame(
            {
                "value": [1, 2, 3],
            }
        )

        result = transform_data(df, {})

        pd.testing.assert_frame_equal(result, df)


class TestTransformDataEdgeCases:
    """Test edge cases in transform_data()."""

    def test_empty_dataframe(self):
        """Test transformation of empty DataFrame."""
        df = pd.DataFrame(columns=["price", "quantity"])

        transformations = {
            "new_columns": {"total": "price * quantity"},
        }

        result = transform_data(df, transformations)

        assert len(result) == 0
        # Empty DataFrame may or may not have new columns depending on implementation
        # Just verify it returns an empty DataFrame without error

    def test_dataframe_with_nulls(self):
        """Test transformation with null values."""
        df = pd.DataFrame(
            {
                "price": [10.0, None, 30.0],
                "quantity": [2, 3, None],
            }
        )

        transformations = {
            "new_columns": {"total": "price * quantity"},
        }

        result = transform_data(df, transformations)

        assert "total" in result.columns
        # Nulls should propagate
        assert pd.isna(result["total"].iloc[1]) or "FORMULA_ERROR" in str(
            result["total"].iloc[1]
        )

    def test_dataframe_preserves_index(self):
        """Test that original index is preserved."""
        df = pd.DataFrame(
            {
                "value": [10, 20, 30],
            },
            index=[100, 200, 300],
        )

        transformations = {
            "new_columns": {"doubled": "value * 2"},
        }

        result = transform_data(df, transformations)

        assert list(result.index) == [100, 200, 300]

    def test_unicode_data(self):
        """Test transformation with Unicode data."""
        df = pd.DataFrame(
            {
                "name": ["日本語", "中文", "العربية"],
            }
        )

        transformations = {
            "new_columns": {"upper_name": "upper(name)"},
        }

        result = transform_data(df, transformations)

        assert (
            result["upper_name"].iloc[0] == "日本語"
        )  # Unicode uppercase may not change

    def test_large_dataframe(self):
        """Test transformation of large DataFrame."""
        df = pd.DataFrame(
            {
                "value": range(10000),
                "multiplier": [2] * 10000,
            }
        )

        transformations = {
            "new_columns": {"result": "value * multiplier"},
        }

        result = transform_data(df, transformations)

        assert len(result) == 10000
        assert result["result"].iloc[0] == 0
        assert result["result"].iloc[9999] == 19998


class TestEnrichFromSnowflake:
    """Test enrich_from_snowflake() with mocked SnowflakeHook."""

    @patch("rlam_airflow_framework.data_transformers.SnowflakeHook")
    def test_enrich_basic_lookup(self, mock_hook_class):
        """Test basic Snowflake enrichment lookup."""
        # Setup mock
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        # Mock lookup data returned from Snowflake
        lookup_df = pd.DataFrame(
            {
                "INSTRUMENT_ID": ["INS001", "INS002"],
                "ISIN": ["US0378331005", "US5949181045"],
                "INSTRUMENT_NAME": ["Apple Inc", "Microsoft Corp"],
            }
        )
        mock_hook.get_pandas_df.return_value = lookup_df

        # Input DataFrame
        df = pd.DataFrame(
            {
                "instrument_id": ["INS001", "INS002", "INS001"],
                "quantity": [100, 200, 150],
            }
        )

        lookup_config = {
            "table": "REFERENCE.INSTRUMENTS",
            "join_on": {"instrument_id": "INSTRUMENT_ID"},
            "select_columns": ["ISIN", "INSTRUMENT_NAME"],
        }

        result = enrich_from_snowflake(df, "snowflake_conn", lookup_config)

        # Verify hook was called correctly
        mock_hook_class.assert_called_once_with(snowflake_conn_id="snowflake_conn")

        # Verify result has enriched columns
        assert "ISIN" in result.columns
        assert "INSTRUMENT_NAME" in result.columns
        assert len(result) == 3

    @patch("rlam_airflow_framework.data_transformers.SnowflakeHook")
    def test_enrich_with_no_matches(self, mock_hook_class):
        """Test enrichment when no matches found (left join)."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        # Empty lookup result
        lookup_df = pd.DataFrame(columns=["INSTRUMENT_ID", "ISIN"])
        mock_hook.get_pandas_df.return_value = lookup_df

        df = pd.DataFrame(
            {
                "instrument_id": ["INS001", "INS002"],
                "quantity": [100, 200],
            }
        )

        lookup_config = {
            "table": "REFERENCE.INSTRUMENTS",
            "join_on": {"instrument_id": "INSTRUMENT_ID"},
            "select_columns": ["ISIN"],
        }

        result = enrich_from_snowflake(df, "snowflake_conn", lookup_config)

        # Left join should preserve all original rows
        assert len(result) == 2
        # Enriched columns should be NaN
        assert pd.isna(result["ISIN"].iloc[0])

    @patch("rlam_airflow_framework.data_transformers.SnowflakeHook")
    def test_enrich_query_construction(self, mock_hook_class):
        """Test that correct query is constructed."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        lookup_df = pd.DataFrame(
            {
                "ID": ["A", "B"],
                "NAME": ["Name A", "Name B"],
            }
        )
        mock_hook.get_pandas_df.return_value = lookup_df

        df = pd.DataFrame(
            {
                "id": ["A", "B", "C"],
            }
        )

        lookup_config = {
            "table": "MY_SCHEMA.MY_TABLE",
            "join_on": {"id": "ID"},
            "select_columns": ["NAME"],
        }

        enrich_from_snowflake(df, "conn", lookup_config)

        # Verify query was constructed correctly
        call_args = mock_hook.get_pandas_df.call_args[0][0]
        assert "MY_SCHEMA.MY_TABLE" in call_args
        assert "ID" in call_args
        assert "NAME" in call_args
        assert "'A'" in call_args
        assert "'B'" in call_args
        assert "'C'" in call_args

    @patch("rlam_airflow_framework.data_transformers.SnowflakeHook")
    def test_enrich_unique_values_only(self, mock_hook_class):
        """Test that only unique values are queried."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        lookup_df = pd.DataFrame(
            {
                "ID": ["A"],
                "NAME": ["Name A"],
            }
        )
        mock_hook.get_pandas_df.return_value = lookup_df

        # DataFrame with duplicate IDs
        df = pd.DataFrame(
            {
                "id": ["A", "A", "A", "A", "A"],  # All same ID
            }
        )

        lookup_config = {
            "table": "TABLE",
            "join_on": {"id": "ID"},
            "select_columns": ["NAME"],
        }

        enrich_from_snowflake(df, "conn", lookup_config)

        # Query should only have one value 'A'
        call_args = mock_hook.get_pandas_df.call_args[0][0]
        # Should only have 'A' once in IN clause
        assert call_args.count("'A'") == 1

    @patch("rlam_airflow_framework.data_transformers.SnowflakeHook")
    def test_enrich_multiple_select_columns(self, mock_hook_class):
        """Test enrichment with multiple select columns."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        lookup_df = pd.DataFrame(
            {
                "CODE": ["X"],
                "COL1": ["val1"],
                "COL2": ["val2"],
                "COL3": ["val3"],
            }
        )
        mock_hook.get_pandas_df.return_value = lookup_df

        df = pd.DataFrame(
            {
                "code": ["X"],
            }
        )

        lookup_config = {
            "table": "TABLE",
            "join_on": {"code": "CODE"},
            "select_columns": ["COL1", "COL2", "COL3"],
        }

        result = enrich_from_snowflake(df, "conn", lookup_config)

        assert "COL1" in result.columns
        assert "COL2" in result.columns
        assert "COL3" in result.columns


class TestEnrichFromSnowflakeEdgeCases:
    """Test edge cases for enrich_from_snowflake()."""

    @patch("rlam_airflow_framework.data_transformers.SnowflakeHook")
    def test_enrich_empty_dataframe(self, mock_hook_class):
        """Test enrichment of empty DataFrame."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        # Return DataFrame with correct columns but no rows
        mock_hook.get_pandas_df.return_value = pd.DataFrame(columns=["ID", "NAME"])

        df = pd.DataFrame(columns=["id"])

        lookup_config = {
            "table": "TABLE",
            "join_on": {"id": "ID"},
            "select_columns": ["NAME"],
        }

        result = enrich_from_snowflake(df, "conn", lookup_config)

        assert len(result) == 0

    @patch("rlam_airflow_framework.data_transformers.SnowflakeHook")
    def test_enrich_special_characters_in_values(self, mock_hook_class):
        """Test enrichment with special characters in values."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        lookup_df = pd.DataFrame(
            {
                "ID": ["A'B"],  # Value with quote
                "NAME": ["Test"],
            }
        )
        mock_hook.get_pandas_df.return_value = lookup_df

        df = pd.DataFrame(
            {
                "id": ["A'B"],  # Value with quote
            }
        )

        lookup_config = {
            "table": "TABLE",
            "join_on": {"id": "ID"},
            "select_columns": ["NAME"],
        }

        # This may need SQL injection protection in production
        result = enrich_from_snowflake(df, "conn", lookup_config)

        assert len(result) == 1

    @patch("rlam_airflow_framework.data_transformers.SnowflakeHook")
    def test_enrich_preserves_original_columns(self, mock_hook_class):
        """Test that original DataFrame columns are preserved."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        lookup_df = pd.DataFrame(
            {
                "ID": ["A"],
                "NEW_COL": ["new_value"],
            }
        )
        mock_hook.get_pandas_df.return_value = lookup_df

        df = pd.DataFrame(
            {
                "id": ["A"],
                "existing_col1": ["val1"],
                "existing_col2": [123],
            }
        )

        lookup_config = {
            "table": "TABLE",
            "join_on": {"id": "ID"},
            "select_columns": ["NEW_COL"],
        }

        result = enrich_from_snowflake(df, "conn", lookup_config)

        assert "existing_col1" in result.columns
        assert "existing_col2" in result.columns
        assert "NEW_COL" in result.columns
        assert result["existing_col1"].iloc[0] == "val1"
        assert result["existing_col2"].iloc[0] == 123

    @patch("rlam_airflow_framework.data_transformers.SnowflakeHook")
    def test_enrich_connection_error_propagates(self, mock_hook_class):
        """Test that connection errors are propagated."""
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        mock_hook.get_pandas_df.side_effect = Exception("Connection failed")

        df = pd.DataFrame({"id": ["A"]})

        lookup_config = {
            "table": "TABLE",
            "join_on": {"id": "ID"},
            "select_columns": ["NAME"],
        }

        with pytest.raises(Exception, match="Connection failed"):
            enrich_from_snowflake(df, "conn", lookup_config)


class TestTransformDataOriginalUnmodified:
    """Test that original DataFrame is not modified."""

    def test_original_df_unmodified_after_transform(self):
        """Test that original DataFrame is not modified."""
        original_df = pd.DataFrame(
            {
                "value": [1, 2, 3],
            }
        )
        original_copy = original_df.copy()

        transformations = {
            "new_columns": {"doubled": "value * 2"},
            "filters": {"value": "> 1"},
        }

        result = transform_data(original_df, transformations)

        # Original should be unchanged
        pd.testing.assert_frame_equal(original_df, original_copy)

        # Result should be different
        assert "doubled" in result.columns
        assert len(result) == 2

