# File: tests/unit/test_transformers.py
"""
Unit tests for the transformers/ package - the Transformer strategies that
back the unified, ordered ``transformations`` pipeline.

Covers each strategy in isolation (TypeCast, Formula, Filter, Aggregation,
SnowflakeLookup) plus the factory/pipeline runner that chains them.

Airflow (SnowflakeHook) is mocked process-wide via conftest.py; the
SnowflakeHook reference is additionally patched where it's imported
(patch-where-used) for per-test isolation, matching test_destination_loaders.py.
"""

import pandas as pd
import pytest
from unittest.mock import patch

from rlam_airflow_framework.transformers import (
    AggregationTransformer,
    FilterTransformer,
    FormulaTransformer,
    SnowflakeLookupTransformer,
    TransformationError,
    TypeCastTransformer,
    apply_pipeline_transformations,
    get_transformer,
)

_HOOK = "rlam_airflow_framework.transformers.enrichments.snowflake.SnowflakeHook"


class TestTypeCastTransformer:
    def test_convert_to_datetime(self):
        df = pd.DataFrame({"date_str": ["2026-01-01", "2026-02-15", "2026-03-20"]})
        result = TypeCastTransformer().transform(
            df, {"columns": {"date_str": "datetime"}}
        )
        assert pd.api.types.is_datetime64_any_dtype(result["date_str"])
        assert result["date_str"].iloc[0].year == 2026

    def test_convert_to_float(self):
        df = pd.DataFrame({"value": ["10", "20", "30"]})
        result = TypeCastTransformer().transform(df, {"columns": {"value": "float"}})
        assert pd.api.types.is_numeric_dtype(result["value"])
        assert result["value"].iloc[0] == 10.0

    def test_convert_to_int(self):
        df = pd.DataFrame({"count": [1.0, 2.0, 3.0]})
        result = TypeCastTransformer().transform(df, {"columns": {"count": "int"}})
        assert pd.api.types.is_integer_dtype(result["count"])

    def test_convert_to_string(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        result = TypeCastTransformer().transform(df, {"columns": {"id": "string"}})
        assert str(result["id"].iloc[0]) == "1"

    def test_skip_nonexistent_column(self):
        df = pd.DataFrame({"value": [1, 2, 3]})
        result = TypeCastTransformer().transform(
            df, {"columns": {"nonexistent": "float"}}
        )
        assert len(result) == 3

    def test_invalid_dtype_raises_transformation_error(self):
        df = pd.DataFrame({"value": ["a", "b"]})
        with pytest.raises(TransformationError):
            TypeCastTransformer().transform(
                df, {"columns": {"value": "not_a_real_dtype"}}
            )


class TestFormulaTransformer:
    def test_create_simple_formula_column(self):
        df = pd.DataFrame({"price": [10.0, 20.0, 30.0], "quantity": [2, 3, 1]})
        result = FormulaTransformer().transform(
            df, {"columns": {"total": "price * quantity"}}
        )
        assert list(result["total"]) == [20.0, 60.0, 30.0]

    def test_create_multiple_formula_columns(self):
        df = pd.DataFrame({"price": [100.0, 200.0], "quantity": [2, 3]})
        result = FormulaTransformer().transform(
            df,
            {"columns": {"total": "price * quantity", "discounted": "price * 0.9"}},
        )
        assert result["total"].iloc[0] == 200.0
        assert result["discounted"].iloc[0] == 90.0

    def test_formula_with_functions(self):
        df = pd.DataFrame({"name": ["alice", "bob"]})
        result = FormulaTransformer().transform(
            df, {"columns": {"upper_name": "upper(name)"}}
        )
        assert result["upper_name"].iloc[0] == "ALICE"

    def test_formula_error_propagates(self):
        df = pd.DataFrame({"value": [1, 2, 3]})
        from rlam_airflow_framework.formula_engine import FormulaError

        with pytest.raises(FormulaError):
            FormulaTransformer().transform(
                df, {"columns": {"bad_col": "nonexistent_column * 2"}}
            )


class TestFilterTransformer:
    def test_simple_filter(self):
        df = pd.DataFrame({"value": [10, 20, 30, 40, 50]})
        result = FilterTransformer().transform(df, {"condition": "value > 25"})
        assert list(result["value"]) == [30, 40, 50]

    def test_filter_equality(self):
        df = pd.DataFrame({"status": ["A", "B", "A", "C"]})
        result = FilterTransformer().transform(df, {"condition": "status == 'A'"})
        assert len(result) == 2
        assert all(result["status"] == "A")

    def test_chained_filters_via_pipeline(self):
        df = pd.DataFrame(
            {"value": [10, 20, 30, 40, 50], "category": ["A", "B", "A", "B", "A"]}
        )
        result = apply_pipeline_transformations(
            df,
            [
                {"type": "filter", "condition": "value > 15"},
                {"type": "filter", "condition": "category == 'A'"},
            ],
        )
        assert list(result["value"]) == [30, 50]

    def test_no_condition_is_noop(self):
        df = pd.DataFrame({"value": [1, 2, 3]})
        result = FilterTransformer().transform(df, {})
        assert len(result) == 3

    def test_invalid_condition_raises_transformation_error(self):
        df = pd.DataFrame({"value": [1, 2, 3]})
        with pytest.raises(TransformationError):
            FilterTransformer().transform(df, {"condition": "nonexistent_col > 1"})


class TestAggregationTransformer:
    def test_sum_aggregation(self):
        df = pd.DataFrame({"value": [10, 20, 30, 40]})
        result = AggregationTransformer().transform(
            df, {"aggregations": {"sum": ["value"]}}
        )
        assert len(result) == 1
        assert result["value_sum"].iloc[0] == 100

    def test_multiple_aggregations(self):
        df = pd.DataFrame({"value": [10, 20, 30]})
        result = AggregationTransformer().transform(
            df,
            {
                "aggregations": {
                    "sum": ["value"],
                    "mean": ["value"],
                    "min": ["value"],
                    "max": ["value"],
                }
            },
        )
        assert result["value_sum"].iloc[0] == 60
        assert result["value_mean"].iloc[0] == 20.0
        assert result["value_min"].iloc[0] == 10
        assert result["value_max"].iloc[0] == 30

    def test_skip_nonexistent_column(self):
        df = pd.DataFrame({"value": [10, 20, 30]})
        result = AggregationTransformer().transform(
            df, {"aggregations": {"sum": ["value", "nonexistent"]}}
        )
        assert "value_sum" in result.columns
        assert "nonexistent_sum" not in result.columns

    def test_empty_dataframe_returned_unchanged(self):
        df = pd.DataFrame(columns=["value"])
        result = AggregationTransformer().transform(
            df, {"aggregations": {"sum": ["value"]}}
        )
        assert result.empty


class TestSnowflakeLookupTransformer:
    def _config(self, **overrides):
        config = {
            "table": "REFERENCE.INSTRUMENTS",
            "join_on": {"instrument_id": "INSTRUMENT_ID"},
            "select_columns": ["ISIN", "INSTRUMENT_NAME"],
        }
        config.update(overrides)
        return config

    @patch(_HOOK)
    def test_basic_lookup(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.get_pandas_df.return_value = pd.DataFrame(
            {
                "INSTRUMENT_ID": ["INS001", "INS002"],
                "ISIN": ["US0378331005", "US5949181045"],
                "INSTRUMENT_NAME": ["Apple Inc", "Microsoft Corp"],
            }
        )
        df = pd.DataFrame(
            {"instrument_id": ["INS001", "INS002", "INS001"], "quantity": [100, 200, 150]}
        )

        result = SnowflakeLookupTransformer().transform(df, self._config())

        mock_hook_class.assert_called_once_with(snowflake_conn_id="snowflake-default")
        assert "ISIN" in result.columns
        assert "INSTRUMENT_NAME" in result.columns
        assert len(result) == 3

    @patch(_HOOK)
    def test_custom_connection_id(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.get_pandas_df.return_value = pd.DataFrame(
            columns=["INSTRUMENT_ID", "ISIN", "INSTRUMENT_NAME"]
        )
        df = pd.DataFrame({"instrument_id": ["INS001"]})

        SnowflakeLookupTransformer().transform(
            df, self._config(connection_id="my-conn")
        )

        mock_hook_class.assert_called_once_with(snowflake_conn_id="my-conn")

    @patch(_HOOK)
    def test_no_matches_left_join_preserves_rows(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.get_pandas_df.return_value = pd.DataFrame(
            columns=["INSTRUMENT_ID", "ISIN"]
        )
        df = pd.DataFrame({"instrument_id": ["INS001", "INS002"], "quantity": [100, 200]})

        result = SnowflakeLookupTransformer().transform(
            df, self._config(select_columns=["ISIN"])
        )

        assert len(result) == 2
        assert pd.isna(result["ISIN"].iloc[0])

    @patch(_HOOK)
    def test_query_construction_uses_unique_escaped_values(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.get_pandas_df.return_value = pd.DataFrame(
            {"ID": ["A"], "NAME": ["Name A"]}
        )
        df = pd.DataFrame({"id": ["A", "A", "A"]})

        SnowflakeLookupTransformer().transform(
            df,
            {"table": "MY_SCHEMA.MY_TABLE", "join_on": {"id": "ID"}, "select_columns": ["NAME"]},
        )

        query = mock_hook.get_pandas_df.call_args[0][0]
        assert "MY_SCHEMA.MY_TABLE" in query
        assert query.count("'A'") == 1

    @patch(_HOOK)
    def test_preserves_original_columns(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.get_pandas_df.return_value = pd.DataFrame(
            {"ID": ["A"], "NEW_COL": ["new_value"]}
        )
        df = pd.DataFrame({"id": ["A"], "existing_col": ["val1"]})

        result = SnowflakeLookupTransformer().transform(
            df, {"table": "TABLE", "join_on": {"id": "ID"}, "select_columns": ["NEW_COL"]}
        )

        assert result["existing_col"].iloc[0] == "val1"
        assert result["NEW_COL"].iloc[0] == "new_value"

    @patch(_HOOK)
    def test_connection_error_wrapped_in_transformation_error(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.get_pandas_df.side_effect = Exception("Connection failed")
        df = pd.DataFrame({"id": ["A"]})

        with pytest.raises(TransformationError, match="Connection failed"):
            SnowflakeLookupTransformer().transform(
                df, {"table": "TABLE", "join_on": {"id": "ID"}, "select_columns": ["NAME"]}
            )

    def test_missing_required_key_raises_value_error(self):
        df = pd.DataFrame({"id": ["A"]})
        with pytest.raises(ValueError):
            SnowflakeLookupTransformer().transform(df, {"table": "TABLE"})

    def test_empty_dataframe_returns_copy(self):
        df = pd.DataFrame(columns=["id"])
        result = SnowflakeLookupTransformer().transform(df, self._config())
        assert result.empty


class TestApplyPipelineTransformations:
    def test_ordered_steps_execute_sequentially(self):
        df = pd.DataFrame({"price": ["10", "20", "30"], "quantity": [2, 3, 1]})
        result = apply_pipeline_transformations(
            df,
            [
                {"type": "type_cast", "columns": {"price": "float"}},
                {"type": "formula", "columns": {"total": "price * quantity"}},
                {"type": "filter", "condition": "total > 25"},
            ],
        )
        assert list(result["total"]) == [60.0, 30.0]

    def test_empty_list_returns_copy(self):
        df = pd.DataFrame({"value": [1, 2, 3]})
        result = apply_pipeline_transformations(df, [])
        pd.testing.assert_frame_equal(result, df)

    def test_empty_dataframe_short_circuits(self):
        df = pd.DataFrame(columns=["price", "quantity"])
        result = apply_pipeline_transformations(
            df, [{"type": "formula", "columns": {"total": "price * quantity"}}]
        )
        assert result.empty

    def test_original_dataframe_not_mutated(self):
        original_df = pd.DataFrame({"value": [1, 2, 3]})
        original_copy = original_df.copy()

        result = apply_pipeline_transformations(
            original_df,
            [
                {"type": "formula", "columns": {"doubled": "value * 2"}},
                {"type": "filter", "condition": "value > 1"},
            ],
        )

        pd.testing.assert_frame_equal(original_df, original_copy)
        assert "doubled" in result.columns
        assert len(result) == 2

    def test_unknown_type_raises_value_error(self):
        df = pd.DataFrame({"value": [1, 2, 3]})
        with pytest.raises(ValueError, match="Unsupported transformation type"):
            apply_pipeline_transformations(df, [{"type": "not_a_real_type"}])

    def test_get_transformer_unknown_type_raises(self):
        with pytest.raises(ValueError):
            get_transformer("not_a_real_type")

    def test_get_transformer_returns_correct_instance(self):
        assert isinstance(get_transformer("type_cast"), TypeCastTransformer)
        assert isinstance(get_transformer("formula"), FormulaTransformer)
        assert isinstance(get_transformer("filter"), FilterTransformer)
        assert isinstance(get_transformer("aggregation"), AggregationTransformer)
        assert isinstance(
            get_transformer("snowflake_lookup"), SnowflakeLookupTransformer
        )
