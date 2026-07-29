# File: tests/unit/test_engine.py
"""
Unit tests for the new Engine architecture (Pipeline Planner, DuckDB Transformers).
Migrated from the legacy pandas-based test_transformers.py.
"""

import polars as pl
import pytest
from unittest.mock import patch, MagicMock
from typing import cast, Any

import duckdb
from pydantic import ValidationError

from rlam_airflow_framework.engine.data import DataBackend, ExecutionData, DuckDBData
from rlam_airflow_framework.engine.context import ExecutionContext
from rlam_airflow_framework.engine.config import (
    TypeCastStepConfig, FormulaStepConfig, FilterStepConfig, AggregationStepConfig, SnowflakeLookupConfig
)
from rlam_airflow_framework.engine.transformers.type_cast import DuckDBTypeCastStep
from rlam_airflow_framework.engine.transformers.formula import DuckDBFormulaStep
from rlam_airflow_framework.engine.transformers.filter import DuckDBFilterStep
from rlam_airflow_framework.engine.transformers.aggregation import DuckDBAggregationStep
from rlam_airflow_framework.engine.transformers.snowflake_lookup import DuckDBSnowflakeLookupStep


def _dummy_context():
    return ExecutionContext(
        correlation_id="test",
        pipeline_id="test_dag",
        task_id="test_task",
        attempt_number=1,
        logger=MagicMock()
    )


def _apply_step(transformer, df: pl.DataFrame, config) -> pl.DataFrame:
    ctx = _dummy_context()
    rel = duckdb.sql("SELECT * FROM df")
    data = DuckDBData(rel)
    result_data = transformer.transform(data, config, ctx)
    return result_data.value.pl()


class TestDuckDBTypeCastStep:
    def test_convert_to_datetime(self):
        df = pl.DataFrame({"date_str": ["2026-01-01", "2026-02-15", "2026-03-20"]})
        config = TypeCastStepConfig(type="type_cast", columns={"date_str": "datetime"})
        result = _apply_step(DuckDBTypeCastStep(), df, config)
        assert result["date_str"].dtype in [pl.Date, pl.Datetime]
        assert result["date_str"][0].year == 2026

    def test_convert_to_float(self):
        df = pl.DataFrame({"value": ["10", "20", "30"]})
        config = TypeCastStepConfig(type="type_cast", columns={"value": "float"})
        result = _apply_step(DuckDBTypeCastStep(), df, config)
        assert result["value"].dtype.is_numeric()
        assert result["value"][0] == 10.0

    def test_convert_to_int(self):
        df = pl.DataFrame({"count": [1.0, 2.0, 3.0]})
        config = TypeCastStepConfig(type="type_cast", columns={"count": "int"})
        result = _apply_step(DuckDBTypeCastStep(), df, config)
        assert result["count"].dtype.is_integer()

    def test_convert_to_string(self):
        df = pl.DataFrame({"id": [1, 2, 3]})
        config = TypeCastStepConfig(type="type_cast", columns={"id": "string"})
        result = _apply_step(DuckDBTypeCastStep(), df, config)
        assert str(result["id"][0]) == "1"

    def test_skip_nonexistent_column(self):
        df = pl.DataFrame({"value": [1, 2, 3]})
        config = TypeCastStepConfig(type="type_cast", columns={"nonexistent": "float"})
        result = _apply_step(DuckDBTypeCastStep(), df, config)
        assert len(result) == 3

    def test_invalid_dtype_raises_transformation_error(self):
        df = pl.DataFrame({"value": ["a", "b"]})
        config = TypeCastStepConfig(type="type_cast", columns={"value": "not_a_real_dtype"})
        with pytest.raises(ValueError):
            _apply_step(DuckDBTypeCastStep(), df, config)


class TestDuckDBFormulaStep:
    def test_create_simple_formula_column(self):
        df = pl.DataFrame({"price": [10.0, 20.0, 30.0], "quantity": [2, 3, 1]})
        config = FormulaStepConfig(type="formula", columns={"total": "price * quantity"})
        result = _apply_step(DuckDBFormulaStep(), df, config)
        assert list(result["total"]) == [20.0, 60.0, 30.0]

    def test_create_multiple_formula_columns(self):
        df = pl.DataFrame({"price": [100.0, 200.0], "quantity": [2, 3]})
        config = FormulaStepConfig(
            type="formula", columns={"total": "price * quantity", "discounted": "price * 0.9"}
        )
        result = _apply_step(DuckDBFormulaStep(), df, config)
        assert result["total"][0] == 200.0
        assert result["discounted"][0] == 90.0

    def test_formula_with_functions(self):
        df = pl.DataFrame({"name": ["alice", "bob"]})
        config = FormulaStepConfig(type="formula", columns={"upper_name": "upper(name)"})
        result = _apply_step(DuckDBFormulaStep(), df, config)
        assert result["upper_name"][0] == "ALICE"

    def test_formula_error_propagates(self):
        df = pl.DataFrame({"value": [1, 2, 3]})
        config = FormulaStepConfig(type="formula", columns={"bad_col": "nonexistent_column * 2"})
        with pytest.raises(duckdb.BinderException):
            _apply_step(DuckDBFormulaStep(), df, config)


class TestDuckDBFilterStep:
    def test_simple_filter(self):
        df = pl.DataFrame({"value": [10, 20, 30, 40, 50]})
        config = FilterStepConfig(type="filter", condition="value > 25")
        result = _apply_step(DuckDBFilterStep(), df, config)
        assert list(result["value"]) == [30, 40, 50]

    def test_filter_equality(self):
        df = pl.DataFrame({"status": ["A", "B", "A", "C"]})
        config = FilterStepConfig(type="filter", condition="status = 'A'")
        result = _apply_step(DuckDBFilterStep(), df, config)
        assert len(result) == 2
        assert all(val == "A" for val in result["status"])

    def test_chained_filters_via_pipeline(self):
        from rlam_airflow_framework.engine.planner import PipelinePlanner
        from rlam_airflow_framework.engine.config import PipelineConfig
        from rlam_airflow_framework.engine.io import ParquetDataSource
        from rlam_airflow_framework.engine.base import SourceSpec, DestinationSpec
        
        df = pl.DataFrame({"value": [10, 20, 30, 40, 50], "category": ["A", "B", "A", "B", "A"]})
        rel = duckdb.sql("SELECT * FROM df")
        data = DuckDBData(rel)
        
        config = PipelineConfig.model_validate({
            "transformations": [
                {"type": "filter", "condition": "value > 15"},
                {"type": "filter", "condition": "category = 'A'"},
            ]
        })
        plan = PipelinePlanner.create_plan(config, SourceSpec(path="dummy"), DestinationSpec(path="dummy"))
        ctx = _dummy_context()
        
        from rlam_airflow_framework.engine.transformers.duckdb_stage import DuckDBStage
        from rlam_airflow_framework.engine.planner import PlannedStep
        
        for planned_step in plan.steps:
            if isinstance(planned_step, DuckDBStage):
                for sub_step in planned_step.steps:
                    data = sub_step.transformer.transform(data, sub_step.config, ctx)
            elif isinstance(planned_step, PlannedStep):
                data = planned_step.transformer.transform(data, planned_step.config, ctx)
            
        rel = cast(Any, data.value)
        result = rel.pl()
        assert list(result["value"]) == [30, 50]

    def test_no_condition_is_noop(self):
        df = pl.DataFrame({"value": [1, 2, 3]})
        config = FilterStepConfig(type="filter", condition="true")
        result = _apply_step(DuckDBFilterStep(), df, config)
        assert len(result) == 3

    def test_invalid_condition_raises_transformation_error(self):
        df = pl.DataFrame({"value": [1, 2, 3]})
        config = FilterStepConfig(type="filter", condition="nonexistent_col > 1")
        with pytest.raises(duckdb.BinderException):
            _apply_step(DuckDBFilterStep(), df, config)


class TestDuckDBAggregationStep:
    def test_sum_aggregation(self):
        df = pl.DataFrame({"value": [10, 20, 30, 40]})
        config = AggregationStepConfig(type="aggregation", aggregations={"sum": ["value"]})
        result = _apply_step(DuckDBAggregationStep(), df, config)
        assert len(result) == 1
        assert result["value_sum"][0] == 100

    def test_multiple_aggregations(self):
        df = pl.DataFrame({"value": [10, 20, 30]})
        config = AggregationStepConfig(
            type="aggregation",
            aggregations={"sum": ["value"], "mean": ["value"], "min": ["value"], "max": ["value"]}
        )
        result = _apply_step(DuckDBAggregationStep(), df, config)
        assert result["value_sum"][0] == 60
        assert result["value_mean"][0] == 20.0
        assert result["value_min"][0] == 10
        assert result["value_max"][0] == 30

    def test_skip_nonexistent_column(self):
        df = pl.DataFrame({"value": [10, 20, 30]})
        config = AggregationStepConfig(type="aggregation", aggregations={"sum": ["value", "nonexistent"]})
        result = _apply_step(DuckDBAggregationStep(), df, config)
        assert "value_sum" in result.columns
        assert "nonexistent_sum" not in result.columns

    def test_empty_dataframe_returned_unchanged(self):
        df = pl.DataFrame(schema={"value": pl.Int64})
        config = AggregationStepConfig(type="aggregation", aggregations={"sum": ["value"]})
        result = _apply_step(DuckDBAggregationStep(), df, config)
        assert result.is_empty()


class TestDuckDBSnowflakeLookupStep:
    def _config(self, **overrides):
        config_dict = {
            "type": "snowflake_lookup",
            "table": "REFERENCE.INSTRUMENTS",
            "join_on": {"instrument_id": "INSTRUMENT_ID"},
            "select_columns": ["ISIN", "INSTRUMENT_NAME"],
        }
        config_dict.update(overrides)
        return SnowflakeLookupConfig.model_validate(config_dict)

    @patch("rlam_airflow_framework.engine.transformers.snowflake_lookup.SnowflakeLookupProvider.get_lookup_dataset")
    def test_basic_lookup(self, mock_get):
        def _mock_dataset(config, keys, temp_dir):
            import pyarrow as pa
            import pyarrow.parquet as pq
            table = pa.Table.from_pydict({
                "INSTRUMENT_ID": ["INS001", "INS002"],
                "ISIN": ["US0378331005", "US5949181045"],
                "INSTRUMENT_NAME": ["Apple Inc", "Microsoft Corp"],
            })
            pq.write_table(table, temp_dir / "part-0.parquet")
            return temp_dir
        mock_get.side_effect = _mock_dataset
        
        df = pl.DataFrame({"instrument_id": ["INS001", "INS002", "INS001"], "quantity": [100, 200, 150]})
        result = _apply_step(DuckDBSnowflakeLookupStep(), df, self._config())
        assert "ISIN" in result.columns
        assert "INSTRUMENT_NAME" in result.columns
        assert len(result) == 3

    @patch("rlam_airflow_framework.engine.transformers.snowflake_lookup.SnowflakeLookupProvider.get_lookup_dataset")
    def test_no_matches_left_join_preserves_rows(self, mock_get):
        def _mock_dataset(config, keys, temp_dir):
            import pyarrow as pa
            import pyarrow.parquet as pq
            schema = pa.schema([("INSTRUMENT_ID", pa.string()), ("ISIN", pa.string())])
            table = pa.Table.from_batches([], schema=schema)
            pq.write_table(table, temp_dir / "part-0.parquet")
            return temp_dir
        mock_get.side_effect = _mock_dataset
        
        df = pl.DataFrame({"instrument_id": ["INS001", "INS002"], "quantity": [100, 200]})
        result = _apply_step(DuckDBSnowflakeLookupStep(), df, self._config(select_columns=["ISIN"]))
        assert len(result) == 2
        assert result["ISIN"][0] is None

    @patch("rlam_airflow_framework.engine.transformers.snowflake_lookup.SnowflakeLookupProvider.get_lookup_dataset")
    def test_empty_dataframe_returns_copy(self, mock_get):
        def _mock_dataset(config, keys, temp_dir):
            import pyarrow as pa
            import pyarrow.parquet as pq
            schema = pa.schema([("INSTRUMENT_ID", pa.string()), ("ISIN", pa.string()), ("INSTRUMENT_NAME", pa.string())])
            table = pa.Table.from_batches([], schema=schema)
            pq.write_table(table, temp_dir / "part-0.parquet")
            return temp_dir
        mock_get.side_effect = _mock_dataset
        
        df = pl.DataFrame(schema={"instrument_id": pl.Utf8, "quantity": pl.Int64})
        result = _apply_step(DuckDBSnowflakeLookupStep(), df, self._config())
        assert len(result) == 0
        assert "ISIN" in result.columns
