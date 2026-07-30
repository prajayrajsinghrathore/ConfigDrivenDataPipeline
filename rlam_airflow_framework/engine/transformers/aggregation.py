# File: rlam_airflow_framework/engine/transformers/aggregation.py
"""
Aggregation transformer for DuckDB.
"""

from typing import cast
import duckdb

from rlam_airflow_framework.engine.base import TransformationStep
from rlam_airflow_framework.engine.data import DataBackend, ExecutionData, DuckDBData
from rlam_airflow_framework.engine.config import AggregationStepConfig
from rlam_airflow_framework.engine.context import ExecutionContext


class DuckDBAggregationStep(TransformationStep[AggregationStepConfig]):
    config_type = AggregationStepConfig

    @property
    def accepted_backends(self) -> frozenset[DataBackend]:
        return frozenset([DataBackend.DUCKDB])

    def output_backend(
        self, input_backend: DataBackend, config: AggregationStepConfig
    ) -> DataBackend:
        return DataBackend.DUCKDB

    def transform(
        self,
        data: ExecutionData,
        config: AggregationStepConfig,
        context: ExecutionContext,
    ) -> ExecutionData:
        rel = cast(duckdb.DuckDBPyRelation, data.value)

        # DuckDB aggregation mapping
        # Example: {"sum": ["value"], "mean": ["value"]} -> SUM(value) AS value_sum, AVG(value) AS value_mean

        func_mapping = {
            "sum": "SUM",
            "mean": "AVG",
            "min": "MIN",
            "max": "MAX",
            "count": "COUNT",
        }

        agg_exprs = []
        for agg_func, columns in config.aggregations.items():
            duck_func = func_mapping.get(agg_func)
            if not duck_func:
                raise ValueError(f"Unsupported aggregation function: {agg_func}")

            for col in columns:
                if col in rel.columns:
                    agg_exprs.append(f"{duck_func}({col}) AS {col}_{agg_func}")

        if not agg_exprs:
            agg_exprs.append("COUNT(*) AS _count")

        # If empty, return unchanged
        if len(rel.limit(1).fetchall()) == 0:
            return DuckDBData(rel)

        new_rel = rel.aggregate(", ".join(agg_exprs))

        # Drop _count if it was injected
        if "_count" in new_rel.columns and len(new_rel.columns) == 1:
            pass

        return DuckDBData(new_rel)
