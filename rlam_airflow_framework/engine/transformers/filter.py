# File: rlam_airflow_framework/engine/transformers/filter.py
"""
Filter transformer for DuckDB.
"""

from typing import cast
import duckdb

from rlam_airflow_framework.engine.base import TransformationStep
from rlam_airflow_framework.engine.data import DataBackend, ExecutionData, DuckDBData
from rlam_airflow_framework.engine.config import FilterStepConfig
from rlam_airflow_framework.engine.context import ExecutionContext


class DuckDBFilterStep(TransformationStep[FilterStepConfig]):
    config_type = FilterStepConfig
    
    @property
    def accepted_backends(self) -> frozenset[DataBackend]:
        return frozenset([DataBackend.DUCKDB])
        
    def output_backend(self, input_backend: DataBackend, config: FilterStepConfig) -> DataBackend:
        return DataBackend.DUCKDB
        
    def transform(self, data: ExecutionData, config: FilterStepConfig, context: ExecutionContext) -> ExecutionData:
        rel = cast(duckdb.DuckDBPyRelation, data.value)
        
        # Apply the SQL filter condition
        new_rel = rel.filter(config.condition)
        return DuckDBData(new_rel)
