# File: rlam_airflow_framework/engine/transformers/type_cast.py
"""
Type cast transformer for DuckDB.
"""

from typing import cast
import duckdb

from rlam_airflow_framework.engine.base import TransformationStep
from rlam_airflow_framework.engine.data import DataBackend, ExecutionData, DuckDBData
from rlam_airflow_framework.engine.config import TypeCastStepConfig
from rlam_airflow_framework.engine.context import ExecutionContext


class DuckDBTypeCastStep(TransformationStep[TypeCastStepConfig]):
    config_type = TypeCastStepConfig
    
    @property
    def accepted_backends(self) -> frozenset[DataBackend]:
        return frozenset([DataBackend.DUCKDB])
        
    def output_backend(self, input_backend: DataBackend, config: TypeCastStepConfig) -> DataBackend:
        return DataBackend.DUCKDB
        
    def transform(self, data: ExecutionData, config: TypeCastStepConfig, context: ExecutionContext) -> ExecutionData:
        rel = cast(duckdb.DuckDBPyRelation, data.value)
        
        # DuckDB type mapping
        type_mapping = {
            "datetime": "TIMESTAMP",
            "float": "DOUBLE",
            "int": "BIGINT",
            "string": "VARCHAR"
        }
        
        select_exprs = []
        for col in rel.columns:
            if col in config.columns:
                target_type = config.columns[col]
                duck_type = type_mapping.get(target_type)
                if not duck_type:
                    raise ValueError(f"Unsupported type cast target: {target_type}")
                select_exprs.append(f"CAST({col} AS {duck_type}) AS {col}")
            else:
                select_exprs.append(col)
                
        new_rel = rel.project(", ".join(select_exprs))
        return DuckDBData(new_rel)
