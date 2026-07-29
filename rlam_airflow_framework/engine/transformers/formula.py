# File: rlam_airflow_framework/engine/transformers/formula.py
"""
Formula transformers for DuckDB and Polars.
"""

from typing import cast, Any
import duckdb
import polars as pl

from rlam_airflow_framework.engine.base import TransformationStep
from rlam_airflow_framework.engine.data import DataBackend, ExecutionData, DuckDBData
from rlam_airflow_framework.engine.config import FormulaStepConfig
from rlam_airflow_framework.engine.context import ExecutionContext


class DuckDBFormulaStep(TransformationStep[FormulaStepConfig]):
    config_type = FormulaStepConfig
    
    @property
    def accepted_backends(self) -> frozenset[DataBackend]:
        return frozenset([DataBackend.DUCKDB])
        
    def output_backend(self, input_backend: DataBackend, config: FormulaStepConfig) -> DataBackend:
        return DataBackend.DUCKDB
        
    def transform(self, data: ExecutionData, config: FormulaStepConfig, context: ExecutionContext) -> ExecutionData:
        rel = cast(duckdb.DuckDBPyRelation, data.value)
        
        # Build a projection list: all existing columns + new formula columns
        select_exprs = ["*"]
        for col_name, formula in config.columns.items():
            select_exprs.append(f"({formula}) AS {col_name}")
            
        new_rel = rel.project(", ".join(select_exprs))
        return DuckDBData(new_rel)


class PolarsFormulaStep(TransformationStep[FormulaStepConfig]):
    """
    Experimental: evaluates SQL formulas inside Polars using sql_expr.
    This is generally discouraged compared to DuckDB SQL dialect, but 
    included if the user sets `execution: dataframe` for a formula step.
    """
    config_type = FormulaStepConfig
    
    @property
    def accepted_backends(self) -> frozenset[DataBackend]:
        return frozenset([DataBackend.POLARS_LAZY, DataBackend.POLARS_EAGER])
        
    def output_backend(self, input_backend: DataBackend, config: FormulaStepConfig) -> DataBackend:
        return input_backend
        
    def transform(self, data: ExecutionData, config: FormulaStepConfig, context: ExecutionContext) -> ExecutionData:
        lf = cast(Any, data.value)
        
        exprs = []
        for col_name, formula in config.columns.items():
            # pl.sql_expr requires Polars >= 0.20+
            exprs.append(pl.sql_expr(formula).alias(col_name))
            
        new_lf = lf.with_columns(exprs)
        
        if data.backend == DataBackend.POLARS_LAZY:
            from rlam_airflow_framework.engine.data import PolarsLazyData
            return PolarsLazyData(cast(pl.LazyFrame, new_lf))
        else:
            from rlam_airflow_framework.engine.data import PolarsEagerData
            return PolarsEagerData(cast(pl.DataFrame, new_lf))
