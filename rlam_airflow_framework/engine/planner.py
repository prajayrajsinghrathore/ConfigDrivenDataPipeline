# File: rlam_airflow_framework/engine/planner.py
"""
Pipeline Planner responsible for validating configurations, resolving steps,
and building an optimized ExecutionPlan.
"""

from dataclasses import dataclass
from typing import Union, Any

from rlam_airflow_framework.engine.data import DataBackend
from rlam_airflow_framework.engine.base import TransformationStep, SourceSpec, DestinationSpec
from rlam_airflow_framework.engine.config import PipelineConfig, TransformationConfig
from rlam_airflow_framework.engine.converters import BackendConversionStep


@dataclass(frozen=True)
class PlannedStep:
    transformer: TransformationStep
    config: TransformationConfig


@dataclass(frozen=True)
class ExecutionPlan:
    steps: tuple[Union[PlannedStep, BackendConversionStep, Any], ...]

    def explain(self) -> str:
        lines = []
        stage_idx = 1
        
        from rlam_airflow_framework.engine.transformers.duckdb_stage import DuckDBStage
        
        for stage_idx, step in enumerate(self.steps, start=1):
            if isinstance(step, BackendConversionStep):
                lines.append("\nBoundary:")
                lines.append(f"  Target Backend: {step.target_backend.value}")
            elif isinstance(step, DuckDBStage):
                lines.append(f"\nStage {stage_idx} — DuckDB")
                for j, sub_step in enumerate(step.steps, start=1):
                    lines.append(f"  {j}. {sub_step.config.type}")
                stage_idx += 1
            elif isinstance(step, PlannedStep):
                lines.append(f"\nStage {stage_idx} — Generic Step")
                lines.append(f"  1. {step.config.type}")
        return "\n".join(lines)


class BackendSelectionPolicy:
    """Rules for resolving a config step to a backend."""
    
    @staticmethod
    def select_backend(step_config: TransformationConfig) -> DataBackend:
        if step_config.execution == "sql":
            return DataBackend.DUCKDB
        elif step_config.execution == "dataframe":
            if getattr(step_config, "requires_eager", False):
                return DataBackend.POLARS_EAGER
            return DataBackend.POLARS_LAZY
        return DataBackend.DUCKDB


class TransformerResolver:
    """Resolves a config type to its implementation based on backend."""
    
    @staticmethod
    def resolve(step_config: TransformationConfig, backend: DataBackend) -> TransformationStep:
        # Import dynamically to avoid circular imports during setup
        from rlam_airflow_framework.engine.transformers.formula import DuckDBFormulaStep, PolarsFormulaStep
        from rlam_airflow_framework.engine.transformers.filter import DuckDBFilterStep
        from rlam_airflow_framework.engine.transformers.type_cast import DuckDBTypeCastStep
        from rlam_airflow_framework.engine.transformers.aggregation import DuckDBAggregationStep
        from rlam_airflow_framework.engine.transformers.snowflake_lookup import DuckDBSnowflakeLookupStep
        
        if step_config.type == "formula":
            if backend == DataBackend.DUCKDB:
                return DuckDBFormulaStep()
            elif backend in (DataBackend.POLARS_LAZY, DataBackend.POLARS_EAGER):
                return PolarsFormulaStep()
                
        elif step_config.type == "filter":
            if backend == DataBackend.DUCKDB:
                return DuckDBFilterStep()

        elif step_config.type == "type_cast":
            if backend == DataBackend.DUCKDB:
                return DuckDBTypeCastStep()
                
        elif step_config.type == "aggregation":
            if backend == DataBackend.DUCKDB:
                return DuckDBAggregationStep()
                
        elif step_config.type == "snowflake_lookup":
            if backend == DataBackend.DUCKDB:
                return DuckDBSnowflakeLookupStep()
                
        raise ValueError(
            f"No transformer registered for type '{step_config.type}' on backend '{backend}'"
        )


class PipelinePlanner:
    """Constructs and validates the execution plan."""
    
    @staticmethod
    def create_plan(
        config: PipelineConfig,
        source: SourceSpec,
        destination: DestinationSpec,
    ) -> ExecutionPlan:
        # 1. Resolve steps and target backends
        resolved_steps = []
        for step_config in config.transformations:
            target_backend = BackendSelectionPolicy.select_backend(step_config)
            transformer = TransformerResolver.resolve(step_config, target_backend)
            resolved_steps.append((target_backend, PlannedStep(transformer, step_config)))
            
        # 2. Group adjacent DuckDB steps into Stages and insert explicit boundaries
        from rlam_airflow_framework.engine.transformers.duckdb_stage import DuckDBStage
        
        final_plan = []
        current_duckdb_steps = []
        current_backend = DataBackend.DUCKDB # Initial default from parquet scan
        
        for target_backend, step in resolved_steps:
            if target_backend == DataBackend.DUCKDB:
                if current_backend != DataBackend.DUCKDB:
                    # Flush DuckDB steps if there are any (unlikely case since we're switching back to DuckDB)
                    if current_duckdb_steps:
                        final_plan.append(DuckDBStage(tuple(current_duckdb_steps)))
                        current_duckdb_steps = []
                    # Insert explicit boundary back to DuckDB
                    final_plan.append(BackendConversionStep(DataBackend.DUCKDB))
                    current_backend = DataBackend.DUCKDB
                
                # Coalesce DuckDB step
                current_duckdb_steps.append(step)
            
            else:
                # If we are leaving DuckDB, flush the DuckDB stage
                if current_duckdb_steps:
                    final_plan.append(DuckDBStage(tuple(current_duckdb_steps)))
                    current_duckdb_steps = []
                
                # Check if we need a boundary transition
                if current_backend != target_backend:
                    final_plan.append(BackendConversionStep(target_backend))
                    current_backend = target_backend
                    
                final_plan.append(step)
                
        # Flush trailing DuckDB steps
        if current_duckdb_steps:
            final_plan.append(DuckDBStage(tuple(current_duckdb_steps)))
            
        return ExecutionPlan(tuple(final_plan))

