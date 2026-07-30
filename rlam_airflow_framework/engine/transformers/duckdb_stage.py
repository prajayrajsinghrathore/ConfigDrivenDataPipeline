# File: rlam_airflow_framework/engine/transformers/duckdb_stage.py
"""
Coalesces DuckDB relational steps to avoid unnecessary materialization.
DuckDB's Relational API is fully lazy, so chaining projects and filters builds
a single optimized query plan.
"""

from dataclasses import dataclass
from typing import Tuple, Any, cast
import duckdb

from rlam_airflow_framework.engine.data import DuckDBData, ExecutionData, DataBackend
from rlam_airflow_framework.engine.context import ExecutionContext


# We use forward references or typing imports if needed
# but since this runs at execution time, we can assume steps are PlannedStep
@dataclass(frozen=True)
class DuckDBStage:
    steps: Tuple[Any, ...]  # Tuple[PlannedStep, ...]

    def transform(
        self, data: ExecutionData, context: ExecutionContext
    ) -> ExecutionData:
        if data.backend != DataBackend.DUCKDB:
            raise ValueError(f"DuckDBStage requires DuckDBData, got {data.backend}")

        relation = data.value

        context.logger.info("Executing DuckDB Stage", step_count=len(self.steps))

        for step in self.steps:
            # step is a PlannedStep containing a DuckDB-specific TransformationStep
            context.logger.debug(
                "Applying DuckDB Relational Step", type=step.config.type
            )
            # We unwrap data to DuckDBData inside the step for type-safety
            duck_data = DuckDBData(cast(duckdb.DuckDBPyRelation, relation))
            result = step.transformer.transform(duck_data, step.config, context)
            relation = cast(duckdb.DuckDBPyRelation, result.value)

        return DuckDBData(cast(duckdb.DuckDBPyRelation, relation))
