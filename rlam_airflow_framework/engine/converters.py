# File: rlam_airflow_framework/engine/converters.py
"""
Explicit data boundaries for transitioning between engine backends.
Ensures tracking, execution overhead, and type-safety are explicitly managed.
"""

from rlam_airflow_framework.engine.data import (
    DataBackend,
    ExecutionData,
    DuckDBData,
    PolarsLazyData,
    PolarsEagerData,
)
from rlam_airflow_framework.engine.context import ExecutionContext

import duckdb
import polars as pl
import structlog
from typing import cast

log = structlog.get_logger(__name__)

# Default ceiling for accidental full-dataset eager materialization.
# Callers with a legitimate need for larger eager pulls (e.g. lookups already
# bounded by a distinct key set) should pass an explicit max_rows override.
DEFAULT_MAX_EAGER_ROWS = 5_000_000


class EagerMaterializationLimitError(RuntimeError):
    """Raised when materializing a DuckDB relation eagerly would exceed the safe row limit."""


def materialize_eager(
    rel: duckdb.DuckDBPyRelation, max_rows: int = DEFAULT_MAX_EAGER_ROWS
) -> pl.DataFrame:
    """Safely pull a DuckDB relation into memory as an eager Polars DataFrame.

    Checks the relation's row count before materializing so an accidental full-table
    `.pl()` call fails loudly instead of silently ballooning memory. This is the only
    sanctioned way to get an eager Polars DataFrame out of DuckDB relation.
    """
    row_count = rel.shape[0]
    if row_count > max_rows:
        raise EagerMaterializationLimitError(
            f"Refusing to eagerly materialize {row_count} rows (limit is {max_rows}). "
            "Use lazy/out-of-core execution instead, or pass an explicit max_rows "
            "override if this materialization is intentional."
        )
    return cast(pl.DataFrame, rel.pl())


class BackendConverter:
    """Handles explicit transition of execution boundaries."""

    @staticmethod
    def convert(
        data: ExecutionData, target_backend: DataBackend, context: ExecutionContext
    ) -> ExecutionData:
        """Converts from one DataBackend to another."""
        source = data.backend

        if source == target_backend:
            return data

        context.logger.info(
            "Engine conversion boundary crossed",
            source_backend=source,
            target_backend=target_backend,
        )

        if source == DataBackend.DUCKDB and target_backend == DataBackend.POLARS_LAZY:
            # DuckDB -> Polars Lazy (Warning: this evaluates out-of-core but fetches into memory if eager is invoked downstream)
            rel = cast(duckdb.DuckDBPyRelation, data.value)
            lf = cast(pl.LazyFrame, rel.pl(lazy=True))  # type: ignore
            return PolarsLazyData(lf)

        elif (
            source == DataBackend.DUCKDB and target_backend == DataBackend.POLARS_EAGER
        ):
            # DuckDB -> Polars Eager (Immediate execution and materialization)
            rel = cast(duckdb.DuckDBPyRelation, data.value)
            df = materialize_eager(rel)
            return PolarsEagerData(df)

        elif source == DataBackend.POLARS_LAZY and target_backend == DataBackend.DUCKDB:
            # Polars Lazy -> DuckDB
            # DuckDB can query Polars Dataframes/Lazyframes natively, but we need to register it
            # or pass it as an argument. DuckDB auto-registers local variables, but explicitly
            # doing `duckdb.sql("SELECT * FROM df")` is safer.
            lf = cast(pl.LazyFrame, data.value)
            # We must use duckdb connection or global namespace to query
            rel = duckdb.sql("SELECT * FROM lf")
            return DuckDBData(rel)

        elif (
            source == DataBackend.POLARS_EAGER and target_backend == DataBackend.DUCKDB
        ):
            df = cast(pl.DataFrame, data.value)
            rel = duckdb.sql("SELECT * FROM df")
            return DuckDBData(rel)

        raise NotImplementedError(
            f"Conversion from {source} to {target_backend} is not implemented."
        )


class BackendConversionStep:
    """An explicit planned node for logging and conversion logic."""

    def __init__(self, target_backend: DataBackend):
        self.target_backend = target_backend

    def transform(
        self, data: ExecutionData, context: ExecutionContext
    ) -> ExecutionData:
        return BackendConverter.convert(data, self.target_backend, context)
