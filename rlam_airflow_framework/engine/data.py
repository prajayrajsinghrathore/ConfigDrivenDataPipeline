# File: rlam_airflow_framework/engine/data.py
"""
Data wrappers and backend definitions for the hybrid pipeline.
Ensures strict type boundaries and prevents invalid engine data passing.
"""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TypeAlias, Union, Any

import duckdb
import polars as pl
import pyarrow as pa


class DataBackend(StrEnum):
    DUCKDB = "duckdb"
    POLARS_LAZY = "polars_lazy"
    POLARS_EAGER = "polars_eager"
    ARROW = "arrow"


@dataclass(frozen=True)
class DuckDBData:
    value: duckdb.DuckDBPyRelation
    scratch: Any = field(default=None, compare=False, repr=False)

    @property
    def backend(self) -> DataBackend:
        return DataBackend.DUCKDB


@dataclass(frozen=True)
class PolarsLazyData:
    value: pl.LazyFrame

    @property
    def backend(self) -> DataBackend:
        return DataBackend.POLARS_LAZY


@dataclass(frozen=True)
class PolarsEagerData:
    value: pl.DataFrame

    @property
    def backend(self) -> DataBackend:
        return DataBackend.POLARS_EAGER


@dataclass(frozen=True)
class ArrowData:
    value: Union[pa.Table, pa.RecordBatchReader]

    @property
    def backend(self) -> DataBackend:
        return DataBackend.ARROW


ExecutionData: TypeAlias = DuckDBData | PolarsLazyData | PolarsEagerData | ArrowData
