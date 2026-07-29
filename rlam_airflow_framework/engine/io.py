# File: rlam_airflow_framework/engine/io.py
"""
Data sources and sinks for the hybrid engine.
"""

from typing import Any, Mapping, cast
import duckdb

from rlam_airflow_framework.engine.base import DataSource, DataSink, SourceSpec, DestinationSpec, WriteResult
from rlam_airflow_framework.engine.data import ExecutionData, DuckDBData, DataBackend
from rlam_airflow_framework.engine.context import ExecutionContext


class ParquetDataSource(DataSource):
    """Loads Parquet files natively into DuckDB."""
    
    def load(self, source: SourceSpec, context: ExecutionContext) -> ExecutionData:
        if source.format != "parquet":
            raise ValueError(f"ParquetDataSource cannot load format: {source.format}")
            
        context.logger.info("Loading Parquet source", path=source.path)
        rel = duckdb.read_parquet(source.path)
        return DuckDBData(rel)


class ParquetDataSink(DataSink):
    """Sinks ExecutionData to Parquet."""
    
    def save(
        self,
        data: ExecutionData,
        destination: DestinationSpec,
        context: ExecutionContext,
    ) -> WriteResult:
        
        context.logger.info("Saving ExecutionData to Parquet", destination=destination.path, backend=data.backend)
        
        if data.backend == DataBackend.DUCKDB:
            rel = cast(duckdb.DuckDBPyRelation, data.value)
            # Execute and write out-of-core
            rel.write_parquet(destination.path)
            
            # Since DuckDB write_parquet doesn't return row counts easily without a subquery, 
            # we provide basic metadata.
            return WriteResult(
                destination=destination.path,
                row_count=None,
                byte_count=None,
                schema_fingerprint=None,
                metadata={"backend": "duckdb"}
            )
            
        elif data.backend in (DataBackend.POLARS_LAZY, DataBackend.POLARS_EAGER):
            import polars as pl
            df_or_lf = data.value
            
            if data.backend == DataBackend.POLARS_LAZY:
                lf = cast(pl.LazyFrame, df_or_lf)
                # collect and write, or sink_parquet
                lf.sink_parquet(destination.path)
            else:
                df = cast(pl.DataFrame, df_or_lf)
                df.write_parquet(destination.path)
                
            return WriteResult(
                destination=destination.path,
                row_count=None,
                byte_count=None,
                schema_fingerprint=None,
                metadata={"backend": "polars"}
            )
            
        raise NotImplementedError(f"Cannot save backend {data.backend} to Parquet directly yet.")
