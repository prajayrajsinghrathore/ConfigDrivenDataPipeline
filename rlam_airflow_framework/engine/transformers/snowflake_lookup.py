# File: rlam_airflow_framework/engine/transformers/snowflake_lookup.py
"""
Federated Snowflake Lookup transformer for DuckDB.
"""

from typing import cast
import duckdb
from pathlib import Path
import tempfile
import pyarrow.parquet as pq

from rlam_airflow_framework.engine.base import TransformationStep
from rlam_airflow_framework.engine.data import DataBackend, ExecutionData, DuckDBData
from rlam_airflow_framework.engine.config import SnowflakeLookupConfig
from rlam_airflow_framework.engine.context import ExecutionContext
from rlam_airflow_framework.engine.converters import materialize_eager

# Attempt to import SnowflakeHook, but we don't want to crash if airflow isn't installed
try:
    # Airflow 2/3 Provider
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
except ImportError:
    # Fallback or mock for testing
    class SnowflakeHook:
        def __init__(self, snowflake_conn_id):
            pass

        def get_conn(self):
            from unittest.mock import MagicMock

            return MagicMock()  # Used only in tests when patched


class SnowflakeLookupProvider:
    """Isolates the Snowflake connection and data fetching."""

    @staticmethod
    def get_lookup_dataset(
        config: SnowflakeLookupConfig, unique_keys: list, output_dir: Path
    ) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        hook = SnowflakeHook(snowflake_conn_id=config.connection_id)

        select_cols = list(config.join_on.values()) + config.select_columns
        select_clause = ", ".join(select_cols)
        remote_key = list(config.join_on.values())[0]

        def write_empty_schema_parquet():
            sql = f"SELECT {select_clause} FROM {config.table} LIMIT 0"
            with hook.get_conn() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(sql)
                    empty_table = cursor.fetch_arrow_all(force_microsecond_precision=True)
                    if empty_table is not None:
                        pq.write_table(empty_table, output_dir / "part-empty.parquet")

        if not unique_keys:
            write_empty_schema_parquet()
            return output_dir

        formatted_keys = [
            f"'{str(k).replace(chr(39), chr(39) + chr(39))}'" for k in unique_keys
        ]

        BATCH_SIZE = 10000
        try:
            with hook.get_conn() as connection:
                with connection.cursor() as cursor:
                    part_number = 0
                    for i in range(0, len(formatted_keys), BATCH_SIZE):
                        batch_keys = formatted_keys[i:i + BATCH_SIZE]
                        in_clause = ", ".join(batch_keys)
                        sql = f"SELECT {select_clause} FROM {config.table} WHERE {remote_key} IN ({in_clause})"
                        
                        cursor.execute(sql)
                        for table in cursor.fetch_arrow_batches(
                            force_microsecond_precision=True
                        ):
                            if table is None or table.num_rows == 0:
                                continue

                            path = output_dir / f"part-{part_number:05d}.parquet"
                            pq.write_table(table, path)
                            part_number += 1

            # If no parts were written (empty result), write an empty parquet to avoid read errors
            if part_number == 0:
                write_empty_schema_parquet()

            return output_dir
        except Exception as e:
            raise ValueError(f"Snowflake lookup failed: {str(e)}")


class DuckDBSnowflakeLookupStep(TransformationStep[SnowflakeLookupConfig]):
    """
    Performs a LEFT JOIN in DuckDB against a DataFrame fetched from Snowflake.
    """

    config_type = SnowflakeLookupConfig

    @property
    def accepted_backends(self) -> frozenset[DataBackend]:
        return frozenset([DataBackend.DUCKDB])

    def output_backend(
        self, input_backend: DataBackend, config: SnowflakeLookupConfig
    ) -> DataBackend:
        return DataBackend.DUCKDB

    def transform(
        self,
        data: ExecutionData,
        config: SnowflakeLookupConfig,
        context: ExecutionContext,
    ) -> ExecutionData:
        rel = cast(duckdb.DuckDBPyRelation, data.value)

        # Find unique keys to fetch
        local_key = list(config.join_on.keys())[0]
        remote_key = list(config.join_on.values())[0]

        if local_key not in rel.columns:
            raise ValueError(f"Required join key '{local_key}' not found in relation.")

        # Get unique keys to query from snowflake (fetching into memory to build the query)
        unique_keys_rel = rel.project(local_key).distinct()
        unique_keys_df = materialize_eager(unique_keys_rel)
        unique_keys = unique_keys_df[local_key].to_list()

        # Create a temporary directory that lives for the duration of this task
        temp_dir_obj = tempfile.TemporaryDirectory()
        temp_dir = Path(temp_dir_obj.name)

        # Fetch dataset from Snowflake directly into Parquet partitions
        lookup_dataset_path = SnowflakeLookupProvider.get_lookup_dataset(
            config, unique_keys, temp_dir
        )

        # Use DuckDB to natively read the partitioned parquet files
        # The glob pattern handles one or more parts written out
        parquet_glob = f"{lookup_dataset_path}/*.parquet"
        lookup_rel = duckdb.sql(f"SELECT * FROM read_parquet('{parquet_glob}')")

        # Perform LEFT JOIN in DuckDB using aliases to prevent ambiguous column names
        joined_rel = rel.set_alias("lhs").join(
            lookup_rel.set_alias("rhs"), f"lhs.{local_key} = rhs.{remote_key}", "left"
        )

        select_exprs = ["lhs.*"] + [f"rhs.{c}" for c in config.select_columns]
        final_rel = joined_rel.project(", ".join(select_exprs))

        # Tie the temporary directory's lifecycle to the ExecutionData so it doesn't get cleaned up
        # before the query executes, since DuckDB evaluation is lazy.
        return DuckDBData(final_rel, scratch=temp_dir_obj)
