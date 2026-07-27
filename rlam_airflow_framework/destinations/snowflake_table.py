# File: rlam_airflow_framework/destinations/snowflake_table.py
"""Snowflake table loader (parameterized, transactional insert)."""

import time

import pandas as pd
import structlog
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from tenacity import RetryError

from rlam_airflow_framework.validation import validate_identifier
from rlam_airflow_framework.destinations.base import DestinationLoader
from rlam_airflow_framework.destinations.primitives import (
    DataLoadError,
    DEFAULT_BATCH_SIZE,
    DEFAULT_RETRY_ATTEMPTS,
    is_transient_snowflake_error,
    snowflake_retry,
)

_log = structlog.get_logger(__name__)


class SnowflakeTableLoader(DestinationLoader):
    """
    Load into a Snowflake table with a parameterized, transactional insert.

    All row values and the partition key are bound as query parameters (never
    interpolated into SQL text). The whole DML runs on a single connection with
    an explicit transaction; connection-level errors are retried, and a
    SQLAlchemy fallback covers non-transient primary-path failures.
    """

    dest_type = "snowflake_table"

    def _write(self, df, dest_config, ctx):
        parts = str(dest_config["table"]).split(".")
        table_name = validate_identifier(parts[-1], "table name")
        schema = validate_identifier(
            parts[-2] if len(parts) > 1 else "PUBLIC", "schema"
        )
        snowflake_conn_id = dest_config.get("connection_id", "snowflake-default")
        if_exists = dest_config.get("mode", "append")
        batch_size = DEFAULT_BATCH_SIZE
        partition_column = ctx.partition_column
        partition_value = ctx.partition_value
        logger = _log.bind(trace_id=ctx.correlation_id)

        # Validate columns (interpolated as quoted identifiers below)
        columns = df.columns.tolist()
        for col in columns:
            validate_identifier(col, f"column '{col}'")

        if partition_column:
            partition_column = validate_identifier(partition_column, "partition column")

        full_table_name = f'"{schema}"."{table_name}"'

        logger.info(
            f"Starting Snowflake load: table={full_table_name}, "
            f"rows={len(df)}, if_exists={if_exists}"
        )

        @snowflake_retry
        def _execute_transactional_load():
            """Run the whole DML transaction on a single connection/cursor."""
            hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
            conn = None
            cur = None
            transaction_started = False

            try:
                start_time = time.time()

                conn = hook.get_conn()
                cur = conn.cursor()

                # DDL (auto-commit in Snowflake, run outside the transaction)
                if if_exists == "replace" and not partition_column:
                    logger.info(f"Dropping existing table {full_table_name}")
                    cur.execute(f"DROP TABLE IF EXISTS {full_table_name}")

                quoted_columns = ", ".join([f'"{col}" VARCHAR' for col in columns])
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {full_table_name} ({quoted_columns})"
                )
                logger.debug(f"Table created/verified: {full_table_name}")

                conn.autocommit(False)
                transaction_started = True
                logger.debug("Transaction started")

                # partition_value bound as a parameter, never interpolated
                if if_exists == "replace" and partition_column and partition_value:
                    delete_sql = (
                        f'DELETE FROM {full_table_name} WHERE "{partition_column}" = %s'
                    )
                    cur.execute(delete_sql, (partition_value,))
                    logger.info(
                        f"Deleted partition data from {full_table_name} "
                        f"for partition {partition_column}={partition_value}"
                    )

                # Values bound via executemany (driver chunks binds safely)
                quoted_col_names = ", ".join([f'"{col}"' for col in columns])
                placeholders = ", ".join(["%s"] * len(columns))
                insert_sql = (
                    f"INSERT INTO {full_table_name} ({quoted_col_names}) "
                    f"VALUES ({placeholders})"
                )

                total_rows = len(df)
                batches_processed = 0

                for i in range(0, total_rows, batch_size):
                    batch_df = df.iloc[i : i + batch_size]
                    rows = [
                        tuple(None if pd.isna(val) else val for val in row)
                        for row in batch_df.itertuples(index=False, name=None)
                    ]
                    cur.executemany(insert_sql, rows)
                    batches_processed += 1
                    if batches_processed % 10 == 0:
                        logger.debug(f"Processed {i + len(batch_df)}/{total_rows} rows")

                conn.commit()
                transaction_started = False
                logger.debug("Transaction committed")

                elapsed = time.time() - start_time
                logger.info(
                    f"Successfully loaded {total_rows} rows to {full_table_name} "
                    f"in {batches_processed} batches, elapsed={elapsed:.2f}s"
                )
                return f"Loaded {total_rows} rows to {full_table_name}"

            except Exception as e:
                if transaction_started and conn is not None:
                    try:
                        conn.rollback()
                        logger.info("Transaction rolled back due to error")
                    except Exception as rollback_error:
                        logger.warning(f"Failed to rollback: {rollback_error}")

                if is_transient_snowflake_error(e):
                    logger.warning(f"Transient error detected, may retry: {e}")
                    raise  # Let tenacity handle retry

                logger.warning(
                    f"Primary insert method failed: {e}, trying SQLAlchemy fallback"
                )
                engine = None
                try:
                    engine = hook.get_sqlalchemy_engine()
                    df.to_sql(
                        name=table_name,
                        con=engine,
                        schema=schema,
                        if_exists=if_exists,
                        index=False,
                        method="multi",
                    )
                    logger.info(f"Loaded {len(df)} rows using SQLAlchemy fallback")
                    return (
                        f"Loaded {len(df)} rows to {schema}.{table_name} (fallback method)"
                    )
                except Exception as e2:
                    logger.error("Both load methods failed", exc_info=True)
                    raise DataLoadError(
                        f"Both methods failed. SQL method: {str(e)}, "
                        f"SQLAlchemy method: {str(e2)}",
                        destination=full_table_name,
                        original_error=e2,
                    ) from e2
                finally:
                    if engine:
                        engine.dispose()
            finally:
                if cur is not None:
                    try:
                        cur.close()
                    except Exception:
                        pass
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

        try:
            return _execute_transactional_load()
        except RetryError as e:
            logger.error("All retry attempts exhausted", exc_info=True)
            raise DataLoadError(
                f"Failed to load data after {DEFAULT_RETRY_ATTEMPTS} attempts",
                destination=full_table_name,
                original_error=e,
            ) from e
