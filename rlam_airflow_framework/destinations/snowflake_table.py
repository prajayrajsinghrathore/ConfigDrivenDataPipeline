# File: rlam_airflow_framework/destinations/snowflake_table.py
"""Snowflake table loader (parameterized, transactional insert)."""

import time
from typing import Dict, Any
import structlog
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from tenacity import RetryError

from rlam_airflow_framework.utils.validation import validate_identifier
from rlam_airflow_framework.destinations.base import DestinationLoader, LoadContext
from rlam_airflow_framework.destinations.primitives import (
    DataLoadError,
    TransientDataLoadError,
    DEFAULT_RETRY_ATTEMPTS,
    SNOWFLAKE_TRANSIENT_ERRORS,
    is_transient_snowflake_error,
    snowflake_retry,
)

_log = structlog.get_logger(__name__)


@DestinationLoader.register("snowflake_table")
class SnowflakeTableLoader(DestinationLoader):
    """
    Load staged Parquet files into a Snowflake table natively via PUT and COPY INTO.
    """

    dest_type = "snowflake_table"

    def _write(
        self, df_path: str, dest_config: Dict[str, Any], ctx: LoadContext
    ) -> str:
        parts = str(dest_config.get("table")).split(".")
        table_name = validate_identifier(parts[-1], "table name")
        schema = validate_identifier(
            parts[-2] if len(parts) > 1 else "PUBLIC", "schema"
        )
        snowflake_conn_id = dest_config.get("connection_id", "snowflake_default")
        if_exists = dest_config.get("mode", "append")
        partition_column = ctx.partition_column
        partition_value = ctx.partition_value
        logger = _log.bind(trace_id=ctx.correlation_id)

        full_table_name = f'"{schema}"."{table_name}"'

        logger.info(
            f"Starting Snowflake load: table={full_table_name}, "
            f"if_exists={if_exists}, method=PUT+COPY"
        )

        @snowflake_retry
        def _execute_native_load():
            hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
            conn = None
            cur = None
            transaction_started = False

            try:
                start_time = time.time()
                conn = hook.get_conn()
                cur = conn.cursor()

                # Determine the target for COPY INTO
                copy_target_table = full_table_name
                temp_table_name = f'"{schema}"."{table_name}_tmp_{int(time.time())}"'

                # If replace mode without partition, we load into a temp table then swap
                if if_exists == "replace" and not partition_column:
                    logger.info(
                        f"Replace mode: will load into temp table {temp_table_name} and swap"
                    )
                    cur.execute(
                        f"CREATE TABLE IF NOT EXISTS {full_table_name} (dummy INT)"
                    )
                    cur.execute(
                        f"CREATE OR REPLACE TABLE {temp_table_name} CLONE {full_table_name}"
                    )
                    cur.execute(f"TRUNCATE TABLE {temp_table_name}")
                    copy_target_table = temp_table_name

                # 1. PUT the Parquet file into an internal stage
                stage_name = f"~/{table_name}_{ctx.correlation_id}"

                # Windows path fix for PUT command
                normalized_path = df_path.replace("\\", "/")
                put_sql = (
                    f"PUT 'file://{normalized_path}' @{stage_name} AUTO_COMPRESS=TRUE"
                )

                logger.debug(f"Uploading Parquet: {put_sql}")
                cur.execute(put_sql)

                # Start transaction for the COPY and potential DELETE/SWAP
                conn.autocommit(False)
                transaction_started = True

                # If replacing a partition, delete old data first
                if if_exists == "replace" and partition_column and partition_value:
                    delete_sql = (
                        f'DELETE FROM {full_table_name} WHERE "{partition_column}" = %s'
                    )
                    cur.execute(delete_sql, (partition_value,))
                    logger.info(
                        f"Deleted partition {partition_column}={partition_value}"
                    )

                # 2. COPY INTO
                match_by = dest_config.get("match_by_column_name", "CASE_SENSITIVE")
                copy_sql = (
                    f"COPY INTO {copy_target_table} "
                    f"FROM @{stage_name} "
                    f"FILE_FORMAT = (TYPE = PARQUET) "
                    f"MATCH_BY_COLUMN_NAME = {match_by}"
                )

                logger.debug(f"Executing COPY INTO: {copy_sql}")
                cur.execute(copy_sql)

                copy_result = cur.fetchall()
                rows_loaded = sum(row[3] for row in copy_result) if copy_result else 0

                # 3. Swap if replace mode
                if if_exists == "replace" and not partition_column:
                    swap_sql = (
                        f"ALTER TABLE {copy_target_table} SWAP WITH {full_table_name}"
                    )
                    cur.execute(swap_sql)
                    cur.execute(f"DROP TABLE {copy_target_table}")
                    logger.info(
                        f"Swapped temp table {copy_target_table} with {full_table_name}"
                    )

                conn.commit()
                transaction_started = False

                # Cleanup stage
                try:
                    conn.autocommit(True)
                    cur.execute(f"REMOVE @{stage_name}")
                except Exception as e:
                    logger.warning(f"Failed to remove temporary stage files: {e}")

                elapsed = time.time() - start_time
                logger.info(
                    f"Successfully loaded {rows_loaded} rows via Parquet PUT/COPY "
                    f"in {elapsed:.2f}s"
                )
                return f"Loaded {rows_loaded} rows to {full_table_name} via Parquet"

            except Exception as e:
                if transaction_started and conn is not None:
                    try:
                        conn.rollback()
                        logger.info("Transaction rolled back due to error")
                    except Exception as rollback_error:
                        logger.warning(f"Failed to rollback: {rollback_error}")

                if is_transient_snowflake_error(e):
                    logger.warning(f"Transient error detected, may retry: {e}")
                    raise

                raise DataLoadError(
                    f"Failed to load Parquet to Snowflake table {full_table_name}: {e}",
                    destination=self.dest_type,
                ) from e
            finally:
                if cur:
                    cur.close()
                if conn:
                    conn.close()

        try:
            return _execute_native_load()
        except (RetryError, *SNOWFLAKE_TRANSIENT_ERRORS) as e:
            # snowflake_retry sets reraise=True, so tenacity re-raises the
            # LAST attempt's own exception once retries are exhausted rather
            # than wrapping it in RetryError - the RetryError case is kept
            # only as a defensive fallback. Either way, every attempt here
            # already matched SNOWFLAKE_TRANSIENT_ERRORS (is_transient_
            # snowflake_error above) and still failed; a longer Airflow-level
            # retry delay may outlast the underlying outage.
            logger.error("All retry attempts exhausted")
            raise TransientDataLoadError(
                f"Failed to load data after {DEFAULT_RETRY_ATTEMPTS} attempts",
                destination=full_table_name,
                original_error=e,
            ) from e
