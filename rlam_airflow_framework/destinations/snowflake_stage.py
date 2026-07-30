# File: rlam_airflow_framework/destinations/snowflake_stage.py
"""Snowflake stage loader (uploads the DataFrame as a file via PUT)."""

import os
import tempfile
import time

import structlog
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from rlam_airflow_framework.utils.validation import validate_identifier
from rlam_airflow_framework.destinations.base import DestinationLoader
from rlam_airflow_framework.destinations.primitives import (
    DataLoadError,
    TransientDataLoadError,
    is_transient_snowflake_error,
    sanitize_for_filename,
)

_log = structlog.get_logger(__name__)


@DestinationLoader.register("snowflake_stage")
class SnowflakeStageLoader(DestinationLoader):
    """Upload the DataFrame as a file to a Snowflake stage via PUT."""

    dest_type = "snowflake_stage"

    def _write(self, df_path: str, dest_config, ctx):
        stage_name = validate_identifier(
            dest_config.get("stage_name", "DATA_STAGE"), "stage name"
        )
        # Deterministic per-task-instance suffix (not wall-clock) so retries
        # and cleared task instances overwrite the same staged file instead
        # of producing a duplicate that downstream consumers double-count.
        run_token = sanitize_for_filename(ctx.correlation_id)
        file_name = dest_config.get("file_name", f"data_{run_token}.csv")
        file_format = dest_config.get("format", "csv")
        snowflake_conn_id = dest_config.get("connection_id", "snowflake-default")
        logger = _log.bind(trace_id=ctx.correlation_id)

        import duckdb

        try:
            res = duckdb.query(f"SELECT count(*) FROM '{df_path}'").fetchone()
            row_count = res[0] if res else 0
        except Exception:
            row_count = 0

        logger.info(
            f"Starting Snowflake stage load: stage={stage_name}, "
            f"file={file_name}, format={file_format}, rows={row_count}"
        )

        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

        base_name, ext = os.path.splitext(file_name)
        if not ext:
            ext = f".{file_format}"
        run_scoped_file_name = f"{base_name}_{run_token}{ext}"

        needs_cleanup = False
        if file_format == "parquet":
            tmp_file_path = df_path
        else:
            tmp_file_path = os.path.join(tempfile.gettempdir(), run_scoped_file_name)
            duckdb_format = "CSV" if file_format == "csv" else "JSON"
            duckdb.query(
                f"COPY (SELECT * FROM '{df_path}') TO '{tmp_file_path}' (FORMAT {duckdb_format})"
            )
            needs_cleanup = True

        try:
            start_time = time.time()

            stage_full_path = f"@{stage_name}"
            # file path is a local system path, not user input
            put_sql = (
                f"PUT file://{tmp_file_path} {stage_full_path} "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            hook.run(put_sql)

            elapsed = time.time() - start_time
            logger.info(
                f"Successfully uploaded to stage {stage_full_path}/{run_scoped_file_name}, "
                f"elapsed={elapsed:.2f}s"
            )
            return f"Loaded to stage {stage_full_path}/{run_scoped_file_name}"

        except Exception as e:
            logger.error(f"Snowflake stage upload failed: {e}")
            error_cls = (
                TransientDataLoadError
                if is_transient_snowflake_error(e)
                else DataLoadError
            )
            raise error_cls(
                f"Failed to upload to Snowflake stage: {str(e)}",
                destination=f"{stage_name}/{file_name}",
                original_error=e,
            ) from e
        finally:
            if needs_cleanup and os.path.exists(tmp_file_path):
                try:
                    os.unlink(tmp_file_path)
                except Exception as e:
                    logger.warning(f"Failed to clean up temp file: {e}")
