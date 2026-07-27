# File: rlam_airflow_framework/destinations/snowflake_stage.py
"""Snowflake stage loader (uploads the DataFrame as a file via PUT)."""

import os
import tempfile
import time
from datetime import datetime
from typing import cast

import structlog
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from rlam_airflow_framework.validation import validate_identifier
from rlam_airflow_framework.destinations.base import DestinationLoader
from rlam_airflow_framework.destinations.primitives import DataLoadError

_log = structlog.get_logger(__name__)


class SnowflakeStageLoader(DestinationLoader):
    """Upload the DataFrame as a file to a Snowflake stage via PUT."""

    dest_type = "snowflake_stage"

    def _write(self, df, dest_config, ctx):
        stage_name = validate_identifier(
            dest_config.get("stage_name", "DATA_STAGE"), "stage name"
        )
        file_name = dest_config.get(
            "file_name", f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        file_format = dest_config.get("format", "csv")
        snowflake_conn_id = dest_config.get("connection_id", "snowflake-default")
        logger = _log.bind(trace_id=ctx.correlation_id)

        logger.info(
            f"Starting Snowflake stage load: stage={stage_name}, "
            f"file={file_name}, format={file_format}, rows={len(df)}"
        )

        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

        # These no-path serializations return str (never None); the pandas
        # stubs type them as Optional, so cast to satisfy tmp_file.write.
        if file_format == "csv":
            data = cast(str, df.to_csv(index=False))
        elif file_format == "json":
            data = cast(str, df.to_json(orient="records"))
        else:
            raise ValueError(f"Unsupported format: {file_format}. Supported: csv, json")

        date_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name, ext = os.path.splitext(file_name)
        if not ext:
            ext = f".{file_format}"
        dated_file_name = f"{base_name}_{date_suffix}{ext}"

        tmp_file_path = os.path.join(tempfile.gettempdir(), dated_file_name)

        try:
            start_time = time.time()

            with open(tmp_file_path, "w", encoding="utf-8") as tmp_file:
                tmp_file.write(data)

            stage_full_path = f"@{stage_name}"
            # file path is a local system path, not user input
            put_sql = (
                f"PUT file://{tmp_file_path} {stage_full_path} "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            hook.run(put_sql)

            elapsed = time.time() - start_time
            logger.info(
                f"Successfully uploaded to stage {stage_full_path}/{dated_file_name}, "
                f"elapsed={elapsed:.2f}s"
            )
            return f"Loaded to stage {stage_full_path}/{dated_file_name}"

        except Exception as e:
            logger.error(f"Snowflake stage upload failed: {e}", exc_info=True)
            raise DataLoadError(
                f"Failed to upload to Snowflake stage: {str(e)}",
                destination=f"{stage_name}/{file_name}",
                original_error=e,
            ) from e
        finally:
            if os.path.exists(tmp_file_path):
                try:
                    os.unlink(tmp_file_path)
                except Exception as e:
                    logger.warning(f"Failed to clean up temp file: {e}")
