# File: rlam_airflow_framework/destinations/local_file.py
"""Local filesystem loader."""

import os
import time
from datetime import datetime

import structlog

from rlam_airflow_framework.destinations.base import DestinationLoader
from rlam_airflow_framework.destinations.primitives import DataLoadError

_log = structlog.get_logger(__name__)


class LocalFileLoader(DestinationLoader):
    """Write the DataFrame to a timestamped local file."""

    dest_type = "local_file"

    def _write(self, df, dest_config, ctx):
        file_path = dest_config["path"]
        file_format = dest_config.get("format", "parquet")
        logger = _log.bind(trace_id=ctx.correlation_id)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base, ext = os.path.splitext(file_path)
        if not ext:
            ext = f".{file_format}"
        full_path = f"{base}_{timestamp}{ext}"

        logger.info(
            f"Saving to local file: {full_path}, format={file_format}, rows={len(df)}"
        )

        try:
            start_time = time.time()
            os.makedirs(os.path.dirname(full_path) or ".", exist_ok=True)

            if file_format == "parquet":
                df.to_parquet(full_path, index=False)
            elif file_format == "csv":
                df.to_csv(full_path, index=False)
            elif file_format == "json":
                df.to_json(full_path, orient="records", indent=2)
            else:
                raise ValueError(
                    f"Unsupported format: {file_format}. Supported: parquet, csv, json"
                )

            elapsed = time.time() - start_time
            logger.info(f"Successfully saved to {full_path}, elapsed={elapsed:.2f}s")
            return full_path

        except Exception as e:
            logger.error(f"Local file save failed: {e}", exc_info=True)
            raise DataLoadError(
                f"Failed to save to local file: {str(e)}",
                destination=file_path,
                original_error=e,
            ) from e
