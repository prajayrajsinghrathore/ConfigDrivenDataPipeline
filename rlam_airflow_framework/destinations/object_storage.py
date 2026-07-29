# File: rlam_airflow_framework/destinations/object_storage.py
"""Object storage loader (Azure/S3/GCS via Airflow ObjectStoragePath)."""

import time
from typing import cast

import structlog
from airflow.sdk import ObjectStoragePath

from rlam_airflow_framework.destinations.base import DestinationLoader
from rlam_airflow_framework.destinations.primitives import (
    ObjectStorageError,
    TransientObjectStorageError,
    is_transient_object_storage_error,
    sanitize_for_filename,
)

_log = structlog.get_logger(__name__)


class ObjectStorageLoader(DestinationLoader):
    """Write to cloud object storage (supports ``uri`` or ``path`` + ``conn_id``)."""

    dest_type = "object_storage"

    def _write(self, df, dest_config, ctx):
        uri = dest_config.get("uri")
        path = dest_config.get("path")
        conn_id = dest_config.get("conn_id")
        file_format = dest_config.get("format", "parquet")
        logger = _log.bind(trace_id=ctx.correlation_id)

        try:
            if uri:
                storage_path = ObjectStoragePath(uri)
                destination_str = uri
            elif path and conn_id:
                # both narrowed to non-None here, satisfying the checker
                storage_path = ObjectStoragePath(path, conn_id=conn_id)
                destination_str = f"{path}@{conn_id}"
            else:
                raise ValueError(
                    "Must provide either 'uri' parameter "
                    "(e.g., 'az://container@conn_id/path/file.ext') "
                    "or both 'path' and 'conn_id' parameters"
                )

            # Deterministic per-task-instance suffix (not wall-clock) so
            # retries and cleared task instances overwrite the same object
            # instead of producing a duplicate that downstream consumers
            # double-count.
            run_token = sanitize_for_filename(ctx.correlation_id)
            parent = storage_path.parent
            stem = storage_path.stem
            suffix = storage_path.suffix or f".{file_format}"
            run_scoped_path = parent / f"{stem}_{run_token}{suffix}"

            logger.info(
                f"Starting object storage upload: {run_scoped_path}, "
                f"format={file_format}, rows={len(df)}"
            )
            start_time = time.time()

            # These no-path serializations return bytes/str (never None); the
            # pandas stubs type them as Optional, so cast to satisfy write_*.
            if file_format == "parquet":
                run_scoped_path.write_bytes(cast(bytes, df.to_parquet(index=False)))
            elif file_format == "csv":
                run_scoped_path.write_text(cast(str, df.to_csv(index=False)))
            elif file_format == "json":
                run_scoped_path.write_text(
                    cast(str, df.to_json(orient="records", indent=2))
                )
            else:
                raise ValueError(
                    f"Unsupported format: {file_format}. Supported: parquet, csv, json"
                )

            elapsed = time.time() - start_time
            logger.info(
                f"Successfully uploaded to object storage: {run_scoped_path}, "
                f"elapsed={elapsed:.2f}s"
            )
            return str(run_scoped_path)

        except ValueError:
            raise  # Re-raise validation errors directly
        except Exception as e:
            logger.error(f"Object storage upload failed: {e}", exc_info=True)
            error_cls = (
                TransientObjectStorageError
                if is_transient_object_storage_error(e)
                else ObjectStorageError
            )
            raise error_cls(
                f"Failed to upload to object storage: {str(e)}",
                destination=destination_str,
                original_error=e,
            ) from e
