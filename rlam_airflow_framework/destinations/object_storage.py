# File: rlam_airflow_framework/destinations/object_storage.py
"""Object storage loader (Azure/S3/GCS via Airflow ObjectStoragePath)."""

import time
from typing import cast, BinaryIO

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


@DestinationLoader.register("object_storage")
class ObjectStorageLoader(DestinationLoader):
    """Write to cloud object storage (supports ``uri`` or ``path`` + ``conn_id``)."""

    dest_type = "object_storage"

    def _write(self, df_path: str, dest_config, ctx):
        uri = dest_config.get("uri")
        path = dest_config.get("path")
        conn_id = dest_config.get("conn_id")
        file_format = dest_config.get("format", "parquet")
        logger = _log.bind(trace_id=ctx.correlation_id)

        destination_str = uri or (
            f"{path}@{conn_id}" if path and conn_id else "unknown"
        )

        try:
            if uri:
                storage_path = ObjectStoragePath(uri)
            elif path and conn_id:
                # both narrowed to non-None here, satisfying the checker
                storage_path = ObjectStoragePath(path, conn_id=conn_id)
            else:
                raise ValueError(
                    "Must provide either 'uri' parameter "
                    "(e.g., 'az://container@conn_id/path/file.ext') "
                    "or both 'path' and 'conn_id' parameters"
                )

            # Deterministic per-task-instance suffix
            run_token = sanitize_for_filename(ctx.correlation_id)
            parent = storage_path.parent
            stem = storage_path.stem
            suffix = storage_path.suffix or f".{file_format}"
            run_scoped_path = parent / f"{stem}_{run_token}{suffix}"

            logger.info(
                f"Starting object storage upload: {run_scoped_path}, "
                f"format={file_format}"
            )
            start_time = time.time()

            import duckdb
            import tempfile
            import shutil
            import os

            local_source = df_path

            # If a different format is requested, use DuckDB to convert out-of-core
            if file_format != "parquet":
                tmp = tempfile.NamedTemporaryFile(
                    delete=False, suffix=f".{file_format}"
                )
                tmp.close()
                local_source = tmp.name

                try:
                    if file_format == "csv":
                        duckdb.execute(
                            f"COPY (SELECT * FROM read_parquet('{df_path}')) TO '{local_source}' (HEADER, FORMAT CSV)"
                        )
                    elif file_format == "json":
                        duckdb.execute(
                            f"COPY (SELECT * FROM read_parquet('{df_path}')) TO '{local_source}' (FORMAT JSON, ARRAY TRUE)"
                        )
                    else:
                        raise ValueError(
                            f"Unsupported format: {file_format}. Supported: parquet, csv, json"
                        )
                except Exception as e:
                    if os.path.exists(local_source):
                        os.remove(local_source)
                    raise e

            # Stream out-of-core to the remote object storage
            with open(local_source, "rb") as f_in:
                with run_scoped_path.open("wb") as f_out:
                    shutil.copyfileobj(f_in, cast(BinaryIO, f_out))

            if local_source != df_path and os.path.exists(local_source):
                os.remove(local_source)

            elapsed = time.time() - start_time
            logger.info(
                f"Successfully uploaded to object storage: {run_scoped_path}, "
                f"elapsed={elapsed:.2f}s"
            )
            return str(run_scoped_path)

        except ValueError:
            raise  # Re-raise validation errors directly
        except Exception as e:
            logger.error(f"Object storage upload failed: {e}")
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
