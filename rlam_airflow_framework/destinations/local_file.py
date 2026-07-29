# File: rlam_airflow_framework/destinations/local_file.py
"""Local filesystem loader."""

import os
import time
from pathlib import Path

import structlog

from rlam_airflow_framework.destinations.base import DestinationLoader
from rlam_airflow_framework.destinations.primitives import (
    DataLoadError,
    sanitize_for_filename,
)

_log = structlog.get_logger(__name__)

# Sandbox root for local_file destinations. dest_config["path"] comes straight
# from pipeline YAML, so it must never be trusted as a literal filesystem
# path — a config like "../../../../opt/airflow/airflow.cfg" would otherwise
# let a pipeline overwrite arbitrary files on the worker pod.
_DEFAULT_OUTPUT_ROOT = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "data" / "local_file_output"
LOCAL_FILE_OUTPUT_ROOT = Path(
    os.getenv("LOCAL_FILE_OUTPUT_ROOT", str(_DEFAULT_OUTPUT_ROOT))
).resolve()


class LocalFileLoader(DestinationLoader):
    """Write the DataFrame to a run-scoped local file, sandboxed under LOCAL_FILE_OUTPUT_ROOT."""

    dest_type = "local_file"

    @staticmethod
    def _resolve_safe_path(raw_path: str) -> Path:
        """
        Resolve dest_config["path"] to a real path guaranteed to live under
        LOCAL_FILE_OUTPUT_ROOT.

        Every segment is validated BEFORE any pathlib join happens: ".."
        segments are rejected outright, and so is any segment containing a
        colon. The colon check matters even for relative-looking input —
        pathlib/os.path treat a bare "C:" (or "C:\\evil") segment as a new
        drive anchor and silently DISCARD everything joined before it, on
        Windows, no matter where in the path it appears or whether it's
        passed as one joined string or multiple join args. Rejecting colons
        up front closes that reset-the-root escape route rather than
        relying on a post-hoc containment check that a join like that would
        already have defeated.
        """
        normalized = str(raw_path).replace("\\", "/")
        segments = [s for s in normalized.split("/") if s not in ("", ".")]

        for segment in segments:
            if segment == ".." or ":" in segment:
                raise DataLoadError(
                    f"Configured path '{raw_path}' must be a relative path "
                    f"under the local file output root — '..' and drive/"
                    f"colon segments are not allowed",
                    destination=str(raw_path),
                )

        candidate = LOCAL_FILE_OUTPUT_ROOT.joinpath(*segments) if segments else LOCAL_FILE_OUTPUT_ROOT
        candidate = candidate.resolve()

        try:
            candidate.relative_to(LOCAL_FILE_OUTPUT_ROOT)
        except ValueError:
            raise DataLoadError(
                f"Configured path '{raw_path}' escapes the local file output "
                f"root '{LOCAL_FILE_OUTPUT_ROOT}'",
                destination=str(raw_path),
            )
        return candidate

    def _write(self, df, dest_config, ctx):
        file_path = dest_config["path"]
        file_format = dest_config.get("format", "parquet")
        logger = _log.bind(trace_id=ctx.correlation_id)

        safe_path = self._resolve_safe_path(file_path)

        # Deterministic per-task-instance suffix (not wall-clock) so retries
        # and cleared task instances overwrite the same file instead of
        # producing a duplicate that downstream consumers double-count.
        run_token = sanitize_for_filename(ctx.correlation_id)
        base, ext = os.path.splitext(str(safe_path))
        if not ext:
            ext = f".{file_format}"
        full_path = f"{base}_{run_token}{ext}"

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
