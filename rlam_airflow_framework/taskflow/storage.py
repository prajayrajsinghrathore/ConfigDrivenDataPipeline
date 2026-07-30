# File: rlam_airflow_framework/taskflow/storage.py
"""
DataFrame file-based storage — save / load / cleanup parquet temp files.

Replaces the module-level ``TEMP_DATA_DIR``, ``_save_dataframe``,
``_load_dataframe``, and ``_cleanup_dataframe`` free functions from
``taskflow_tasks.py`` with a single class that owns its directory lifecycle.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import polars as pl
import structlog

log = structlog.get_logger(__name__)

# Default base directory for temporary DataFrames
_DEFAULT_BASE_DIR = (
    Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "tmp" / "dataframes"
)


class DataFrameStorage:
    """
    Manages parquet-based DataFrame persistence for inter-task data passing.

    Each instance is scoped to a base directory.  The default mirrors the
    original ``TEMP_DATA_DIR`` constant so behaviour is unchanged for
    production code.

    In tests, pass an explicit ``base_dir`` (e.g. ``tmp_path``) to avoid
    needing ``patch("...TEMP_DATA_DIR", ...)`` — though the old patch path
    still works via the compatibility shim in ``taskflow_tasks.py``.
    """

    def __init__(self, base_dir: Path | None = None) -> None:
        self._base_dir = base_dir or _DEFAULT_BASE_DIR
        try:
            self._base_dir.mkdir(parents=True, exist_ok=True)
        except (PermissionError, OSError):
            # Directory will be created on demand by save() if needed
            pass

    @property
    def base_dir(self) -> Path:
        """Return the resolved base directory."""
        return self._base_dir

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_path(self, task_id: str, run_id: str) -> Path:
        """Get the expected path for a parquet file."""
        self._base_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{task_id}_{run_id}.parquet"
        return self._base_dir / filename

    def save(self, df: pl.DataFrame, task_id: str, run_id: str) -> str:
        """Save *df* to a parquet file and return the absolute path."""
        filepath = self.get_path(task_id, run_id)
        df.write_parquet(filepath, compression="snappy")
        log.info(
            f"Saved DataFrame to {filepath}",
            rows=len(df),
            size_mb=filepath.stat().st_size / 1024 / 1024,
        )
        return str(filepath)

    @staticmethod
    def load(filepath: str) -> pl.DataFrame:
        """Load a DataFrame from a parquet file."""
        df = pl.read_parquet(filepath)
        log.info(f"Loaded DataFrame from {filepath}", rows=len(df))
        return df

    @staticmethod
    def cleanup(filepath: str) -> None:
        """Delete a temporary DataFrame file (best-effort)."""
        try:
            Path(filepath).unlink(missing_ok=True)
            log.info(f"Cleaned up DataFrame file: {filepath}")
        except Exception as e:
            log.warning(f"Failed to cleanup DataFrame file: {filepath}", error=str(e))

    @staticmethod
    def get_row_count(filepath: str) -> int:
        """Get row count of a parquet file using DuckDB (out-of-core)."""
        import duckdb

        try:
            res = duckdb.query(f"SELECT count(*) FROM '{filepath}'").fetchone()
            return res[0] if res else 0
        except Exception as e:
            log.warning(f"Failed to get row count for {filepath}", error=str(e))
            return 0

    @staticmethod
    def apply_watermark_filter(
        filepath: str, watermark_column: str, watermark_value: Any
    ) -> None:
        """Filter a parquet file in-place using DuckDB (out-of-core)."""
        import duckdb
        import tempfile
        import shutil

        temp_fd, temp_path = tempfile.mkstemp(suffix=".parquet")
        os.close(temp_fd)

        val = str(watermark_value).replace("'", "''")
        try:
            duckdb.query(
                f"COPY (SELECT * FROM '{filepath}' WHERE \"{watermark_column}\" > '{val}') TO '{temp_path}' (FORMAT PARQUET)"
            )
            shutil.move(temp_path, filepath)
            log.info(
                "Applied watermark filter via DuckDB",
                watermark_column=watermark_column,
                watermark_value=val,
            )
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise e
