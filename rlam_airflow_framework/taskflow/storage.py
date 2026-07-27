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

import pandas as pd
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

    def save(self, df: pd.DataFrame, task_id: str, run_id: str) -> str:
        """Save *df* to a parquet file and return the absolute path."""
        # Ensure directory exists (in case constructor creation failed)
        self._base_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{task_id}_{run_id}.parquet"
        filepath = self._base_dir / filename
        df.to_parquet(filepath, index=False, compression="snappy")
        log.info(
            f"Saved DataFrame to {filepath}",
            rows=len(df),
            size_mb=filepath.stat().st_size / 1024 / 1024,
        )
        return str(filepath)

    @staticmethod
    def load(filepath: str) -> pd.DataFrame:
        """Load a DataFrame from a parquet file."""
        df = pd.read_parquet(filepath)
        log.info(f"Loaded DataFrame from {filepath}", rows=len(df))
        return df

    @staticmethod
    def cleanup(filepath: str) -> None:
        """Delete a temporary DataFrame file (best-effort)."""
        try:
            Path(filepath).unlink(missing_ok=True)
            log.info(f"Cleaned up DataFrame file: {filepath}")
        except Exception as e:
            log.warning(
                f"Failed to cleanup DataFrame file: {filepath}", error=str(e)
            )
