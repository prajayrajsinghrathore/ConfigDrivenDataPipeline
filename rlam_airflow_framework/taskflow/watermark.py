# File: rlam_airflow_framework/taskflow/watermark.py
"""
Incremental-load watermark management.

Extracts the ~100 lines of watermark resolution, lookback adjustment,
DataFrame filtering, and Variable persistence that were inlined across
``ingest_data`` and ``load_data`` in ``taskflow_tasks.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Union, cast

import pandas as pd
import structlog

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class WatermarkConfig:
    """
    Parsed incremental-load settings from the pipeline config.

    Build via :meth:`from_config` to avoid raw-dict access everywhere.
    """

    enabled: bool
    watermark_column: Optional[str]
    initial_watermark: Optional[str]
    lookback: int
    strict: bool

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "WatermarkConfig":
        """Extract incremental settings from a full pipeline *config* dict."""
        inc = config.get("incremental", {})
        return cls(
            enabled=inc.get("enabled", False),
            watermark_column=inc.get("watermark_column"),
            initial_watermark=inc.get("initial_watermark"),
            lookback=inc.get("lookback", 0),
            strict=inc.get("strict", False),
        )


class WatermarkManager:
    """
    Manages the lifecycle of an incremental-load watermark.

    Typical usage inside a ``@task`` function::

        wm = WatermarkManager(dag_id, WatermarkConfig.from_config(config))
        adjusted = wm.resolve()          # read Variable, apply lookback
        df = wm.filter_dataframe(df)      # post-fetch filtering
        ...
        wm.update(df)                     # persist new high-watermark
    """

    def __init__(self, dag_id: str, wm_config: WatermarkConfig) -> None:
        self._dag_id = dag_id
        self._cfg = wm_config
        self._adjusted_watermark: Optional[str] = None

    @property
    def config(self) -> WatermarkConfig:
        return self._cfg

    @property
    def adjusted_watermark(self) -> Optional[str]:
        """The watermark value after lookback adjustment (set by :meth:`resolve`)."""
        return self._adjusted_watermark

    # ------------------------------------------------------------------
    # Resolve
    # ------------------------------------------------------------------

    def resolve(self) -> Optional[str]:
        """
        Read the current watermark from Airflow Variables, apply lookback,
        and return the adjusted value.

        Returns ``None`` when incremental loading is disabled.
        """
        if not self._cfg.enabled:
            return None

        from airflow.sdk import Variable

        current_watermark = Variable.get(
            f"{self._dag_id}.high_watermark", default=None
        )
        if current_watermark:
            log.info(
                "Loaded watermark from Airflow Variable",
                watermark=current_watermark,
            )

        if not current_watermark:
            first_run = Variable.get(
                f"{self._dag_id}.first_run_completed", default=None
            )
            if self._cfg.strict and first_run:
                raise ValueError(
                    "Incremental strict mode: no watermark found but first run "
                    "is marked completed."
                )
            current_watermark = self._cfg.initial_watermark
            log.warning(
                "incremental configured but no watermark found — performing "
                "FULL load from initial_watermark"
            )

        log.info("Resolved incremental watermark", current_watermark=current_watermark)

        # Apply lookback
        adjusted = self._apply_lookback(current_watermark)
        self._adjusted_watermark = adjusted
        return adjusted

    # ------------------------------------------------------------------
    # Filter
    # ------------------------------------------------------------------

    def filter_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Post-fetch filtering: keep only rows whose watermark column value
        exceeds the adjusted watermark.

        Tries numeric → datetime → lexicographic comparison in that order.
        Returns *df* unchanged when incremental loading is disabled or the
        watermark column is missing.
        """
        if not self._cfg.enabled or not self._cfg.watermark_column:
            return df
        if df.empty:
            return df
        if self._adjusted_watermark is None:
            return df
        if self._cfg.watermark_column not in df.columns:
            return df

        col = df[self._cfg.watermark_column]
        wm = cast(Any, self._adjusted_watermark)
        mask = None
        comparison = "unknown"

        try:
            mask = cast(Any, pd.to_numeric(col)) > float(wm)
            comparison = "numeric"
        except (ValueError, TypeError):
            try:
                mask = pd.to_datetime(col) > pd.to_datetime(wm)
                comparison = "datetime"
            except Exception:
                try:
                    mask = col.astype(str) > str(wm)
                    comparison = "string (lexicographic — verify ordering!)"
                except Exception as ex:
                    log.error(
                        "Failed to filter DataFrame by watermark", error=str(ex)
                    )

        if mask is not None:
            df = cast(pd.DataFrame, df[mask])
            log.info(
                "Filtered DataFrame by watermark column",
                comparison=comparison,
                remaining_rows=len(df),
                watermark=self._adjusted_watermark,
            )

        return df

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------

    def update(self, df_or_path: Union[pd.DataFrame, str]) -> None:
        """
        Compute ``max(watermark_column)`` from *df_or_path* and persist to Airflow
        Variables.  No-op when incremental loading is disabled, the column
        is missing, or the input is empty.
        """
        if not self._cfg.enabled or not self._cfg.watermark_column:
            return
            
        if isinstance(df_or_path, str):
            import duckdb
            try:
                res = duckdb.execute(f"SELECT MAX({self._cfg.watermark_column}) FROM read_parquet('{df_or_path}')").fetchone()
                if not res or res[0] is None:
                    return
                max_val = res[0]
            except Exception:
                return
        else:
            if df_or_path.empty:
                return
            if self._cfg.watermark_column not in df_or_path.columns:
                return
            max_val = df_or_path[self._cfg.watermark_column].max()

        from airflow.sdk import Variable

        new_watermark = (
            max_val.isoformat() if hasattr(max_val, "isoformat") else str(max_val)
        )

        Variable.set(f"{self._dag_id}.high_watermark", new_watermark)
        Variable.set(f"{self._dag_id}.first_run_completed", "true")
        log.info("Saved watermark to Airflow Variable", watermark=new_watermark)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _apply_lookback(self, watermark: Optional[str]) -> Optional[str]:
        """Subtract ``lookback`` seconds from *watermark* if configured."""
        if not self._cfg.lookback or not watermark:
            return watermark

        try:
            import pendulum

            dt = pendulum.parse(watermark)
            if not isinstance(dt, pendulum.DateTime):
                raise TypeError(
                    f"Watermark did not parse to a DateTime: {watermark!r}"
                )
            adjusted_dt = dt.subtract(seconds=self._cfg.lookback)
            adjusted = adjusted_dt.isoformat()
            log.info(
                "Adjusted watermark with lookback",
                lookback_seconds=self._cfg.lookback,
                adjusted_watermark=adjusted,
            )
            return adjusted
        except Exception as e:
            log.warning(
                "Failed to apply lookback to watermark, using raw watermark",
                error=str(e),
            )
            return watermark
