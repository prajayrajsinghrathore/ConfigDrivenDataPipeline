# File: rlam_airflow_framework/destinations/base.py
"""
Base abstractions for destination loaders: LoadContext and DestinationLoader.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Optional

import pandas as pd
import structlog

from rlam_airflow_framework.validation import validate_dataframe

_log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class LoadContext:
    """
    Immutable per-load context passed to every loader.

    Carries the cross-cutting values (correlation id, partition coordinates,
    event topic) so loader signatures stay uniform regardless of sink type.
    """

    topic: str
    correlation_id: str
    dest_label: str
    partition_column: Optional[str] = None
    partition_value: Optional[str] = None


class DestinationLoader(ABC):
    """
    Strategy interface for loading a DataFrame into one destination type.

    Subclasses declare the config ``type`` they handle via :attr:`dest_type`
    and implement :meth:`_write`. :meth:`load` is a template method that runs
    the shared guards, then dispatches to :meth:`_write` polymorphically.
    """

    #: The destination config ``type`` value this loader handles.
    dest_type: ClassVar[str]

    #: Whether this loader consumes the DataFrame. Action-style sinks (e.g. a
    #: stored procedure) set this False so an empty frame does not skip them.
    consumes_dataframe: ClassVar[bool] = True

    def load(
        self, df: pd.DataFrame, dest_config: Dict[str, Any], ctx: LoadContext
    ) -> str:
        """Template method: shared guards + logging, then delegate to ``_write``."""
        if self.consumes_dataframe:
            validate_dataframe(df, self.dest_type)
            if df.empty:
                _log.bind(correlation_id=ctx.correlation_id).warning(
                    "Empty DataFrame provided; nothing to load",
                    dest_type=self.dest_type,
                )
                return "No data to load (empty DataFrame)"
        return self._write(df, dest_config, ctx)

    @abstractmethod
    def _write(
        self, df: pd.DataFrame, dest_config: Dict[str, Any], ctx: LoadContext
    ) -> str:
        """Perform the actual load; return a human-readable summary."""
        raise NotImplementedError
