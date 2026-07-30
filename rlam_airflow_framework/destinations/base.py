# File: rlam_airflow_framework/destinations/base.py
"""
Base abstractions for destination loaders: LoadContext and DestinationLoader.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Optional

import structlog

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


LOADER_REGISTRY = {}


class DestinationLoader(ABC):
    """
    Strategy interface for loading a DataFrame into one destination type.

    Subclasses declare the config ``type`` they handle via :attr:`dest_type`
    and implement :meth:`_write`. :meth:`load` is a template method that runs
    the shared guards, then dispatches to :meth:`_write` polymorphically.
    """

    @classmethod
    def register(cls, dest_type: str):
        """Decorator to register a destination loader for a specific type."""

        def decorator(subclass):
            subclass.dest_type = dest_type
            LOADER_REGISTRY[dest_type] = subclass
            return subclass

        return decorator

    #: The destination config ``type`` value this loader handles.
    dest_type: ClassVar[str]

    #: Whether this loader consumes the DataFrame. Action-style sinks (e.g. a
    #: stored procedure) set this False so an empty frame does not skip them.
    consumes_dataframe: ClassVar[bool] = True

    def load(self, df_path: str, dest_config: Dict[str, Any], ctx: LoadContext) -> str:
        """Template method: shared guards + logging, then delegate to ``_write``."""
        import os

        if self.consumes_dataframe:
            if not os.path.exists(df_path):
                _log.bind(correlation_id=ctx.correlation_id).warning(
                    "Missing Parquet file provided; nothing to load",
                    dest_type=self.dest_type,
                )
                return "No data to load (missing file)"
        return self._write(df_path, dest_config, ctx)

    @abstractmethod
    def _write(
        self, df_path: str, dest_config: Dict[str, Any], ctx: LoadContext
    ) -> str:
        """Perform the actual load; return a human-readable summary."""
        raise NotImplementedError
