# File: rlam_airflow_framework/destinations/registry.py
"""Destination loader registry and the process-wide default instance."""

from typing import Dict, List, Optional

from rlam_airflow_framework.destinations.base import DestinationLoader
from rlam_airflow_framework.destinations.snowflake_table import SnowflakeTableLoader
from rlam_airflow_framework.destinations.snowflake_stage import SnowflakeStageLoader
from rlam_airflow_framework.destinations.object_storage import ObjectStorageLoader
from rlam_airflow_framework.destinations.local_file import LocalFileLoader
from rlam_airflow_framework.destinations.stored_procedure import StoredProcedureLoader
from rlam_airflow_framework.destinations.print_logs import PrintLogsLoader


class DestinationRegistry:
    """
    Registry mapping a destination config ``type`` to a loader strategy.

    Encapsulates the lookup table so callers never branch on type themselves.
    """

    def __init__(self) -> None:
        self._loaders: Dict[str, DestinationLoader] = {}

    def register(self, loader: DestinationLoader) -> None:
        """Register (or override) the loader that handles ``loader.dest_type``."""
        self._loaders[loader.dest_type] = loader

    def get(self, dest_type: Optional[str]) -> DestinationLoader:
        """Return the loader for ``dest_type`` or raise ValueError if unknown."""
        try:
            return self._loaders[dest_type]  # type: ignore[index]
        except KeyError:
            raise ValueError(
                f"Unsupported destination type: {dest_type}. "
                f"Supported: {', '.join(self.supported_types())}"
            )

    def supported_types(self) -> List[str]:
        """Return the registered destination types (sorted)."""
        return sorted(self._loaders)


def _build_default_registry() -> DestinationRegistry:
    """Construct the registry with all built-in loaders registered."""
    registry = DestinationRegistry()
    for loader in (
        SnowflakeTableLoader(),
        SnowflakeStageLoader(),
        ObjectStorageLoader(),
        LocalFileLoader(),
        StoredProcedureLoader(),
        PrintLogsLoader(),
    ):
        registry.register(loader)
    return registry


#: Process-wide default registry used by the pipeline tasks.
DESTINATION_REGISTRY = _build_default_registry()
