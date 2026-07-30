# File: rlam_airflow_framework/destinations/registry.py
"""Destination loader registry and the process-wide default instance."""

from typing import List, Optional

from rlam_airflow_framework.destinations.base import DestinationLoader, LOADER_REGISTRY


class DestinationRegistry:
    """
    Registry mapping a destination config ``type`` to a loader strategy.

    Encapsulates the lookup table so callers never branch on type themselves.
    """

    def __init__(self) -> None:
        pass

    def get(self, dest_type: Optional[str]) -> DestinationLoader:
        """Return the loader for ``dest_type`` or raise ValueError if unknown."""
        try:
            return LOADER_REGISTRY[dest_type]()  # type: ignore[index]
        except KeyError:
            raise ValueError(
                f"Unsupported destination type: {dest_type}. "
                f"Supported: {', '.join(self.supported_types())}"
            )

    def supported_types(self) -> List[str]:
        """Return the registered destination types (sorted)."""
        return sorted(LOADER_REGISTRY.keys())


#: Process-wide default registry used by the pipeline tasks.
DESTINATION_REGISTRY = DestinationRegistry()
