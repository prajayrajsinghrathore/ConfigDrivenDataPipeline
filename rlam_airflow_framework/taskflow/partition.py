# File: rlam_airflow_framework/taskflow/partition.py
"""
Partition resolution and path scoping.

Encapsulates the ``_resolve_partition_value`` helper and the
``partition_scoped_path`` utility from ``taskflow_tasks.py`` into a
single data class that carries the resolved state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class PartitionInfo:
    """
    Immutable resolved partition state for a pipeline run.

    Build via :meth:`resolve` from a pipeline config dict and Airflow
    context, then use the properties and methods to thread partition
    info through ingestion, templating, and loading.
    """

    enabled: bool
    column: Optional[str]
    value: Optional[str]

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def resolve(
        cls, config: Dict[str, Any], context: Dict[str, Any]
    ) -> "PartitionInfo":
        """
        Resolve partition info from pipeline *config* and run *context*.

        A ``partition_date`` (datetime-like) is formatted as ``YYYY-MM-DD``;
        otherwise a ``partition_key`` is used verbatim.
        """
        partition_config = config.get("partition", {})
        enabled = partition_config.get("enabled", False)
        column = partition_config.get("column")

        value: Optional[str] = None
        if enabled:
            partition_date = context.get("partition_date")
            partition_key = context.get("partition_key")
            if partition_date is not None:
                if hasattr(partition_date, "strftime"):
                    value = partition_date.strftime("%Y-%m-%d")
                else:
                    value = str(partition_date)
            elif partition_key is not None:
                value = str(partition_key)

        return cls(enabled=enabled, column=column, value=value)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @property
    def metadata(self) -> Dict[str, str]:
        """Return a ``partition_key`` metadata dict for Kafka events (or empty)."""
        if self.value is not None:
            return {"partition_key": self.value}
        return {}

    def scope_path(self, path_or_uri: str) -> str:
        """
        Insert a partition folder segment into *path_or_uri*.

        No-op if partitioning is disabled, if required fields are missing,
        or if the path already contains partition information.
        """
        if not self.enabled or not self.column or not self.value or not path_or_uri:
            return path_or_uri

        if f"{self.column}=" in path_or_uri or self.value in path_or_uri:
            # Already has partition info
            return path_or_uri

        parts = path_or_uri.rsplit("/", 1)
        if len(parts) == 2:
            return f"{parts[0]}/{self.column}={self.value}/{parts[1]}"
        else:
            return f"{self.column}={self.value}/{path_or_uri}"

    def adjust_dest_paths(self, dest_config: Dict[str, Any]) -> None:
        """
        Mutate *dest_config* in-place, scoping paths for partitioned runs.

        Handles ``primary``, ``backup``, and ``archive`` destination blocks
        for both ``object_storage`` and ``local_file`` types.
        """
        if not self.enabled or not self.column or not self.value:
            return

        for key in ("primary", "backup", "archive"):
            dest = dest_config.get(key)
            if dest is None:
                continue
            dest_type = dest.get("type")
            if dest_type == "object_storage":
                if "uri" in dest:
                    dest["uri"] = self.scope_path(dest["uri"])
                if "path" in dest:
                    dest["path"] = self.scope_path(dest["path"])
            elif dest_type == "local_file":
                if "path" in dest:
                    dest["path"] = self.scope_path(dest["path"])
