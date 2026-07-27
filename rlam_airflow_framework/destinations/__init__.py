# File: rlam_airflow_framework/destinations/__init__.py
"""
Destination loader strategies — Open/Closed dispatch for data sinks.

Each sink type is a self-contained :class:`DestinationLoader` subclass in its own
module that owns its full behavior (config parsing, connection, write, retry,
validation). Dispatch is polymorphic via :data:`DESTINATION_REGISTRY`.

This package re-exports the full public API, so
``from rlam_airflow_framework.destinations import DESTINATION_REGISTRY`` (and any
loader class) keeps working regardless of the internal module layout.

Adding a new sink = add a ``DestinationLoader`` subclass module and register it
in ``registry.py``; no existing code changes (Open/Closed Principle).
"""

from rlam_airflow_framework.destinations.base import DestinationLoader, LoadContext
from rlam_airflow_framework.destinations.registry import (
    DestinationRegistry,
    DESTINATION_REGISTRY,
)
from rlam_airflow_framework.destinations.snowflake_table import SnowflakeTableLoader
from rlam_airflow_framework.destinations.snowflake_stage import SnowflakeStageLoader
from rlam_airflow_framework.destinations.object_storage import ObjectStorageLoader
from rlam_airflow_framework.destinations.local_file import LocalFileLoader
from rlam_airflow_framework.destinations.stored_procedure import StoredProcedureLoader
from rlam_airflow_framework.destinations.print_logs import PrintLogsLoader

__all__ = [
    "DestinationLoader",
    "LoadContext",
    "DestinationRegistry",
    "DESTINATION_REGISTRY",
    "SnowflakeTableLoader",
    "SnowflakeStageLoader",
    "ObjectStorageLoader",
    "LocalFileLoader",
    "StoredProcedureLoader",
    "PrintLogsLoader",
]
