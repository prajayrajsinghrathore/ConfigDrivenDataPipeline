# File: rlam_airflow_framework/taskflow/__init__.py
"""
TaskFlow OOP building blocks.

This subpackage contains the single-responsibility classes that back
the ``@task``-decorated functions in ``taskflow_tasks.py``:

- :class:`TaskExecutionContext` — Airflow runtime metadata snapshot
- :class:`DataFrameStorage` — parquet temp-file I/O
- :class:`WatermarkConfig` / :class:`WatermarkManager` — incremental-load watermarks
- :class:`PartitionInfo` — partition resolution and path scoping
"""

from rlam_airflow_framework.taskflow.context import TaskExecutionContext
from rlam_airflow_framework.taskflow.storage import DataFrameStorage
from rlam_airflow_framework.taskflow.watermark import WatermarkConfig, WatermarkManager
from rlam_airflow_framework.taskflow.partition import PartitionInfo

__all__ = [
    "TaskExecutionContext",
    "DataFrameStorage",
    "WatermarkConfig",
    "WatermarkManager",
    "PartitionInfo",
]
