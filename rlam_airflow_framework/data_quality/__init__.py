# File: rlam_airflow_framework/data_quality/__init__.py
"""
Data Quality package: Soda 4-driven validation, quarantine, and HITL approval.

Layout:
- ``engines.py``: ``SodaEngine`` — verifies a SodaCL contract directly
  against a Parquet file via DuckDB (out-of-core). It is the
  only validation engine; ``DataQualityChecker`` skips validation entirely
  when Soda 4 isn't configured or isn't installed.
- ``checker.py``: ``DataQualityChecker``, the facade that selects the engine
  and publishes DQ metrics to Kafka.
- ``quarantine.py``: ``QuarantineHandler`` and HITL (Human-in-the-Loop)
  approval workflow functions for invalid-record lifecycle management.
"""

from rlam_airflow_framework.data_quality.engines import (
    ValidationEngine,
    SodaEngine,
)
from rlam_airflow_framework.data_quality.checker import (
    DataQualityChecker,
    run_data_quality_checks,
)
from rlam_airflow_framework.data_quality.quarantine import (
    QuarantineHandler,
    create_hitl_quarantine_approval_task,
    process_hitl_approval_result,
)

__all__ = [
    "ValidationEngine",
    "SodaEngine",
    "DataQualityChecker",
    "run_data_quality_checks",
    "QuarantineHandler",
    "create_hitl_quarantine_approval_task",
    "process_hitl_approval_result",
]
