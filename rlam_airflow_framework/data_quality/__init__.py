# File: rlam_airflow_framework/data_quality/__init__.py
"""
Data Quality package: SodaCL-driven validation, quarantine, and HITL approval.

Layout:
- ``base.py`` / ``registry.py`` / one module per check type: the check
  strategy family (Open/Closed dispatch via :data:`QUALITY_CHECK_REGISTRY`).
- ``engines.py``: ValidationEngine strategies (Soda / basic-registry / legacy)
  that evaluate a DataFrame against configured rules.
- ``checker.py``: ``DataQualityChecker``, the facade that selects an engine
  and publishes DQ metrics to Kafka.
- ``quarantine.py``: ``QuarantineHandler`` and HITL (Human-in-the-Loop)
  approval workflow functions for invalid-record lifecycle management.
"""

from rlam_airflow_framework.data_quality.base import CheckOutcome, QualityCheck
from rlam_airflow_framework.data_quality.registry import (
    QualityCheckRegistry,
    QUALITY_CHECK_REGISTRY,
)
from rlam_airflow_framework.data_quality.row_count import RowCountCheck
from rlam_airflow_framework.data_quality.missing_count import MissingCountCheck
from rlam_airflow_framework.data_quality.duplicate_count import DuplicateCountCheck
from rlam_airflow_framework.data_quality.invalid_percent import InvalidPercentCheck
from rlam_airflow_framework.data_quality.max_value import MaxCheck
from rlam_airflow_framework.data_quality.values_in_set import ValuesInSetCheck
from rlam_airflow_framework.data_quality.range_check import RangeCheck
from rlam_airflow_framework.data_quality.freshness import FreshnessCheck

from rlam_airflow_framework.data_quality.engines import (
    ValidationEngine,
    SodaEngine,
    BasicEngine,
    LegacyEngine,
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
    "CheckOutcome",
    "QualityCheck",
    "QualityCheckRegistry",
    "QUALITY_CHECK_REGISTRY",
    "RowCountCheck",
    "MissingCountCheck",
    "DuplicateCountCheck",
    "InvalidPercentCheck",
    "MaxCheck",
    "ValuesInSetCheck",
    "RangeCheck",
    "FreshnessCheck",
    "ValidationEngine",
    "SodaEngine",
    "BasicEngine",
    "LegacyEngine",
    "DataQualityChecker",
    "run_data_quality_checks",
    "QuarantineHandler",
    "create_hitl_quarantine_approval_task",
    "process_hitl_approval_result",
]
