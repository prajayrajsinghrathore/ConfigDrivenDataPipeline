# File: rlam_airflow_framework/data_quality/registry.py
"""Registry of basic data-quality check strategies."""

from typing import List, Optional

from rlam_airflow_framework.data_quality.base import QualityCheck
from rlam_airflow_framework.data_quality.row_count import RowCountCheck
from rlam_airflow_framework.data_quality.missing_count import MissingCountCheck
from rlam_airflow_framework.data_quality.duplicate_count import DuplicateCountCheck
from rlam_airflow_framework.data_quality.invalid_percent import InvalidPercentCheck
from rlam_airflow_framework.data_quality.max_value import MaxCheck
from rlam_airflow_framework.data_quality.values_in_set import ValuesInSetCheck
from rlam_airflow_framework.data_quality.range_check import RangeCheck
from rlam_airflow_framework.data_quality.freshness import FreshnessCheck


class QualityCheckRegistry:
    """
    Registry mapping a check config ``type`` to a QualityCheck strategy.

    Lookup returns None for unknown types (an unknown check is skipped and
    treated as a pass, matching the legacy dispatch that had no ``else`` branch).
    """

    def __init__(self) -> None:
        self._checks = {}

    def register(self, check: QualityCheck) -> None:
        """Register (or override) the strategy for ``check.check_type``."""
        self._checks[check.check_type] = check

    def get(self, check_type: Optional[str]) -> Optional[QualityCheck]:
        """Return the strategy for ``check_type``, or None if unregistered."""
        return self._checks.get(check_type)

    def supported_types(self) -> List[str]:
        """Return the registered check types (sorted)."""
        return sorted(self._checks)


def _build_default_registry() -> QualityCheckRegistry:
    """Construct the registry with all built-in checks registered."""
    registry = QualityCheckRegistry()
    for check in (
        RowCountCheck(),
        MissingCountCheck(),
        DuplicateCountCheck(),
        InvalidPercentCheck(),
        MaxCheck(),
        ValuesInSetCheck(),
        RangeCheck(),
        FreshnessCheck(),
    ):
        registry.register(check)
    return registry


#: Process-wide default registry used by DataQualityChecker.
QUALITY_CHECK_REGISTRY = _build_default_registry()
