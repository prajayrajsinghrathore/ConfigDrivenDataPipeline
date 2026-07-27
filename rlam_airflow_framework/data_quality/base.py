# File: rlam_airflow_framework/data_quality/base.py
"""
Base abstractions for the basic data-quality check strategies.

Each check type (row_count, missing_count, …) is a :class:`QualityCheck`
subclass that knows how to evaluate itself against a DataFrame and, where
applicable, render itself as a SodaCL line. This replaces the former
``if check_type == …`` chains in ``DataQualityChecker`` (Open/Closed dispatch).
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, Optional

import pandas as pd


@dataclass
class CheckOutcome:
    """
    Result of evaluating one check against a DataFrame.

    Attributes:
        outcome: "pass" or "fail".
        diagnostics: Check-specific diagnostic values (surfaced in DQ metrics).
        invalid_rows: Optional boolean mask of rows to quarantine. None means
            the check is dataset-level (no specific rows to flag).
    """

    outcome: str
    diagnostics: Dict[str, Any] = field(default_factory=dict)
    invalid_rows: Optional[pd.Series] = None


class QualityCheck(ABC):
    """
    Strategy for one basic data-quality check type.

    Subclasses set :attr:`check_type` (the config ``type`` value they handle),
    implement :meth:`evaluate`, and optionally override :meth:`to_sodacl`.
    """

    #: The config ``type`` value this check handles.
    check_type: ClassVar[str]

    #: Whether the check needs a ``column``. Column-less checks with no column
    #: configured are skipped (treated as pass), matching the legacy behavior.
    requires_column: ClassVar[bool] = True

    @abstractmethod
    def evaluate(self, df: pd.DataFrame, check: Dict[str, Any]) -> CheckOutcome:
        """Evaluate the check against ``df`` given its ``check`` config."""
        raise NotImplementedError

    def to_sodacl(self, check: Dict[str, Any]) -> Optional[str]:
        """Render this check as a SodaCL line, or None if it has no SodaCL form."""
        return None
