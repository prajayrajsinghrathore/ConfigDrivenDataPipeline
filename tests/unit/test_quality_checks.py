# File: tests/unit/test_quality_checks.py
"""
Tests for the data-quality check strategy family and registry.

The per-check behavior is largely pinned by the existing tests that drive
``DataQualityChecker._execute_basic_check`` / ``_build_sodacl_yaml``. This file
adds coverage for the gaps (the ``max`` check and ``invalid_percent`` SodaCL) and
locks in the registry / Open-Closed extension point.
"""

import pandas as pd
import pytest

from rlam_airflow_framework.data_quality import (
    CheckOutcome,
    InvalidPercentCheck,
    MaxCheck,
    QUALITY_CHECK_REGISTRY,
    QualityCheck,
    QualityCheckRegistry,
)


class TestRegistry:
    def test_all_builtin_types_registered(self):
        assert QUALITY_CHECK_REGISTRY.supported_types() == [
            "duplicate_count",
            "freshness",
            "invalid_percent",
            "max",
            "missing_count",
            "range",
            "row_count",
            "values_in_set",
        ]

    def test_unknown_type_returns_none(self):
        assert QUALITY_CHECK_REGISTRY.get("nope") is None

    def test_requires_column_flags(self):
        row_count_check = QUALITY_CHECK_REGISTRY.get("row_count")
        missing_count_check = QUALITY_CHECK_REGISTRY.get("missing_count")
        assert row_count_check is not None
        assert missing_count_check is not None
        assert row_count_check.requires_column is False
        assert missing_count_check.requires_column is True


class TestMaxCheck:
    """Gap: the 'max' check type had no direct coverage before this refactor."""

    def test_fails_and_flags_rows_at_or_above_threshold(self):
        df = pd.DataFrame({"v": [1, 5, 10]})
        outcome = MaxCheck().evaluate(df, {"column": "v", "threshold": 5})
        assert outcome.outcome == "fail"
        assert outcome.diagnostics["actual_max"] == 10.0
        # rows >= threshold are flagged (5 and 10)
        assert outcome.invalid_rows is not None
        assert outcome.invalid_rows.tolist() == [False, True, True]

    def test_passes_when_max_below_threshold(self):
        df = pd.DataFrame({"v": [1, 2, 3]})
        outcome = MaxCheck().evaluate(df, {"column": "v", "threshold": 5})
        assert outcome.outcome == "pass"
        assert outcome.invalid_rows is None


class TestInvalidPercentSodaCL:
    """Gap: invalid_percent SodaCL rendering was untested."""

    def test_renders_sodacl_line(self):
        line = InvalidPercentCheck().to_sodacl(
            {"column": "email", "max_percent": 10}
        )
        assert line == "  - invalid_percent(email) < 10%"

    def test_no_column_yields_no_line(self):
        assert InvalidPercentCheck().to_sodacl({}) is None


class TestExtensionPoint:
    def test_custom_check_registered_without_touching_existing_code(self):
        """OCP: a new check is a subclass implementing evaluate + one register()."""

        class AlwaysFailCheck(QualityCheck):
            check_type = "always_fail"
            requires_column = False

            def evaluate(self, df, check):
                return CheckOutcome("fail", {"why": "test"})

        registry = QualityCheckRegistry()
        registry.register(AlwaysFailCheck())

        registered_check = registry.get("always_fail")
        assert registered_check is not None
        outcome = registered_check.evaluate(pd.DataFrame({"a": [1]}), {})
        assert outcome.outcome == "fail"
        assert "always_fail" in registry.supported_types()

    def test_abstract_base_cannot_be_instantiated(self):
        with pytest.raises(TypeError):
            QualityCheck()  # type: ignore[abstract]
