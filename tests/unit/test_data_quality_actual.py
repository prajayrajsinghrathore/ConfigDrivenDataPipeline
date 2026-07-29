# File: tests/unit/test_data_quality_actual.py
"""
Unit tests for the actual DataQualityChecker implementation.

Tests cover:
- run_checks() main entry point
- _run_basic_checks() without Soda Core
- _execute_basic_check() for each check type
- _build_sodacl_yaml() YAML generation
- _determine_status() quality gate logic
- _publish_dq_metrics() with mocked Kafka
- QuarantineHandler class with HITL support
- HITL quarantine approval functions
- run_data_quality_checks() convenience function
- Edge cases: empty DataFrames, null handling, error paths
"""

import pytest
import pandas as pd
import json
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

# Import actual implementation
from rlam_airflow_framework.data_quality import (
    DataQualityChecker,
    QuarantineHandler,
    run_data_quality_checks,
    create_hitl_quarantine_approval_task,
    process_hitl_approval_result,
)
from rlam_airflow_framework.data_quality.engines import (
    ValidationEngine,
    BasicEngine,
    SodaEngine,
)


class TestDataQualityCheckerInit:
    """Test DataQualityChecker initialization."""

    def test_init_with_minimal_config(self):
        """Test initialization with minimal config."""
        config = {
            "data_source": {"name": "test_source"},
            "validation": {},
        }
        checker = DataQualityChecker(config, "test_dag", "test_task")

        assert checker.dag_id == "test_dag"
        assert checker.task_id == "test_task"
        assert checker.source_name == "test_source"

    def test_init_with_full_config(self):
        """Test initialization with full validation config."""
        config = {
            "data_source": {"name": "full_source"},
            "validation": {
                "soda_checks": {"checks": [{"type": "row_count", "min": 1}]},
                "quality_gates": {"fail_threshold": 0.5, "warn_threshold": 0.9},
            },
            "event": {"topic": "custom-topic"},
        }
        checker = DataQualityChecker(config, "dag1", "task1")

        assert checker.source_name == "full_source"
        assert checker.dq_topic == "custom-topic_dq_metrics"
        assert checker.quality_gates["fail_threshold"] == 0.5

    def test_init_default_source_name(self):
        """Test default source name when not provided."""
        config = {"validation": {}}
        checker = DataQualityChecker(config, "dag", "task")
        assert checker.source_name == "unknown"


class TestDataQualityCheckerRunChecks:
    """Test run_checks() method."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame for testing."""
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", None, "David", "Eve"],
                "price": [10.0, 20.0, 15.0, -5.0, 25.0],
                "category": ["A", "B", "A", "C", "B"],
            }
        )

    @pytest.fixture
    def checker_with_checks(self):
        """Create checker with soda_checks configured."""
        config = {
            "data_source": {"name": "test_source"},
            "validation": {
                "soda_checks": {
                    "checks": [
                        {"type": "row_count", "min": 1},
                        {"type": "missing_count", "column": "name", "max": 0},
                    ]
                },
                "quality_gates": {"fail_threshold": 0.5, "warn_threshold": 0.9},
            },
        }
        return DataQualityChecker(config, "test_dag", "test_task")

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_run_checks_empty_dataframe(self, mock_kafka, tmp_path):
        """Test run_checks with empty DataFrame returns early."""
        config = {"data_source": {"name": "test"}, "validation": {}}
        checker = DataQualityChecker(config, "dag", "task")

        empty_df = pd.DataFrame(columns=["id", "name"])
        df_path = str(tmp_path / "empty.parquet")
        empty_df.to_parquet(df_path)
        valid_path, invalid_path, results = checker.run_checks(df_path)

        assert results["status"] == "skipped"
        assert results["total_rows"] == 0
        assert valid_path == ""
        assert invalid_path == ""

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_run_checks_no_soda_config_uses_legacy(self, mock_kafka, sample_df, tmp_path):
        """Test run_checks falls back to legacy validation when no soda_checks."""
        config = {
            "data_source": {"name": "test"},
            "validation": {},
            "validation_rules": [
                {"field": "price", "type": "numeric", "range": [0, 100]},
            ],
        }
        checker = DataQualityChecker(config, "dag", "task")

        df_path = str(tmp_path / "sample.parquet")
        sample_df.to_parquet(df_path)
        valid_path, invalid_path, results = checker.run_checks(df_path)

        # Should have processed legacy validation rules
        assert results["total_rows"] == 5
        # Price -5.0 is out of range, should fail
        assert results["failed"] > 0


class TestDataQualityCheckerBasicChecks:
    """Test _run_basic_checks() and _execute_basic_check()."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame."""
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", None, "David", "Eve"],
                "price": [10.0, 20.0, 15.0, -5.0, 25.0],
                "email": [
                    "a@test.com",
                    "b@test.com",
                    "c@test.com",
                    "invalid",
                    "e@test.com",
                ],
                "category": ["A", "B", "A", "C", "A"],
                "timestamp": pd.date_range("2026-02-01", periods=5, freq="h"),
            }
        )

    @pytest.fixture
    def checker(self):
        """Create a BasicEngine for testing (owns _execute_basic_check)."""
        return BasicEngine(
            source_name="test_source",
            quality_gates={"fail_threshold": 0.0, "warn_threshold": 0.9},
            soda_checks={"checks": []},
        )

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_row_count_check_pass(self, mock_kafka, checker, sample_df):
        """Test row_count check passes when count exceeds minimum."""
        check = {"type": "row_count", "min": 1}
        invalid_mask = pd.Series([False] * len(sample_df), index=sample_df.index)

        result = checker._execute_basic_check(sample_df, check, invalid_mask)

        assert result["outcome"] == "pass"
        assert result["diagnostics"]["actual"] == 5

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_row_count_check_fail(self, mock_kafka, checker, sample_df):
        """Test row_count check fails when count is below minimum."""
        check = {"type": "row_count", "min": 10}
        invalid_mask = pd.Series([False] * len(sample_df), index=sample_df.index)

        result = checker._execute_basic_check(sample_df, check, invalid_mask)

        assert result["outcome"] == "fail"

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_missing_count_check_pass(self, mock_kafka, checker, sample_df):
        """Test missing_count check passes when no nulls."""
        check = {"type": "missing_count", "column": "id", "max": 0}
        invalid_mask = pd.Series([False] * len(sample_df), index=sample_df.index)

        result = checker._execute_basic_check(sample_df, check, invalid_mask)

        assert result["outcome"] == "pass"
        assert result["diagnostics"]["missing_count"] == 0

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_missing_count_check_fail(self, mock_kafka, checker, sample_df):
        """Test missing_count check fails when nulls exceed max."""
        check = {"type": "missing_count", "column": "name", "max": 0}
        invalid_mask = pd.Series([False] * len(sample_df), index=sample_df.index)

        result = checker._execute_basic_check(sample_df, check, invalid_mask)

        assert result["outcome"] == "fail"
        assert result["diagnostics"]["missing_count"] == 1
        # Check that invalid_mask was updated
        assert invalid_mask.sum() == 1

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_duplicate_count_check_pass(self, mock_kafka, checker, sample_df):
        """Test duplicate_count check passes when no duplicates."""
        check = {"type": "duplicate_count", "column": "id", "max": 0}
        invalid_mask = pd.Series([False] * len(sample_df), index=sample_df.index)

        result = checker._execute_basic_check(sample_df, check, invalid_mask)

        assert result["outcome"] == "pass"
        assert result["diagnostics"]["duplicate_count"] == 0

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_duplicate_count_check_fail(self, mock_kafka, checker):
        """Test duplicate_count check fails when duplicates exist."""
        df_with_dups = pd.DataFrame(
            {
                "id": [1, 1, 2, 3, 3],
            }
        )
        check = {"type": "duplicate_count", "column": "id", "max": 0}
        invalid_mask = pd.Series([False] * len(df_with_dups), index=df_with_dups.index)

        result = checker._execute_basic_check(df_with_dups, check, invalid_mask)

        assert result["outcome"] == "fail"
        assert result["diagnostics"]["duplicate_count"] == 2

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_invalid_percent_check_with_regex(self, mock_kafka, checker, sample_df):
        """Test invalid_percent check with regex pattern."""
        check = {
            "type": "invalid_percent",
            "column": "email",
            "max_percent": 10,
            "valid_regex": r".*@.*\..*",
        }
        invalid_mask = pd.Series([False] * len(sample_df), index=sample_df.index)

        result = checker._execute_basic_check(sample_df, check, invalid_mask)

        # "invalid" email should fail regex, that's 20% (1/5)
        assert result["outcome"] == "fail"
        assert result["diagnostics"]["invalid_percent"] == 20.0

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_values_in_set_check_pass(self, mock_kafka, checker):
        """Test values_in_set check passes when all values are valid."""
        df = pd.DataFrame({"status": ["A", "B", "A"]})
        check = {
            "type": "values_in_set",
            "column": "status",
            "valid_values": ["A", "B", "C"],
        }
        invalid_mask = pd.Series([False] * len(df), index=df.index)

        result = checker._execute_basic_check(df, check, invalid_mask)

        assert result["outcome"] == "pass"

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_values_in_set_check_fail(self, mock_kafka, checker):
        """Test values_in_set check fails when invalid values exist."""
        df = pd.DataFrame({"status": ["A", "X", "B"]})
        check = {
            "type": "values_in_set",
            "column": "status",
            "valid_values": ["A", "B", "C"],
        }
        invalid_mask = pd.Series([False] * len(df), index=df.index)

        result = checker._execute_basic_check(df, check, invalid_mask)

        assert result["outcome"] == "fail"
        assert result["diagnostics"]["invalid_count"] == 1

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_range_check_pass(self, mock_kafka, checker):
        """Test range check passes when values are in range."""
        df = pd.DataFrame({"value": [10, 20, 30]})
        check = {"type": "range", "column": "value", "min": 0, "max": 100}
        invalid_mask = pd.Series([False] * len(df), index=df.index)

        result = checker._execute_basic_check(df, check, invalid_mask)

        assert result["outcome"] == "pass"

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_range_check_fail(self, mock_kafka, checker):
        """Test range check fails when values are out of range."""
        df = pd.DataFrame({"value": [10, 150, 30]})
        check = {"type": "range", "column": "value", "min": 0, "max": 100}
        invalid_mask = pd.Series([False] * len(df), index=df.index)

        result = checker._execute_basic_check(df, check, invalid_mask)

        assert result["outcome"] == "fail"
        assert result["diagnostics"]["invalid_count"] == 1

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_freshness_check_pass(self, mock_kafka, checker):
        """Test freshness check passes when data is recent."""
        df = pd.DataFrame(
            {"timestamp": [datetime.now(timezone.utc) - timedelta(hours=1)]}
        )
        check = {"type": "freshness", "column": "timestamp", "max_hours": 24}
        invalid_mask = pd.Series([False] * len(df), index=df.index)

        result = checker._execute_basic_check(df, check, invalid_mask)

        assert result["outcome"] == "pass"

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_freshness_check_fail(self, mock_kafka, checker):
        """Test freshness check fails when data is stale."""
        df = pd.DataFrame(
            {"timestamp": [datetime.now(timezone.utc) - timedelta(hours=48)]}
        )
        check = {"type": "freshness", "column": "timestamp", "max_hours": 24}
        invalid_mask = pd.Series([False] * len(df), index=df.index)

        result = checker._execute_basic_check(df, check, invalid_mask)

        assert result["outcome"] == "fail"
        assert result["diagnostics"]["age_hours"] > 24

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_check_with_error_handling(self, mock_kafka, checker):
        """Test check handles errors gracefully."""
        df = pd.DataFrame({"value": [1, 2, 3]})
        # Invalid check configuration
        check = {"type": "range", "column": "nonexistent_column", "min": 0, "max": 100}
        invalid_mask = pd.Series([False] * len(df), index=df.index)

        result = checker._execute_basic_check(df, check, invalid_mask)

        assert result["outcome"] == "error"
        assert "error" in result["diagnostics"]


class TestDataQualityCheckerBuildSodaCL:
    """Test SodaEngine._build_sodacl_yaml() method."""

    def test_build_sodacl_empty_checks(self):
        """Test building SodaCL with no checks returns None."""
        engine = SodaEngine("test", {}, {"checks": []})

        result = engine._build_sodacl_yaml()
        assert result is None

    def test_build_sodacl_row_count(self):
        """Test building SodaCL for row_count check."""
        engine = SodaEngine(
            "test_table", {}, {"checks": [{"type": "row_count", "min": 100}]}
        )

        result = engine._build_sodacl_yaml()
        assert result is not None
        assert "checks for test_table:" in result
        assert "row_count > 100" in result

    def test_build_sodacl_missing_count(self):
        """Test building SodaCL for missing_count check."""
        engine = SodaEngine(
            "test_table",
            {},
            {"checks": [{"type": "missing_count", "column": "email", "max": 0}]},
        )

        result = engine._build_sodacl_yaml()
        assert result is not None
        assert "missing_count(email) = 0" in result

    def test_build_sodacl_duplicate_count(self):
        """Test building SodaCL for duplicate_count check."""
        engine = SodaEngine(
            "test_table",
            {},
            {"checks": [{"type": "duplicate_count", "column": "id", "max": 0}]},
        )

        result = engine._build_sodacl_yaml()
        assert result is not None
        assert "duplicate_count(id) = 0" in result

    def test_build_sodacl_multiple_checks(self):
        """Test building SodaCL with multiple checks."""
        engine = SodaEngine(
            "test_table",
            {},
            {
                "checks": [
                    {"type": "row_count", "min": 1},
                    {"type": "missing_count", "column": "id", "max": 0},
                    {"type": "duplicate_count", "column": "id", "max": 0},
                ]
            },
        )

        result = engine._build_sodacl_yaml()
        assert result is not None
        assert "row_count > 1" in result
        assert "missing_count(id) = 0" in result
        assert "duplicate_count(id) = 0" in result


class TestDataQualityCheckerQualityGates:
    """Test ValidationEngine._determine_status() quality gate logic."""

    def test_determine_status_passed(self):
        """Test status is 'passed' when pass_rate exceeds warn_threshold."""
        engine = ValidationEngine(
            "test", {"fail_threshold": 0.5, "warn_threshold": 0.9}
        )

        results = {"pass_rate": 0.95}
        status = engine._determine_status(results)
        assert status == "passed"

    def test_determine_status_warning(self):
        """Test status is 'warning' when pass_rate is between thresholds."""
        engine = ValidationEngine(
            "test", {"fail_threshold": 0.5, "warn_threshold": 0.9}
        )

        results = {"pass_rate": 0.7}
        status = engine._determine_status(results)
        assert status == "warning"

    def test_determine_status_failed(self):
        """Test status is 'failed' when pass_rate is below fail_threshold."""
        engine = ValidationEngine(
            "test", {"fail_threshold": 0.5, "warn_threshold": 0.9}
        )

        results = {"pass_rate": 0.3}
        status = engine._determine_status(results)
        assert status == "failed"

    def test_determine_status_default_thresholds(self):
        """Test default thresholds when not specified."""
        engine = ValidationEngine("test", {})

        # Default: fail_threshold=0.0, warn_threshold=0.95
        results = {"pass_rate": 0.5}
        status = engine._determine_status(results)
        # 0.5 is above fail (0.0) but below warn (0.95)
        assert status == "warning"


class TestDataQualityCheckerPublishMetrics:
    """Test _publish_dq_metrics() with mocked Kafka."""

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_publish_dq_metrics_success(self, mock_kafka):
        """Test successful metrics publishing."""
        config = {
            "data_source": {"name": "test"},
            "validation": {},
            "event": {"topic": "test-topic"},
        }
        checker = DataQualityChecker(config, "test_dag", "test_task")

        results = {"status": "passed", "pass_rate": 1.0}
        checker._publish_dq_metrics(results)

        mock_kafka.publish_pipeline_event.assert_called_once()
        call_args = mock_kafka.publish_pipeline_event.call_args
        assert call_args.kwargs["dag_id"] == "test_dag"
        assert call_args.kwargs["task_id"] == "test_task"
        assert call_args.kwargs["event_type"] == "data_quality_metrics"
        assert call_args.kwargs["topic"] == "test-topic_dq_metrics"
        assert call_args.kwargs["metadata"] == results

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_publish_dq_metrics_failure_handled(self, mock_kafka):
        """Test metrics publishing failure is handled gracefully."""
        mock_kafka.publish_pipeline_event.side_effect = Exception(
            "Kafka connection failed"
        )

        config = {
            "data_source": {"name": "test"},
            "validation": {},
        }
        checker = DataQualityChecker(config, "dag", "task")

        # Should not raise exception
        checker._publish_dq_metrics({"status": "passed"})


class TestDataQualityCheckerHelperMethods:
    """Test helper methods."""

    def test_should_fail_pipeline_true(self):
        """Test should_fail_pipeline returns True when status is failed."""
        config = {"data_source": {"name": "test"}, "validation": {}}
        checker = DataQualityChecker(config, "dag", "task")

        results = {"status": "failed"}
        assert checker.should_fail_pipeline(results) is True

    def test_should_fail_pipeline_false(self):
        """Test should_fail_pipeline returns False for other statuses."""
        config = {"data_source": {"name": "test"}, "validation": {}}
        checker = DataQualityChecker(config, "dag", "task")

        assert checker.should_fail_pipeline({"status": "passed"}) is False
        assert checker.should_fail_pipeline({"status": "warning"}) is False

    def test_should_quarantine_true(self):
        """Test should_quarantine returns True when enabled."""
        config = {
            "data_source": {"name": "test"},
            "validation": {"quality_gates": {"quarantine_invalid": True}},
        }
        checker = DataQualityChecker(config, "dag", "task")

        assert checker.should_quarantine() is True

    def test_should_quarantine_false(self):
        """Test should_quarantine returns False when disabled."""
        config = {
            "data_source": {"name": "test"},
            "validation": {"quality_gates": {"quarantine_invalid": False}},
        }
        checker = DataQualityChecker(config, "dag", "task")

        assert checker.should_quarantine() is False

    def test_empty_results(self):
        """Test _empty_results returns proper structure."""
        config = {"data_source": {"name": "test"}, "validation": {}}
        checker = DataQualityChecker(config, "dag", "task")

        results = checker._empty_results()

        assert results["status"] == "skipped"
        assert results["total_rows"] == 0
        assert results["pass_rate"] == 1.0
        assert "scan_id" in results
        assert "timestamp" in results


class TestQuarantineHandler:
    """Test QuarantineHandler class."""

    def test_init_with_quarantine_config(self):
        """Test initialization with quarantine configuration."""
        config = {
            "destination": {
                "quarantine": {
                    "type": "snowflake_table",
                    "table": "CUSTOM.QUARANTINE",
                }
            }
        }
        handler = QuarantineHandler(config)

        assert handler.quarantine_config["table"] == "CUSTOM.QUARANTINE"

    def test_init_without_quarantine_config(self):
        """Test initialization without quarantine configuration."""
        config = {"destination": {}}
        handler = QuarantineHandler(config)

        assert handler.default_table == "DQ_AUDIT.QUARANTINE_RECORDS"

    def test_prepare_quarantine_records(self):
        """Test preparing quarantine records with metadata."""
        config = {"destination": {}}
        handler = QuarantineHandler(config)

        invalid_df = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
            }
        )

        result = handler.prepare_quarantine_records(
            invalid_df=invalid_df,
            source_pipeline="test_pipeline",
            source_table="test_table",
            failed_checks=["null_check", "range_check"],
        )

        assert len(result) == 2
        assert "quarantine_id" in result.columns
        assert "source_pipeline" in result.columns
        assert "failed_checks" in result.columns
        assert "record_data" in result.columns
        assert "quarantined_at" in result.columns

        # Check first record
        assert result.iloc[0]["source_pipeline"] == "test_pipeline"
        assert result.iloc[0]["source_table"] == "test_table"
        assert json.loads(result.iloc[0]["failed_checks"]) == [
            "null_check",
            "range_check",
        ]

    def test_prepare_quarantine_records_empty_df(self):
        """Test preparing quarantine records with empty DataFrame."""
        config = {"destination": {}}
        handler = QuarantineHandler(config)

        result = handler.prepare_quarantine_records(
            invalid_df=pd.DataFrame(),
            source_pipeline="test",
            source_table="test",
            failed_checks=[],
        )

        assert result.empty

    def test_get_quarantine_destination_custom(self):
        """Test getting custom quarantine destination."""
        config = {
            "destination": {
                "quarantine": {
                    "type": "snowflake_table",
                    "table": "CUSTOM.QUARANTINE",
                    "write_mode": "append",
                }
            }
        }
        handler = QuarantineHandler(config)

        dest = handler.get_quarantine_destination()
        assert dest["table"] == "CUSTOM.QUARANTINE"

    def test_get_quarantine_destination_default(self):
        """Test getting default quarantine destination."""
        config = {"destination": {}}
        handler = QuarantineHandler(config)

        dest = handler.get_quarantine_destination()
        assert dest["type"] == "snowflake_table"
        assert dest["table"] == "DQ_AUDIT.QUARANTINE_RECORDS"


class TestRunDataQualityChecksConvenience:
    """Test run_data_quality_checks() convenience function."""

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_run_data_quality_checks(self, mock_kafka, tmp_path):
        """Test convenience function creates checker and runs checks."""
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        df_path = str(tmp_path / "data.parquet")
        df.to_parquet(df_path)
        config = {
            "data_source": {"name": "test"},
            "validation": {
                "soda_checks": {"checks": [{"type": "row_count", "min": 1}]}
            },
        }

        valid_df, invalid_df, results = run_data_quality_checks(
            df_path=df_path,
            config=config,
            dag_id="test_dag",
            task_id="test_task",
            destination_table="test_table",
        )

        assert results["total_rows"] == 3
        assert results["destination_table"] == "test_table"


class TestDataQualityCheckerLegacyValidation:
    """Test legacy validation rules processing."""

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_legacy_numeric_range_validation(self, mock_kafka, tmp_path):
        """Test legacy numeric range validation."""
        df = pd.DataFrame(
            {
                "price": [10, 50, 150, 200],  # 150 and 200 are out of range
            }
        )
        df_path = str(tmp_path / "prices.parquet")
        df.to_parquet(df_path)
        config = {
            "data_source": {"name": "test"},
            "validation": {},
            "validation_rules": [
                {"field": "price", "type": "numeric", "range": [0, 100]},
            ],
        }
        checker = DataQualityChecker(config, "dag", "task")

        valid_path, invalid_path, results = checker.run_checks(df_path)

        # Legacy engine does not persist a separate quarantine file; it only
        # reports the split via results, and returns the original path as valid.
        assert results["invalid_rows"] == 2  # 150 and 200
        assert results["valid_rows"] == 2  # 10 and 50
        assert invalid_path == ""
        assert valid_path == df_path

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_legacy_string_max_length_validation(self, mock_kafka, tmp_path):
        """Test legacy string max_length validation."""
        df = pd.DataFrame(
            {
                "name": ["Alice", "Bob", "Christopher"],  # Christopher exceeds 5
            }
        )
        df_path = str(tmp_path / "names.parquet")
        df.to_parquet(df_path)
        config = {
            "data_source": {"name": "test"},
            "validation": {},
            "validation_rules": [
                {"field": "name", "type": "string", "max_length": 5},
            ],
        }
        checker = DataQualityChecker(config, "dag", "task")

        valid_path, invalid_path, results = checker.run_checks(df_path)

        assert results["invalid_rows"] == 1  # Christopher
        assert results["valid_rows"] == 2
        assert invalid_path == ""
        assert valid_path == df_path

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_legacy_missing_required_field(self, mock_kafka, tmp_path):
        """Test legacy validation with missing required field."""
        df = pd.DataFrame(
            {
                "name": ["Alice", "Bob"],
            }
        )
        df_path = str(tmp_path / "names.parquet")
        df.to_parquet(df_path)
        config = {
            "data_source": {"name": "test"},
            "validation": {},
            "validation_rules": [
                {"field": "email", "type": "string", "required": True},
            ],
        }
        checker = DataQualityChecker(config, "dag", "task")

        valid_path, invalid_path, results = checker.run_checks(df_path)

        # Should have a failed check for missing required field
        assert results["failed"] >= 1

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_legacy_no_validation_rules_skipped(self, mock_kafka, tmp_path):
        """Test legacy validation skips when no rules defined."""
        df = pd.DataFrame({"id": [1, 2, 3]})
        df_path = str(tmp_path / "ids.parquet")
        df.to_parquet(df_path)
        config = {
            "data_source": {"name": "test"},
            "validation": {},
            "validation_rules": [],
        }
        checker = DataQualityChecker(config, "dag", "task")

        valid_path, invalid_path, results = checker.run_checks(df_path)

        assert results["status"] == "skipped"


class TestDataQualityCheckerEdgeCases:
    """Test edge cases and error handling."""

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_unicode_data_handling(self, mock_kafka, tmp_path):
        """Test handling of Unicode characters in data."""
        df = pd.DataFrame(
            {
                "name": ["日本語", "العربية", "עברית", "中文"],
            }
        )
        df_path = str(tmp_path / "unicode.parquet")
        df.to_parquet(df_path)
        config = {
            "data_source": {"name": "test"},
            "validation": {
                "soda_checks": {"checks": [{"type": "row_count", "min": 1}]}
            },
        }
        checker = DataQualityChecker(config, "dag", "task")

        valid_path, invalid_path, results = checker.run_checks(df_path)

        assert results["total_rows"] == 4
        assert results["valid_rows"] == 4
        assert valid_path == df_path

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_null_heavy_dataframe(self, mock_kafka, tmp_path):
        """Test DataFrame with many null values."""
        df = pd.DataFrame(
            {
                "id": [1, None, None, 4, None],
                "name": [None, None, "Charlie", None, None],
            }
        )
        df_path = str(tmp_path / "nulls.parquet")
        df.to_parquet(df_path)
        config = {
            "data_source": {"name": "test"},
            "validation": {
                "soda_checks": {
                    "checks": [
                        {"type": "missing_count", "column": "id", "max": 1},
                    ]
                }
            },
        }
        checker = DataQualityChecker(config, "dag", "task")

        valid_path, invalid_path, results = checker.run_checks(df_path)

        # Verify the check was executed and results returned
        assert results["total_rows"] == 5
        assert results["status"] in ("passed", "failed", "warning")

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_special_characters_in_column_names(self, mock_kafka, tmp_path):
        """Test handling columns with special characters."""
        df = pd.DataFrame(
            {
                "column with spaces": [1, 2, 3],
                "column-with-dashes": [4, 5, 6],
            }
        )
        df_path = str(tmp_path / "special_cols.parquet")
        df.to_parquet(df_path)
        config = {
            "data_source": {"name": "test"},
            "validation": {
                "soda_checks": {"checks": [{"type": "row_count", "min": 1}]}
            },
        }
        checker = DataQualityChecker(config, "dag", "task")

        valid_path, invalid_path, results = checker.run_checks(df_path)

        assert results["total_rows"] == 3


# =============================================================================
# HITL (Human-in-the-Loop) Quarantine Tests - Airflow 3.1.6
# =============================================================================


class TestQuarantineHandlerHITL:
    """Test QuarantineHandler HITL functionality (Airflow 3.1.6)."""

    def test_init_with_hitl_enabled(self):
        """Test initialization with HITL enabled."""
        config = {
            "destination": {
                "quarantine": {
                    "type": "snowflake_table",
                    "table": "TEST.QUARANTINE",
                    "hitl": {
                        "enabled": True,
                        "timeout_hours": 48,
                        "allowed_roles": ["data-steward"],
                    },
                }
            }
        }
        handler = QuarantineHandler(config)

        assert handler.hitl_enabled is True
        assert handler.hitl_timeout_hours == 48
        assert handler.hitl_config.get("allowed_roles") == ["data-steward"]

    def test_init_with_hitl_disabled_by_default(self):
        """Test HITL is disabled by default."""
        config = {
            "destination": {
                "quarantine": {"type": "snowflake_table", "table": "TEST.QUARANTINE"}
            }
        }
        handler = QuarantineHandler(config)

        assert handler.hitl_enabled is False
        assert handler.hitl_timeout_hours == 24  # default

    def test_requires_hitl_approval_when_enabled(self):
        """Test requires_hitl_approval returns True when HITL enabled."""
        config = {"destination": {"quarantine": {"hitl": {"enabled": True}}}}
        handler = QuarantineHandler(config)

        assert handler.requires_hitl_approval() is True

    def test_requires_hitl_approval_when_disabled(self):
        """Test requires_hitl_approval returns False when HITL disabled."""
        config = {"destination": {}}
        handler = QuarantineHandler(config)

        assert handler.requires_hitl_approval() is False

    def test_prepare_records_sets_pending_status_when_hitl_enabled(self):
        """Test quarantine records get 'pending' status when HITL enabled."""
        config = {"destination": {"quarantine": {"hitl": {"enabled": True}}}}
        handler = QuarantineHandler(config)

        invalid_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        result = handler.prepare_quarantine_records(
            invalid_df=invalid_df,
            source_pipeline="test_pipeline",
            source_table="test_table",
            failed_checks=["null_check"],
        )

        assert len(result) == 2
        assert all(result["approval_status"] == "pending")

    def test_prepare_records_sets_auto_approved_when_hitl_disabled(self):
        """Test quarantine records get 'auto_approved' status when HITL disabled."""
        config = {"destination": {}}
        handler = QuarantineHandler(config)

        invalid_df = pd.DataFrame({"id": [1], "name": ["Alice"]})
        result = handler.prepare_quarantine_records(
            invalid_df=invalid_df,
            source_pipeline="test_pipeline",
            source_table="test_table",
            failed_checks=["null_check"],
        )

        assert len(result) == 1
        assert result.iloc[0]["approval_status"] == "auto_approved"


class TestCreateHitlQuarantineApprovalTask:
    """Test create_hitl_quarantine_approval_task function."""

    @pytest.fixture
    def sample_quarantine_df(self):
        """Create sample quarantine DataFrame."""
        return pd.DataFrame(
            {
                "quarantine_id": ["q1", "q2", "q3"],
                "source_pipeline": ["test_dag", "test_dag", "test_dag"],
                "source_table": ["TEST.DATA", "TEST.DATA", "TEST.DATA"],
                "failed_checks": [
                    json.dumps(["null_check", "range_check"]),
                    json.dumps(["range_check"]),
                    json.dumps(["null_check"]),
                ],
                "record_data": [
                    json.dumps({"id": 1, "value": -5}),
                    json.dumps({"id": 2, "value": 999999}),
                    json.dumps({"id": 3, "value": None}),
                ],
            }
        )

    @pytest.fixture
    def hitl_config(self):
        """Create HITL-enabled config."""
        return {
            "destination": {
                "quarantine": {
                    "hitl": {
                        "enabled": True,
                        "timeout_hours": 12,
                        "allowed_roles": ["data-steward", "admin"],
                    }
                }
            }
        }

    def test_builds_correct_summary_structure(self, sample_quarantine_df, hitl_config):
        """Test HITL context has correct summary structure."""
        result = create_hitl_quarantine_approval_task(
            dag_id="test_dag",
            quarantine_records=sample_quarantine_df,
            config=hitl_config,
        )

        assert "quarantine_summary" in result
        summary = result["quarantine_summary"]
        assert summary["dag_id"] == "test_dag"
        assert summary["total_records"] == 3
        assert "quarantine_time" in summary

    def test_extracts_unique_failed_checks(self, sample_quarantine_df, hitl_config):
        """Test unique failed checks are extracted from records."""
        result = create_hitl_quarantine_approval_task(
            dag_id="test_dag",
            quarantine_records=sample_quarantine_df,
            config=hitl_config,
        )

        failed_checks = result["quarantine_summary"]["failed_checks"]
        # Should have unique checks: null_check, range_check
        assert "null_check" in failed_checks
        assert "range_check" in failed_checks
        assert len(failed_checks) == 2

    def test_includes_sample_records(self, sample_quarantine_df, hitl_config):
        """Test sample records are included for review."""
        result = create_hitl_quarantine_approval_task(
            dag_id="test_dag",
            quarantine_records=sample_quarantine_df,
            config=hitl_config,
        )

        sample_records = result["quarantine_summary"]["sample_records"]
        assert len(sample_records) == 3  # all 3 records (less than 5)

    def test_limits_sample_to_five_records(self, hitl_config):
        """Test sample records are limited to 5."""
        large_df = pd.DataFrame(
            {
                "quarantine_id": [f"q{i}" for i in range(10)],
                "source_pipeline": ["test"] * 10,
                "failed_checks": [json.dumps(["check"]) for _ in range(10)],
                "record_data": [json.dumps({"id": i}) for i in range(10)],
            }
        )

        result = create_hitl_quarantine_approval_task(
            dag_id="test_dag", quarantine_records=large_df, config=hitl_config
        )

        assert len(result["quarantine_summary"]["sample_records"]) == 5

    def test_returns_correct_form_fields(self, sample_quarantine_df, hitl_config):
        """Test approval form fields are correctly structured."""
        result = create_hitl_quarantine_approval_task(
            dag_id="test_dag",
            quarantine_records=sample_quarantine_df,
            config=hitl_config,
        )

        form_fields = result["approval_form_fields"]
        assert "action" in form_fields
        assert form_fields["action"]["type"] == "select"
        assert "approve_release" in form_fields["action"]["options"]
        assert "reject_release" in form_fields["action"]["options"]
        assert "reprocess" in form_fields["action"]["options"]
        assert "notes" in form_fields

    def test_uses_configured_timeout_hours(self, sample_quarantine_df, hitl_config):
        """Test timeout_hours comes from config."""
        result = create_hitl_quarantine_approval_task(
            dag_id="test_dag",
            quarantine_records=sample_quarantine_df,
            config=hitl_config,
        )

        assert result["timeout_hours"] == 12

    def test_handles_empty_quarantine_records(self, hitl_config):
        """Test handles empty DataFrame gracefully."""
        empty_df = pd.DataFrame(columns=["id", "name"])

        result = create_hitl_quarantine_approval_task(
            dag_id="test_dag", quarantine_records=empty_df, config=hitl_config
        )

        assert result["quarantine_summary"]["total_records"] == 0
        assert result["quarantine_summary"]["sample_records"] == []


class TestProcessHitlApprovalResult:
    """Test process_hitl_approval_result function."""

    @pytest.fixture
    def sample_quarantine_df(self):
        """Create sample quarantine DataFrame for processing."""
        return pd.DataFrame(
            {
                "quarantine_id": ["q1", "q2"],
                "source_pipeline": ["test_dag", "test_dag"],
                "source_table": ["TEST.DATA", "TEST.DATA"],
                "approval_status": ["pending", "pending"],
                "reprocessed": [False, False],
            }
        )

    @pytest.fixture
    def config(self):
        """Create config for processing."""
        return {
            "data_source": {"name": "test_source"},
            "event": {"topic": "test-events"},
        }

    def test_approve_release_updates_status(self, sample_quarantine_df, config, mocker):
        """Test approve_release sets correct status."""
        mocker.patch("rlam_airflow_framework.data_quality.quarantine.kafka_publisher")

        approval = {
            "action": "approve_release",
            "approved_by": "test_steward",
            "notes": "Approved after review",
        }

        result_df, action = process_hitl_approval_result(
            approval_result=approval,
            quarantine_records=sample_quarantine_df,
            config=config,
        )

        assert action == "approve_release"
        assert all(result_df["approval_status"] == "approved")
        assert all(result_df["reprocessed"])
        assert all(result_df["approved_by"] == "test_steward")

    def test_reject_release_updates_status(self, sample_quarantine_df, config, mocker):
        """Test reject_release sets correct status."""
        mocker.patch("rlam_airflow_framework.data_quality.quarantine.kafka_publisher")

        approval = {
            "action": "reject_release",
            "approved_by": "test_steward",
            "notes": "Data quality issues not resolved",
        }

        result_df, action = process_hitl_approval_result(
            approval_result=approval,
            quarantine_records=sample_quarantine_df,
            config=config,
        )

        assert action == "reject_release"
        assert all(result_df["approval_status"] == "rejected")
        assert all(result_df["approved_by"] == "test_steward")

    def test_reprocess_sets_pending_status(self, sample_quarantine_df, config, mocker):
        """Test reprocess action sets pending_reprocess status."""
        mocker.patch("rlam_airflow_framework.data_quality.quarantine.kafka_publisher")

        approval = {
            "action": "reprocess",
            "approved_by": "test_steward",
            "notes": "Rerun with updated rules",
        }

        result_df, action = process_hitl_approval_result(
            approval_result=approval,
            quarantine_records=sample_quarantine_df,
            config=config,
        )

        assert action == "reprocess"
        assert all(result_df["approval_status"] == "pending_reprocess")

    def test_updates_approval_metadata(self, sample_quarantine_df, config, mocker):
        """Test approval metadata is correctly set."""
        mocker.patch("rlam_airflow_framework.data_quality.quarantine.kafka_publisher")

        approval = {
            "action": "approve_release",
            "approved_by": "data_steward_1",
            "notes": "LGTM",
        }

        result_df, _ = process_hitl_approval_result(
            approval_result=approval,
            quarantine_records=sample_quarantine_df,
            config=config,
        )

        assert all(result_df["approved_by"] == "data_steward_1")
        assert all(result_df["approval_notes"] == "LGTM")
        # approved_at should be set
        assert all(result_df["approved_at"].notna())

    def test_publishes_kafka_event(self, sample_quarantine_df, config, mocker):
        """Test Kafka event is published on approval."""
        mock_kafka = mocker.patch("rlam_airflow_framework.data_quality.quarantine.kafka_publisher")

        approval = {
            "action": "approve_release",
            "approved_by": "test_steward",
            "notes": "",
        }

        process_hitl_approval_result(
            approval_result=approval,
            quarantine_records=sample_quarantine_df,
            config=config,
        )

        mock_kafka.publish_pipeline_event.assert_called_once()
        call_kwargs = mock_kafka.publish_pipeline_event.call_args.kwargs
        assert call_kwargs["event_type"] == "hitl_quarantine_decision"
        assert call_kwargs["metadata"]["action"] == "approve_release"
        assert call_kwargs["metadata"]["record_count"] == 2

    def test_handles_kafka_publish_failure(self, sample_quarantine_df, config, mocker):
        """Test gracefully handles Kafka publish failure."""
        mock_kafka = mocker.patch("rlam_airflow_framework.data_quality.quarantine.kafka_publisher")
        mock_kafka.publish_pipeline_event.side_effect = Exception("Kafka down")

        approval = {
            "action": "approve_release",
            "approved_by": "test_steward",
            "notes": "",
        }

        # Should not raise, just log warning
        result_df, action = process_hitl_approval_result(
            approval_result=approval,
            quarantine_records=sample_quarantine_df,
            config=config,
        )

        # Result should still be processed
        assert action == "approve_release"
        assert all(result_df["approval_status"] == "approved")

