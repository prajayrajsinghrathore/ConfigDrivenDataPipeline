# File: tests/unit/test_data_quality_actual.py
"""
Unit tests for the actual DataQualityChecker implementation.

This covers the integrated data quality checks logic,
including engine selection, validation rules processing,
and metrics publishing.
"""

import pytest
import polars as pl
import json
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
    SodaEngine,
)

# =============================================================================
# SodaEngine Tests
# =============================================================================


class TestSodaEngine:
    """Test SodaEngine functionality."""

    def test_build_sodacl_row_count(self):
        """Test building SodaCL for row_count check."""
        engine = SodaEngine(
            "test_table",
            {},
            {"checks": [{"type": "row_count", "min": 100}]},
        )
        result = engine._build_sodacl_yaml()
        assert result and "dataset: pipeline_duckdb/main/dq_candidate" in result
        assert result and "row_count:" in result
        assert result and "must_be_greater_than_or_equal_to: 100" in result

    def test_build_sodacl_missing_count(self):
        """Test building SodaCL for missing_count check."""
        engine = SodaEngine(
            "test_table",
            {},
            {"checks": [{"type": "missing_count", "column": "email", "max": 0}]},
        )

        result = engine._build_sodacl_yaml()
        assert result and "dataset: pipeline_duckdb/main/dq_candidate" in result
        assert result and "- name: email" in result
        assert result and "- missing:" in result
        assert result and "must_be_less_than_or_equal_to: 0" in result

    def test_build_sodacl_duplicate_count(self):
        """Test building SodaCL for duplicate_count check."""
        engine = SodaEngine(
            "test_table",
            {},
            {"checks": [{"type": "duplicate_count", "column": "id", "max": 0}]},
        )

        result = engine._build_sodacl_yaml()
        assert result and "dataset: pipeline_duckdb/main/dq_candidate" in result
        assert result and "- name: id" in result
        assert result and "- duplicate:" in result
        assert result and "must_be_less_than_or_equal_to: 0" in result

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
        assert result and "dataset: pipeline_duckdb/main/dq_candidate" in result
        assert result and "row_count:" in result
        assert result and "- name: id" in result
        assert result and "- missing:" in result
        assert result and "- duplicate:" in result


# =============================================================================
# ValidationEngine Quality Gates
# =============================================================================


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

        results = {"pass_rate": 0.75}
        status = engine._determine_status(results)
        assert status == "warning"

    def test_determine_status_failed(self):
        """Test status is 'failed' when pass_rate is below fail_threshold."""
        engine = ValidationEngine(
            "test", {"fail_threshold": 0.5, "warn_threshold": 0.9}
        )

        results = {"pass_rate": 0.4}
        status = engine._determine_status(results)
        assert status == "failed"

    def test_determine_status_edge_cases(self):
        """Test threshold boundary conditions."""
        engine = ValidationEngine(
            "test", {"fail_threshold": 0.5, "warn_threshold": 0.9}
        )

        assert engine._determine_status({"pass_rate": 0.9}) == "passed"
        assert engine._determine_status({"pass_rate": 0.5}) == "warning"


# =============================================================================
# DataQualityChecker Run Checks
# =============================================================================


class TestDataQualityCheckerRunChecks:
    """Test top-level DataQualityChecker orchestration logic."""

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_run_checks_soda_config(self, mock_kafka, tmp_path):
        """Test running checks using SodaEngine when soda config is provided."""
        df = pl.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        df_path = str(tmp_path / "data.parquet")
        df.write_parquet(df_path)

        config = {
            "data_source": {"name": "test"},
            "validation": {
                "soda_checks": {"checks": [{"type": "row_count", "min": 1}]}
            },
        }
        checker = DataQualityChecker(config, "dag", "task")

        valid_path, invalid_path, results = checker.run_checks(df_path)
        assert results["total_rows"] == 3
        assert results["valid_rows"] == 3
        assert invalid_path == ""

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
                    "table": "CUSTOM.QUARANTINE_RECORDS",
                }
            }
        }
        handler = QuarantineHandler(config)
        assert handler.quarantine_config["table"] == "CUSTOM.QUARANTINE_RECORDS"
        assert handler.hitl_enabled is False

    def test_init_without_quarantine_config(self):
        """Test initialization falls back to defaults."""
        config = {"destination": {}}
        handler = QuarantineHandler(config)
        assert handler.default_table == "DQ_AUDIT.QUARANTINE_RECORDS"

    def test_prepare_quarantine_records(self, tmp_path):
        """Test preparing quarantine records with metadata."""
        config = {"destination": {}}
        handler = QuarantineHandler(config)

        invalid_df = pl.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
            }
        )
        in_path = str(tmp_path / "in.parquet")
        out_path = str(tmp_path / "out.parquet")
        invalid_df.write_parquet(in_path)

        count = handler.prepare_quarantine_records(
            invalid_df_path=in_path,
            output_path=out_path,
            source_pipeline="test_pipeline",
            source_table="test_table",
            failed_checks=["null_check", "range_check"],
        )

        assert count == 2
        result = pl.read_parquet(out_path)
        cols = result.columns
        assert "quarantine_id" in cols
        assert "source_pipeline" in cols
        assert "failed_checks" in cols
        assert "record_data" in cols
        assert "quarantined_at" in cols

        # Check first record
        assert result.item(0, "source_pipeline") == "test_pipeline"
        assert result.item(0, "source_table") == "test_table"
        assert json.loads(result.item(0, "failed_checks")) == [
            "null_check",
            "range_check",
        ]

    def test_prepare_quarantine_records_empty_df(self, tmp_path):
        """Test preparing quarantine records with empty DataFrame."""
        config = {"destination": {}}
        handler = QuarantineHandler(config)

        in_path = str(tmp_path / "in.parquet")
        out_path = str(tmp_path / "out.parquet")
        pl.DataFrame(schema={"id": pl.Int64, "name": pl.Utf8}).write_parquet(in_path)

        count = handler.prepare_quarantine_records(
            invalid_df_path=in_path,
            output_path=out_path,
            source_pipeline="test",
            source_table="test",
            failed_checks=[],
        )
        assert count == 0


class TestRunDataQualityChecksConvenience:
    """Test run_data_quality_checks() convenience function."""

    @patch("rlam_airflow_framework.data_quality.checker.kafka_publisher")
    def test_run_data_quality_checks(self, mock_kafka, tmp_path):
        """Test convenience function creates checker and runs checks."""
        df = pl.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        df_path = str(tmp_path / "data.parquet")
        df.write_parquet(df_path)
        config = {
            "data_source": {"name": "test"},
            "validation": {
                "soda_checks": {"checks": [{"type": "row_count", "min": 1}]}
            },
        }

        valid_df_path, invalid_df_path, results = run_data_quality_checks(
            df_path=df_path,
            config=config,
            dag_id="test_dag",
            task_id="test_task",
            destination_table="test_table",
        )

        assert results["total_rows"] == 3
        assert results["destination_table"] == "test_table"


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

    def test_prepare_records_sets_pending_status_when_hitl_enabled(self, tmp_path):
        """Test quarantine records get 'pending' status when HITL enabled."""
        config = {"destination": {"quarantine": {"hitl": {"enabled": True}}}}
        handler = QuarantineHandler(config)

        invalid_df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        in_path = str(tmp_path / "in.parquet")
        out_path = str(tmp_path / "out.parquet")
        invalid_df.write_parquet(in_path)

        count = handler.prepare_quarantine_records(
            invalid_df_path=in_path,
            output_path=out_path,
            source_pipeline="test_pipeline",
            source_table="test_table",
            failed_checks=["null_check"],
        )

        assert count == 2
        result = pl.read_parquet(out_path)
        assert all(v == "pending" for v in result["approval_status"])

    def test_prepare_records_sets_auto_approved_when_hitl_disabled(self, tmp_path):
        """Test quarantine records get 'auto_approved' status when HITL disabled."""
        config = {"destination": {}}
        handler = QuarantineHandler(config)

        invalid_df = pl.DataFrame({"id": [1], "name": ["Alice"]})
        in_path = str(tmp_path / "in.parquet")
        out_path = str(tmp_path / "out.parquet")
        invalid_df.write_parquet(in_path)

        count = handler.prepare_quarantine_records(
            invalid_df_path=in_path,
            output_path=out_path,
            source_pipeline="test_pipeline",
            source_table="test_table",
            failed_checks=["null_check"],
        )

        assert count == 1
        result = pl.read_parquet(out_path)
        assert result.item(0, "approval_status") == "auto_approved"


class TestCreateHitlQuarantineApprovalTask:
    """Test create_hitl_quarantine_approval_task function."""

    @pytest.fixture
    def sample_quarantine_df_path(self, tmp_path):
        """Create sample quarantine Parquet file."""
        df = pl.DataFrame(
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
        p = str(tmp_path / "q1.parquet")
        df.write_parquet(p)
        return p

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

    def test_builds_correct_summary_structure(
        self, sample_quarantine_df_path, hitl_config
    ):
        """Test HITL context has correct summary structure."""
        result = create_hitl_quarantine_approval_task(
            dag_id="test_dag",
            quarantine_df_path=sample_quarantine_df_path,
            config=hitl_config,
        )

        assert "quarantine_summary" in result
        summary = result["quarantine_summary"]
        assert summary["dag_id"] == "test_dag"
        assert summary["total_records"] == 3
        assert "quarantine_time" in summary

    def test_extracts_unique_failed_checks(
        self, sample_quarantine_df_path, hitl_config
    ):
        """Test unique failed checks are extracted from records."""
        result = create_hitl_quarantine_approval_task(
            dag_id="test_dag",
            quarantine_df_path=sample_quarantine_df_path,
            config=hitl_config,
        )

        failed_checks = result["quarantine_summary"]["failed_checks"]
        assert "null_check" in failed_checks
        assert "range_check" in failed_checks
        assert len(failed_checks) == 2

    def test_includes_sample_records(self, sample_quarantine_df_path, hitl_config):
        """Test sample records are included for review."""
        result = create_hitl_quarantine_approval_task(
            dag_id="test_dag",
            quarantine_df_path=sample_quarantine_df_path,
            config=hitl_config,
        )

        sample_records = result["quarantine_summary"]["sample_records"]
        assert len(sample_records) == 3

    def test_limits_sample_to_five_records(self, hitl_config, tmp_path):
        """Test sample records are limited to 5."""
        large_df = pl.DataFrame(
            {
                "quarantine_id": [f"q{i}" for i in range(10)],
                "source_pipeline": ["test"] * 10,
                "failed_checks": [json.dumps(["check"]) for _ in range(10)],
                "record_data": [json.dumps({"id": i}) for i in range(10)],
            }
        )
        p = str(tmp_path / "large.parquet")
        large_df.write_parquet(p)

        result = create_hitl_quarantine_approval_task(
            dag_id="test_dag", quarantine_df_path=p, config=hitl_config
        )

        assert len(result["quarantine_summary"]["sample_records"]) == 5

    def test_returns_correct_form_fields(self, sample_quarantine_df_path, hitl_config):
        """Test approval form fields are correctly structured."""
        result = create_hitl_quarantine_approval_task(
            dag_id="test_dag",
            quarantine_df_path=sample_quarantine_df_path,
            config=hitl_config,
        )

        form_fields = result["approval_form_fields"]
        assert "action" in form_fields
        assert form_fields["action"]["type"] == "select"
        assert "approve_release" in form_fields["action"]["options"]
        assert "reject_release" in form_fields["action"]["options"]
        assert "reprocess" in form_fields["action"]["options"]
        assert "notes" in form_fields

    def test_uses_configured_timeout_hours(
        self, sample_quarantine_df_path, hitl_config
    ):
        """Test timeout_hours comes from config."""
        result = create_hitl_quarantine_approval_task(
            dag_id="test_dag",
            quarantine_df_path=sample_quarantine_df_path,
            config=hitl_config,
        )

        assert result["timeout_hours"] == 12

    def test_handles_empty_quarantine_records(self, hitl_config, tmp_path):
        """Test handles empty DataFrame gracefully."""
        empty_df = pl.DataFrame(
            schema={
                "quarantine_id": pl.Utf8,
                "source_pipeline": pl.Utf8,
                "failed_checks": pl.Utf8,
                "record_data": pl.Utf8,
            }
        )
        p = str(tmp_path / "empty.parquet")
        empty_df.write_parquet(p)

        result = create_hitl_quarantine_approval_task(
            dag_id="test_dag", quarantine_df_path=p, config=hitl_config
        )

        assert result["quarantine_summary"]["total_records"] == 0
        assert result["quarantine_summary"]["sample_records"] == []


class TestProcessHitlApprovalResult:
    """Test process_hitl_approval_result function."""

    @pytest.fixture
    def sample_quarantine_df_path(self, tmp_path):
        """Create sample quarantine Parquet file for processing."""
        df = pl.DataFrame(
            {
                "quarantine_id": ["q1", "q2"],
                "source_pipeline": ["test_dag", "test_dag"],
                "source_table": ["TEST.DATA", "TEST.DATA"],
                "approval_status": ["pending", "pending"],
                "reprocessed": [False, False],
                "reprocessed_at": ["", ""],
                "approved_by": ["", ""],
                "approval_notes": ["", ""],
                "approved_at": ["", ""],
            }
        )
        p = str(tmp_path / "processing.parquet")
        df.write_parquet(p)
        return p

    @pytest.fixture
    def config(self):
        """Create config for processing."""
        return {
            "data_source": {"name": "test_source"},
            "event": {"topic": "test-events"},
        }

    def test_approve_release_updates_status(
        self, sample_quarantine_df_path, config, mocker, tmp_path
    ):
        """Test approve_release sets correct status."""
        mocker.patch("rlam_airflow_framework.data_quality.quarantine.kafka_publisher")

        approval = {
            "action": "approve_release",
            "approved_by": "test_steward",
            "notes": "Approved after review",
        }

        out_p = str(tmp_path / "out.parquet")
        count, action = process_hitl_approval_result(
            approval_result=approval,
            quarantine_df_path=sample_quarantine_df_path,
            output_path=out_p,
            config=config,
        )
        result_df = pl.read_parquet(out_p)

        assert action == "approve_release"
        assert all(v == "approved" for v in result_df["approval_status"])
        assert all(result_df["reprocessed"])
        assert all(v == "test_steward" for v in result_df["approved_by"])

    def test_reject_release_updates_status(
        self, sample_quarantine_df_path, config, mocker, tmp_path
    ):
        """Test reject_release sets correct status."""
        mocker.patch("rlam_airflow_framework.data_quality.quarantine.kafka_publisher")

        approval = {
            "action": "reject_release",
            "approved_by": "test_steward",
            "notes": "Data quality issues not resolved",
        }

        out_p = str(tmp_path / "out.parquet")
        count, action = process_hitl_approval_result(
            approval_result=approval,
            quarantine_df_path=sample_quarantine_df_path,
            output_path=out_p,
            config=config,
        )
        result_df = pl.read_parquet(out_p)

        assert action == "reject_release"
        assert all(v == "rejected" for v in result_df["approval_status"])
        assert all(v == "test_steward" for v in result_df["approved_by"])

    def test_reprocess_sets_pending_status(
        self, sample_quarantine_df_path, config, mocker, tmp_path
    ):
        """Test reprocess action sets pending_reprocess status."""
        mocker.patch("rlam_airflow_framework.data_quality.quarantine.kafka_publisher")

        approval = {
            "action": "reprocess",
            "approved_by": "test_steward",
            "notes": "Rerun with updated rules",
        }

        out_p = str(tmp_path / "out.parquet")
        count, action = process_hitl_approval_result(
            approval_result=approval,
            quarantine_df_path=sample_quarantine_df_path,
            output_path=out_p,
            config=config,
        )
        result_df = pl.read_parquet(out_p)

        assert action == "reprocess"
        assert all(v == "pending_reprocess" for v in result_df["approval_status"])

    def test_updates_approval_metadata(
        self, sample_quarantine_df_path, config, mocker, tmp_path
    ):
        """Test approval metadata is correctly set."""
        mocker.patch("rlam_airflow_framework.data_quality.quarantine.kafka_publisher")

        approval = {
            "action": "approve_release",
            "approved_by": "data_steward_1",
            "notes": "LGTM",
        }

        out_p = str(tmp_path / "out.parquet")
        count, _ = process_hitl_approval_result(
            approval_result=approval,
            quarantine_df_path=sample_quarantine_df_path,
            output_path=out_p,
            config=config,
        )
        result_df = pl.read_parquet(out_p)

        assert all(v == "data_steward_1" for v in result_df["approved_by"])
        assert all(v == "LGTM" for v in result_df["approval_notes"])
        assert all(v is not None and v != "" for v in result_df["approved_at"])

    def test_publishes_kafka_event(
        self, sample_quarantine_df_path, config, mocker, tmp_path
    ):
        """Test Kafka event is published on approval."""
        mock_kafka = mocker.patch(
            "rlam_airflow_framework.data_quality.quarantine.kafka_publisher"
        )

        approval = {
            "action": "approve_release",
            "approved_by": "test_steward",
            "notes": "",
        }

        out_p = str(tmp_path / "out.parquet")
        count, action = process_hitl_approval_result(
            approval_result=approval,
            quarantine_df_path=sample_quarantine_df_path,
            output_path=out_p,
            config=config,
        )

        mock_kafka.publish_pipeline_event.assert_called_once()
        call_kwargs = mock_kafka.publish_pipeline_event.call_args.kwargs
        assert call_kwargs["event_type"] == "hitl_quarantine_decision"
        assert call_kwargs["metadata"]["action"] == "approve_release"
        assert call_kwargs["metadata"]["record_count"] == 2

    def test_handles_kafka_publish_failure(
        self, sample_quarantine_df_path, config, mocker, tmp_path
    ):
        """Test gracefully handles Kafka publish failure."""
        mock_kafka = mocker.patch(
            "rlam_airflow_framework.data_quality.quarantine.kafka_publisher"
        )
        mock_kafka.publish_pipeline_event.side_effect = Exception("Kafka down")

        approval = {
            "action": "approve_release",
            "approved_by": "test_steward",
            "notes": "",
        }

        out_p = str(tmp_path / "out.parquet")
        count, action = process_hitl_approval_result(
            approval_result=approval,
            quarantine_df_path=sample_quarantine_df_path,
            output_path=out_p,
            config=config,
        )
        result_df = pl.read_parquet(out_p)

        assert action == "approve_release"
        assert all(v == "approved" for v in result_df["approval_status"])
