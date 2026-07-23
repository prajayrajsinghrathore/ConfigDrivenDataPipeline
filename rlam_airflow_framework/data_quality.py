 # File: dags/utils/data_quality.py
"""
Data Quality module using Soda Core for validation and quality checks.

This module provides:
- SodaCL-based data quality checks
- Quality gate enforcement (fail/warn thresholds)
- Quarantine handling for invalid records with HITL approval (Airflow 3.1.6)
- DQ metrics publishing to Kafka

Airflow 3.1.6 Features:
- Human-in-the-Loop (HITL) approval for quarantine release
- Structlog for structured logging
"""

import pandas as pd
import json
import uuid
from typing import Dict, Any, List, Optional, Tuple, cast
from datetime import datetime, timezone
import structlog

try:
    from soda.scan import Scan  # pyright: ignore[reportMissingImports]

    SODA_AVAILABLE = True
except ImportError:
    SODA_AVAILABLE = False

from rlam_airflow_framework.kafka_publisher import kafka_publisher

log = structlog.get_logger(__name__)


class DataQualityChecker:
    """
    Data Quality checker using Soda Core.

    Supports:
    - SodaCL check definitions from YAML config
    - Quality gates with fail/warn thresholds
    - Quarantine routing for invalid records
    - DQ metrics publishing to Kafka
    """

    def __init__(self, config: Dict[str, Any], dag_id: str, task_id: str):
        """
        Initialize the data quality checker.

        Args:
            config: The full pipeline configuration containing validation settings
            dag_id: The DAG identifier for metrics
            task_id: The task identifier for metrics
        """
        self.config = config
        self.dag_id = dag_id
        self.task_id = task_id
        self.validation_config = config.get("validation", {})
        self.soda_checks = self.validation_config.get("soda_checks", {})
        self.quality_gates = self.validation_config.get("quality_gates", {})

        # Get data source name for context
        self.source_name = config.get("data_source", {}).get("name", "unknown")

        # Kafka topic for DQ events
        event_config = config.get("event", {})
        self.dq_topic = event_config.get("topic", "data-quality") + "_dq_metrics"

    def run_checks(
        self, df: pd.DataFrame, destination_table: Optional[str] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
        """
        Run data quality checks on a DataFrame.

        Args:
            df: Input DataFrame to validate
            destination_table: Optional destination table name for context

        Returns:
            Tuple of:
                - valid_df: DataFrame with valid records
                - invalid_df: DataFrame with invalid records (for quarantine)
                - results: Dictionary with DQ metrics and check results
        """
        if df.empty:
            log.warning("Empty DataFrame provided for data quality checks")
            return df, pd.DataFrame(), self._empty_results()

        results = {
            "scan_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": self.source_name,
            "destination_table": destination_table,
            "total_rows": len(df),
            "checks": [],
            "passed": 0,
            "failed": 0,
            "warnings": 0,
            "pass_rate": 1.0,
            "status": "passed",
        }

        # If no Soda checks configured, use legacy validation
        if not self.soda_checks:
            log.info("No Soda checks configured, using legacy validation rules")
            return self._run_legacy_validation(df, results)

        # Run Soda Core checks if available
        if SODA_AVAILABLE:
            return self._run_soda_checks(df, results)
        else:
            log.warning(
                "Soda Core not available", install_cmd="pip install soda-core-pandas"
            )
            return self._run_basic_checks(df, results)

    def _run_soda_checks(
        self, df: pd.DataFrame, results: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
        """
        Run Soda Core checks on DataFrame.
        """
        try:
            scan = Scan()
            scan.set_scan_definition_name(f"{self.source_name}_scan")
            scan.set_data_source_name("pandas_df")

            # Add pandas DataFrame as data source
            scan.add_pandas_dataframe(dataset_name=self.source_name, pandas_df=df)

            # Build SodaCL checks from config
            sodacl_yaml = self._build_sodacl_yaml()
            if sodacl_yaml:
                scan.add_sodacl_yaml_str(sodacl_yaml)

            # Execute scan
            scan.execute()

            # Process results
            scan_results = scan.get_scan_results()

            # Extract check outcomes
            for check in scan_results.get("checks", []):
                check_result = {
                    "name": check.get("name", "unknown"),
                    "outcome": check.get("outcome", "unknown"),
                    "column": check.get("column"),
                    "diagnostics": check.get("diagnostics", {}),
                }
                results["checks"].append(check_result)

                if check_result["outcome"] == "pass":
                    results["passed"] += 1
                elif check_result["outcome"] == "fail":
                    results["failed"] += 1
                elif check_result["outcome"] == "warn":
                    results["warnings"] += 1

            # Calculate pass rate
            total_checks = results["passed"] + results["failed"] + results["warnings"]
            if total_checks > 0:
                results["pass_rate"] = results["passed"] / total_checks

            # Determine overall status
            results["status"] = self._determine_status(results)

            # Get invalid row indices from failed row-level checks
            invalid_indices = self._extract_invalid_indices(scan_results)

            # Split valid and invalid records
            if invalid_indices:
                invalid_df = df.loc[df.index.isin(invalid_indices)].copy()
                valid_df = df.loc[~df.index.isin(invalid_indices)].copy()
            else:
                valid_df = df.copy()
                invalid_df = pd.DataFrame()

            results["valid_rows"] = len(valid_df)
            results["invalid_rows"] = len(invalid_df)

            # Publish DQ metrics to Kafka
            self._publish_dq_metrics(results)

            return valid_df, invalid_df, results

        except Exception as e:
            log.error("Soda scan failed", error=str(e))
            results["status"] = "error"
            results["error"] = str(e)
            self._publish_dq_metrics(results)
            return df, pd.DataFrame(), results

    def _run_basic_checks(
        self, df: pd.DataFrame, results: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
        """
        Run basic checks when Soda Core is not available.
        Implements common SodaCL check patterns manually.
        """
        invalid_mask = pd.Series([False] * len(df), index=df.index)

        checks_config = self.soda_checks.get("checks", [])

        log.info("::group::Data Quality Validation")
        for check in checks_config:
            check_result = self._execute_basic_check(df, check, invalid_mask)
            results["checks"].append(check_result)

            if check_result["outcome"] == "pass":
                results["passed"] += 1
            elif check_result["outcome"] == "fail":
                results["failed"] += 1
        log.info("::endgroup::")

        # Calculate pass rate
        total_checks = results["passed"] + results["failed"]
        if total_checks > 0:
            results["pass_rate"] = results["passed"] / total_checks

        results["status"] = self._determine_status(results)

        # Split valid and invalid
        valid_df = cast(pd.DataFrame, df[~invalid_mask].copy())
        invalid_df = cast(pd.DataFrame, df[invalid_mask].copy())

        results["valid_rows"] = len(valid_df)
        results["invalid_rows"] = len(invalid_df)

        self._publish_dq_metrics(results)

        return valid_df, invalid_df, results

    def _execute_basic_check(
        self, df: pd.DataFrame, check: Dict[str, Any], invalid_mask: pd.Series
    ) -> Dict[str, Any]:
        """
        Execute a single basic data quality check.
        """
        check_name = check.get("name", "unnamed_check")
        check_type = check.get("type", "custom")
        column = check.get("column")

        result: Dict[str, Any] = {
            "name": check_name,
            "type": check_type,
            "column": column,
            "outcome": "pass",
            "diagnostics": {},
        }

        try:
            if check_type == "row_count":
                # row_count > 0
                min_count = check.get("min", 0)
                actual_count = len(df)
                result["diagnostics"] = {
                    "actual": actual_count,
                    "expected_min": min_count,
                }
                if actual_count <= min_count:
                    result["outcome"] = "fail"

            elif check_type == "missing_count" and column:
                # missing_count(column) = 0
                max_missing = check.get("max", 0)
                missing_count = cast(Any, df[column].isna().sum())
                result["diagnostics"] = {
                    "missing_count": int(missing_count),
                    "max_allowed": max_missing,
                }
                if missing_count > max_missing:
                    result["outcome"] = "fail"
                    # Mark rows with missing values as invalid
                    invalid_mask |= df[column].isna()

            elif check_type == "duplicate_count" and column:
                # duplicate_count(column) = 0
                max_duplicates = check.get("max", 0)
                duplicate_count = df[column].duplicated().sum()
                result["diagnostics"] = {
                    "duplicate_count": int(duplicate_count),
                    "max_allowed": max_duplicates,
                }
                if duplicate_count > max_duplicates:
                    result["outcome"] = "fail"
                    # Mark duplicate rows as invalid (keep first)
                    invalid_mask |= df[column].duplicated(keep="first")

            elif check_type == "invalid_percent" and column:
                # invalid_percent(column) < 5%
                max_percent = check.get("max_percent", 5)
                regex_pattern = check.get("valid_regex")

                if regex_pattern:
                    invalid_rows = ~df[column].astype(str).str.match(
                        regex_pattern, na=False
                    )
                    invalid_percent = (invalid_rows.sum() / len(df)) * 100
                    result["diagnostics"] = {
                        "invalid_percent": round(invalid_percent, 2),
                        "max_allowed_percent": max_percent,
                    }
                    if invalid_percent > max_percent:
                        result["outcome"] = "fail"
                        invalid_mask |= invalid_rows

            elif check_type == "max" and column:
                threshold = check.get("threshold", 0)
                actual_max = cast(Any, df[column].max())
                result["diagnostics"] = {
                    "actual_max": float(actual_max),
                    "threshold": threshold,
                }
                if actual_max >= threshold:
                    result["outcome"] = "fail"
                    # Mark rows that exceed threshold as invalid
                    invalid_mask |= df[column] >= threshold

            elif check_type == "values_in_set" and column:
                # Check column values are in allowed set
                allowed_values = check.get("valid_values", [])
                invalid_rows = ~df[column].isin(allowed_values)
                invalid_count = cast(Any, invalid_rows.sum())
                result["diagnostics"] = {"invalid_count": int(invalid_count)}
                if invalid_count > 0:
                    result["outcome"] = "fail"
                    invalid_mask |= invalid_rows

            elif check_type == "range" and column:
                # Check numeric range
                min_val = check.get("min")
                max_val = check.get("max")

                invalid_rows = pd.Series([False] * len(df), index=df.index)
                if min_val is not None:
                    invalid_rows |= df[column] < min_val
                if max_val is not None:
                    invalid_rows |= df[column] > max_val

                invalid_count = invalid_rows.sum()
                result["diagnostics"] = {
                    "invalid_count": int(invalid_count),
                    "min": min_val,
                    "max": max_val,
                }
                if invalid_count > 0:
                    result["outcome"] = "fail"
                    invalid_mask |= invalid_rows

            elif check_type == "freshness" and column:
                # Check data freshness
                max_hours = check.get("max_hours", 24)
                latest_value = pd.to_datetime(df[column]).max()
                age_hours = (
                    datetime.now(timezone.utc)
                    - latest_value.replace(tzinfo=timezone.utc)
                ).total_seconds() / 3600

                result["diagnostics"] = {
                    "latest_timestamp": str(latest_value),
                    "age_hours": round(age_hours, 2),
                    "max_hours": max_hours,
                }
                if age_hours > max_hours:
                    result["outcome"] = "fail"

        except Exception as e:
            log.warning("Check failed with error", check_name=check_name, error=str(e))
            result["outcome"] = "error"
            result["diagnostics"] = {**result.get("diagnostics", {}), "error": str(e)}

        return result

    def _run_legacy_validation(
        self, df: pd.DataFrame, results: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
        """
        Run legacy validation rules (from validation_rules config section).
        """
        validation_rules = self.config.get("validation_rules", [])
        if not validation_rules:
            results["status"] = "skipped"
            return df, pd.DataFrame(), results

        invalid_mask = pd.Series([False] * len(df), index=df.index)

        for rule in validation_rules:
            field = rule.get("field")
            rule_type = rule.get("type")

            if field not in df.columns:
                if rule.get("required", False):
                    results["checks"].append(
                        {
                            "name": f"required_{field}",
                            "outcome": "fail",
                            "diagnostics": {
                                "error": f"Required field {field} not found"
                            },
                        }
                    )
                    results["failed"] += 1
                continue

            check_result = {
                "name": f"{rule_type}_{field}",
                "column": field,
                "outcome": "pass",
                "diagnostics": {},
            }

            if rule_type == "numeric" and "range" in rule:
                min_val, max_val = rule["range"]
                invalid_rows = (df[field] < min_val) | (df[field] > max_val)
                if invalid_rows.any():
                    check_result["outcome"] = "fail"
                    check_result["diagnostics"] = {
                        "invalid_count": int(invalid_rows.sum())
                    }
                    invalid_mask |= invalid_rows

            elif rule_type == "string" and "max_length" in rule:
                max_len = rule["max_length"]
                invalid_rows = df[field].astype(str).str.len() > max_len
                if invalid_rows.any():
                    check_result["outcome"] = "fail"
                    check_result["diagnostics"] = {
                        "invalid_count": int(invalid_rows.sum())
                    }
                    invalid_mask |= invalid_rows

            results["checks"].append(check_result)
            if check_result["outcome"] == "pass":
                results["passed"] += 1
            else:
                results["failed"] += 1

        total_checks = results["passed"] + results["failed"]
        if total_checks > 0:
            results["pass_rate"] = results["passed"] / total_checks

        results["status"] = self._determine_status(results)

        valid_df = cast(pd.DataFrame, df[~invalid_mask].copy())
        invalid_df = cast(pd.DataFrame, df[invalid_mask].copy())

        results["valid_rows"] = len(valid_df)
        results["invalid_rows"] = len(invalid_df)

        self._publish_dq_metrics(results)

        return valid_df, invalid_df, results

    def _build_sodacl_yaml(self) -> Optional[str]:
        """
        Build SodaCL YAML from config.
        """
        checks = self.soda_checks.get("checks", [])
        if not checks:
            return None

        # Convert simplified config to SodaCL format
        sodacl_lines = [f"checks for {self.source_name}:"]

        for check in checks:
            check_type = check.get("type")
            column = check.get("column")

            if check_type == "row_count":
                min_val = check.get("min", 0)
                sodacl_lines.append(f"  - row_count > {min_val}")

            elif check_type == "missing_count" and column:
                max_val = check.get("max", 0)
                sodacl_lines.append(f"  - missing_count({column}) = {max_val}")

            elif check_type == "duplicate_count" and column:
                max_val = check.get("max", 0)
                sodacl_lines.append(f"  - duplicate_count({column}) = {max_val}")

            elif check_type == "invalid_percent" and column:
                max_percent = check.get("max_percent", 5)
                sodacl_lines.append(f"  - invalid_percent({column}) < {max_percent}%")

        return "\n".join(sodacl_lines)

    def _determine_status(self, results: Dict[str, Any]) -> str:
        """
        Determine overall status based on quality gates.
        """
        fail_threshold = self.quality_gates.get("fail_threshold", 0.0)
        warn_threshold = self.quality_gates.get("warn_threshold", 0.95)

        pass_rate = results.get("pass_rate", 1.0)

        if pass_rate < fail_threshold:
            return "failed"
        elif pass_rate < warn_threshold:
            return "warning"
        else:
            return "passed"

    def _extract_invalid_indices(self, scan_results: Dict[str, Any]) -> List[int]:
        """
        Extract indices of invalid rows from Soda scan results.
        """
        # This would need to parse Soda's diagnostic output
        # For now, return empty list (Soda doesn't always return row-level details)
        return []

    def _empty_results(self) -> Dict[str, Any]:
        """Return empty results structure."""
        return {
            "scan_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": self.source_name,
            "total_rows": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "checks": [],
            "passed": 0,
            "failed": 0,
            "warnings": 0,
            "pass_rate": 1.0,
            "status": "skipped",
        }

    def _publish_dq_metrics(self, results: Dict[str, Any]) -> None:
        """
        Publish DQ metrics to Kafka.
        """
        try:
            kafka_publisher.publish_pipeline_event(
                dag_id=self.dag_id,
                task_id=self.task_id,
                event_type="data_quality_metrics",
                status=results.get("status", "unknown"),
                message=(
                    f"DQ scan for {self.source_name}: "
                    f"{results.get('passed', 0)} passed, "
                    f"{results.get('failed', 0)} failed, "
                    f"{results.get('warnings', 0)} warnings"
                ),
                execution_date=results.get(
                    "timestamp", datetime.now(timezone.utc).isoformat()
                ),
                topic=self.dq_topic,
                metadata=results,
            )
        except Exception as e:
            log.warning("Failed to publish DQ metrics to Kafka", error=str(e))

    def should_fail_pipeline(self, results: Dict[str, Any]) -> bool:
        """
        Determine if the pipeline should fail based on DQ results.
        """
        return results.get("status") == "failed"

    def should_quarantine(self) -> bool:
        """
        Check if quarantine is enabled in config.
        """
        return self.quality_gates.get("quarantine_invalid", False)


class QuarantineHandler:
    """
    Handles quarantine logic for invalid records with HITL approval support.

    Airflow 3.1.6 Feature: Human-in-the-Loop (HITL) approval workflow
    for quarantine record release. The data-steward role can approve
    quarantine releases through the Airflow UI.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize quarantine handler.

        Args:
            config: Pipeline configuration
        """
        self.config = config
        self.destination_config = config.get("destination", {})
        self.quarantine_config = self.destination_config.get("quarantine", {})

        # Default quarantine table pattern
        self.default_table = "DQ_AUDIT.QUARANTINE_RECORDS"

        # HITL configuration
        self.hitl_config = self.quarantine_config.get("hitl", {})
        self.hitl_enabled = self.hitl_config.get("enabled", False)
        self.hitl_timeout_hours = self.hitl_config.get("timeout_hours", 24)

    def prepare_quarantine_records(
        self,
        invalid_df: pd.DataFrame,
        source_pipeline: str,
        source_table: str,
        failed_checks: List[str],
    ) -> pd.DataFrame:
        """
        Prepare invalid records for quarantine with metadata.

        Args:
            invalid_df: DataFrame with invalid records
            source_pipeline: Name of the source pipeline/DAG
            source_table: Original destination table name
            failed_checks: List of failed check names

        Returns:
            DataFrame formatted for quarantine table
        """
        if invalid_df.empty:
            return pd.DataFrame()

        quarantine_records = []

        for _, row in invalid_df.iterrows():
            record = {
                "quarantine_id": str(uuid.uuid4()),
                "source_pipeline": source_pipeline,
                "source_table": source_table,
                "failed_checks": json.dumps(failed_checks),
                "record_data": json.dumps(row.to_dict(), default=str),
                "quarantined_at": datetime.now(timezone.utc).isoformat(),
                "reprocessed": False,
                "reprocessed_at": None,
                "approval_status": "pending" if self.hitl_enabled else "auto_approved",
                "approved_by": None,
                "approved_at": None,
            }
            quarantine_records.append(record)

        log.info(
            "Prepared quarantine records",
            count=len(quarantine_records),
            source_pipeline=source_pipeline,
            hitl_enabled=self.hitl_enabled,
        )

        return pd.DataFrame(quarantine_records)

    def get_quarantine_destination(self) -> Dict[str, Any]:
        """
        Get the quarantine destination configuration.
        """
        if self.quarantine_config:
            return self.quarantine_config

        # Return default configuration
        return {
            "type": "snowflake_table",
            "table": self.default_table,
            "write_mode": "append",
        }

    def requires_hitl_approval(self) -> bool:
        """
        Check if HITL approval is required for quarantine release.

        Returns:
            True if HITL workflow is enabled
        """
        return self.hitl_enabled


# =============================================================================
# HITL (Human-in-the-Loop) Quarantine Approval Tasks
# =============================================================================


def create_hitl_quarantine_approval_task(
    dag_id: str, quarantine_records: pd.DataFrame, config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Create context for HITL quarantine approval task.

    This function prepares the data needed for an Airflow 3.1.x @task.hitl()
    decorated task that will pause execution until a data-steward approves
    the quarantine record release.

    Args:
        dag_id: The DAG identifier
        quarantine_records: DataFrame of quarantined records
        config: Pipeline configuration

    Returns:
        Dictionary with HITL task context including:
        - quarantine_summary: Summary of quarantined records
        - approval_form_fields: Fields for the approval UI form
        - timeout_hours: How long to wait for approval
    """
    quarantine_config = config.get("destination", {}).get("quarantine", {})
    hitl_config = quarantine_config.get("hitl", {})

    # Build summary for approval UI
    summary = {
        "dag_id": dag_id,
        "total_records": len(quarantine_records),
        "quarantine_time": datetime.now(timezone.utc).isoformat(),
        "failed_checks": [],
        "sample_records": [],
    }

    # Extract unique failed checks
    if "failed_checks" in quarantine_records.columns:
        all_checks = set()
        for checks_json in quarantine_records["failed_checks"].dropna():
            try:
                checks = json.loads(checks_json)
                all_checks.update(checks if isinstance(checks, list) else [checks])
            except (json.JSONDecodeError, TypeError):
                pass
        summary["failed_checks"] = list(all_checks)

    # Include sample records (first 5) for review
    if len(quarantine_records) > 0:
        sample = quarantine_records.head(5)
        summary["sample_records"] = sample.to_dict("records")

    return {
        "quarantine_summary": summary,
        "approval_form_fields": {
            "action": {
                "type": "select",
                "label": "Action",
                "options": ["approve_release", "reject_release", "reprocess"],
                "required": True,
            },
            "notes": {"type": "textarea", "label": "Approval Notes", "required": False},
        },
        "timeout_hours": hitl_config.get("timeout_hours", 24),
        "allowed_roles": ["data-steward", "admin"],
    }


def process_hitl_approval_result(
    approval_result: Dict[str, Any],
    quarantine_records: pd.DataFrame,
    config: Dict[str, Any],
) -> Tuple[pd.DataFrame, str]:
    """
    Process the result of a HITL quarantine approval.

    Args:
        approval_result: Result from HITL task containing:
            - action: 'approve_release', 'reject_release', or 'reprocess'
            - notes: Optional notes from approver
            - approved_by: Username of approver
        quarantine_records: The quarantined records DataFrame
        config: Pipeline configuration

    Returns:
        Tuple of (processed_records, action_taken)
    """
    action = approval_result.get("action", "reject_release")
    approved_by = approval_result.get("approved_by", "unknown")
    notes = approval_result.get("notes", "")

    log.info(
        "Processing HITL approval result",
        action=action,
        approved_by=approved_by,
        record_count=len(quarantine_records),
    )

    # Update records based on approval decision
    quarantine_records = quarantine_records.copy()
    quarantine_records["approved_by"] = approved_by
    quarantine_records["approved_at"] = datetime.now(timezone.utc).isoformat()
    quarantine_records["approval_notes"] = notes

    if action == "approve_release":
        quarantine_records["approval_status"] = "approved"
        quarantine_records["reprocessed"] = True
        quarantine_records["reprocessed_at"] = datetime.now(timezone.utc).isoformat()
        log.info(
            "Quarantine records approved for release", count=len(quarantine_records)
        )

    elif action == "reject_release":
        quarantine_records["approval_status"] = "rejected"
        log.info("Quarantine release rejected", count=len(quarantine_records))

    elif action == "reprocess":
        quarantine_records["approval_status"] = "pending_reprocess"
        log.info(
            "Quarantine records marked for reprocessing", count=len(quarantine_records)
        )

    # Publish event to Kafka
    try:
        kafka_publisher.publish_pipeline_event(
            dag_id=config.get("data_source", {}).get("name", "unknown"),
            task_id="quarantine_approval",
            event_type="hitl_quarantine_decision",
            status="success",
            message=f"Quarantine approval: {action} by {approved_by}",
            execution_date=datetime.now(timezone.utc).isoformat(),
            topic=config.get("event", {}).get("topic", "pipeline-events"),
            metadata={
                "action": action,
                "approved_by": approved_by,
                "record_count": len(quarantine_records),
                "notes": notes,
            },
        )
    except Exception as e:
        log.warning("Failed to publish HITL approval event to Kafka", error=str(e))

    return quarantine_records, action


def run_data_quality_checks(
    df: pd.DataFrame,
    config: Dict[str, Any],
    dag_id: str,
    task_id: str,
    destination_table: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Convenience function to run data quality checks.

    Args:
        df: Input DataFrame
        config: Pipeline configuration
        dag_id: DAG identifier
        task_id: Task identifier
        destination_table: Optional destination table name

    Returns:
        Tuple of (valid_df, invalid_df, results)
    """
    checker = DataQualityChecker(config, dag_id, task_id)
    return checker.run_checks(df, destination_table)
