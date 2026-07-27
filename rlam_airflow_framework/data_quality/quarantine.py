# File: rlam_airflow_framework/data_quality/quarantine.py
"""
Quarantine lifecycle management for invalid records, with HITL approval
(Airflow 3.1.6). This is distinct from rule evaluation (``engines.py``): it
manages what happens to records once they've already been flagged invalid.
"""

import pandas as pd
import json
import uuid
from typing import Dict, Any, List, Tuple
from datetime import datetime, timezone
import structlog

from rlam_airflow_framework.kafka_publisher import kafka_publisher

log = structlog.get_logger(__name__)


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
