# File: rlam_airflow_framework/data_quality/quarantine.py
"""
Quarantine lifecycle management for invalid records, with HITL approval
(Airflow 3.1.6). This is distinct from rule evaluation (``engines.py``): it
manages what happens to records once they've already been flagged invalid.
"""

import json
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

        # Every pipeline belongs to a tenant (enforced at DAG-parse time by
        # DAGFactoryV2._validate_pipeline_connections/create_dag_from_config),
        # so this is always populated for a real pipeline config.
        self.tenant_id = config.get("metadata", {}).get("tenant", "unknown")

        # Default quarantine table pattern. Records from every tenant land
        # here unless a pipeline overrides destination.quarantine, so the
        # tenant_id column below (not the table name) is what a data steward
        # must filter/authorize on to avoid viewing another tenant's records.
        self.default_table = "DQ_AUDIT.QUARANTINE_RECORDS"

        # HITL configuration
        self.hitl_config = self.quarantine_config.get("hitl", {})
        self.hitl_enabled = self.hitl_config.get("enabled", False)
        self.hitl_timeout_hours = self.hitl_config.get("timeout_hours", 24)

    def prepare_quarantine_records(
        self,
        invalid_df_path: str,
        output_path: str,
        source_pipeline: str,
        source_table: str,
        failed_checks: List[str],
    ) -> int:
        """
        Prepare invalid records for quarantine with metadata using DuckDB.

        Args:
            invalid_df_path: Path to Parquet file with invalid records
            output_path: Path to write quarantine records
            source_pipeline: Name of the source pipeline/DAG
            source_table: Original destination table name
            failed_checks: List of failed check names

        Returns:
            Number of rows written
        """
        import duckdb

        fc_json = json.dumps(failed_checks).replace("'", "''")
        tenant = self.tenant_id.replace("'", "''")
        src_pipe = source_pipeline.replace("'", "''")
        src_tbl = source_table.replace("'", "''")
        now_iso = datetime.now(timezone.utc).isoformat()

        try:
            count_res = duckdb.execute(
                f"SELECT count(*) FROM read_parquet('{invalid_df_path}')"
            ).fetchone()
            count = count_res[0] if count_res else 0
        except Exception:
            count = 0

        if count == 0:
            return 0

        status = "pending" if self.hitl_enabled else "auto_approved"

        query = f"""
        COPY (
            SELECT 
                uuid() AS quarantine_id,
                '{tenant}' AS tenant_id,
                '{src_pipe}' AS source_pipeline,
                '{src_tbl}' AS source_table,
                '{fc_json}' AS failed_checks,
                to_json(t) AS record_data,
                '{now_iso}' AS quarantined_at,
                false AS reprocessed,
                NULL AS reprocessed_at,
                '{status}' AS approval_status,
                NULL AS approved_by,
                NULL AS approved_at
            FROM read_parquet('{invalid_df_path}') as t
        ) TO '{output_path}' (FORMAT PARQUET)
        """
        duckdb.execute(query)

        log.info(
            "Prepared quarantine records",
            count=count,
            source_pipeline=source_pipeline,
            hitl_enabled=self.hitl_enabled,
        )

        return count

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
    dag_id: str, quarantine_df_path: str, config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Create context for HITL quarantine approval task using DuckDB out-of-core.

    This function prepares the data needed for an Airflow 3.1.x @task.hitl()
    decorated task that will pause execution until a data-steward approves
    the quarantine record release.

    Args:
        dag_id: The DAG identifier
        quarantine_df_path: Path to DataFrame of quarantined records
        config: Pipeline configuration

    Returns:
        Dictionary with HITL task context including:
        - quarantine_summary: Summary of quarantined records
        - approval_form_fields: Fields for the approval UI form
        - timeout_hours: How long to wait for approval
    """
    import duckdb

    quarantine_config = config.get("destination", {}).get("quarantine", {})
    hitl_config = quarantine_config.get("hitl", {})

    # Build summary for approval UI
    tenant_id = config.get("metadata", {}).get("tenant", "unknown")

    try:
        count_res = duckdb.execute(
            f"SELECT count(*) FROM read_parquet('{quarantine_df_path}')"
        ).fetchone()
        total_records = count_res[0] if count_res else 0
    except Exception:
        total_records = 0

    summary = {
        "dag_id": dag_id,
        "tenant_id": tenant_id,
        "total_records": total_records,
        "quarantine_time": datetime.now(timezone.utc).isoformat(),
        "failed_checks": [],
        "sample_records": [],
    }

    if total_records > 0:
        # Extract unique failed checks
        try:
            fc_rows = duckdb.execute(
                f"SELECT DISTINCT failed_checks FROM read_parquet('{quarantine_df_path}') WHERE failed_checks IS NOT NULL"
            ).fetchall()
            all_checks = set()
            for row in fc_rows:
                if row[0]:
                    try:
                        checks = json.loads(row[0])
                        all_checks.update(
                            checks if isinstance(checks, list) else [checks]
                        )
                    except (json.JSONDecodeError, TypeError):
                        pass
            summary["failed_checks"] = list(all_checks)
        except Exception:
            pass

        # Include sample records (first 5) for review
        try:
            res = duckdb.execute(
                f"SELECT * FROM read_parquet('{quarantine_df_path}') LIMIT 5"
            )
            cols = [desc[0] for desc in res.description]
            sample_rows = []
            for row in res.fetchall():
                sample_rows.append(dict(zip(cols, row)))
            summary["sample_records"] = sample_rows
        except Exception:
            pass

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
    quarantine_df_path: str,
    output_path: str,
    config: Dict[str, Any],
) -> Tuple[int, str]:
    """
    Process the result of a HITL quarantine approval out-of-core.

    Args:
        approval_result: Result from HITL task containing:
            - action: 'approve_release', 'reject_release', or 'reprocess'
            - notes: Optional notes from approver
            - approved_by: Username of approver
        quarantine_df_path: Path to the quarantined records DataFrame
        output_path: Path to write the updated records
        config: Pipeline configuration

    Returns:
        Tuple of (processed_records_count, action_taken)
    """
    import duckdb

    action = approval_result.get("action", "reject_release")
    approved_by = approval_result.get("approved_by", "unknown").replace("'", "''")
    notes = approval_result.get("notes", "").replace("'", "''")
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        count_res = duckdb.execute(
            f"SELECT count(*) FROM read_parquet('{quarantine_df_path}')"
        ).fetchone()
        record_count = count_res[0] if count_res else 0
    except Exception:
        record_count = 0

    log.info(
        "Processing HITL approval result",
        action=action,
        approved_by=approved_by,
        record_count=record_count,
    )

    if action == "approve_release":
        app_status = "approved"
        rep = "true"
        rep_at = f"'{now_iso}'"
        log.info("Quarantine records approved for release", count=record_count)
    elif action == "reject_release":
        app_status = "rejected"
        rep = "reprocessed"
        rep_at = "reprocessed_at"
        log.info("Quarantine release rejected", count=record_count)
    else:
        app_status = "pending_reprocess"
        rep = "reprocessed"
        rep_at = "reprocessed_at"
        log.info("Quarantine records marked for reprocessing", count=record_count)

    query = f"""
    COPY (
        SELECT 
            * REPLACE (
                '{app_status}' AS approval_status, 
                '{approved_by}' AS approved_by, 
                '{now_iso}' AS approved_at, 
                {rep} AS reprocessed, 
                {rep_at} AS reprocessed_at,
                '{notes}' AS approval_notes
            )
        FROM read_parquet('{quarantine_df_path}')
    ) TO '{output_path}' (FORMAT PARQUET)
    """
    duckdb.execute(query)

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
                "record_count": record_count,
                "notes": notes,
            },
        )
    except Exception as e:
        log.warning("Failed to publish HITL approval event to Kafka", error=str(e))

    return record_count, action
