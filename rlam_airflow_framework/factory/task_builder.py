# File: rlam_airflow_framework/factory/task_builder.py
"""
TaskBuilder - handles TaskFlow graph wiring and deadline alert creation.
"""

from typing import Dict, Any, List, Optional, cast
import structlog
from datetime import timedelta

from airflow.sdk.definitions.asset import Asset
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference
from airflow.sdk.definitions.callback import SyncCallback

from rlam_airflow_framework.taskflow_tasks import (
    ingest_data,
    transform_data,
    validate_data_quality,
    route_dq_results,
    quarantine_invalid_data,
    prepare_hitl_approval_context,
    process_approval_decision,
    load_data,
)
from rlam_airflow_framework.health_checks import wait_for_kafka_health
from rlam_airflow_framework.deadline_callbacks import CompositeDeadlineNotifier

log = structlog.get_logger(__name__)


class TaskBuilder:
    """Builds the TaskFlow graph and configures alerts for the DAG."""

    @staticmethod
    def build_task_graph(
        config: Dict[str, Any],
        pool: Optional[str],
        inlets: List[Asset],
        outlets: List[Asset],
        dag_id: str,
        tenant_id: str,
        resolve_kafka_bootstrap_servers_fn,
    ) -> None:
        """Wire the TaskFlow task graph inside an active @dag context."""
        event_config = config.get("event", {})
        has_kafka_destination = bool(event_config.get("topic"))

        kafka_health_task = None
        if has_kafka_destination:
            if wait_for_kafka_health is None:
                raise RuntimeError(
                    f"DAG '{dag_id}' configures event.topic but the Kafka "
                    f"health sensor is unavailable."
                )
            bootstrap_servers = resolve_kafka_bootstrap_servers_fn(
                event_config, tenant_id
            )
            kafka_health_task = wait_for_kafka_health.override(
                task_id="check_kafka_health",
                pool=pool,
            )(bootstrap_servers=bootstrap_servers)
            log.debug(
                "Added Kafka health sensor to DAG",
                dag_id=dag_id,
                topic=event_config.get("topic"),
                bootstrap_servers=bootstrap_servers,
            )

        # Task 1: Data Ingestion
        ingest_task = ingest_data.override(
            task_id="fetch_source_data",
            inlets=inlets or [],
            pool=pool,
        )(config)

        # Task 2: Data Transformation
        has_transformation = "transformations" in config or "validation_rules" in config

        if has_transformation:
            transform_task = transform_data.override(
                task_id="apply_transformations",
                pool=pool,
            )(cast(str, ingest_task), config)
            current_df = transform_task
        else:
            current_df = ingest_task

        # Task 3: Data Quality Validation
        validation_config = config.get("validation", {})
        has_validation = (
            "soda_checks" in validation_config or "quality_gates" in validation_config
        )

        if has_validation:
            dq_task = cast(
                Any,
                validate_data_quality.override(
                    task_id="run_quality_checks",
                    pool=pool,
                )(cast(str, current_df), config),
            )

            valid_df = dq_task["valid_path"]
            invalid_df = dq_task["invalid_path"]
            dq_results = dq_task["results"]

            quality_gates = validation_config.get("quality_gates", {})
            should_quarantine = quality_gates.get("quarantine_invalid", False)

            if should_quarantine:
                router = route_dq_results.override(
                    task_id="evaluate_quality_gates",
                    pool=pool,
                )(dq_results, config)

                load_valid_task = load_data.override(
                    task_id="load_valid_data",
                    outlets=outlets or [],
                    pool=pool,
                )(valid_df, config)

                quarantine_task = quarantine_invalid_data.override(
                    task_id="quarantine_invalid_data",
                    pool=pool,
                )(invalid_df, dq_results, config)

                quarantine_config = config.get("destination", {}).get("quarantine", {})
                hitl_config = quarantine_config.get("hitl", {})
                hitl_enabled = hitl_config.get("enabled", False)

                if hitl_enabled:
                    from airflow.providers.standard.operators.hitl import (
                        ApprovalOperator,
                    )
                    from airflow.sdk import Param
                    from datetime import timedelta

                    timeout_hours = hitl_config.get("timeout_hours", 24)

                    approval_context = prepare_hitl_approval_context.override(
                        task_id="prepare_hitl_approval_context",
                        pool=pool,
                    )(cast(str, quarantine_task), config)

                    hitl_approval_task = ApprovalOperator(
                        task_id="quarantine_approval",
                        subject="Quarantine Release Approval for {{ dag.dag_id }}",
                        body="""
## Quarantine Summary
- **Total Records**: {{ ti.xcom_pull(task_ids='prepare_hitl_approval_context')['quarantine_summary']['total_records'] }}
- **Failed Checks**: {{ ti.xcom_pull(task_ids='prepare_hitl_approval_context')['quarantine_summary']['failed_checks'] | join(', ') }}
- **Quarantine Time**: {{ ti.xcom_pull(task_ids='prepare_hitl_approval_context')['quarantine_summary']['quarantine_time'] }}

Please review the quarantined records.
""",
                        params={
                            "notes": Param(
                                type="string", default="", description="Approval Notes"
                            )
                        },
                        response_timeout=timedelta(hours=timeout_hours),
                        pool=pool,
                    )

                    process_decision_task = process_approval_decision.override(
                        task_id="process_approval_decision",
                        pool=pool,
                    )(
                        approval_result=cast(Dict[str, Any], hitl_approval_task.output),
                        quarantine_df_path=cast(str, quarantine_task),
                        config=config,
                    )

                    load_quarantine_task = load_data.override(
                        task_id="load_quarantine_records",
                        pool=pool,
                    )(cast(str, process_decision_task), config)

                    router >> [load_valid_task, quarantine_task]  # pyright: ignore
                    (
                        quarantine_task
                        >> approval_context
                        >> hitl_approval_task
                        >> process_decision_task
                        >> load_quarantine_task
                    )  # pyright: ignore
                else:
                    load_quarantine_task = load_data.override(
                        task_id="load_quarantine_records",
                        pool=pool,
                    )(cast(str, quarantine_task), config)

                    router >> [load_valid_task, quarantine_task]  # pyright: ignore
                    quarantine_task >> load_quarantine_task  # pyright: ignore
            else:
                load_data.override(
                    task_id="load_data",
                    outlets=outlets or [],
                    pool=pool,
                )(valid_df, config)
        else:
            load_data.override(
                task_id="load_data",
                outlets=outlets or [],
                pool=pool,
            )(cast(str, current_df), config)

        if kafka_health_task:
            kafka_health_task >> ingest_task  # pyright: ignore

    @staticmethod
    def create_deadline_alert(
        dag_id: str,
        schedule_config: Dict[str, Any],
        tenant_id: str,
        resolve_kafka_bootstrap_servers_fn,
    ) -> Optional[List[DeadlineAlert]]:
        """Create deadline alert configuration from schedule config."""
        deadline_config = schedule_config.get("deadline", {})

        if not deadline_config:
            return None

        tiers = deadline_config.get("tiers", [deadline_config])

        alerts: List[DeadlineAlert] = []
        for tier in tiers:
            if not tier.get("enabled", False):
                continue

            if CompositeDeadlineNotifier is None:
                log.warning(
                    "Deadline alerts disabled - CompositeDeadlineNotifier not available",
                    dag_id=dag_id,
                )
                continue

            timeout_minutes = tier.get("timeout_minutes", 30)
            email_enabled = tier.get("email_enabled", False)
            email_recipients = tier.get("email_recipients", [])
            kafka_topic = tier.get("kafka_topic", "pipeline-alerts")
            bootstrap_servers = resolve_kafka_bootstrap_servers_fn(tier, tenant_id)

            ref_str = tier.get("reference", "queued_at").upper()
            reference = (
                DeadlineReference.AVERAGE_RUNTIME()
                if "AVERAGE" in ref_str
                else DeadlineReference.DAGRUN_QUEUED_AT
            )

            log.info(
                "Creating deadline alert tier",
                dag_id=dag_id,
                timeout_minutes=timeout_minutes,
                email_enabled=email_enabled,
                kafka_topic=kafka_topic,
                reference=ref_str,
            )

            alerts.append(
                DeadlineAlert(
                    reference=reference,
                    interval=timedelta(minutes=timeout_minutes),
                    callback=SyncCallback(
                        CompositeDeadlineNotifier,
                        kwargs={
                            "topic": kafka_topic,
                            "message": f"Pipeline {dag_id} missed deadline of {timeout_minutes} minutes",
                            "email_enabled": email_enabled,
                            "email_recipients": email_recipients,
                            "email_subject": f"🚨 Deadline Alert: {dag_id}",
                            "bootstrap_servers": bootstrap_servers,
                        },
                    ),
                )
            )

        return alerts if alerts else None
