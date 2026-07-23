# File: dags/utils/taskflow_tasks.py
"""
TaskFlow API tasks for Airflow 3.x data pipelines.

This module provides modern @task decorated functions replacing traditional operators:
- Data ingestion from REST APIs and SFTP
- Data transformation with formula engine
- Data quality validation with quarantine
- Data loading to Snowflake, Azure, and local destinations
- Human-in-the-Loop (HITL) quarantine approval

Features:
- Automatic XCom serialization for DataFrames
- Clean function signatures with type hints
- get_current_context() for context access
- Conditional branching for DQ routing
- HITL approval workflow integration
"""

import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from pathlib import Path
import os
import structlog

from airflow.sdk import task, get_current_context

from rlam_airflow_framework.data_fetchers import fetch_http_data, fetch_sftp_data
from rlam_airflow_framework.data_transformers import enrich_from_snowflake
from rlam_airflow_framework.data_loaders import (
    load_to_snowflake,
    load_to_snowflake_stage,
    load_to_object_storage,
    call_stored_procedure,
    load_to_local_file,
)
from rlam_airflow_framework.data_quality import (
    run_data_quality_checks,
    QuarantineHandler,
    create_hitl_quarantine_approval_task,
    process_hitl_approval_result,
)
from rlam_airflow_framework.kafka_publisher import kafka_publisher
from rlam_airflow_framework.formula_engine import get_formula_engine, FormulaError

log = structlog.get_logger(__name__)

# Initialize formula engine
formula_engine = get_formula_engine()

# Temporary storage directory for DataFrames
TEMP_DATA_DIR = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "tmp" / "dataframes"

# Create directory only if we're not on Windows or if AIRFLOW_HOME is set
# This prevents import errors during unit tests on Windows
try:
    TEMP_DATA_DIR.mkdir(parents=True, exist_ok=True)
except (PermissionError, OSError):
    # Directory will be created on demand by _save_dataframe if needed
    pass


def _save_dataframe(df: pd.DataFrame, task_id: str, run_id: str) -> str:
    """Save DataFrame to parquet and return the file path."""
    # Ensure directory exists (in case module-level creation failed)
    TEMP_DATA_DIR.mkdir(parents=True, exist_ok=True)

    filename = f"{task_id}_{run_id}.parquet"
    filepath = TEMP_DATA_DIR / filename
    df.to_parquet(filepath, index=False, compression="snappy")
    log.info(
        f"Saved DataFrame to {filepath}",
        rows=len(df),
        size_mb=filepath.stat().st_size / 1024 / 1024,
    )
    return str(filepath)


def _load_dataframe(filepath: str) -> pd.DataFrame:
    """Load DataFrame from parquet file."""
    df = pd.read_parquet(filepath)
    log.info(f"Loaded DataFrame from {filepath}", rows=len(df))
    return df


def _cleanup_dataframe(filepath: str) -> None:
    """Delete the temporary DataFrame file."""
    try:
        Path(filepath).unlink(missing_ok=True)
        log.info(f"Cleaned up DataFrame file: {filepath}")
    except Exception as e:
        log.warning(f"Failed to cleanup DataFrame file: {filepath}", error=str(e))


@task
def ingest_data(config: Dict[str, Any]) -> str:
    """
    Ingest data from configured source (REST API or SFTP).

    Args:
        config: Pipeline configuration containing data_source section

    Returns:
        File path to saved DataFrame (parquet format)
    """
    context = get_current_context()
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id = context["run_id"]
    correlation_id = f"{dag_id}_{run_id}_{task_id}"

    partition_config = config.get("partition", {})
    is_partitioned = partition_config.get("enabled", False)
    partition_column = partition_config.get("column")

    partition_key = context.get("partition_key")
    partition_date = context.get("partition_date")

    partition_value = None
    if is_partitioned:
        if partition_date is not None:
            if hasattr(partition_date, "strftime"):
                partition_value = partition_date.strftime("%Y-%m-%d")
            else:
                partition_value = str(partition_date)
        elif partition_key is not None:
            partition_value = str(partition_key)

    # Resolve incremental load configurations
    incremental_config = config.get("incremental", {})
    is_incremental = incremental_config.get("enabled", False)
    watermark_column = incremental_config.get("watermark_column")
    initial_watermark = incremental_config.get("initial_watermark")
    lookback = incremental_config.get("lookback", 0)

    current_watermark = None
    if is_incremental:
        from airflow.sdk import Variable

        strict_mode = incremental_config.get("strict", False)

        # Read from Airflow Variable
        current_watermark = Variable.get(f"{dag_id}.high_watermark", default=None)
        if current_watermark:
            log.info(
                "Loaded watermark from Airflow Variable",
                watermark=current_watermark,
            )

        if not current_watermark:
            first_run = Variable.get(f"{dag_id}.first_run_completed", default=None)
            if strict_mode and first_run:
                raise ValueError(
                    "Incremental strict mode: no watermark found but first run is marked completed."
                )

            current_watermark = initial_watermark
            log.warning(
                "incremental configured but no watermark found - performing FULL load from initial_watermark"
            )

        log.info("Resolved incremental watermark", current_watermark=current_watermark)

    # Apply lookback to watermark if configured
    adjusted_watermark = current_watermark
    if is_incremental and lookback and current_watermark:
        try:
            import pendulum

            dt = pendulum.parse(current_watermark)
            adjusted_dt = dt.subtract(seconds=lookback)
            adjusted_watermark = adjusted_dt.isoformat()
            log.info(
                "Adjusted watermark with lookback",
                lookback_seconds=lookback,
                adjusted_watermark=adjusted_watermark,
            )
        except Exception as e:
            log.warning(
                "Failed to apply lookback to watermark, using raw watermark",
                error=str(e),
            )

    # Substitute partition and watermark template variables in config copy
    import copy

    data_source_config = copy.deepcopy(config["data_source"])

    def render_val(val: str) -> str:
        if not isinstance(val, str):
            return val
        res = val
        if partition_value is not None:
            res = res.replace("{{ partition_key }}", partition_value)
            res = res.replace("{{ partition_date }}", partition_value)
            res = res.replace("{{ ds }}", partition_value)
        if adjusted_watermark is not None:
            res = res.replace("{{ watermark }}", adjusted_watermark)
            res = res.replace("{{ last_watermark }}", adjusted_watermark)
        return res

    # Render all config values recursively
    for k, v in list(data_source_config.items()):
        if isinstance(v, str):
            data_source_config[k] = render_val(v)
        elif isinstance(v, dict):
            for sub_k, sub_v in list(v.items()):
                if isinstance(sub_v, str):
                    v[sub_k] = render_val(sub_v)

    source_type = data_source_config["type"]
    source_name = data_source_config["name"]

    # Auto-apply parameters if not explicitly templated
    if is_partitioned and partition_column and partition_value:
        if source_type == "rest_api":
            if "request_config" not in data_source_config:
                data_source_config["request_config"] = {}
            req_cfg = data_source_config["request_config"]
            if "params" not in req_cfg:
                req_cfg["params"] = {}
            if partition_column not in req_cfg["params"]:
                req_cfg["params"][partition_column] = partition_value

    if is_incremental and watermark_column and adjusted_watermark:
        if source_type == "rest_api":
            if "request_config" not in data_source_config:
                data_source_config["request_config"] = {}
            req_cfg = data_source_config["request_config"]
            if "params" not in req_cfg:
                req_cfg["params"] = {}
            if watermark_column not in req_cfg["params"]:
                req_cfg["params"][watermark_column] = adjusted_watermark

    log.info(
        "Starting data ingestion",
        correlation_id=correlation_id,
        source_type=source_type,
        source_name=source_name,
        partition_value=partition_value,
        watermark=adjusted_watermark,
    )

    # Publish ingestion start event (partition threaded into metadata for traceability)
    topic = config.get("event", {}).get("topic", "pipeline-events")
    partition_metadata = (
        {"partition_key": partition_value} if partition_value is not None else {}
    )
    kafka_publisher.publish_pipeline_event(
        dag_id=dag_id,
        task_id=task_id,
        event_type="ingestion_started",
        status="running",
        message=f"Started ingesting from {source_name}",
        execution_date=datetime.now(timezone.utc),
        topic=topic,
        metadata=partition_metadata or None,
    )

    try:
        if source_type == "rest_api":
            req_cfg = data_source_config.get("request_config", {})
            headers = req_cfg.get("headers")
            params = req_cfg.get("params")

            df = fetch_http_data(
                url=data_source_config["endpoint"],
                headers=headers,
                params=params,
                format=data_source_config.get("response_format", "json"),
                correlation_id=correlation_id,
            )
        elif source_type == "sftp":
            df = fetch_sftp_data(data_source_config, correlation_id=correlation_id)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

        # Post-fetch incremental filtering on DataFrame.
        # IMPORTANT: comparisons must never mutate the watermark column — the
        # stored watermark is later computed from it in load_data. Numeric must
        # be tried BEFORE datetime: pd.to_datetime() silently converts integers
        # to epoch-nanosecond timestamps (id=5 -> 1970-01-01T00:00:00.000000005),
        # which corrupted the stored watermark (found in the 3B.4 e2e).
        if is_incremental and watermark_column and not df.empty:
            if watermark_column in df.columns:
                mask = None
                col = df[watermark_column]
                try:
                    mask = pd.to_numeric(col) > float(adjusted_watermark)
                    comparison = "numeric"
                except (ValueError, TypeError):
                    try:
                        mask = pd.to_datetime(col) > pd.to_datetime(adjusted_watermark)
                        comparison = "datetime"
                    except Exception:
                        try:
                            mask = col.astype(str) > str(adjusted_watermark)
                            comparison = "string (lexicographic — verify ordering!)"
                        except Exception as ex:
                            log.error(
                                "Failed to filter DataFrame by watermark", error=str(ex)
                            )
                if mask is not None:
                    df = df[mask]
                    log.info(
                        "Filtered DataFrame by watermark column",
                        comparison=comparison,
                        remaining_rows=len(df),
                        watermark=adjusted_watermark,
                    )

        if df.empty:
            log.warning("No data fetched from source", source_name=source_name)
        else:
            log.info(
                "Data ingestion complete",
                correlation_id=correlation_id,
                rows=len(df),
                columns=len(df.columns),
            )

        # Publish success event
        kafka_publisher.publish_pipeline_event(
            dag_id=dag_id,
            task_id=task_id,
            event_type="ingestion_completed",
            status="success",
            message=f"Ingested {len(df)} rows from {source_name}",
            execution_date=datetime.now(timezone.utc),
            topic=topic,
            metadata={"row_count": len(df), **partition_metadata},
        )

        # Save DataFrame and return path
        filepath = _save_dataframe(df, task_id, run_id)
        return filepath

    except Exception as e:
        log.error("Data ingestion failed", error=str(e), correlation_id=correlation_id)

        # Publish failure event
        kafka_publisher.publish_pipeline_event(
            dag_id=dag_id,
            task_id=task_id,
            event_type="ingestion_failed",
            status="failure",
            message=f"Ingestion failed: {str(e)}",
            execution_date=datetime.now(timezone.utc),
            topic=topic,
        )
        raise


@task
def transform_data(df_path: str, config: Dict[str, Any]) -> str:
    """
    Apply transformations and enrichment to DataFrame.

    Args:
        df_path: File path to input DataFrame from ingestion
        config: Pipeline configuration with transformation/enrichment sections

    Returns:
        File path to transformed DataFrame
    """
    context = get_current_context()
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id = context["run_id"]
    correlation_id = f"{dag_id}_{run_id}_{task_id}"

    # Load DataFrame
    df = _load_dataframe(df_path)

    # Cleanup input file
    _cleanup_dataframe(df_path)

    log.info(
        "Starting data transformation", correlation_id=correlation_id, rows=len(df)
    )

    if df.empty:
        log.warning("Empty DataFrame received for transformation")
        filepath = _save_dataframe(df, task_id, run_id)
        return filepath

    topic = config.get("event", {}).get("topic", "pipeline-events")

    try:
        # Apply transformations
        transformation_config = config.get("transformation", {})
        if transformation_config:
            df = _apply_transformations(df, transformation_config, correlation_id)

        # Apply enrichment
        enrichment_config = config.get("enrichment", [])
        if enrichment_config:
            df = _apply_enrichment(df, enrichment_config, correlation_id)

        log.info(
            "Data transformation complete",
            correlation_id=correlation_id,
            rows=len(df),
        )

        # Publish success event
        kafka_publisher.publish_pipeline_event(
            dag_id=dag_id,
            task_id=task_id,
            event_type="transformation_completed",
            status="success",
            message=f"Transformed {len(df)} rows",
            execution_date=datetime.now(timezone.utc),
            topic=topic,
        )

        # Save and return filepath
        filepath = _save_dataframe(df, task_id, run_id)
        return filepath

    except Exception as e:
        log.error("Transformation failed", error=str(e), correlation_id=correlation_id)

        kafka_publisher.publish_pipeline_event(
            dag_id=dag_id,
            task_id=task_id,
            event_type="transformation_failed",
            status="failure",
            message=f"Transformation failed: {str(e)}",
            execution_date=datetime.now(timezone.utc),
            topic=topic,
        )
        raise


@task
def validate_data_quality(
    df_path: str, config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Run data quality checks and split valid/invalid records.

    Args:
        df_path: File path to input DataFrame to validate
        config: Pipeline configuration with validation section

    Returns:
        Tuple of (valid_df_path, invalid_df_path, dq_results)
    """
    context = get_current_context()
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id = context["run_id"]

    # Load DataFrame
    df = _load_dataframe(df_path)
    _cleanup_dataframe(df_path)

    destination_table = (
        config.get("destination", {}).get("primary", {}).get("table", "unknown")
    )

    log.info("Starting data quality validation", dag_id=dag_id, rows=len(df))

    valid_df, invalid_df, results = run_data_quality_checks(
        df=df,
        config=config,
        dag_id=dag_id,
        task_id=task_id,
        destination_table=destination_table,
    )

    log.info(
        "Data quality validation complete",
        valid_rows=len(valid_df),
        invalid_rows=len(invalid_df),
        status=results.get("status"),
    )

    # Save DataFrames and return paths
    valid_path = _save_dataframe(valid_df, f"{task_id}_valid", run_id)
    invalid_path = _save_dataframe(invalid_df, f"{task_id}_invalid", run_id)

    return {"valid_path": valid_path, "invalid_path": invalid_path, "results": results}


@task.branch(do_xcom_push=False)
def route_dq_results(dq_results: Dict[str, Any], config: Dict[str, Any]) -> str:
    """
    Route pipeline based on data quality results.

    Args:
        dq_results: Data quality check results
        config: Pipeline configuration

    Returns:
        Task ID to execute next ("load_valid_data" or "quarantine_invalid_data")
    """
    context = get_current_context()

    quality_gates = config.get("validation", {}).get("quality_gates", {})
    should_quarantine = quality_gates.get("quarantine_invalid", False)

    invalid_count = dq_results.get("invalid_rows", 0)

    if should_quarantine and invalid_count > 0:
        log.info(
            "Routing to quarantine",
            invalid_rows=invalid_count,
            dag_id=context["dag"].dag_id,
        )
        return "quarantine_invalid_data"
    else:
        log.info("Routing to load", dag_id=context["dag"].dag_id)
        return "load_valid_data"


@task
def quarantine_invalid_data(
    invalid_df_path: str, dq_results: Dict[str, Any], config: Dict[str, Any]
) -> str:
    """
    Prepare invalid records for quarantine storage.

    Args:
        invalid_df_path: File path to DataFrame with invalid records
        dq_results: Data quality check results
        config: Pipeline configuration

    Returns:
        File path to quarantine records DataFrame
    """
    context = get_current_context()
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    task_id = context["task"].task_id

    # Load DataFrame
    invalid_df = _load_dataframe(invalid_df_path)
    _cleanup_dataframe(invalid_df_path)

    handler = QuarantineHandler(config)

    # Extract failed checks
    failed_checks = [
        check["name"]
        for check in dq_results.get("checks", [])
        if check.get("outcome") == "fail"
    ]

    destination_table = (
        config.get("destination", {}).get("primary", {}).get("table", "unknown")
    )

    quarantine_df = handler.prepare_quarantine_records(
        invalid_df=invalid_df,
        source_pipeline=dag_id,
        source_table=destination_table,
        failed_checks=failed_checks,
    )

    log.info("Prepared quarantine records", count=len(quarantine_df))

    # Save and return path
    filepath = _save_dataframe(quarantine_df, task_id, run_id)
    return filepath


@task
def prepare_hitl_approval_context(
    quarantine_df_path: str, config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Prepare context for Human-in-the-Loop approval task.

    NOTE: This task prepares the approval context data. The actual HITL approval
    is done via ApprovalOperator in the DAG definition (see dag_factory_v2.py).
    HITL operators cannot be used as @task decorators - they must be instantiated
    as operators in the DAG.

    Args:
        quarantine_df_path: File path to quarantined records awaiting approval
        config: Pipeline configuration

    Returns:
        Approval context for HITL operator
    """
    context = get_current_context()
    dag_id = context["dag"].dag_id

    # Load DataFrame
    quarantine_df = _load_dataframe(quarantine_df_path)
    # Do NOT cleanup yet - we need the file for process_approval_decision after HITL approval

    # Create HITL approval context
    approval_context = create_hitl_quarantine_approval_task(
        dag_id=dag_id,
        quarantine_records=quarantine_df,
        config=config,
    )

    log.info(
        "Prepared quarantine approval context",
        dag_id=dag_id,
        records=len(quarantine_df),
        timeout_hours=approval_context["timeout_hours"],
    )

    # Return context for HITL operator to use
    return approval_context


@task
def process_approval_decision(
    approval_result: Dict[str, Any],
    quarantine_df_path: str,
    config: Dict[str, Any],
) -> str:
    """
    Process HITL approval decision and update quarantine records.

    Args:
        approval_result: Approval decision from steward
        quarantine_df_path: Path to quarantine records DataFrame
        config: Pipeline configuration

    Returns:
        Updated quarantine records with approval metadata
    """
    # Load DataFrame
    quarantine_df = _load_dataframe(quarantine_df_path)

    # Process approval result
    # We construct the actual result dict based on the HITL Trigger event payload
    chosen_options = approval_result.get("chosen_options", [])

    if not chosen_options:
        raise ValueError("No option chosen in HITL response")

    raw_action = chosen_options[0]
    action_map = {
        "Approve": "approve_release",
        "Reject": "reject_release",
        "Reprocess": "reprocess",
    }

    if raw_action not in action_map:
        raise ValueError(f"Unmapped HITL action received: {raw_action}")

    action = action_map[raw_action]
    notes = approval_result.get("params_input", {}).get("notes", "")

    responder = approval_result.get("responded_by_user")
    approved_by = responder.get("id", "unknown") if responder else "unknown"

    formatted_result = {"action": action, "notes": notes, "approved_by": approved_by}

    updated_df, action = process_hitl_approval_result(
        approval_result=formatted_result,
        quarantine_records=quarantine_df,
        config=config,
    )

    # Cleanup the original quarantine file now that we're done
    _cleanup_dataframe(quarantine_df_path)

    # Save and return the updated records
    context = get_current_context()
    task_id = context["task"].task_id
    run_id = context["dag_run"].run_id

    filepath = _save_dataframe(updated_df, task_id, run_id)

    log.info("Processed approval decision", action=action, records=len(updated_df))

    return filepath


def partition_scoped_path(
    path_or_uri: str, partition_column: str, partition_value: str
) -> str:
    """Structure a file path/URI by inserting a partition folder."""
    if not path_or_uri or not partition_column or not partition_value:
        return path_or_uri
    if f"{partition_column}=" in path_or_uri or partition_value in path_or_uri:
        # Already has partition info
        return path_or_uri

    parts = path_or_uri.rsplit("/", 1)
    if len(parts) == 2:
        return f"{parts[0]}/{partition_column}={partition_value}/{parts[1]}"
    else:
        return f"{partition_column}={partition_value}/{path_or_uri}"


@task
def load_data(
    df_path: str,
    config: Dict[str, Any],
) -> str:
    """
    Load data to configured destinations.

    Args:
        df_path: File path to DataFrame to load
        config: Pipeline configuration with destination section

    Returns:
        Summary of load operations
    """
    context = get_current_context()
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    correlation_id = f"{dag_id}_{context['run_id']}_{task_id}"

    # Resolve partition config
    partition_config = config.get("partition", {})
    is_partitioned = partition_config.get("enabled", False)
    partition_column = partition_config.get("column")

    partition_key = context.get("partition_key")
    partition_date = context.get("partition_date")

    partition_value = None
    if is_partitioned:
        if partition_date is not None:
            if hasattr(partition_date, "strftime"):
                partition_value = partition_date.strftime("%Y-%m-%d")
            else:
                partition_value = str(partition_date)
        elif partition_key is not None:
            partition_value = str(partition_key)

    # Resolve incremental config
    incremental_config = config.get("incremental", {})
    is_incremental = incremental_config.get("enabled", False)
    watermark_column = incremental_config.get("watermark_column")

    # Load DataFrame
    df = _load_dataframe(df_path)
    _cleanup_dataframe(df_path)

    log.info(
        "Starting data load",
        correlation_id=correlation_id,
        rows=len(df),
        partition_value=partition_value,
    )

    if df.empty:
        log.warning("Empty DataFrame provided for loading")
        return "No data to load (empty DataFrame)"

    # Substitute partition templates in destination config
    import copy

    destination_config = copy.deepcopy(config.get("destination", {}))

    def render_val(val: str) -> str:
        if not isinstance(val, str):
            return val
        res = val
        if partition_value is not None:
            res = res.replace("{{ partition_key }}", partition_value)
            res = res.replace("{{ partition_date }}", partition_value)
            res = res.replace("{{ ds }}", partition_value)
        return res

    def render_dict(d: dict) -> None:
        for k, v in list(d.items()):
            if isinstance(v, str):
                d[k] = render_val(v)
            elif isinstance(v, dict):
                render_dict(v)

    render_dict(destination_config)

    # Auto-structure paths for object storage / local files if partitioned
    if is_partitioned and partition_column and partition_value:

        def adjust_path(dest: dict) -> None:
            if dest.get("type") == "object_storage":
                if "uri" in dest:
                    dest["uri"] = partition_scoped_path(
                        dest["uri"], partition_column, partition_value
                    )
                if "path" in dest:
                    dest["path"] = partition_scoped_path(
                        dest["path"], partition_column, partition_value
                    )
            elif dest.get("type") == "local_file":
                if "path" in dest:
                    dest["path"] = partition_scoped_path(
                        dest["path"], partition_column, partition_value
                    )

        for k in ["primary", "backup", "archive"]:
            if k in destination_config:
                adjust_path(destination_config[k])

    topic = config.get("event", {}).get("topic", "pipeline-events")
    results = []

    try:
        # Load to primary destination
        if "primary" in destination_config:
            result = _load_to_destination(
                df,
                destination_config["primary"],
                topic,
                correlation_id,
                "primary",
                partition_column=partition_column,
                partition_value=partition_value,
            )
            results.append(f"Primary: {result}")

        # Load to backup destination
        if "backup" in destination_config:
            result = _load_to_destination(
                df,
                destination_config["backup"],
                topic,
                correlation_id,
                "backup",
                partition_column=partition_column,
                partition_value=partition_value,
            )
            results.append(f"Backup: {result}")

        # Load to archive destination
        if "archive" in destination_config:
            result = _load_to_destination(
                df,
                destination_config["archive"],
                topic,
                correlation_id,
                "archive",
                partition_column=partition_column,
                partition_value=partition_value,
            )
            results.append(f"Archive: {result}")

        summary = " | ".join(results) if results else "No destinations configured"

        log.info("Data load complete", summary=summary, correlation_id=correlation_id)

        # Update watermark using Airflow Variable
        if is_incremental and watermark_column and not df.empty:
            if watermark_column in df.columns:
                from airflow.sdk import Variable

                max_val = df[watermark_column].max()
                new_watermark = (
                    max_val.isoformat()
                    if hasattr(max_val, "isoformat")
                    else str(max_val)
                )

                Variable.set(f"{dag_id}.high_watermark", new_watermark)
                Variable.set(f"{dag_id}.first_run_completed", "true")
                log.info(
                    "Saved watermark to Airflow Variable", watermark=new_watermark
                )

        # Emit standard OpenLineage dataset for observability.
        # partition_key rides along so the "data landed" event is traceable to
        # its partition (closes the gap left by the 2A.4.2 ingest-only threading).
        kafka_publisher.publish_data(
            dag_id=dag_id,
            data=df.to_dict(orient="records"),
            topic=topic,
            status="success",
            correlation_id=correlation_id,
            metadata=(
                {"partition_key": partition_value}
                if partition_value is not None
                else None
            ),
        )

        return summary

    except Exception as e:
        log.error("Data load failed", error=str(e), correlation_id=correlation_id)

        kafka_publisher.publish_pipeline_event(
            dag_id=dag_id,
            task_id=task_id,
            event_type="load_failed",
            status="failure",
            message=f"Load failed: {str(e)}",
            execution_date=datetime.now(timezone.utc),
            topic=topic,
            metadata=(
                {"partition_key": partition_value}
                if partition_value is not None
                else None
            ),
        )
        raise


# =============================================================================
# Helper Functions
# =============================================================================


def _apply_transformations(
    df: pd.DataFrame, transformation_config: Dict[str, Any], correlation_id: str
) -> pd.DataFrame:
    """Apply column types, new columns, and filters."""
    # Set column types
    column_types = transformation_config.get("column_types", {})
    for col, dtype in column_types.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                log.warning(
                    "Failed to convert column type",
                    column=col,
                    dtype=dtype,
                    error=str(e),
                )

    # Add new columns using formula engine
    new_columns = transformation_config.get("new_columns", {})
    for col_name, formula in new_columns.items():
        try:
            df[col_name] = formula_engine.evaluate(formula, df)
        except FormulaError as e:
            log.error("Formula evaluation failed", column=col_name, error=str(e))
            raise

    # Apply filters
    filters = transformation_config.get("filters", {})
    for filter_name, filter_expr in filters.items():
        try:
            mask = formula_engine.evaluate(filter_expr, df)
            df = df[mask]
            log.info("Applied filter", filter=filter_name, remaining_rows=len(df))
        except FormulaError as e:
            log.error("Filter evaluation failed", filter=filter_name, error=str(e))
            raise

    return df


def _apply_enrichment(
    df: pd.DataFrame, enrichment_config: List[Dict[str, Any]], correlation_id: str
) -> pd.DataFrame:
    """Apply enrichment from Snowflake lookups."""
    for enrichment in enrichment_config:
        enrichment_type = enrichment.get("type")

        if enrichment_type == "snowflake_lookup":
            df = enrich_from_snowflake(
                df=df,
                snowflake_conn_id=enrichment.get("connection_id", "snowflake-default"),
                lookup_config=enrichment,
                correlation_id=correlation_id,
            )
        else:
            log.warning("Unsupported enrichment type", type=enrichment_type)

    return df


def _load_to_destination(
    df: pd.DataFrame,
    dest_config: Dict[str, Any],
    topic: str,
    correlation_id: str,
    dest_label: str,
    partition_column: Optional[str] = None,
    partition_value: Optional[str] = None,
) -> str:
    """Load data to a specific destination."""
    dest_type = dest_config.get("type")

    log.info(
        "Loading to destination",
        destination_type=dest_type,
        label=dest_label,
        correlation_id=correlation_id,
    )

    if dest_type == "snowflake_table":
        table = dest_config["table"]
        mode = dest_config.get("mode", "append")
        conn_id = dest_config.get("connection_id", "snowflake-default")

        # Parse schema.table
        parts = table.split(".")
        table_name = parts[-1]
        schema = parts[-2] if len(parts) > 1 else "PUBLIC"

        return load_to_snowflake(
            df=df,
            snowflake_conn_id=conn_id,
            table_name=table_name,
            schema=schema,
            if_exists=mode,
            correlation_id=correlation_id,
            partition_column=partition_column,
            partition_value=partition_value,
        )

    elif dest_type == "snowflake_stage":
        stage_name = dest_config.get("stage_name", "DATA_STAGE")
        file_name = dest_config.get(
            "file_name", f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        file_format = dest_config.get("format", "csv")
        conn_id = dest_config.get("connection_id", "snowflake-default")

        return load_to_snowflake_stage(
            df=df,
            snowflake_conn_id=conn_id,
            stage_name=stage_name,
            file_name=file_name,
            file_format=file_format,
            correlation_id=correlation_id,
        )

    elif dest_type == "object_storage":
        # Support both uri and path+conn_id patterns
        uri = dest_config.get("uri")
        path = dest_config.get("path")
        conn_id = dest_config.get("conn_id")
        file_format = dest_config.get("format", "parquet")

        return load_to_object_storage(
            df=df,
            uri=uri,
            path=path,
            conn_id=conn_id,
            file_format=file_format,
            correlation_id=correlation_id,
        )

    elif dest_type == "local_file":
        path = dest_config["path"]
        file_format = dest_config.get("format", "parquet")

        return load_to_local_file(
            df=df,
            file_path=path,
            file_format=file_format,
            correlation_id=correlation_id,
        )

    elif dest_type == "print_logs":
        max_rows = dest_config.get("max_rows", 10)
        log.info(f"=== DATA OUTPUT ({len(df)} total rows) ===")
        log.info(f"Columns: {list(df.columns)}")
        log.info(f"Data types:\n{df.dtypes}")
        log.info(f"First {max_rows} rows:\n{df.head(max_rows).to_string()}")
        if len(df) > max_rows:
            log.info(f"... and {len(df) - max_rows} more rows")
        log.info("=== END DATA OUTPUT ===")
        return f"Printed {len(df)} rows to logs"

    elif dest_type == "stored_procedure":
        procedure_name = dest_config["procedure"]
        parameters = dest_config.get("parameters", [])
        capture_result = dest_config.get("capture_result", True)
        conn_id = dest_config.get("connection_id", "snowflake-default")

        call_stored_procedure(
            snowflake_conn_id=conn_id,
            procedure_name=procedure_name,
            parameters=parameters,
            capture_result=capture_result,
            correlation_id=correlation_id,
        )

        return f"Called stored procedure {procedure_name}"

    else:
        raise ValueError(f"Unsupported destination type: {dest_type}")
