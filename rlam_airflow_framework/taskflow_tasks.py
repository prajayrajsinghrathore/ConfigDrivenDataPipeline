# File: rlam_airflow_framework/taskflow_tasks.py
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

Architecture:
- Each @task function is a thin adapter that delegates to OOP building
  blocks in ``rlam_airflow_framework.taskflow``:
  - ``TaskExecutionContext`` — Airflow runtime metadata
  - ``DataFrameStorage`` — parquet temp-file I/O
  - ``WatermarkManager`` — incremental-load watermark lifecycle
  - ``PartitionInfo`` — partition resolution and path scoping
"""

import copy

import pandas as pd
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from pathlib import Path
import os
import structlog

from airflow.sdk import task, get_current_context

from rlam_airflow_framework.data_fetchers import get_data_fetcher
from rlam_airflow_framework.destinations import DESTINATION_REGISTRY, LoadContext
from rlam_airflow_framework.data_quality import (
    run_data_quality_checks,
    QuarantineHandler,
    create_hitl_quarantine_approval_task,
    process_hitl_approval_result,
)
from rlam_airflow_framework.kafka_publisher import kafka_publisher
from rlam_airflow_framework.transformers import apply_pipeline_transformations

from rlam_airflow_framework.taskflow.context import TaskExecutionContext
from rlam_airflow_framework.taskflow.storage import DataFrameStorage
from rlam_airflow_framework.taskflow.watermark import WatermarkConfig, WatermarkManager
from rlam_airflow_framework.taskflow.partition import PartitionInfo

log = structlog.get_logger(__name__)

# =============================================================================
# Backward-compatibility shims
# =============================================================================
# Existing tests and external code patch / import these names from this module.
# They now delegate to the OOP classes but keep the old call signatures.

# DEPRECATED: use DataFrameStorage directly.
TEMP_DATA_DIR = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "tmp" / "dataframes"

# Create directory only if we're not on Windows or if AIRFLOW_HOME is set
# This prevents import errors during unit tests on Windows
try:
    TEMP_DATA_DIR.mkdir(parents=True, exist_ok=True)
except (PermissionError, OSError):
    # Directory will be created on demand by _save_dataframe if needed
    pass


def _save_dataframe(df: pd.DataFrame, task_id: str, run_id: str) -> str:
    """Save DataFrame to parquet and return the file path.

    .. deprecated:: Use ``DataFrameStorage.save()`` instead.
    """
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
    """Load DataFrame from parquet file.

    .. deprecated:: Use ``DataFrameStorage.load()`` instead.
    """
    df = pd.read_parquet(filepath)
    log.info(f"Loaded DataFrame from {filepath}", rows=len(df))
    return df


def _cleanup_dataframe(filepath: str) -> None:
    """Delete the temporary DataFrame file.

    .. deprecated:: Use ``DataFrameStorage.cleanup()`` instead.
    """
    try:
        Path(filepath).unlink(missing_ok=True)
        log.info(f"Cleaned up DataFrame file: {filepath}")
    except Exception as e:
        log.warning(f"Failed to cleanup DataFrame file: {filepath}", error=str(e))


def _build_context() -> TaskExecutionContext:
    """
    Build a TaskExecutionContext using the module-level ``get_current_context``.

    This indirection exists so that tests can patch
    ``rlam_airflow_framework.taskflow_tasks.get_current_context`` and
    have the mock take effect in all @task functions.
    """
    from typing import cast as _cast

    ctx = _cast(Dict[str, Any], get_current_context())
    dag_id = ctx["dag"].dag_id
    task_id = ctx["task"].task_id
    run_id = ctx["run_id"]
    return TaskExecutionContext(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        correlation_id=f"{dag_id}_{run_id}_{task_id}",
        raw_context=ctx,
    )


def _task_context() -> Dict[str, Any]:
    """
    Fetch the current Airflow task context as a plain dict.

    .. deprecated:: Use ``_build_context()`` or ``TaskExecutionContext`` instead.
    """
    return _build_context().raw_context


def _resolve_partition_value(
    config: Dict[str, Any], context: Dict[str, Any]
) -> tuple:
    """
    Resolve (is_partitioned, partition_column, partition_value).

    .. deprecated:: Use ``PartitionInfo.resolve()`` instead.
    """
    p = PartitionInfo.resolve(config, context)
    return p.enabled, p.column, p.value


# =============================================================================
# Public helpers (not deprecated)
# =============================================================================


def render_config_templates(
    cfg: Dict[str, Any], substitutions: Dict[str, Optional[str]]
) -> Dict[str, Any]:
    """
    Recursively replace ``{{ var }}`` placeholders in every string value of a config dict.

    Args:
        cfg: Config dict to render (mutated in place, and returned for convenience).
        substitutions: Map of template variable name -> replacement value; entries
            whose value is None are skipped.

    Returns:
        The same ``cfg`` dict with placeholders substituted.
    """
    active = {name: val for name, val in substitutions.items() if val is not None}

    def render_val(val: Any) -> Any:
        if not isinstance(val, str):
            return val
        for name, value in active.items():
            val = val.replace(f"{{{{ {name} }}}}", value)
        return val

    def walk(node: Dict[str, Any]) -> None:
        for key, val in list(node.items()):
            if isinstance(val, str):
                node[key] = render_val(val)
            elif isinstance(val, dict):
                walk(val)
            elif isinstance(val, list):
                node[key] = [render_val(item) for item in val]

    walk(cfg)
    return cfg


def partition_scoped_path(
    path_or_uri: str, partition_column: str, partition_value: str
) -> str:
    """Structure a file path/URI by inserting a partition folder.

    .. deprecated:: Use ``PartitionInfo.scope_path()`` instead.
    """
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


# =============================================================================
# @task functions — thin adapters delegating to OOP building blocks
# =============================================================================


@task
def ingest_data(config: Dict[str, Any]) -> str:
    """
    Ingest data from configured source (REST API or SFTP).

    Args:
        config: Pipeline configuration containing data_source section

    Returns:
        File path to saved DataFrame (parquet format)
    """
    ctx = _build_context()
    storage = DataFrameStorage()
    partition = PartitionInfo.resolve(config, ctx.raw_context)
    wm = WatermarkManager(ctx.dag_id, WatermarkConfig.from_config(config))

    adjusted_watermark = wm.resolve()

    # Substitute partition and watermark template variables in config copy
    data_source_config = copy.deepcopy(config["data_source"])
    data_source_config = render_config_templates(
        data_source_config,
        {
            "partition_key": partition.value,
            "partition_date": partition.value,
            "ds": partition.value,
            "watermark": adjusted_watermark,
            "last_watermark": adjusted_watermark,
        },
    )

    source_type = data_source_config["type"]
    source_name = data_source_config["name"]

    # Auto-apply parameters if not explicitly templated
    if partition.enabled and partition.column and partition.value:
        if source_type == "rest_api":
            if "request_config" not in data_source_config:
                data_source_config["request_config"] = {}
            req_cfg = data_source_config["request_config"]
            if "params" not in req_cfg:
                req_cfg["params"] = {}
            if partition.column not in req_cfg["params"]:
                req_cfg["params"][partition.column] = partition.value

    if (
        wm.config.enabled
        and wm.config.watermark_column
        and adjusted_watermark
    ):
        if source_type == "rest_api":
            if "request_config" not in data_source_config:
                data_source_config["request_config"] = {}
            req_cfg = data_source_config["request_config"]
            if "params" not in req_cfg:
                req_cfg["params"] = {}
            if wm.config.watermark_column not in req_cfg["params"]:
                req_cfg["params"][wm.config.watermark_column] = adjusted_watermark

    log.info(
        "Starting data ingestion",
        correlation_id=ctx.correlation_id,
        source_type=source_type,
        source_name=source_name,
        partition_value=partition.value,
        watermark=adjusted_watermark,
    )

    # Publish ingestion start event
    topic = config.get("event", {}).get("topic", "pipeline-events")
    kafka_publisher.publish_pipeline_event(
        dag_id=ctx.dag_id,
        task_id=ctx.task_id,
        event_type="ingestion_started",
        status="running",
        message=f"Started ingesting from {source_name}",
        execution_date=datetime.now(timezone.utc),
        topic=topic,
        metadata=partition.metadata or None,
    )

    try:
        fetcher = get_data_fetcher(source_type)
        fetch_task_id = f"fetch_{partition.value}" if partition.enabled else "fetch"
        target_path = storage.get_path(task_id=fetch_task_id, run_id=ctx.run_id)

        fetch_result = fetcher.fetch(
            data_source_config, correlation_id=ctx.correlation_id, target_path=target_path
        )

        row_count = 0
        if isinstance(fetch_result, Path):
            # Streaming fetcher (out-of-core Parquet)
            output_path = str(fetch_result)
            
            # Post-fetch incremental filtering via DuckDB
            if wm.config.enabled and wm.config.watermark_column and adjusted_watermark:
                import duckdb
                import tempfile
                
                temp_fd, temp_path = tempfile.mkstemp(suffix=".parquet")
                os.close(temp_fd)
                
                col = wm.config.watermark_column
                val = str(adjusted_watermark).replace("'", "''")
                
                # Filter into a temp file
                duckdb.query(f"COPY (SELECT * FROM '{output_path}' WHERE \"{col}\" > '{val}') TO '{temp_path}' (FORMAT PARQUET)")
                
                # Overwrite original with filtered
                os.replace(temp_path, output_path)
                log.info("Applied watermark filter via DuckDB", watermark_column=col, watermark_value=val)
            
            # Get row count via DuckDB
            import duckdb
            try:
                row_count = duckdb.query(f"SELECT count(*) FROM '{output_path}'").fetchone()[0]
            except Exception:
                row_count = 0
                
            if row_count == 0:
                log.warning("No data fetched from source (or all filtered out)", source_name=source_name)
            else:
                log.info("Data ingestion complete", correlation_id=ctx.correlation_id, rows=row_count)
                
        else:
            # Legacy fetcher (in-memory pd.DataFrame)
            df = fetch_result
            # Post-fetch incremental filtering
            df = wm.filter_dataframe(df)

            if df.empty:
                log.warning("No data fetched from source", source_name=source_name)
            else:
                row_count = len(df)
                log.info(
                    "Data ingestion complete",
                    correlation_id=ctx.correlation_id,
                    rows=row_count,
                    columns=len(df.columns),
                )
                
            output_path = storage.save(
                df,
                task_id=fetch_task_id,
                run_id=ctx.run_id,
            )

        # Publish success event
        kafka_publisher.publish_pipeline_event(
            dag_id=ctx.dag_id,
            task_id=ctx.task_id,
            event_type="ingestion_completed",
            status="success",
            message=f"Ingested {row_count} rows from {source_name}",
            execution_date=datetime.now(timezone.utc),
            topic=topic,
            metadata={"row_count": row_count, **(partition.metadata or {})},
        )

        return output_path

    except Exception as e:
        log.error(
            "Data ingestion failed", error=str(e), correlation_id=ctx.correlation_id
        )

        # Publish failure event
        kafka_publisher.publish_pipeline_event(
            dag_id=ctx.dag_id,
            task_id=ctx.task_id,
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
    ctx = _build_context()
    storage = DataFrameStorage()

    # Load DataFrame. Cleanup of df_path is deferred until this task fully
    # succeeds (see returns below) so a mid-task transient failure lets an
    # Airflow retry reload the same input file instead of hitting a
    # FileNotFoundError from a file we already deleted on attempt 1.
    df = storage.load(df_path)

    log.info(
        "Starting data transformation",
        correlation_id=ctx.correlation_id,
        rows=len(df),
    )

    if df.empty:
        log.warning("Empty DataFrame received for transformation")
        result_path = storage.save(df, ctx.task_id, ctx.run_id)
        storage.cleanup(df_path)
        return result_path

    topic = config.get("event", {}).get("topic", "pipeline-events")

    try:
        # Apply the unified, ordered transformations pipeline
        transformations_list = config.get("transformations", [])
        if transformations_list:
            df = apply_pipeline_transformations(
                df, transformations_list, ctx.correlation_id
            )

        log.info(
            "Data transformation complete",
            correlation_id=ctx.correlation_id,
            rows=len(df),
        )

        # Publish success event
        kafka_publisher.publish_pipeline_event(
            dag_id=ctx.dag_id,
            task_id=ctx.task_id,
            event_type="transformation_completed",
            status="success",
            message=f"Transformed {len(df)} rows",
            execution_date=datetime.now(timezone.utc),
            topic=topic,
        )

        result_path = storage.save(df, ctx.task_id, ctx.run_id)
        storage.cleanup(df_path)
        return result_path

    except Exception as e:
        log.error(
            "Transformation failed", error=str(e), correlation_id=ctx.correlation_id
        )

        kafka_publisher.publish_pipeline_event(
            dag_id=ctx.dag_id,
            task_id=ctx.task_id,
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
    ctx = _build_context()
    storage = DataFrameStorage()

    # Load DataFrame. Cleanup deferred until DQ checks + saves succeed, so a
    # retry after a mid-task failure can still reload df_path.
    df = storage.load(df_path)

    destination_table = (
        config.get("destination", {}).get("primary", {}).get("table", "unknown")
    )

    log.info("Starting data quality validation", dag_id=ctx.dag_id, rows=len(df))

    valid_df, invalid_df, results = run_data_quality_checks(
        df=df,
        config=config,
        dag_id=ctx.dag_id,
        task_id=ctx.task_id,
        destination_table=destination_table,
    )

    log.info(
        "Data quality validation complete",
        valid_rows=len(valid_df),
        invalid_rows=len(invalid_df),
        status=results.get("status"),
    )

    # Save DataFrames and return paths
    valid_path = storage.save(valid_df, f"{ctx.task_id}_valid", ctx.run_id)
    invalid_path = storage.save(invalid_df, f"{ctx.task_id}_invalid", ctx.run_id)
    storage.cleanup(df_path)

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
    ctx = _build_context()

    quality_gates = config.get("validation", {}).get("quality_gates", {})
    should_quarantine = quality_gates.get("quarantine_invalid", False)

    invalid_count = dq_results.get("invalid_rows", 0)

    if should_quarantine and invalid_count > 0:
        log.info(
            "Routing to quarantine",
            invalid_rows=invalid_count,
            dag_id=ctx.dag_id,
        )
        return "quarantine_invalid_data"
    else:
        log.info("Routing to load", dag_id=ctx.dag_id)
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
    ctx = _build_context()
    storage = DataFrameStorage()

    # Load DataFrame. Cleanup deferred until the quarantine record save
    # succeeds, so a retry after a mid-task failure can reload invalid_df_path.
    invalid_df = storage.load(invalid_df_path)

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
        source_pipeline=ctx.dag_id,
        source_table=destination_table,
        failed_checks=failed_checks,
    )

    log.info("Prepared quarantine records", count=len(quarantine_df))

    result_path = storage.save(quarantine_df, ctx.task_id, ctx.run_id)
    storage.cleanup(invalid_df_path)
    return result_path


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
    ctx = _build_context()
    storage = DataFrameStorage()

    # Load DataFrame
    quarantine_df = storage.load(quarantine_df_path)
    # Do NOT cleanup yet - we need the file for process_approval_decision after HITL approval

    # Create HITL approval context
    approval_context = create_hitl_quarantine_approval_task(
        dag_id=ctx.dag_id,
        quarantine_records=quarantine_df,
        config=config,
    )

    log.info(
        "Prepared quarantine approval context",
        dag_id=ctx.dag_id,
        records=len(quarantine_df),
        timeout_hours=approval_context["timeout_hours"],
    )

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
    ctx = _build_context()
    storage = DataFrameStorage()

    # Load DataFrame
    quarantine_df = storage.load(quarantine_df_path)

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
    storage.cleanup(quarantine_df_path)

    filepath = storage.save(updated_df, ctx.task_id, ctx.run_id)

    log.info("Processed approval decision", action=action, records=len(updated_df))

    return filepath


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
    ctx = _build_context()
    storage = DataFrameStorage()
    partition = PartitionInfo.resolve(config, ctx.raw_context)
    wm = WatermarkManager(ctx.dag_id, WatermarkConfig.from_config(config))

    # Load DataFrame. Cleanup deferred until the destination writes (and the
    # watermark/lineage updates that depend on them) fully succeed, so a
    # retry after a mid-task failure can reload df_path.
    df = storage.load(df_path)

    log.info(
        "Starting data load",
        correlation_id=ctx.correlation_id,
        rows=len(df),
        partition_value=partition.value,
    )

    if df.empty:
        log.warning("Empty DataFrame provided for loading")
        storage.cleanup(df_path)
        return "No data to load (empty DataFrame)"

    # Substitute partition templates in destination config
    destination_config = copy.deepcopy(config.get("destination", {}))

    destination_config = render_config_templates(
        destination_config,
        {
            "partition_key": partition.value,
            "partition_date": partition.value,
            "ds": partition.value,
        },
    )

    # Auto-structure paths for partitioned runs
    partition.adjust_dest_paths(destination_config)

    topic = config.get("event", {}).get("topic", "pipeline-events")
    results = []

    try:
        # Load to each configured destination
        for dest_label in ("primary", "backup", "archive"):
            if dest_label in destination_config:
                result = _load_to_destination(
                    df,
                    destination_config[dest_label],
                    topic,
                    ctx.correlation_id,
                    dest_label,
                    partition_column=partition.column,
                    partition_value=partition.value,
                )
                results.append(f"{dest_label.capitalize()}: {result}")

        summary = " | ".join(results) if results else "No destinations configured"

        log.info(
            "Data load complete", summary=summary, correlation_id=ctx.correlation_id
        )

        # Update watermark
        wm.update(df)

        # Emit standard OpenLineage dataset for observability. This is a
        # lineage/summary event, not a data-export channel: it carries schema
        # and row-count facets only. Dumping full row content here would risk
        # exceeding Kafka's message.max.bytes (default 1MB) on any
        # non-trivial DataFrame and would OOM the worker serializing it.
        # partition_key rides along so the "data landed" event is traceable to
        # its partition (closes the gap left by the 2A.4.2 ingest-only threading).
        kafka_publisher.publish_data(
            dag_id=ctx.dag_id,
            data={
                "row_count": len(df),
                "columns": list(df.columns),
                "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            },
            topic=topic,
            status="success",
            correlation_id=ctx.correlation_id,
            metadata=(
                {"partition_key": partition.value}
                if partition.value is not None
                else None
            ),
        )

        storage.cleanup(df_path)
        return summary

    except Exception as e:
        log.error(
            "Data load failed", error=str(e), correlation_id=ctx.correlation_id
        )

        kafka_publisher.publish_pipeline_event(
            dag_id=ctx.dag_id,
            task_id=ctx.task_id,
            event_type="load_failed",
            status="failure",
            message=f"Load failed: {str(e)}",
            execution_date=datetime.now(timezone.utc),
            topic=topic,
            metadata=(
                {"partition_key": partition.value}
                if partition.value is not None
                else None
            ),
        )
        raise


# =============================================================================
# Helper Functions
# =============================================================================


def _load_to_destination(
    df: pd.DataFrame,
    dest_config: Dict[str, Any],
    topic: str,
    correlation_id: str,
    dest_label: str,
    partition_column: Optional[str] = None,
    partition_value: Optional[str] = None,
) -> str:
    """
    Load data to a specific destination via the destination registry.

    Dispatch is polymorphic: the registry resolves the config ``type`` to a
    DestinationLoader strategy. Adding a sink type means registering a new
    loader in ``destinations.py`` - this function does not change.
    """
    dest_type = dest_config.get("type")

    log.info(
        "Loading to destination",
        destination_type=dest_type,
        label=dest_label,
        correlation_id=correlation_id,
    )

    loader = DESTINATION_REGISTRY.get(dest_type)
    load_ctx = LoadContext(
        topic=topic,
        correlation_id=correlation_id,
        dest_label=dest_label,
        partition_column=partition_column,
        partition_value=partition_value,
    )
    return loader.load(df, dest_config, load_ctx)
