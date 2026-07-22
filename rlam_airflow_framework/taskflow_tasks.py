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
from typing import Dict, Any, List, Tuple
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
    log.info(f"Saved DataFrame to {filepath}", rows=len(df), size_mb=filepath.stat().st_size / 1024 / 1024)
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
    
    data_source_config = config["data_source"]
    source_type = data_source_config["type"]
    source_name = data_source_config["name"]
    
    log.info(
        "Starting data ingestion",
        correlation_id=correlation_id,
        source_type=source_type,
        source_name=source_name,
    )
    
    # Publish ingestion start event
    topic = config.get("event", {}).get("topic", "pipeline-events")
    kafka_publisher.publish_pipeline_event(
        dag_id=dag_id,
        task_id=task_id,
        event_type="ingestion_started",
        status="running",
        message=f"Started ingesting from {source_name}",
        execution_date=datetime.now(timezone.utc),
        topic=topic,
    )
    
    try:
        if source_type == "rest_api":
            df = fetch_http_data(
                url=data_source_config["endpoint"],
                format=data_source_config.get("response_format", "json"),
                correlation_id=correlation_id,
            )
        elif source_type == "sftp":
            df = fetch_sftp_data(data_source_config, correlation_id=correlation_id)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
        
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
            metadata={"row_count": len(df)},
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
    
    log.info("Starting data transformation", correlation_id=correlation_id, rows=len(df))
    
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
) -> Tuple[str, str, Dict[str, Any]]:
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
    
    return valid_path, invalid_path, results


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
    _cleanup_dataframe(quarantine_df_path)
    
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
    quarantine_df: pd.DataFrame,
    config: Dict[str, Any],
) -> pd.DataFrame:
    """
    Process HITL approval decision and update quarantine records.
    
    Args:
        approval_result: Approval decision from steward
        quarantine_df: Quarantine records
        config: Pipeline configuration
        
    Returns:
        Updated quarantine records with approval metadata
    """
    updated_df, action = process_hitl_approval_result(
        approval_result=approval_result,
        quarantine_records=quarantine_df,
        config=config,
    )
    
    log.info("Processed approval decision", action=action, records=len(updated_df))
    
    return updated_df


@task
def load_data(df_path: str, config: Dict[str, Any]) -> str:
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
    
    # Load DataFrame
    df = _load_dataframe(df_path)
    _cleanup_dataframe(df_path)
    
    log.info("Starting data load", correlation_id=correlation_id, rows=len(df))
    
    if df.empty:
        log.warning("Empty DataFrame provided for loading")
        return "No data to load (empty DataFrame)"
    
    destination_config = config.get("destination", {})
    topic = config.get("event", {}).get("topic", "pipeline-events")
    results = []
    
    try:
        # Load to primary destination
        if "primary" in destination_config:
            result = _load_to_destination(
                df, destination_config["primary"], topic, correlation_id, "primary"
            )
            results.append(f"Primary: {result}")
        
        # Load to backup destination
        if "backup" in destination_config:
            result = _load_to_destination(
                df, destination_config["backup"], topic, correlation_id, "backup"
            )
            results.append(f"Backup: {result}")
        
        # Load to archive destination
        if "archive" in destination_config:
            result = _load_to_destination(
                df, destination_config["archive"], topic, correlation_id, "archive"
            )
            results.append(f"Archive: {result}")
        
        summary = " | ".join(results) if results else "No destinations configured"
        
        log.info("Data load complete", summary=summary, correlation_id=correlation_id)
        
        # Publish success event
        kafka_publisher.publish_data(
            dag_id=dag_id,
            data=df.to_dict(orient="records"),
            topic=topic,
            status="success",
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
                enrichment_config=enrichment,
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
        )
    
    elif dest_type == "snowflake_stage":
        stage_name = dest_config.get("stage_name", "DATA_STAGE")
        file_name = dest_config.get("file_name", f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
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
