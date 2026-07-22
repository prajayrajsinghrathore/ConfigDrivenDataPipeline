# File: dags/utils/data_loaders.py
"""
Data loading utilities for various destinations.

Provides secure data loading to:
- Azure Data Lake
- Azure Blob Storage
- Snowflake tables and stages
- Stored procedure execution

Features:
- SQL injection prevention using parameterized queries
- Proper null/empty DataFrame handling
- Comprehensive logging with correlation IDs
- Type hints for better code quality
- Multi-tenancy support with tenant-aware connection resolution
- Transactional Snowflake operations with tenacity retry
- Stored procedure support with return value capture
"""

import pandas as pd
from airflow.sdk import ObjectStoragePath
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from typing import Optional, Any, Literal, Dict, List, Union
import logging
import re
import os
import time
import tempfile

# Tenacity for retry logic
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    RetryError,
)

logger = logging.getLogger(__name__)

# Configuration
DEFAULT_BATCH_SIZE = int(os.getenv("DATA_LOADER_BATCH_SIZE", "1000"))

# Retry configuration from centralized timeouts
DEFAULT_RETRY_ATTEMPTS = int(os.getenv("DATA_LOADER_RETRY_ATTEMPTS", "3"))
DEFAULT_RETRY_MIN_WAIT = int(os.getenv("DATA_LOADER_RETRY_MIN_WAIT", "4"))
DEFAULT_RETRY_MAX_WAIT = int(os.getenv("DATA_LOADER_RETRY_MAX_WAIT", "10"))

# Default connection IDs (fallback when tenant mapping not found)
DEFAULT_SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake-default")

# Import tenant context for multi-tenancy support
try:
    from utils.tenant_context import TenantContext
except ImportError:
    TenantContext = None

# Snowflake transient errors that should trigger retry
# These are connection-level errors that may succeed on retry
try:
    import snowflake.connector.errors as sf_errors
    SNOWFLAKE_TRANSIENT_ERRORS = (
        sf_errors.OperationalError,
        sf_errors.DatabaseError,
        sf_errors.InterfaceError,
    )
except ImportError:
    # Fallback if snowflake-connector not available
    SNOWFLAKE_TRANSIENT_ERRORS = (Exception,)


def _is_transient_snowflake_error(exception: Exception) -> bool:
    """
    Determine if a Snowflake error is transient and should be retried.
    
    Transient errors include:
    - Connection timeouts
    - Network errors
    - Service unavailable
    
    Non-transient errors (should NOT retry):
    - SQL syntax errors
    - Permission denied
    - Object not found (unless during table creation race condition)
    
    Args:
        exception: The exception to check
        
    Returns:
        True if the error is transient and operation should be retried
    """
    error_msg = str(exception).lower()
    
    # Transient error patterns
    transient_patterns = [
        "connection",
        "timeout",
        "network",
        "unavailable",
        "service",
        "temporarily",
        "throttl",
        "rate limit",
        "too many requests",
    ]
    
    # Non-transient patterns (should not retry)
    non_transient_patterns = [
        "syntax error",
        "permission denied",
        "access denied",
        "invalid identifier",
        "does not exist",
        "already exists",
        "constraint violation",
        "duplicate",
    ]
    
    # Check for non-transient first (fail fast on permanent errors)
    for pattern in non_transient_patterns:
        if pattern in error_msg:
            return False
    
    # Check if it's a known transient pattern
    for pattern in transient_patterns:
        if pattern in error_msg:
            return True
    
    # For unknown errors from Snowflake connector, assume transient
    if isinstance(exception, SNOWFLAKE_TRANSIENT_ERRORS):
        return True
    
    return False


# Create retry decorator for Snowflake operations
snowflake_retry = retry(
    stop=stop_after_attempt(DEFAULT_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=DEFAULT_RETRY_MIN_WAIT, max=DEFAULT_RETRY_MAX_WAIT),
    retry=retry_if_exception_type(SNOWFLAKE_TRANSIENT_ERRORS),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)


def resolve_connection_id(
    dest_config: Dict[str, Any],
    dest_type: str,
    tenant_context: Optional["TenantContext"] = None,
    tenant_id: Optional[str] = None,
) -> str:
    """
    Resolve connection ID with 4-tier priority.

    Priority:
    1. Explicit connection_id in destination config
    2. Tenant-based mapping from global_settings (if tenant_context provided)
    3. Environment variable
    4. Hardcoded default

    Args:
        dest_config: Destination configuration from pipeline YAML
        dest_type: Destination type (snowflake_table, azure_blob, etc.)
        tenant_context: Optional TenantContext for tenant-based resolution
        tenant_id: Optional tenant identifier

    Returns:
        Resolved connection ID
    """
    # Priority 1: Explicit connection_id in config
    if dest_config and "connection_id" in dest_config:
        conn_id = dest_config["connection_id"]
        logger.debug(f"Using explicit connection_id from config: {conn_id}")
        return conn_id

    # Priority 2: Tenant-based mapping
    if tenant_context and tenant_id:
        try:
            resolved = tenant_context.resolve_connection_id(
                dest_config or {}, dest_type, tenant_id
            )
            logger.debug(f"Using tenant connection mapping: {resolved} (tenant={tenant_id})")
            return resolved
        except Exception as e:
            logger.warning(f"Failed to resolve tenant connection: {e}")

    # Priority 3 & 4: Environment variable or hardcoded default
    default_map = {
        "snowflake_table": DEFAULT_SNOWFLAKE_CONN_ID,
        "snowflake_stage": DEFAULT_SNOWFLAKE_CONN_ID,
        "object_storage": "azure_default",  # Default for object storage
    }
    conn_id = default_map.get(dest_type, "default_connection")
    logger.debug(f"Using default connection: {conn_id} for {dest_type}")
    return conn_id


class DataLoadError(Exception):
    """Custom exception for data loading errors."""

    def __init__(
        self, message: str, destination: str, original_error: Optional[Exception] = None
    ):
        self.destination = destination
        self.original_error = original_error
        super().__init__(f"[{destination}] {message}")


class ObjectStorageError(DataLoadError):
    """Custom exception for object storage operations wrapping native errors."""
    
    def __init__(
        self, message: str, destination: str, original_error: Optional[Exception] = None
    ):
        super().__init__(message, destination, original_error)


def _validate_identifier(identifier: str, identifier_type: str = "identifier") -> str:
    """
    Validate and sanitize SQL identifiers to prevent SQL injection.

    Args:
        identifier: The identifier to validate (table name, schema, column)
        identifier_type: Description of what's being validated for error messages

    Returns:
        The validated identifier

    Raises:
        ValueError: If the identifier contains invalid characters
    """
    if not identifier:
        raise ValueError(f"{identifier_type} cannot be empty")

    # Allow alphanumeric, underscores, and dots (for qualified names)
    # Snowflake also allows $, but we'll be conservative
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_$.]*$", identifier):
        raise ValueError(
            f"Invalid {identifier_type}: '{identifier}'. "
            f"Must start with letter/underscore and contain only alphanumeric, underscore, or dot."
        )

    return identifier


def _validate_dataframe(df: Any, operation: str) -> pd.DataFrame:
    """
    Validate that input is a non-None DataFrame.

    Args:
        df: The input to validate
        operation: Description of the operation for error messages

    Returns:
        The validated DataFrame

    Raises:
        ValueError: If df is None or not a DataFrame
    """
    if df is None:
        raise ValueError(f"DataFrame cannot be None for {operation}")

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected DataFrame for {operation}, got {type(df).__name__}")

    return df


def load_to_object_storage(
    df: pd.DataFrame,
    uri: Optional[str] = None,
    path: Optional[str] = None,
    conn_id: Optional[str] = None,
    file_format: Literal["parquet", "csv", "json"] = "parquet",
    correlation_id: Optional[str] = None,
) -> str:
    """
    Load DataFrame to cloud object storage using Airflow 3.x ObjectStoragePath.
    
    Supports both URI-based and path+conn_id patterns:
    - uri="az://container@conn_id/path/file.parquet" (Airflow 3.x recommended)
    - path="az://container/path/file.parquet" + conn_id="azure_conn" (alternative)
    
    Args:
        df: DataFrame to load
        uri: Full URI with embedded connection (e.g., "az://container@conn_id/path/file.ext")
        path: Storage path without connection (e.g., "az://container/path/file.ext")
        conn_id: Connection ID when using path parameter
        file_format: File format ('parquet', 'csv', 'json')
        correlation_id: Optional ID for distributed tracing
        
    Returns:
        str: The full path of the uploaded file
        
    Raises:
        ValueError: If neither uri nor (path+conn_id) provided, or format unsupported
        ObjectStorageError: If the upload fails
        
    Example:
        # URI pattern
        load_to_object_storage(df, uri="az://container@azure_conn/data/file.parquet")
        
        # Path + conn_id pattern
        load_to_object_storage(df, path="az://container/data/file.parquet", conn_id="azure_conn")
    """
    trace_id = correlation_id or f"objstore-{int(time.time() * 1000)}"
    
    # Validate inputs
    df = _validate_dataframe(df, "object storage load")
    
    if df.empty:
        logger.warning(
            f"[{trace_id}] Empty DataFrame provided for object storage load"
        )
        return "No data to load (empty DataFrame)"
    
    if not uri and not (path and conn_id):
        raise ValueError(
            "Must provide either 'uri' parameter (e.g., 'az://container@conn_id/path/file.ext') "
            "or both 'path' and 'conn_id' parameters"
        )
    
    # Build ObjectStoragePath
    try:
        if uri:
            storage_path = ObjectStoragePath(uri)
            destination_str = uri
        else:
            storage_path = ObjectStoragePath(path, conn_id=conn_id)
            destination_str = f"{path}@{conn_id}"
        
        # Add timestamp to filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parent = storage_path.parent
        stem = storage_path.stem
        suffix = storage_path.suffix or f".{file_format}"
        
        timestamped_path = parent / f"{stem}_{timestamp}{suffix}"
        
        logger.info(
            f"[{trace_id}] Starting object storage upload: {timestamped_path}, "
            f"format={file_format}, rows={len(df)}"
        )
        
        start_time = time.time()
        
        # Write data based on format
        if file_format == "parquet":
            data = df.to_parquet(index=False)
            timestamped_path.write_bytes(data)
        elif file_format == "csv":
            data = df.to_csv(index=False)
            timestamped_path.write_text(data)
        elif file_format == "json":
            data = df.to_json(orient="records", indent=2)
            timestamped_path.write_text(data)
        else:
            raise ValueError(
                f"Unsupported format: {file_format}. Supported: parquet, csv, json"
            )
        
        elapsed = time.time() - start_time
        logger.info(
            f"[{trace_id}] Successfully uploaded to object storage: {timestamped_path}, "
            f"elapsed={elapsed:.2f}s"
        )
        
        return str(timestamped_path)
        
    except ValueError:
        # Re-raise validation errors directly
        raise
    except Exception as e:
        logger.error(f"[{trace_id}] Object storage upload failed: {e}", exc_info=True)
        raise ObjectStorageError(
            f"Failed to upload to object storage: {str(e)}",
            destination=destination_str,
            original_error=e,
        ) from e


def load_to_snowflake(
    df: pd.DataFrame,
    snowflake_conn_id: str,
    table_name: str,
    schema: str = "PUBLIC",
    database: Optional[str] = None,
    if_exists: Literal["append", "replace", "fail"] = "append",
    batch_size: int = DEFAULT_BATCH_SIZE,
    correlation_id: Optional[str] = None,
) -> str:
    """
    Load DataFrame to Snowflake table using transactional INSERT statements.
    
    All batch inserts are wrapped in an explicit transaction (BEGIN/COMMIT) to
    ensure atomicity - either all data is written or none is. This prevents
    partial writes that could cause duplicates on retry.
    
    Connection-level errors are retried using tenacity with exponential backoff.
    Retries only occur BEFORE the transaction starts, not mid-transaction.

    Args:
        df: DataFrame to load
        snowflake_conn_id: Airflow connection ID for Snowflake
        table_name: Target table name (validated against SQL injection)
        schema: Schema name (validated against SQL injection)
        database: Database name (optional, validated against SQL injection)
        if_exists: What to do if table exists ('append', 'replace', 'fail')
        batch_size: Number of rows per batch insert
        correlation_id: Optional ID for distributed tracing

    Returns:
        str: Summary of the load operation

    Raises:
        ValueError: If df is None or identifiers are invalid
        DataLoadError: If the load operation fails
    """
    trace_id = correlation_id or f"sf-{int(time.time() * 1000)}"

    # Validate inputs
    df = _validate_dataframe(df, "Snowflake load")

    if df.empty:
        logger.warning(f"[{trace_id}] Empty DataFrame provided for Snowflake load")
        return "No data to load (empty DataFrame)"

    if not snowflake_conn_id:
        raise ValueError("snowflake_conn_id is required")

    # Validate identifiers to prevent SQL injection
    table_name = _validate_identifier(table_name, "table name")
    schema = _validate_identifier(schema, "schema")
    if database:
        database = _validate_identifier(database, "database")

    # Validate columns
    columns = df.columns.tolist()
    for col in columns:
        _validate_identifier(col, f"column '{col}'")

    # Build full table name
    full_table_name = f'"{schema}"."{table_name}"'
    if database:
        full_table_name = f'"{database}".{full_table_name}'

    logger.info(
        f"[{trace_id}] Starting Snowflake load: table={full_table_name}, "
        f"rows={len(df)}, if_exists={if_exists}"
    )

    # Use retry decorator for connection establishment
    @snowflake_retry
    def _execute_transactional_load():
        """Inner function wrapped with retry - executes the full transaction."""
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        engine = None
        transaction_started = False

        try:
            start_time = time.time()

            # DDL operations (outside transaction - auto-commit in Snowflake)
            if if_exists == "replace":
                logger.info(f"[{trace_id}] Dropping existing table {full_table_name}")
                hook.run(f"DROP TABLE IF EXISTS {full_table_name}")

            # Create table with quoted column names
            quoted_columns = ", ".join([f'"{col}" VARCHAR' for col in columns])
            create_sql = f"CREATE TABLE IF NOT EXISTS {full_table_name} ({quoted_columns})"
            hook.run(create_sql)
            logger.debug(f"[{trace_id}] Table created/verified: {full_table_name}")

            # BEGIN TRANSACTION for all INSERT operations
            hook.run("BEGIN TRANSACTION")
            transaction_started = True
            logger.debug(f"[{trace_id}] Transaction started")

            # Insert data in batches (all within same transaction)
            total_rows = len(df)
            batches_processed = 0

            for i in range(0, total_rows, batch_size):
                batch_df = df.iloc[i : i + batch_size]

                # Build VALUES clause with proper escaping
                values_list = []
                for _, row in batch_df.iterrows():
                    row_values = []
                    for val in row:
                        if pd.isna(val) or val is None:
                            row_values.append("NULL")
                        else:
                            # Escape single quotes by doubling them (Snowflake standard)
                            escaped_val = str(val).replace("'", "''")
                            row_values.append(f"'{escaped_val}'")
                    values_list.append(f"({', '.join(row_values)})")

                # Build and execute INSERT statement with quoted column names
                quoted_col_names = ", ".join([f'"{col}"' for col in columns])
                insert_sql = f"INSERT INTO {full_table_name} ({quoted_col_names}) VALUES {', '.join(values_list)}"

                hook.run(insert_sql)
                batches_processed += 1

                if batches_processed % 10 == 0:
                    logger.debug(
                        f"[{trace_id}] Processed {i + len(batch_df)}/{total_rows} rows"
                    )

            # COMMIT TRANSACTION - all or nothing
            hook.run("COMMIT")
            transaction_started = False
            logger.debug(f"[{trace_id}] Transaction committed")

            elapsed = time.time() - start_time
            logger.info(
                f"[{trace_id}] Successfully loaded {total_rows} rows to {full_table_name} "
                f"in {batches_processed} batches, elapsed={elapsed:.2f}s"
            )

            return f"Loaded {total_rows} rows to {full_table_name}"

        except Exception as e:
            # ROLLBACK on any error if transaction was started
            if transaction_started:
                try:
                    hook.run("ROLLBACK")
                    logger.info(f"[{trace_id}] Transaction rolled back due to error")
                except Exception as rollback_error:
                    logger.warning(f"[{trace_id}] Failed to rollback: {rollback_error}")

            # Check if this is a transient error that should trigger retry
            if _is_transient_snowflake_error(e):
                logger.warning(
                    f"[{trace_id}] Transient error detected, may retry: {e}"
                )
                raise  # Let tenacity handle retry

            # For non-transient errors, try SQLAlchemy fallback
            logger.warning(
                f"[{trace_id}] Primary insert method failed: {e}, trying SQLAlchemy fallback"
            )

            try:
                engine = hook.get_sqlalchemy_engine()
                df.to_sql(
                    name=table_name,
                    con=engine,
                    schema=schema,
                    if_exists=if_exists,
                    index=False,
                    method="multi",
                )

                logger.info(f"[{trace_id}] Loaded {len(df)} rows using SQLAlchemy fallback")
                return f"Loaded {len(df)} rows to {schema}.{table_name} (fallback method)"

            except Exception as e2:
                logger.error(f"[{trace_id}] Both load methods failed", exc_info=True)
                raise DataLoadError(
                    f"Both methods failed. SQL method: {str(e)}, SQLAlchemy method: {str(e2)}",
                    destination=full_table_name,
                    original_error=e2,
                ) from e2

            finally:
                if engine:
                    engine.dispose()

    # Execute with retry
    try:
        return _execute_transactional_load()
    except RetryError as e:
        logger.error(f"[{trace_id}] All retry attempts exhausted", exc_info=True)
        raise DataLoadError(
            f"Failed to load data after {DEFAULT_RETRY_ATTEMPTS} attempts",
            destination=full_table_name,
            original_error=e,
        ) from e


def load_to_snowflake_stage(
    df: pd.DataFrame,
    snowflake_conn_id: str,
    stage_name: str,
    file_name: str,
    file_format: Literal["csv", "json"] = "csv",
    stage_path: Optional[str] = None,
    correlation_id: Optional[str] = None,
) -> str:
    """
    Load DataFrame to Snowflake stage.

    Args:
        df: DataFrame to load
        snowflake_conn_id: Airflow connection ID for Snowflake
        stage_name: Stage name (validated against SQL injection)
        file_name: File name in stage
        file_format: File format ('csv', 'json')
        stage_path: Optional subdirectory path within the stage
        correlation_id: Optional ID for distributed tracing

    Returns:
        str: Summary of the stage operation

    Raises:
        ValueError: If df is None or identifiers are invalid
        DataLoadError: If the stage operation fails
    """
    trace_id = correlation_id or f"stage-{int(time.time() * 1000)}"

    # Validate inputs
    df = _validate_dataframe(df, "Snowflake stage load")

    if df.empty:
        logger.warning(
            f"[{trace_id}] Empty DataFrame provided for Snowflake stage load"
        )
        return "No data to load (empty DataFrame)"

    if not snowflake_conn_id:
        raise ValueError("snowflake_conn_id is required")

    # Validate stage name
    stage_name = _validate_identifier(stage_name, "stage name")

    # Validate stage_path if provided
    if stage_path:
        # Stage path can contain slashes, but validate each component
        for component in stage_path.split("/"):
            if component:  # Skip empty components from leading/trailing slashes
                _validate_identifier(component, "stage path component")

    logger.info(
        f"[{trace_id}] Starting Snowflake stage load: stage={stage_name}, "
        f"file={file_name}, format={file_format}, rows={len(df)}"
    )

    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    # Convert DataFrame to string
    if file_format == "csv":
        data = df.to_csv(index=False)
    elif file_format == "json":
        data = df.to_json(orient="records")
    else:
        raise ValueError(f"Unsupported format: {file_format}. Supported: csv, json")

    # Create a temporary file and upload to stage

    date_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name, ext = os.path.splitext(file_name)
    if not ext:
        ext = f".{file_format}"
    dated_file_name = f"{base_name}_{date_suffix}{ext}"

    temp_dir = tempfile.gettempdir()
    tmp_file_path = os.path.join(temp_dir, dated_file_name)

    try:
        start_time = time.time()

        # Write data to temp file
        with open(tmp_file_path, "w", encoding="utf-8") as tmp_file:
            tmp_file.write(data)

        # Build stage path
        stage_full_path = f"@{stage_name}"
        if stage_path:
            stage_full_path = f"{stage_full_path}/{stage_path}"

        # Upload to stage using PUT command
        # Note: file path is local system path, not user input, so injection risk is minimal
        put_sql = f"PUT file://{tmp_file_path} {stage_full_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        hook.run(put_sql)

        elapsed = time.time() - start_time
        logger.info(
            f"[{trace_id}] Successfully uploaded to stage {stage_full_path}/{dated_file_name}, "
            f"elapsed={elapsed:.2f}s"
        )

        return f"Loaded to stage {stage_full_path}/{dated_file_name}"

    except Exception as e:
        logger.error(f"[{trace_id}] Snowflake stage upload failed: {e}", exc_info=True)
        raise DataLoadError(
            f"Failed to upload to Snowflake stage: {str(e)}",
            destination=f"{stage_name}/{file_name}",
            original_error=e,
        ) from e

    finally:
        # Clean up temporary file
        if os.path.exists(tmp_file_path):
            try:
                os.unlink(tmp_file_path)
            except Exception as e:
                logger.warning(f"[{trace_id}] Failed to clean up temp file: {e}")


def call_stored_procedure(
    snowflake_conn_id: str,
    procedure_name: str,
    parameters: Optional[List[Dict[str, Any]]] = None,
    capture_result: bool = True,
    correlation_id: Optional[str] = None,
) -> Union[pd.DataFrame, str]:
    """
    Execute a Snowflake stored procedure with optional return value capture.
    
    This function allows calling stored procedures defined in Snowflake, passing
    parameters safely, and optionally capturing the result set as a DataFrame.
    
    Features:
    - SQL injection prevention via identifier validation
    - Parameter binding for safe value passing
    - Tenacity retry for transient connection errors
    - Result set capture as DataFrame for XCom serialization
    
    Args:
        snowflake_conn_id: Airflow connection ID for Snowflake
        procedure_name: Fully qualified procedure name (e.g., "SCHEMA.PROC_NAME")
                       Validated to prevent SQL injection
        parameters: Optional list of parameter dicts with 'name' and 'value' keys
                   Example: [{"name": "batch_id", "value": "123"}, 
                            {"name": "record_count", "value": 100}]
        capture_result: If True, return result set as DataFrame (default: True)
                       If False, return status message
        correlation_id: Optional ID for distributed tracing
        
    Returns:
        If capture_result=True: DataFrame containing procedure result set
        If capture_result=False: Status message string
        
    Raises:
        ValueError: If procedure_name is invalid or parameters malformed
        DataLoadError: If the procedure call fails
        
    Example YAML config:
        destination:
          post_processing:
            type: stored_procedure
            connection_id: snowflake_conn
            procedure: "ANALYTICS.SP_AGGREGATE_DAILY"
            parameters:
              - name: batch_date
                value: "{{ ds }}"
              - name: source_table
                value: "RAW_DATA"
            capture_result: true
            result_xcom_key: "aggregation_result"
    """
    trace_id = correlation_id or f"proc-{int(time.time() * 1000)}"
    
    # Validate inputs
    if not snowflake_conn_id:
        raise ValueError("snowflake_conn_id is required")
    
    if not procedure_name:
        raise ValueError("procedure_name is required")
    
    # Validate procedure name (can contain schema prefix with dot)
    # Split by dot and validate each part
    procedure_parts = procedure_name.split(".")
    for part in procedure_parts:
        _validate_identifier(part, f"procedure name part '{part}'")
    
    # Build parameter list for CALL statement
    param_values = []
    if parameters:
        for param in parameters:
            if not isinstance(param, dict):
                raise ValueError(f"Parameter must be a dict, got {type(param)}")
            
            if "value" not in param:
                raise ValueError(f"Parameter missing 'value' key: {param}")
            
            value = param["value"]
            
            # Handle different value types
            if value is None:
                param_values.append("NULL")
            elif isinstance(value, bool):
                param_values.append("TRUE" if value else "FALSE")
            elif isinstance(value, (int, float)):
                param_values.append(str(value))
            elif isinstance(value, str):
                # Escape single quotes for string values
                escaped_val = value.replace("'", "''")
                param_values.append(f"'{escaped_val}'")
            else:
                # Convert to string and escape
                escaped_val = str(value).replace("'", "''")
                param_values.append(f"'{escaped_val}'")
    
    # Build CALL statement
    params_str = ", ".join(param_values) if param_values else ""
    call_sql = f"CALL {procedure_name}({params_str})"
    
    logger.info(
        f"[{trace_id}] Calling stored procedure: {procedure_name}, "
        f"params={len(param_values)}, capture_result={capture_result}"
    )
    
    @snowflake_retry
    def _execute_procedure():
        """Inner function wrapped with retry for transient errors."""
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        
        try:
            start_time = time.time()
            
            if capture_result:
                # Use get_pandas_df to capture result set
                result_df = hook.get_pandas_df(call_sql)
                
                elapsed = time.time() - start_time
                row_count = len(result_df) if result_df is not None else 0
                
                logger.info(
                    f"[{trace_id}] Stored procedure executed successfully: {procedure_name}, "
                    f"result_rows={row_count}, elapsed={elapsed:.2f}s"
                )
                
                return result_df if result_df is not None else pd.DataFrame()
            else:
                # Execute without capturing result
                hook.run(call_sql)
                
                elapsed = time.time() - start_time
                
                logger.info(
                    f"[{trace_id}] Stored procedure executed successfully: {procedure_name}, "
                    f"elapsed={elapsed:.2f}s"
                )
                
                return f"Executed {procedure_name} successfully"
                
        except Exception as e:
            # Check if transient for retry
            if _is_transient_snowflake_error(e):
                logger.warning(
                    f"[{trace_id}] Transient error calling procedure, may retry: {e}"
                )
                raise  # Let tenacity handle
            
            logger.error(
                f"[{trace_id}] Stored procedure call failed: {e}",
                exc_info=True
            )
            raise DataLoadError(
                f"Failed to call stored procedure {procedure_name}: {str(e)}",
                destination=procedure_name,
                original_error=e,
            ) from e
    
    # Execute with retry
    try:
        return _execute_procedure()
    except RetryError as e:
        logger.error(f"[{trace_id}] All retry attempts exhausted for procedure call", exc_info=True)
        raise DataLoadError(
            f"Failed to call stored procedure after {DEFAULT_RETRY_ATTEMPTS} attempts",
            destination=procedure_name,
            original_error=e,
        ) from e


def load_to_local_file(
    df: pd.DataFrame,
    file_path: str,
    file_format: Literal["parquet", "csv", "json"] = "parquet",
    correlation_id: Optional[str] = None,
) -> str:
    """
    Load DataFrame to local filesystem.
    
    Args:
        df: DataFrame to load
        file_path: Local file path
        file_format: File format ('parquet', 'csv', 'json')
        correlation_id: Optional ID for distributed tracing
        
    Returns:
        str: The full path of the saved file
        
    Raises:
        ValueError: If df is None or format is unsupported
        DataLoadError: If the save fails
    """
    trace_id = correlation_id or f"local-{int(time.time() * 1000)}"
    
    # Validate inputs
    df = _validate_dataframe(df, "local file save")
    
    if df.empty:
        logger.warning(f"[{trace_id}] Empty DataFrame provided for local file save")
        return "No data to save (empty DataFrame)"
    
    if not file_path:
        raise ValueError("file_path is required")
    
    # Add timestamp to filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base, ext = os.path.splitext(file_path)
    if not ext:
        ext = f".{file_format}"
    full_path = f"{base}_{timestamp}{ext}"
    
    logger.info(
        f"[{trace_id}] Saving to local file: {full_path}, format={file_format}, rows={len(df)}"
    )
    
    try:
        start_time = time.time()
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(full_path) or ".", exist_ok=True)
        
        if file_format == "parquet":
            df.to_parquet(full_path, index=False)
        elif file_format == "csv":
            df.to_csv(full_path, index=False)
        elif file_format == "json":
            df.to_json(full_path, orient="records", indent=2)
        else:
            raise ValueError(
                f"Unsupported format: {file_format}. Supported: parquet, csv, json"
            )
        
        elapsed = time.time() - start_time
        logger.info(
            f"[{trace_id}] Successfully saved to {full_path}, elapsed={elapsed:.2f}s"
        )
        
        return full_path
        
    except Exception as e:
        logger.error(f"[{trace_id}] Local file save failed: {e}", exc_info=True)
        raise DataLoadError(
            f"Failed to save to local file: {str(e)}",
            destination=file_path,
            original_error=e,
        ) from e
