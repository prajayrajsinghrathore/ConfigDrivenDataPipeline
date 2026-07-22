# File: dags/utils/data_transformers.py
"""
Data transformation utilities.

Provides secure data transformation capabilities including:
- Formula-based column calculations using safe sandboxed evaluation
- Data type conversions
- Aggregations and filtering
- Snowflake enrichment with SQL injection prevention

Security: Uses parameterized queries where possible and validates all identifiers.
Airflow 3.1.6: Uses structlog for structured logging.
"""

import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from typing import Dict, Any, List, Optional
import structlog
import re
import time

from rlam_airflow_framework.formula_engine import FormulaError, get_formula_engine

log = structlog.get_logger(__name__)

# Initialize the formula engine (singleton)
formula_engine = get_formula_engine()


class TransformationError(Exception):
    """Custom exception for transformation errors."""

    def __init__(
        self,
        message: str,
        transformation_type: str,
        original_error: Optional[Exception] = None,
    ):
        self.transformation_type = transformation_type
        self.original_error = original_error
        super().__init__(f"[{transformation_type}] {message}")


def _validate_identifier(identifier: str, identifier_type: str = "identifier") -> str:
    """
    Validate and sanitize SQL identifiers to prevent SQL injection.

    Args:
        identifier: The identifier to validate
        identifier_type: Description for error messages

    Returns:
        The validated identifier

    Raises:
        ValueError: If the identifier contains invalid characters
    """
    if not identifier:
        raise ValueError(f"{identifier_type} cannot be empty")

    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_$.]*$", identifier):
        raise ValueError(
            f"Invalid {identifier_type}: '{identifier}'. "
            f"Must start with letter/underscore and contain only alphanumeric, underscore, or dot."
        )

    return identifier


def _validate_dataframe(df: Any, operation: str) -> pd.DataFrame:
    """Validate that input is a non-None DataFrame."""
    if df is None:
        raise ValueError(f"DataFrame cannot be None for {operation}")

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected DataFrame for {operation}, got {type(df).__name__}")

    return df


def transform_data(
    df: pd.DataFrame,
    transformations: Dict[str, Any],
    correlation_id: Optional[str] = None,
) -> pd.DataFrame:
    """
    Apply common transformations to DataFrame using the safe formula engine.

    Args:
        df: Input DataFrame
        transformations: Dict of transformation rules
        correlation_id: Optional ID for distributed tracing

    Example transformations:
        {
            'column_types': {'trade_date': 'datetime', 'quantity': 'float'},
            'new_columns': {'total_value': 'quantity * price'},
            'aggregations': {'sum': ['quantity'], 'mean': ['price']},
            'filters': {'quantity': '> 0'}
        }

    Returns:
        pandas.DataFrame: Transformed data

    Raises:
        ValueError: If df is None
        TransformationError: If transformation fails
    """
    trace_id = correlation_id or f"transform-{int(time.time() * 1000)}"

    # Validate input
    df = _validate_dataframe(df, "data transformation")

    if df.empty:
        log.warning("Empty DataFrame provided for transformation", trace_id=trace_id)
        return df.copy()

    if not transformations:
        log.info(
            "No transformations specified, returning copy of input", trace_id=trace_id
        )
        return df.copy()

    log.info("Starting data transformation", trace_id=trace_id, row_count=len(df))
    start_time = time.time()

    result_df = df.copy()

    try:
        # Change column types
        if "column_types" in transformations:
            result_df = _apply_column_types(
                result_df, transformations["column_types"], trace_id
            )

        # Add new columns using the formula engine
        if "new_columns" in transformations:
            log.info("::group::Applying Transformations")
            result_df = _apply_new_columns(
                result_df, transformations["new_columns"], trace_id
            )
            log.info("::endgroup::")

        # Apply aggregations
        if "aggregations" in transformations:
            result_df = _apply_aggregations(
                result_df, transformations["aggregations"], trace_id
            )

        # Apply filters
        if "filters" in transformations:
            result_df = _apply_filters(result_df, transformations["filters"], trace_id)

        elapsed = time.time() - start_time
        log.info(
            "Transformation complete",
            trace_id=trace_id,
            input_rows=len(df),
            output_rows=len(result_df),
            elapsed_seconds=round(elapsed, 2),
        )

        return result_df

    except FormulaError as e:
        log.error(
            "Formula error during transformation", trace_id=trace_id, error=str(e)
        )
        raise TransformationError(str(e), "formula_evaluation", e) from e
    except Exception as e:
        log.error(
            "Transformation failed", trace_id=trace_id, error=str(e), exc_info=True
        )
        raise TransformationError(str(e), "general", e) from e


def _apply_column_types(
    df: pd.DataFrame, column_types: Dict[str, str], trace_id: str
) -> pd.DataFrame:
    """Apply column type conversions with proper error handling."""
    for col, dtype in column_types.items():
        if col not in df.columns:
            log.warning(
                "Column not found for type conversion, skipping",
                trace_id=trace_id,
                column=col,
            )
            continue

        try:
            if dtype == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif dtype == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dtype == "int":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(
                    "Int64"
                )  # Nullable int
            elif dtype == "string":
                df[col] = df[col].astype(str)
            else:
                df[col] = df[col].astype(dtype)

            log.debug("Converted column", trace_id=trace_id, column=col, dtype=dtype)

        except Exception as e:
            log.error(
                "Failed to convert column",
                trace_id=trace_id,
                column=col,
                dtype=dtype,
                error=str(e),
            )
            raise TransformationError(
                f"Failed to convert column '{col}' to {dtype}: {e}",
                "column_type_conversion",
                e,
            ) from e

    return df


def _apply_new_columns(
    df: pd.DataFrame, new_columns: Dict[str, str], trace_id: str
) -> pd.DataFrame:
    """Apply formula-based new columns with proper error handling."""
    for col_name, formula in new_columns.items():
        try:
            df[col_name] = formula_engine.evaluate_column(df, formula)
            log.debug(
                "Created column using formula",
                trace_id=trace_id,
                column=col_name,
                formula=formula,
            )
        except FormulaError as e:
            log.error(
                "Failed to create column",
                trace_id=trace_id,
                column=col_name,
                error=str(e),
            )
            # Store error indicator instead of error string to avoid downstream issues
            df[col_name] = None
            # Re-raise to let caller decide how to handle
            raise

    return df


def _apply_aggregations(
    df: pd.DataFrame, aggregations: Dict[str, List[str]], trace_id: str
) -> pd.DataFrame:
    """Apply aggregation functions with proper error handling."""
    if df.empty:
        log.warning("Cannot apply aggregations to empty DataFrame", trace_id=trace_id)
        return df

    agg_dict = {}
    for agg_func, columns in aggregations.items():
        for col in columns:
            if col not in df.columns:
                log.warning(
                    "Column not found for aggregation, skipping",
                    trace_id=trace_id,
                    column=col,
                )
                continue

            try:
                agg_dict[f"{col}_{agg_func}"] = df[col].agg(agg_func)
            except Exception as e:
                log.error(
                    "Failed to aggregate column",
                    trace_id=trace_id,
                    column=col,
                    agg_func=agg_func,
                    error=str(e),
                )
                raise TransformationError(
                    f"Failed to aggregate '{col}' with {agg_func}: {e}",
                    "aggregation",
                    e,
                ) from e

    if agg_dict:
        log.debug(
            "Applied aggregations",
            trace_id=trace_id,
            aggregations=list(agg_dict.keys()),
        )
        return pd.DataFrame([agg_dict])

    return df


def _apply_filters(
    df: pd.DataFrame, filters: Dict[str, str], trace_id: str
) -> pd.DataFrame:
    """Apply filter conditions with proper error handling."""
    original_count = len(df)

    for col, condition in filters.items():
        if col not in df.columns:
            log.warning(
                "Column not found for filter, skipping", trace_id=trace_id, column=col
            )
            continue

        try:
            df = df.query(f"`{col}` {condition}")
        except Exception as e:
            log.error(
                "Failed to apply filter",
                trace_id=trace_id,
                column=col,
                condition=condition,
                error=str(e),
            )
            raise TransformationError(
                f"Failed to apply filter '{col} {condition}': {e}", "filter", e
            ) from e

    filtered_count = len(df)
    log.debug(
        "Filters applied",
        trace_id=trace_id,
        original_count=original_count,
        filtered_count=filtered_count,
    )

    return df


def enrich_from_snowflake(
    df: pd.DataFrame,
    snowflake_conn_id: str,
    lookup_config: Dict[str, Any],
    correlation_id: Optional[str] = None,
) -> pd.DataFrame:
    """
    Enrich DataFrame with data from Snowflake using secure parameterized queries.

    Args:
        df: Input DataFrame
        snowflake_conn_id: Airflow connection ID for Snowflake
        lookup_config: Configuration for lookup
        correlation_id: Optional ID for distributed tracing

    Example lookup_config:
        {
            'table': 'REFERENCE.INSTRUMENTS',
            'join_on': {'instrument_id': 'INSTRUMENT_ID'},
            'select_columns': ['ISIN', 'INSTRUMENT_NAME', 'SECTOR']
        }

    Returns:
        pandas.DataFrame: Enriched data

    Raises:
        ValueError: If df is None or lookup_config is invalid
        TransformationError: If the enrichment fails
    """
    trace_id = correlation_id or f"enrich-{int(time.time() * 1000)}"

    # Validate inputs
    df = _validate_dataframe(df, "Snowflake enrichment")

    if df.empty:
        log.warning("Empty DataFrame provided for enrichment", trace_id=trace_id)
        return df.copy()

    if not lookup_config:
        raise ValueError("lookup_config is required")

    # Validate lookup_config structure
    required_keys = ["table", "join_on", "select_columns"]
    for key in required_keys:
        if key not in lookup_config:
            raise ValueError(f"lookup_config missing required key: '{key}'")

    if not lookup_config["join_on"]:
        raise ValueError("lookup_config 'join_on' cannot be empty")

    if not lookup_config["select_columns"]:
        raise ValueError("lookup_config 'select_columns' cannot be empty")

    log.info(
        "Starting Snowflake enrichment",
        trace_id=trace_id,
        table=lookup_config["table"],
        rows=len(df),
    )

    try:
        start_time = time.time()
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

        # Validate and extract lookup configuration
        lookup_column = list(lookup_config["join_on"].keys())[0]
        snowflake_column = lookup_config["join_on"][lookup_column]

        # Validate identifiers to prevent SQL injection
        table_name = _validate_identifier(lookup_config["table"], "table name")
        snowflake_column = _validate_identifier(snowflake_column, "snowflake column")

        validated_select_cols = [
            _validate_identifier(col, f"select column '{col}'")
            for col in lookup_config["select_columns"]
        ]

        # Check if lookup column exists in DataFrame
        if lookup_column not in df.columns:
            raise ValueError(f"Lookup column '{lookup_column}' not found in DataFrame")

        # Get unique non-null values for lookup
        unique_values = df[lookup_column].dropna().unique().tolist()

        if not unique_values:
            log.warning(
                "No values to lookup (all null), returning original DataFrame",
                trace_id=trace_id,
            )
            return df.copy()

        # Build secure query with properly escaped values
        # Escape single quotes in values to prevent injection
        escaped_values = []
        for val in unique_values:
            if val is not None:
                escaped_val = str(val).replace("'", "''")
                escaped_values.append(f"'{escaped_val}'")

        placeholders = ", ".join(escaped_values)

        # Build column list with validated identifiers
        all_select_cols = [f'"{snowflake_column}"'] + [
            f'"{col}"' for col in validated_select_cols
        ]
        select_cols_str = ", ".join(all_select_cols)

        # Build query with quoted identifiers
        query = f"""
        SELECT {select_cols_str}
        FROM {table_name}
        WHERE "{snowflake_column}" IN ({placeholders})
        """

        log.debug(
            "Executing lookup query",
            trace_id=trace_id,
            unique_value_count=len(unique_values),
        )

        # Execute query and get results
        lookup_df = hook.get_pandas_df(query)

        if lookup_df.empty:
            log.warning("No matching records found in lookup table", trace_id=trace_id)
        else:
            log.info(
                "Retrieved lookup records", trace_id=trace_id, count=len(lookup_df)
            )

        # Merge with original DataFrame
        result_df = df.merge(
            lookup_df, left_on=lookup_column, right_on=snowflake_column, how="left"
        )

        elapsed = time.time() - start_time
        log.info(
            "Enrichment complete",
            trace_id=trace_id,
            input_rows=len(df),
            output_rows=len(result_df),
            elapsed_seconds=round(elapsed, 2),
        )

        return result_df

    except Exception as e:
        log.error(
            "Snowflake enrichment failed",
            trace_id=trace_id,
            error=str(e),
            exc_info=True,
        )
        raise TransformationError(
            f"Snowflake enrichment failed: {str(e)}", "snowflake_enrichment", e
        ) from e
