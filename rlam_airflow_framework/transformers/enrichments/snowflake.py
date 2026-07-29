# File: rlam_airflow_framework/transformers/enrichments/snowflake.py
"""
SnowflakeLookupTransformer - enrich a DataFrame with a Snowflake reference lookup.

Security: identifiers (table, join column, select columns) are validated via
``validate_identifier`` and lookup values are quote-escaped before being
inlined into the SQL - both required because Snowflake's Python connector
has no clean parameterized-IN-list support for a dynamic value count.
"""

import time
from typing import Any, Dict, Optional, cast

import pandas as pd
import structlog
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from rlam_airflow_framework.transformers.base import Transformer, TransformationError
from rlam_airflow_framework.utils.validation import validate_dataframe, validate_identifier

log = structlog.get_logger(__name__)


class SnowflakeLookupTransformer(Transformer):
    """Enriches a DataFrame with columns looked up from a Snowflake table.

    Step config::

        type: snowflake_lookup
        connection_id: snowflake-default   # optional, defaults below
        table: REFERENCE.INSTRUMENTS
        join_on: {instrument_id: INSTRUMENT_ID}
        select_columns: [ISIN, SECTOR]
    """

    def transform(
        self,
        df: pd.DataFrame,
        config: Dict[str, Any],
        correlation_id: Optional[str] = None,
    ) -> pd.DataFrame:
        trace_id = correlation_id or f"enrich-{int(time.time() * 1000)}"

        df = validate_dataframe(df, "Snowflake enrichment")

        if df.empty:
            log.warning("Empty DataFrame provided for enrichment", trace_id=trace_id)
            return df.copy()

        required_keys = ["table", "join_on", "select_columns"]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"snowflake_lookup config missing required key: '{key}'")

        if not config["join_on"]:
            raise ValueError("snowflake_lookup 'join_on' cannot be empty")

        if not config["select_columns"]:
            raise ValueError("snowflake_lookup 'select_columns' cannot be empty")

        snowflake_conn_id = config.get("connection_id", "snowflake-default")

        log.info(
            "Starting Snowflake enrichment",
            trace_id=trace_id,
            table=config["table"],
            rows=len(df),
        )

        try:
            start_time = time.time()
            hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

            lookup_column = list(config["join_on"].keys())[0]
            snowflake_column = config["join_on"][lookup_column]

            table_name = validate_identifier(config["table"], "table name")
            snowflake_column = validate_identifier(snowflake_column, "snowflake column")

            validated_select_cols = [
                validate_identifier(col, f"select column '{col}'")
                for col in config["select_columns"]
            ]

            if lookup_column not in df.columns:
                raise ValueError(f"Lookup column '{lookup_column}' not found in DataFrame")

            unique_values = df[lookup_column].dropna().unique().tolist()

            if not unique_values:
                log.warning(
                    "No values to lookup (all null), returning original DataFrame",
                    trace_id=trace_id,
                )
                return df.copy()

            escaped_values = []
            for val in unique_values:
                if val is not None:
                    escaped_val = str(val).replace("'", "''")
                    escaped_values.append(f"'{escaped_val}'")

            placeholders = ", ".join(escaped_values)

            all_select_cols = [f'"{snowflake_column}"'] + [
                f'"{col}"' for col in validated_select_cols
            ]
            select_cols_str = ", ".join(all_select_cols)

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

            lookup_df = cast(pd.DataFrame, hook.get_pandas_df(query))

            if lookup_df.empty:
                log.warning("No matching records found in lookup table", trace_id=trace_id)
            else:
                log.info(
                    "Retrieved lookup records", trace_id=trace_id, count=len(lookup_df)
                )

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
                f"Snowflake enrichment failed: {str(e)}", "snowflake_lookup", e
            ) from e
