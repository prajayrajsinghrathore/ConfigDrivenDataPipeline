# File: rlam_airflow_framework/destinations/stored_procedure.py
"""Stored procedure loader (an action sink, not a bulk load)."""

import time
from typing import Any, Dict, List

import pandas as pd
import structlog
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from tenacity import RetryError

from rlam_airflow_framework.validation import validate_identifier
from rlam_airflow_framework.destinations.base import DestinationLoader
from rlam_airflow_framework.destinations.primitives import (
    DataLoadError,
    DEFAULT_RETRY_ATTEMPTS,
    is_transient_snowflake_error,
    snowflake_retry,
)

_log = structlog.get_logger(__name__)


class StoredProcedureLoader(DestinationLoader):
    """
    Invoke a Snowflake stored procedure (an action sink, not a bulk load).

    Does not consume the DataFrame, so it runs regardless of frame emptiness.
    Procedure name parts and parameter values are validated/escaped.
    """

    dest_type = "stored_procedure"
    consumes_dataframe = False

    def _write(self, df, dest_config, ctx):
        procedure_name = dest_config["procedure"]
        parameters = dest_config.get("parameters", [])
        capture_result = dest_config.get("capture_result", True)
        snowflake_conn_id = dest_config.get("connection_id", "snowflake-default")
        logger = _log.bind(trace_id=ctx.correlation_id)

        for part in procedure_name.split("."):
            validate_identifier(part, f"procedure name part '{part}'")

        param_values = self._build_param_values(parameters)
        params_str = ", ".join(param_values) if param_values else ""
        call_sql = f"CALL {procedure_name}({params_str})"

        logger.info(
            f"Calling stored procedure: {procedure_name}, "
            f"params={len(param_values)}, capture_result={capture_result}"
        )

        @snowflake_retry
        def _execute_procedure():
            hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
            try:
                start_time = time.time()
                if capture_result:
                    result_df = hook.get_pandas_df(call_sql)
                    row_count = len(result_df) if result_df is not None else 0
                    logger.info(
                        f"Stored procedure executed successfully: {procedure_name}, "
                        f"result_rows={row_count}, elapsed={time.time() - start_time:.2f}s"
                    )
                    return result_df if result_df is not None else pd.DataFrame()
                hook.run(call_sql)
                logger.info(
                    f"Stored procedure executed successfully: {procedure_name}, "
                    f"elapsed={time.time() - start_time:.2f}s"
                )
                return f"Executed {procedure_name} successfully"
            except Exception as e:
                if is_transient_snowflake_error(e):
                    logger.warning(f"Transient error calling procedure, may retry: {e}")
                    raise
                logger.error(f"Stored procedure call failed: {e}", exc_info=True)
                raise DataLoadError(
                    f"Failed to call stored procedure {procedure_name}: {str(e)}",
                    destination=procedure_name,
                    original_error=e,
                ) from e

        try:
            _execute_procedure()
        except RetryError as e:
            logger.error(
                "All retry attempts exhausted for procedure call", exc_info=True
            )
            raise DataLoadError(
                f"Failed to call stored procedure after {DEFAULT_RETRY_ATTEMPTS} attempts",
                destination=procedure_name,
                original_error=e,
            ) from e

        return f"Called stored procedure {procedure_name}"

    @staticmethod
    def _build_param_values(parameters: List[Dict[str, Any]]) -> List[str]:
        """Render CALL parameters, escaping string values."""
        param_values: List[str] = []
        for param in parameters or []:
            if not isinstance(param, dict):
                raise ValueError(f"Parameter must be a dict, got {type(param)}")
            if "value" not in param:
                raise ValueError(f"Parameter missing 'value' key: {param}")
            value = param["value"]
            if value is None:
                param_values.append("NULL")
            elif isinstance(value, bool):
                param_values.append("TRUE" if value else "FALSE")
            elif isinstance(value, (int, float)):
                param_values.append(str(value))
            else:
                escaped_val = str(value).replace("'", "''")
                param_values.append(f"'{escaped_val}'")
        return param_values
