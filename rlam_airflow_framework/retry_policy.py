import logging
from typing import Dict, Any

from airflow.sdk.definitions.retry_policy import (
    ExceptionRetryPolicy,
    RetryRule,
    RetryAction,
)

log = logging.getLogger(__name__)

def build_transient_retry_policy(config: Dict[str, Any]) -> ExceptionRetryPolicy:
    """
    Build an ExceptionRetryPolicy that only retries transient errors.
    
    Deterministic errors (ValueError, TypeError, etc.) will fail immediately
    without consuming retries.
    
    Args:
        config: Pipeline retry configuration.
            
    Returns:
        ExceptionRetryPolicy instance configured for transient errors.
        
    Limitations:
        - requests.exceptions.HTTPError is blanket-retried. Airflow 3's ExceptionRetryPolicy 
          matches by exception class name and does not natively support splitting by HTTP 
          status codes (like 401/404 vs 500).
    """
    
    return ExceptionRetryPolicy(
        rules=[
            # Deterministic errors: Fail immediately
            RetryRule(
                exception=[
                    "builtins.ValueError",
                    "builtins.TypeError",
                    "builtins.KeyError",
                    "builtins.AssertionError",
                    "builtins.NotImplementedError",
                    "rlam_airflow_framework.formula_engine.FormulaError",
                ],
                action=RetryAction.FAIL,
                reason="Deterministic error, not retryable",
            ),
            # Transient network/API errors: Retry
            RetryRule(
                exception=[
                    "requests.exceptions.ConnectionError",
                    "requests.exceptions.Timeout",
                    "requests.exceptions.HTTPError",
                    "urllib3.exceptions.ProtocolError",
                    "socket.timeout",
                    "snowflake.connector.errors.DatabaseError",
                    "snowflake.connector.errors.OperationalError",
                ],
                action=RetryAction.RETRY,
                # retry_delay=None ensures it falls back to the DAG/task's own retry_delay logic
                # which supports exponential backoff.
                retry_delay=None,
                reason="Transient connection or timeout error",
            ),
        ],
        default=RetryAction.DEFAULT, # Defer to standard Airflow retry logic for unlisted exceptions
    )
