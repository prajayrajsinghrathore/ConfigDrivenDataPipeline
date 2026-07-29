# File: rlam_airflow_framework/utils/retry_policy.py
import logging
from typing import Dict, Any

from airflow.sdk.definitions.retry_policy import (
    ExceptionRetryPolicy,
    RetryRule,
    RetryAction,
)

log = logging.getLogger(__name__)


class TransientError(Exception):
    """
    Marker mixin for "safe to retry at the task level" errors.

    Every fetcher/loader wraps whatever it catches (requests, paramiko,
    boto3/azure-core, snowflake.connector, ...) into its own framework
    exception before it escapes the task - Airflow's ExceptionRetryPolicy
    never sees the raw library exception, so matching by the raw library's
    dotted class name (e.g. "requests.exceptions.Timeout") can never fire.
    Mixing this marker into a wrapper exception at the point it's raised
    (where the raw exception is still visible for classification) is what
    actually reaches the policy below. New fetchers/loaders don't need to
    touch this file - just raise a TransientError-mixed exception.
    """


def build_transient_retry_policy(config: Dict[str, Any]) -> ExceptionRetryPolicy:
    """
    Build an ExceptionRetryPolicy that only retries transient errors.

    Deterministic errors (ValueError, TypeError, etc.) will fail immediately
    without consuming retries. Transience is decided by whoever wraps the
    raw exception (see TransientError above), not by this policy.

    Args:
        config: Pipeline retry configuration.

    Returns:
        ExceptionRetryPolicy instance configured for transient errors.
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
            # Transient network/API errors: Retry (isinstance match, so this
            # covers TransientDataFetchError/TransientDataLoadError and any
            # future subclass automatically)
            RetryRule(
                exception=TransientError,
                action=RetryAction.RETRY,
                # retry_delay=None ensures it falls back to the DAG/task's own retry_delay logic
                # which supports exponential backoff.
                retry_delay=None,
                reason="Transient connection or timeout error",
            ),
        ],
        default=RetryAction.DEFAULT, # Defer to standard Airflow retry logic for unlisted exceptions
    )
