# File: rlam_airflow_framework/destinations/primitives.py
"""
Shared Snowflake/loader primitives reused by the destination loader strategies.

Holds:
- Transient-error classification for Snowflake operations
- The tenacity retry decorator applied by Snowflake-backed loaders
- The DataLoadError / ObjectStorageError exceptions
- Batch/retry configuration constants
"""

import logging
import os
import re

import structlog
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from typing import Optional

from rlam_airflow_framework.utils.retry_policy import TransientError

# App logging is unified on structlog. `_log` is the module base; loaders
# locally rebind `logger = _log.bind(trace_id=...)` so trace_id rides along as
# a structured field. tenacity's before_sleep_log needs a stdlib logger +
# numeric level, so keep a dedicated one just for the retry hook.
_log = structlog.get_logger(__name__)
_tenacity_logger = logging.getLogger(__name__)

# Configuration
DEFAULT_BATCH_SIZE = int(os.getenv("DATA_LOADER_BATCH_SIZE", "1000"))

# Retry configuration from centralized timeouts
DEFAULT_RETRY_ATTEMPTS = int(os.getenv("DATA_LOADER_RETRY_ATTEMPTS", "3"))
DEFAULT_RETRY_MIN_WAIT = int(os.getenv("DATA_LOADER_RETRY_MIN_WAIT", "4"))
DEFAULT_RETRY_MAX_WAIT = int(os.getenv("DATA_LOADER_RETRY_MAX_WAIT", "10"))

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

# Object storage transient errors (S3/boto3, Azure/azure-core) that should
# trigger retry. Both libraries are optional - ObjectStorageLoader talks to
# whichever backend fsspec resolves for the configured URI, so neither may
# be installed. Falls back to nothing matching (isinstance against an empty
# tuple is always False) rather than treating everything as transient.
_object_storage_transient_types = []
try:
    import botocore.exceptions as _botocore_errors
    _object_storage_transient_types += [
        _botocore_errors.EndpointConnectionError,
        _botocore_errors.ConnectTimeoutError,
        _botocore_errors.ReadTimeoutError,
        _botocore_errors.ConnectionClosedError,
    ]
except ImportError:
    pass
try:
    import azure.core.exceptions as _azure_errors
    _object_storage_transient_types += [
        _azure_errors.ServiceRequestError,
        _azure_errors.ServiceResponseError,
    ]
except ImportError:
    pass
OBJECT_STORAGE_TRANSIENT_ERRORS = tuple(_object_storage_transient_types) + (
    TimeoutError,
    ConnectionError,
)


def sanitize_for_filename(value: str) -> str:
    """
    Make a value safe to embed in a filename / object key.

    Used to turn ``LoadContext.correlation_id`` (``{dag_id}_{run_id}_{task_id}``)
    into a deterministic filename suffix. Airflow run_ids embed an ISO
    timestamp (e.g. ``scheduled__2024-01-01T00:00:00+00:00``), so ``:`` and
    ``+`` need stripping for filesystem/S3 safety.
    """
    return re.sub(r"[^A-Za-z0-9._-]", "_", value)


def is_transient_object_storage_error(exception: Exception) -> bool:
    """Determine if an object storage error (S3/Azure/...) is transient."""
    return isinstance(exception, OBJECT_STORAGE_TRANSIENT_ERRORS)


def is_transient_snowflake_error(exception: Exception) -> bool:
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
    before_sleep=before_sleep_log(_tenacity_logger, logging.WARNING),
    reraise=True,
)


class DataLoadError(Exception):
    """Custom exception for data loading errors."""

    def __init__(
        self, message: str, destination: str, original_error: Optional[Exception] = None
    ):
        self.destination = destination
        self.original_error = original_error
        super().__init__(f"[{destination}] {message}")


class TransientDataLoadError(DataLoadError, TransientError):
    """A DataLoadError worth retrying at the Airflow task level (connection
    reset, timeout, service unavailable, ...)."""


class ObjectStorageError(DataLoadError):
    """Custom exception for object storage operations wrapping native errors."""

    def __init__(
        self, message: str, destination: str, original_error: Optional[Exception] = None
    ):
        super().__init__(message, destination, original_error)


class TransientObjectStorageError(ObjectStorageError, TransientError):
    """An ObjectStorageError worth retrying at the Airflow task level."""
