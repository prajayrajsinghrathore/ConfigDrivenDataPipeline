# File: rlam_airflow_framework/data_fetchers/base.py
"""
DataFetcher interface and shared constants.

Each concrete fetcher (HttpFetcher, SftpFetcher, ...) implements ``fetch()``
against a source-specific ``data_source`` config dict and is resolved
polymorphically via ``factory.get_data_fetcher()``. Callers depend on this
abstraction, not on a specific transport.
"""

import os
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pathlib import Path
from rlam_airflow_framework.utils.retry_policy import TransientError

# =============================================================================
# CENTRALIZED TIMEOUT CONFIGURATION
# =============================================================================
# Environment variables take precedence, then fall back to defaults.
# These align with config/global_settings.yaml timeouts.http section.

DEFAULT_TIMEOUT = int(os.getenv("TIMEOUT_HTTP_REQUEST", os.getenv("DATA_FETCHER_TIMEOUT", "30")))
DEFAULT_MAX_RETRIES = int(os.getenv("TIMEOUT_HTTP_MAX_RETRIES", os.getenv("DATA_FETCHER_MAX_RETRIES", "3")))
DEFAULT_RETRY_BACKOFF = float(os.getenv("TIMEOUT_HTTP_RETRY_BACKOFF", os.getenv("DATA_FETCHER_RETRY_BACKOFF", "1.0")))


class DataFetchError(Exception):
    """Custom exception for data fetching errors with context."""

    def __init__(
        self, message: str, source: str, original_error: Optional[Exception] = None
    ):
        self.source = source
        self.original_error = original_error
        super().__init__(f"[{source}] {message}")


class TransientDataFetchError(DataFetchError, TransientError):
    """A DataFetchError worth retrying at the Airflow task level (network
    timeout, connection reset, 5xx/429 response, ...)."""


class DataFetcher(ABC):
    """Strategy interface for pulling a DataFrame out of a configured source."""

    @abstractmethod
    def fetch(
        self, 
        config: Dict[str, Any], 
        correlation_id: Optional[str] = None,
        target_path: Optional[Path] = None
    ) -> Path:
        """
        Fetch and parse data described by a ``data_source`` config dict.

        Args:
            config: The source-specific slice of pipeline config (e.g. the
                ``data_source`` section) — shape depends on the concrete fetcher.
            correlation_id: Optional ID for distributed tracing.
            target_path: Optional target path to stream data to.

        Returns:
            Path | pd.DataFrame: Path to the local file, or parsed DataFrame (legacy).
        """
        raise NotImplementedError
