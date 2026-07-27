# File: rlam_airflow_framework/data_fetchers/http.py
"""
HttpFetcher: pulls data from a REST API with timeouts, retries, and auth.
"""

import time
from typing import Optional, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import structlog

from rlam_airflow_framework.data_fetchers.base import (
    DataFetcher,
    DataFetchError,
    DEFAULT_TIMEOUT,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_BACKOFF,
)
from rlam_airflow_framework.data_fetchers.parsers import parse_content

_log = structlog.get_logger(__name__)


def _create_retry_session(
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_factor: float = DEFAULT_RETRY_BACKOFF,
    status_forcelist: tuple = (429, 500, 502, 503, 504),
) -> requests.Session:
    """
    Create a requests session with retry logic.

    Args:
        max_retries: Maximum number of retry attempts
        backoff_factor: Exponential backoff factor between retries
        status_forcelist: HTTP status codes that trigger a retry

    Returns:
        Configured requests.Session with retry adapter
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "POST", "PUT", "DELETE"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


class HttpFetcher(DataFetcher):
    """
    Fetches data from an HTTP API with timeout, retry logic, and proper
    error handling.

    Expected config keys (typically the pipeline's ``data_source`` section):
        endpoint: API endpoint URL (required)
        request_config.headers: Additional headers
        request_config.params: Query parameters
        api_key: API key for Bearer auth
        response_format: 'json' (default), 'csv', or 'xml'
        timeout / max_retries: Optional overrides of the module defaults
    """

    def fetch(
        self, config: Dict[str, Any], correlation_id: Optional[str] = None
    ) -> pd.DataFrame:
        url = config["endpoint"]
        request_config = config.get("request_config", {}) or {}
        headers = request_config.get("headers")
        params = request_config.get("params")
        format = config.get("response_format", "json")
        api_key = config.get("api_key")
        timeout = config.get("timeout", DEFAULT_TIMEOUT)
        max_retries = config.get("max_retries", DEFAULT_MAX_RETRIES)

        trace_id = correlation_id or f"http-{int(time.time() * 1000)}"
        log = _log.bind(trace_id=trace_id)

        log.info("Starting HTTP fetch", trace_id=trace_id, url=url, format=format)

        # Validate URL
        if not url or not isinstance(url, str):
            raise ValueError("URL must be a non-empty string")

        if not url.startswith(("http://", "https://")):
            raise ValueError(
                f"Invalid URL scheme. Expected http:// or https://, got: {url}"
            )

        # Set up headers
        request_headers = headers.copy() if headers else {}
        if api_key:
            request_headers["Authorization"] = f"Bearer {api_key}"
            log.debug("Using Bearer token authentication", trace_id=trace_id)

        # Create session with retry logic
        session = _create_retry_session(max_retries=max_retries)

        try:
            start_time = time.time()
            response = session.get(
                url, headers=request_headers, params=params, timeout=timeout
            )
            elapsed_time = time.time() - start_time

            log.info(
                "HTTP response received",
                trace_id=trace_id,
                status=response.status_code,
                elapsed_seconds=round(elapsed_time, 2),
                content_length=len(response.content),
            )

            # Raise for bad status codes
            response.raise_for_status()

            # Check for empty response
            if not response.content:
                log.warning("Empty response received", trace_id=trace_id, url=url)
                return pd.DataFrame()

            # Parse based on format
            df = parse_content(response.text, format, trace_id)

            log.info(
                "Successfully parsed HTTP response", trace_id=trace_id, rows=len(df)
            )
            return df

        except requests.exceptions.Timeout as e:
            log.error(
                "Request timeout", trace_id=trace_id, timeout=timeout, error=str(e)
            )
            raise DataFetchError(
                f"Request timed out after {timeout} seconds",
                source=url,
                original_error=e,
            ) from e

        except requests.exceptions.ConnectionError as e:
            log.error("Connection error", trace_id=trace_id, url=url, error=str(e))
            raise DataFetchError(
                f"Failed to connect to {url}", source=url, original_error=e
            ) from e

        except requests.exceptions.HTTPError as e:
            log.error(
                "HTTP error",
                trace_id=trace_id,
                status=response.status_code,
                error=str(e),
            )
            raise DataFetchError(
                f"HTTP {response.status_code}: {response.reason}",
                source=url,
                original_error=e,
            ) from e

        except Exception as e:
            log.error(
                "Unexpected error during HTTP fetch",
                trace_id=trace_id,
                error=str(e),
                exc_info=True,
            )
            raise DataFetchError(
                f"Unexpected error: {str(e)}", source=url, original_error=e
            ) from e

        finally:
            session.close()
