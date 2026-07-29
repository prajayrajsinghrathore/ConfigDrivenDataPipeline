# File: rlam_airflow_framework/data_fetchers/http.py
"""
HttpFetcher: pulls data from a REST API with timeouts, retries, and auth.
"""

import time
import os
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import duckdb
import structlog

from rlam_airflow_framework.data_fetchers.base import (
    DataFetcher,
    DataFetchError,
    TransientDataFetchError,
    DEFAULT_TIMEOUT,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_BACKOFF,
)
from rlam_airflow_framework.data_fetchers.parsers import parse_content

_log = structlog.get_logger(__name__)

# HTTP statuses the connection-pool retry adapter already retries; if one
# still surfaces as a raised HTTPError, the adapter exhausted its own
# retries, so it's still worth a task-level retry (RETRYABLE_STATUS_CODES
# stays in sync with _create_retry_session's status_forcelist below).
RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)


def _create_retry_session(
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_factor: float = DEFAULT_RETRY_BACKOFF,
    status_forcelist: tuple = RETRYABLE_STATUS_CODES,
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
        self, config: Dict[str, Any], correlation_id: Optional[str] = None, target_path: Optional[Path] = None
    ) -> Path | pd.DataFrame:
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
            with session.get(
                url, headers=request_headers, params=params, timeout=timeout, stream=True
            ) as response:
                
                # Raise for bad status codes
                response.raise_for_status()

                # Check for empty response by peeking
                first_chunk = response.raw.read(10)
                if not first_chunk:
                    log.warning("Empty response received", trace_id=trace_id, url=url)
                    if target_path:
                        pd.DataFrame().to_parquet(target_path, index=False)
                        return target_path
                    return pd.DataFrame()

                if target_path:
                    # Stream to a temp file first
                    temp_fd, temp_raw_path = tempfile.mkstemp(suffix=f".{format}")
                    os.close(temp_fd)
                    try:
                        with open(temp_raw_path, "wb") as f:
                            f.write(first_chunk)
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                                    
                        elapsed_time = time.time() - start_time
                        file_size_mb = os.path.getsize(temp_raw_path) / 1024 / 1024
                        log.info(
                            "HTTP streaming complete",
                            trace_id=trace_id,
                            status=response.status_code,
                            elapsed_seconds=round(elapsed_time, 2),
                            content_length_mb=round(file_size_mb, 2),
                        )

                        if format in ("json", "csv"):
                            read_func = "read_json_auto" if format == "json" else "read_csv_auto"
                            # DuckDB converts the file to parquet in chunks, keeping RAM flat
                            duckdb.query(f"COPY (SELECT * FROM {read_func}('{temp_raw_path}')) TO '{target_path}' (FORMAT PARQUET)")
                            log.info("Converted streamed data to Parquet via DuckDB", trace_id=trace_id)
                        else:
                            # Fallback for XML
                            with open(temp_raw_path, "r", encoding="utf-8") as f:
                                content = f.read()
                            df = parse_content(content, format, trace_id)
                            df.to_parquet(target_path, index=False, compression="snappy")
                            log.info("Converted streamed data to Parquet via Pandas fallback", trace_id=trace_id)

                        return target_path

                    finally:
                        if os.path.exists(temp_raw_path):
                            os.remove(temp_raw_path)
                else:
                    # Legacy fallback if no target path is provided
                    content_bytes = first_chunk + response.content
                    df = parse_content(content_bytes.decode("utf-8"), format, trace_id)
                    elapsed_time = time.time() - start_time
                    log.info(
                        "Successfully parsed HTTP response", trace_id=trace_id, rows=len(df), elapsed_seconds=round(elapsed_time, 2)
                    )
                    return df

        except requests.exceptions.Timeout as e:
            log.error(
                "Request timeout", trace_id=trace_id, timeout=timeout, error=str(e)
            )
            raise TransientDataFetchError(
                f"Request timed out after {timeout} seconds",
                source=url,
                original_error=e,
            ) from e

        except requests.exceptions.ConnectionError as e:
            log.error("Connection error", trace_id=trace_id, url=url, error=str(e))
            raise TransientDataFetchError(
                f"Failed to connect to {url}", source=url, original_error=e
            ) from e

        except requests.exceptions.HTTPError as e:
            log.error(
                "HTTP error",
                trace_id=trace_id,
                status=response.status_code,
                error=str(e),
            )
            # The connection-pool adapter already retries RETRYABLE_STATUS_CODES;
            # if one still got here, that budget is exhausted but a task-level
            # retry (with its own backoff) is still worth it. Other statuses
            # (4xx auth/not-found/etc.) are deterministic - retrying won't help.
            error_cls = (
                TransientDataFetchError
                if response.status_code in RETRYABLE_STATUS_CODES
                else DataFetchError
            )
            raise error_cls(
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
