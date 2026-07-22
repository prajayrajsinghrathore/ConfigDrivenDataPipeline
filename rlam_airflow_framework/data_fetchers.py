# File: dags/utils/data_fetchers.py
"""
Data fetching utilities for external sources.

Provides secure, resilient data fetching from HTTP APIs and SFTP servers with:
- Request timeouts to prevent hanging connections
- Retry logic with exponential backoff for transient failures
- Proper SSH host key verification
- Structlog for structured logging (Airflow 3.1.6)
- Type hints for better code quality
- Centralized timeout configuration support
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import paramiko
import socket
from io import StringIO, BytesIO
import json
import xml.etree.ElementTree as ET
from airflow.hooks.base import BaseHook
from typing import Optional, Dict, Any, List
import structlog
import os
import time

log = structlog.get_logger(__name__)

# =============================================================================
# CENTRALIZED TIMEOUT CONFIGURATION
# =============================================================================
# Environment variables take precedence, then fall back to defaults.
# These align with config/global_settings.yaml timeouts.http section.

DEFAULT_TIMEOUT = int(os.getenv("TIMEOUT_HTTP_REQUEST", os.getenv("DATA_FETCHER_TIMEOUT", "30")))
DEFAULT_MAX_RETRIES = int(os.getenv("TIMEOUT_HTTP_MAX_RETRIES", os.getenv("DATA_FETCHER_MAX_RETRIES", "3")))
DEFAULT_RETRY_BACKOFF = float(os.getenv("TIMEOUT_HTTP_RETRY_BACKOFF", os.getenv("DATA_FETCHER_RETRY_BACKOFF", "1.0")))

# SFTP timeouts - align with config/global_settings.yaml timeouts.sftp section
SFTP_CONNECT_TIMEOUT = int(os.getenv("TIMEOUT_SFTP_CONNECT", "30"))
SFTP_BANNER_TIMEOUT = int(os.getenv("TIMEOUT_SFTP_BANNER", "30"))
SFTP_AUTH_TIMEOUT = int(os.getenv("TIMEOUT_SFTP_AUTH", "30"))
SFTP_CHANNEL_TIMEOUT = int(os.getenv("TIMEOUT_SFTP_CHANNEL", "30"))

KNOWN_HOSTS_FILE = os.getenv(
    "SSH_KNOWN_HOSTS_FILE", os.path.expanduser("~/.ssh/known_hosts")
)


class DataFetchError(Exception):
    """Custom exception for data fetching errors with context."""

    def __init__(
        self, message: str, source: str, original_error: Optional[Exception] = None
    ):
        self.source = source
        self.original_error = original_error
        super().__init__(f"[{source}] {message}")


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


def fetch_http_data(
    url: str,
    api_key: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    format: str = "json",
    timeout: int = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    correlation_id: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch data from HTTP API with timeout, retry logic, and proper error handling.

    Args:
        url: API endpoint URL
        api_key: API key for authentication
        headers: Additional headers
        params: Query parameters
        format: Expected response format ('json', 'csv', 'xml')
        timeout: Request timeout in seconds (default: 30)
        max_retries: Maximum retry attempts for transient failures
        correlation_id: Optional ID for distributed tracing

    Returns:
        pandas.DataFrame: Parsed data

    Raises:
        DataFetchError: If the request fails after all retries
        ValueError: If the response format is unsupported
    """
    trace_id = correlation_id or f"http-{int(time.time() * 1000)}"

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
        df = _parse_response(response, format, trace_id)

        log.info("Successfully parsed HTTP response", trace_id=trace_id, rows=len(df))
        return df

    except requests.exceptions.Timeout as e:
        log.error("Request timeout", trace_id=trace_id, timeout=timeout, error=str(e))
        raise DataFetchError(
            f"Request timed out after {timeout} seconds", source=url, original_error=e
        ) from e

    except requests.exceptions.ConnectionError as e:
        log.error("Connection error", trace_id=trace_id, url=url, error=str(e))
        raise DataFetchError(
            f"Failed to connect to {url}", source=url, original_error=e
        ) from e

    except requests.exceptions.HTTPError as e:
        log.error(
            "HTTP error", trace_id=trace_id, status=response.status_code, error=str(e)
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


def _parse_response(
    response: requests.Response, format: str, trace_id: str
) -> pd.DataFrame:
    """
    Parse HTTP response based on format.

    Args:
        response: The HTTP response object
        format: Expected format ('json', 'csv', 'xml')
        trace_id: Correlation ID for logging

    Returns:
        Parsed DataFrame

    Raises:
        ValueError: If format is unsupported or parsing fails
    """
    try:
        if format == "json":
            data = response.json()
            if data is None:
                log.warning(f"[{trace_id}] JSON response is null")
                return pd.DataFrame()
            if isinstance(data, list) and len(data) == 0:
                log.warning(f"[{trace_id}] JSON response is empty array")
                return pd.DataFrame()
            return pd.json_normalize(data)

        elif format == "csv":
            text = response.text
            if not text or text.isspace():
                log.warning(f"[{trace_id}] CSV response is empty")
                return pd.DataFrame()
            return pd.read_csv(StringIO(text))

        elif format == "xml":
            return _parse_xml_response(response.text, trace_id)

        else:
            raise ValueError(f"Unsupported format: {format}. Supported: json, csv, xml")

    except json.JSONDecodeError as e:
        log.error(f"[{trace_id}] Failed to parse JSON response: {e}")
        raise ValueError(f"Invalid JSON response: {e}") from e

    except pd.errors.EmptyDataError as e:
        log.warning(f"[{trace_id}] Empty data in response: {e}")
        return pd.DataFrame()

    except ET.ParseError as e:
        log.error(f"[{trace_id}] Failed to parse XML response: {e}")
        raise ValueError(f"Invalid XML response: {e}") from e


def _parse_xml_response(xml_text: str, trace_id: str) -> pd.DataFrame:
    """
    Parse XML response to DataFrame.

    Args:
        xml_text: Raw XML string
        trace_id: Correlation ID for logging

    Returns:
        Parsed DataFrame
    """
    if not xml_text or xml_text.isspace():
        log.warning(f"[{trace_id}] XML response is empty")
        return pd.DataFrame()

    root = ET.fromstring(xml_text)
    data: List[Dict[str, Any]] = []

    for child in root:
        item: Dict[str, Any] = {}
        for subchild in child:
            item[subchild.tag] = subchild.text
        if item:  # Only add non-empty items
            data.append(item)

    if not data:
        log.warning(f"[{trace_id}] No data extracted from XML")

    return pd.DataFrame(data)


class SSHHostKeyPolicy(paramiko.MissingHostKeyPolicy):
    """
    Custom SSH host key policy that supports multiple verification modes.

    Modes:
    - 'strict': Reject unknown hosts (production recommended)
    - 'warn': Log warning but accept unknown hosts
    - 'auto_add': Add unknown hosts to known_hosts file (development only)
    """

    def __init__(self, mode: str = "warn", known_hosts_file: Optional[str] = None):
        self.mode = mode
        self.known_hosts_file = known_hosts_file or KNOWN_HOSTS_FILE

    def missing_host_key(
        self, client: paramiko.SSHClient, hostname: str, key: paramiko.PKey
    ) -> None:
        key_type = key.get_name()
        key_fingerprint = key.get_fingerprint().hex()

        if self.mode == "strict":
            log.error(
                f"SSH host key verification failed for {hostname}. "
                f"Key type: {key_type}, Fingerprint: {key_fingerprint}"
            )
            raise paramiko.SSHException(
                f"Unknown host key for {hostname}. Add to known_hosts or set SSH_HOST_KEY_MODE=warn"
            )

        elif self.mode == "warn":
            log.warning(
                f"Unknown SSH host key for {hostname}. Key type: {key_type}, "
                f"Fingerprint: {key_fingerprint}. Consider adding to known_hosts for production."
            )

        elif self.mode == "auto_add":
            log.warning(
                f"Auto-adding SSH host key for {hostname}. "
                f"This is NOT recommended for production environments."
            )
            # Add to known hosts file
            try:
                client.get_host_keys().add(hostname, key_type, key)
                if self.known_hosts_file and os.path.exists(
                    os.path.dirname(self.known_hosts_file)
                ):
                    client.save_host_keys(self.known_hosts_file)
            except Exception as e:
                log.warning(f"Failed to save host key: {e}")


def fetch_sftp_data(
    sftp_conn_id: str,
    remote_path: str,
    file_format: str = "csv",
    timeout: int = DEFAULT_TIMEOUT,
    correlation_id: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch data from SFTP server with proper security and error handling.

    Args:
        sftp_conn_id: Airflow connection ID for SFTP
        remote_path: Path to file on SFTP server
        file_format: File format ('csv', 'json', 'xml')
        timeout: Connection timeout in seconds
        correlation_id: Optional ID for distributed tracing

    Returns:
        pandas.DataFrame: Parsed data

    Raises:
        DataFetchError: If the SFTP operation fails
        ValueError: If the file format is unsupported
    """
    trace_id = correlation_id or f"sftp-{int(time.time() * 1000)}"
    ssh: Optional[paramiko.SSHClient] = None
    sftp: Optional[paramiko.SFTPClient] = None

    log.info(
        f"[{trace_id}] Starting SFTP fetch from connection '{sftp_conn_id}', path: {remote_path}"
    )

    # Validate inputs
    if not sftp_conn_id:
        raise ValueError("sftp_conn_id is required")
    if not remote_path:
        raise ValueError("remote_path is required")
    if file_format not in ("csv", "json", "xml"):
        raise ValueError(
            f"Unsupported file format: {file_format}. Supported: csv, json, xml"
        )

    try:
        # Get connection details from Airflow
        connection = BaseHook.get_connection(sftp_conn_id)

        if not connection.host:
            raise DataFetchError(
                "SFTP host not configured in connection", source=sftp_conn_id
            )

        # Set up SSH client with secure host key policy
        ssh = paramiko.SSHClient()

        # Load system host keys
        ssh.load_system_host_keys()

        # Load known hosts file if exists
        if os.path.exists(KNOWN_HOSTS_FILE):
            try:
                ssh.load_host_keys(KNOWN_HOSTS_FILE)
                log.debug(f"[{trace_id}] Loaded known hosts from {KNOWN_HOSTS_FILE}")
            except Exception as e:
                log.warning(f"[{trace_id}] Failed to load known hosts: {e}")

        # Set host key policy based on environment
        host_key_mode = os.getenv(
            "SSH_HOST_KEY_MODE", "warn"
        )  # strict, warn, or auto_add
        ssh.set_missing_host_key_policy(SSHHostKeyPolicy(mode=host_key_mode))

        log.info(
            f"[{trace_id}] Connecting to SFTP server {connection.host}:{connection.port or 22}"
        )

        # Connect with timeout
        start_time = time.time()
        ssh.connect(
            hostname=connection.host,
            port=connection.port or 22,
            username=connection.login,
            password=connection.password,
            timeout=timeout,
            banner_timeout=timeout,
            auth_timeout=timeout,
        )

        connect_time = time.time() - start_time
        log.info(f"[{trace_id}] SSH connection established in {connect_time:.2f}s")

        sftp = ssh.open_sftp()
        sftp.get_channel().settimeout(timeout)

        # Check if file exists
        try:
            file_stat = sftp.stat(remote_path)
            log.info(
                f"[{trace_id}] File found: {remote_path}, size: {file_stat.st_size} bytes"
            )
        except FileNotFoundError:
            raise DataFetchError(f"File not found: {remote_path}", source=sftp_conn_id)

        # Download file to memory
        file_obj = BytesIO()
        sftp.getfo(remote_path, file_obj)
        file_obj.seek(0)

        download_time = time.time() - start_time - connect_time
        log.info(f"[{trace_id}] File downloaded in {download_time:.2f}s")

        # Parse based on format
        df = _parse_sftp_file(file_obj, file_format, trace_id)

        log.info(f"[{trace_id}] Successfully parsed {len(df)} rows from SFTP file")
        return df

    except paramiko.AuthenticationException as e:
        log.error(f"[{trace_id}] SFTP authentication failed: {e}")
        raise DataFetchError(
            "Authentication failed. Check username/password.",
            source=sftp_conn_id,
            original_error=e,
        ) from e

    except paramiko.SSHException as e:
        log.error(f"[{trace_id}] SSH error: {e}")
        raise DataFetchError(
            f"SSH connection error: {str(e)}", source=sftp_conn_id, original_error=e
        ) from e

    except socket.timeout as e:
        log.error(f"[{trace_id}] SFTP connection timeout: {e}")
        raise DataFetchError(
            f"Connection timed out after {timeout} seconds",
            source=sftp_conn_id,
            original_error=e,
        ) from e

    except Exception as e:
        log.error(f"[{trace_id}] Unexpected SFTP error: {e}", exc_info=True)
        raise DataFetchError(
            f"Unexpected error: {str(e)}", source=sftp_conn_id, original_error=e
        ) from e

    finally:
        # Clean up connections
        if sftp:
            try:
                sftp.close()
                log.debug(f"[{trace_id}] SFTP connection closed")
            except Exception as e:
                log.warning(f"[{trace_id}] Error closing SFTP: {e}")

        if ssh:
            try:
                ssh.close()
                log.debug(f"[{trace_id}] SSH connection closed")
            except Exception as e:
                log.warning(f"[{trace_id}] Error closing SSH: {e}")


def _parse_sftp_file(
    file_obj: BytesIO, file_format: str, trace_id: str
) -> pd.DataFrame:
    """
    Parse downloaded SFTP file based on format.

    Args:
        file_obj: BytesIO object containing file data
        file_format: File format ('csv', 'json', 'xml')
        trace_id: Correlation ID for logging

    Returns:
        Parsed DataFrame
    """
    try:
        if file_format == "csv":
            return pd.read_csv(file_obj)

        elif file_format == "json":
            data = json.load(file_obj)
            if data is None:
                log.warning(f"[{trace_id}] JSON file contains null")
                return pd.DataFrame()
            return pd.json_normalize(data)

        elif file_format == "xml":
            tree = ET.parse(file_obj)
            root = tree.getroot()
            data: List[Dict[str, Any]] = []
            for child in root:
                item: Dict[str, Any] = {}
                for subchild in child:
                    item[subchild.tag] = subchild.text
                if item:
                    data.append(item)
            return pd.DataFrame(data)

        else:
            raise ValueError(f"Unsupported format: {file_format}")

    except pd.errors.EmptyDataError:
        log.warning(f"[{trace_id}] File is empty")
        return pd.DataFrame()

    except json.JSONDecodeError as e:
        log.error(f"[{trace_id}] Invalid JSON in file: {e}")
        raise ValueError(f"Invalid JSON file: {e}") from e

    except ET.ParseError as e:
        log.error(f"[{trace_id}] Invalid XML in file: {e}")
        raise ValueError(f"Invalid XML file: {e}") from e
