# File: rlam_airflow_framework/data_fetchers/sftp.py
"""
SftpFetcher: pulls a file from an SFTP server over an authenticated SSH
session, with host-key verification and configurable timeouts.
"""

import os
import socket
import time
from io import BytesIO
from typing import Optional, Dict, Any
from pathlib import Path
import paramiko
import pandas as pd
import duckdb
import structlog

# Task SDK Connection: resolves via the execution API on workers (airflow.models
# is DB-isolated on Airflow 3 Task SDK workers and must not be used in task code)
from airflow.sdk import Connection

from rlam_airflow_framework.data_fetchers.base import (
    DataFetcher,
    DataFetchError,
    TransientDataFetchError,
)
from rlam_airflow_framework.data_fetchers.parsers import parse_content

_log = structlog.get_logger(__name__)
log = _log

# SFTP timeouts - align with config/global_settings.yaml timeouts.sftp section
SFTP_CONNECT_TIMEOUT = int(os.getenv("TIMEOUT_SFTP_CONNECT", "30"))
SFTP_BANNER_TIMEOUT = int(os.getenv("TIMEOUT_SFTP_BANNER", "30"))
SFTP_AUTH_TIMEOUT = int(os.getenv("TIMEOUT_SFTP_AUTH", "30"))
SFTP_CHANNEL_TIMEOUT = int(os.getenv("TIMEOUT_SFTP_CHANNEL", "30"))

KNOWN_HOSTS_FILE = os.getenv(
    "SSH_KNOWN_HOSTS_FILE", os.path.expanduser("~/.ssh/known_hosts")
)


class SSHHostKeyPolicy(paramiko.MissingHostKeyPolicy):
    """
    Custom SSH host key policy that supports multiple verification modes.

    Modes:
    - 'strict': Reject unknown hosts (production recommended)
    - 'warn': Log warning but accept unknown hosts
    - 'auto_add': Add unknown hosts to known_hosts file (development only)
    """

    def __init__(self, mode: str = "strict", known_hosts_file: Optional[str] = None):
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


class SftpFetcher(DataFetcher):
    """
    Fetches a file from an SFTP server with proper security and error handling.

    Expected config keys (typically the pipeline's ``data_source`` section):
        connection_id: Airflow connection ID for SFTP (required)
        remote_path: Path to file on SFTP server (required)
        file_format: 'csv' (default), 'json', or 'xml'
        timeout: Optional override applied uniformly to the connect, banner,
            auth, and channel timeouts. When omitted, each phase uses its own
            ``TIMEOUT_SFTP_*`` environment variable default.
    """

    def fetch(
        self, config: Dict[str, Any], correlation_id: Optional[str] = None, target_path: Optional[Path] = None
    ) -> Path | pd.DataFrame:
        sftp_conn_id = config["connection_id"]
        remote_path = config["remote_path"]
        file_format = config.get("file_format", "csv")

        timeout_override = config.get("timeout")
        connect_timeout = (
            timeout_override if timeout_override is not None else SFTP_CONNECT_TIMEOUT
        )
        banner_timeout = (
            timeout_override if timeout_override is not None else SFTP_BANNER_TIMEOUT
        )
        auth_timeout = (
            timeout_override if timeout_override is not None else SFTP_AUTH_TIMEOUT
        )
        channel_timeout = (
            timeout_override if timeout_override is not None else SFTP_CHANNEL_TIMEOUT
        )

        trace_id = correlation_id or f"sftp-{int(time.time() * 1000)}"
        log = _log.bind(trace_id=trace_id)
        ssh: Optional[paramiko.SSHClient] = None
        sftp: Optional[paramiko.SFTPClient] = None

        log.info(
            f"Starting SFTP fetch from connection '{sftp_conn_id}', path: {remote_path}"
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
            # Get connection details from Airflow (Task SDK surface)
            connection = Connection.get(sftp_conn_id)

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
                    log.debug(f"Loaded known hosts from {KNOWN_HOSTS_FILE}")
                except Exception as e:
                    log.warning(f"Failed to load known hosts: {e}")

            # Set host key policy based on environment.
            # Defaults to 'strict' so an unknown host key aborts the connection
            # instead of leaking SFTP credentials to a possible MITM. Non-prod can
            # opt down to 'warn' or 'auto_add' by setting SSH_HOST_KEY_MODE.
            host_key_mode = os.getenv(
                "SSH_HOST_KEY_MODE", "strict"
            )  # strict, warn, or auto_add
            ssh.set_missing_host_key_policy(SSHHostKeyPolicy(mode=host_key_mode))

            log.info(
                f"Connecting to SFTP server {connection.host}:{connection.port or 22}"
            )

            # Connect with timeout
            start_time = time.time()
            ssh.connect(
                hostname=connection.host,
                port=connection.port or 22,
                username=connection.login,
                password=connection.password,
                timeout=connect_timeout,
                banner_timeout=banner_timeout,
                auth_timeout=auth_timeout,
            )

            connect_time = time.time() - start_time
            log.info(f"SSH connection established in {connect_time:.2f}s")

            sftp = ssh.open_sftp()
            channel = sftp.get_channel()
            if channel is not None:
                channel.settimeout(channel_timeout)

            # Check if file exists
            try:
                file_stat = sftp.stat(remote_path)
                log.info(
                    f"File found: {remote_path}, size: {file_stat.st_size} bytes"
                )
            except FileNotFoundError:
                raise DataFetchError(
                    f"File not found: {remote_path}", source=sftp_conn_id
                )

            if target_path:
                import tempfile
                temp_fd, temp_raw_path = tempfile.mkstemp(suffix=f".{file_format}")
                os.close(temp_fd)
                try:
                    sftp.get(remote_path, temp_raw_path)
                    download_time = time.time() - start_time - connect_time
                    log.info(f"File downloaded to disk in {download_time:.2f}s")
                    
                    if file_format in ("json", "csv"):
                        read_func = "read_json_auto" if file_format == "json" else "read_csv_auto"
                        duckdb.query(f"COPY (SELECT * FROM {read_func}('{temp_raw_path}')) TO '{target_path}' (FORMAT PARQUET)")
                        log.info("Converted streamed SFTP data to Parquet via DuckDB", trace_id=trace_id)
                    else:
                        with open(temp_raw_path, "r", encoding="utf-8") as f:
                            content = f.read()
                        df = parse_content(content, file_format, trace_id)
                        df.to_parquet(target_path, index=False, compression="snappy")
                        log.info("Converted streamed SFTP data to Parquet via Pandas fallback", trace_id=trace_id)
                        
                    return target_path
                finally:
                    if os.path.exists(temp_raw_path):
                        os.remove(temp_raw_path)
            else:
                # Download file to memory
                file_obj = BytesIO()
                sftp.getfo(remote_path, file_obj)
                file_obj.seek(0)
    
                download_time = time.time() - start_time - connect_time
                log.info(f"File downloaded in {download_time:.2f}s")
    
                # Parse based on format
                content = file_obj.getvalue().decode("utf-8")
                df = parse_content(content, file_format, trace_id)
    
                log.info(f"Successfully parsed {len(df)} rows from SFTP file")
                return df

        except paramiko.AuthenticationException as e:
            log.error(f"SFTP authentication failed: {e}")
            raise DataFetchError(
                "Authentication failed. Check username/password.",
                source=sftp_conn_id,
                original_error=e,
            ) from e

        except paramiko.SSHException as e:
            # Excludes AuthenticationException (caught above, more specific):
            # what's left here is connection/protocol-level (banner timeout,
            # connection reset, etc.) - typically transient, worth a retry.
            log.error(f"SSH error: {e}")
            raise TransientDataFetchError(
                f"SSH connection error: {str(e)}",
                source=sftp_conn_id,
                original_error=e,
            ) from e

        except socket.timeout as e:
            log.error(f"SFTP connection timeout: {e}")
            raise TransientDataFetchError(
                f"Connection timed out after {connect_timeout} seconds",
                source=sftp_conn_id,
                original_error=e,
            ) from e

        except Exception as e:
            log.error(f"Unexpected SFTP error: {e}", exc_info=True)
            raise DataFetchError(
                f"Unexpected error: {str(e)}", source=sftp_conn_id, original_error=e
            ) from e

        finally:
            # Clean up connections
            if sftp:
                try:
                    sftp.close()
                    log.debug("SFTP connection closed")
                except Exception as e:
                    log.warning(f"Error closing SFTP: {e}")

            if ssh:
                try:
                    ssh.close()
                    log.debug("SSH connection closed")
                except Exception as e:
                    log.warning(f"Error closing SSH: {e}")
