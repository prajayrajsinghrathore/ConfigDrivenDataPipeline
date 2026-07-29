"""
Exception classification for HttpFetcher and SftpFetcher.

Pins which raw failures get wrapped as Transient*Error (worth an Airflow
task-level retry) vs plain *Error (deterministic, don't bother retrying).
See rlam_airflow_framework/utils/retry_policy.py for why this classification
has to happen here, at the point the raw exception is still visible.
"""

import socket
from unittest.mock import MagicMock, patch

import paramiko
import pytest
import requests

from rlam_airflow_framework.data_fetchers.base import DataFetchError, TransientDataFetchError
from rlam_airflow_framework.data_fetchers.http import HttpFetcher
from rlam_airflow_framework.data_fetchers.sftp import SftpFetcher


def _http_config(**overrides):
    cfg = {"endpoint": "https://api.example.com/data", "response_format": "json"}
    cfg.update(overrides)
    return cfg


def _http_error_response(status_code):
    response = MagicMock()
    response.status_code = status_code
    response.reason = "error"
    response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=response)
    return response


@patch("rlam_airflow_framework.data_fetchers.http._create_retry_session")
def test_http_timeout_is_transient(mock_create_session):
    mock_session = MagicMock()
    mock_session.get.side_effect = requests.exceptions.Timeout("timed out")
    mock_create_session.return_value = mock_session

    with pytest.raises(TransientDataFetchError):
        HttpFetcher().fetch(_http_config())


@patch("rlam_airflow_framework.data_fetchers.http._create_retry_session")
def test_http_connection_error_is_transient(mock_create_session):
    mock_session = MagicMock()
    mock_session.get.side_effect = requests.exceptions.ConnectionError("refused")
    mock_create_session.return_value = mock_session

    with pytest.raises(TransientDataFetchError):
        HttpFetcher().fetch(_http_config())


@patch("rlam_airflow_framework.data_fetchers.http._create_retry_session")
def test_http_503_after_adapter_retries_exhausted_is_transient(mock_create_session):
    # The connection-pool Retry adapter already retries 503; a raised
    # HTTPError for it means that budget is exhausted, but a task-level
    # retry later is still worth it.
    mock_session = MagicMock()
    mock_session.get.return_value = _http_error_response(503)
    mock_create_session.return_value = mock_session

    with pytest.raises(TransientDataFetchError):
        HttpFetcher().fetch(_http_config())


@patch("rlam_airflow_framework.data_fetchers.http._create_retry_session")
def test_http_404_is_deterministic(mock_create_session):
    mock_session = MagicMock()
    mock_session.get.return_value = _http_error_response(404)
    mock_create_session.return_value = mock_session

    with pytest.raises(DataFetchError) as exc_info:
        HttpFetcher().fetch(_http_config())
    assert not isinstance(exc_info.value, TransientDataFetchError)


def _sftp_config(**overrides):
    cfg = {"connection_id": "sftp_default", "remote_path": "/data/file.csv"}
    cfg.update(overrides)
    return cfg


@patch("paramiko.SSHClient.connect")
def test_sftp_authentication_failure_is_deterministic(mock_connect):
    mock_connect.side_effect = paramiko.AuthenticationException("bad credentials")

    with pytest.raises(DataFetchError) as exc_info:
        SftpFetcher().fetch(_sftp_config())
    assert not isinstance(exc_info.value, TransientDataFetchError)


@patch("paramiko.SSHClient.connect")
def test_sftp_ssh_exception_is_transient(mock_connect):
    mock_connect.side_effect = paramiko.SSHException("connection reset")

    with pytest.raises(TransientDataFetchError):
        SftpFetcher().fetch(_sftp_config())


@patch("paramiko.SSHClient.connect")
def test_sftp_socket_timeout_is_transient(mock_connect):
    mock_connect.side_effect = socket.timeout("timed out")

    with pytest.raises(TransientDataFetchError):
        SftpFetcher().fetch(_sftp_config())
