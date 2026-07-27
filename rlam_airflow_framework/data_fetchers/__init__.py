# File: rlam_airflow_framework/data_fetchers/__init__.py
"""
Data fetching package: pulls data from external sources into DataFrames.

Layout:
- ``base.py``: ``DataFetcher`` interface, ``DataFetchError``, shared timeout
  defaults.
- ``parsers.py``: JSON/CSV/XML parsing shared by every fetcher.
- ``http.py`` / ``sftp.py``: concrete fetchers (Strategy pattern) — secure,
  resilient fetching with timeouts and retry/backoff.
- ``factory.py``: ``get_data_fetcher(source_type)`` resolves a
  ``data_source.type`` string to its fetcher (Factory pattern), so call sites
  never branch on source type themselves.
"""

from rlam_airflow_framework.data_fetchers.base import DataFetcher, DataFetchError
from rlam_airflow_framework.data_fetchers.http import HttpFetcher
from rlam_airflow_framework.data_fetchers.sftp import SftpFetcher, SSHHostKeyPolicy
from rlam_airflow_framework.data_fetchers.factory import (
    FETCHER_REGISTRY,
    get_data_fetcher,
)

__all__ = [
    "DataFetcher",
    "DataFetchError",
    "HttpFetcher",
    "SftpFetcher",
    "SSHHostKeyPolicy",
    "FETCHER_REGISTRY",
    "get_data_fetcher",
]
