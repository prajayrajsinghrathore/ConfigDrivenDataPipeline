"""
Unit tests for the transient-error classification hierarchy.

build_transient_retry_policy matches by isinstance(exc, TransientError) - not
by the raw library exception's dotted name, since every fetcher/loader wraps
whatever it catches before it reaches Airflow's retry policy (see
rlam_airflow_framework/utils/retry_policy.py's TransientError docstring).
These tests pin the exception hierarchy itself; the per-source classification
(which raw errors get wrapped as Transient*) is covered in
test_data_fetchers.py and test_destination_loaders.py.
"""

from rlam_airflow_framework.utils.retry_policy import TransientError
from rlam_airflow_framework.data_fetchers.base import DataFetchError, TransientDataFetchError
from rlam_airflow_framework.destinations.primitives import (
    DataLoadError,
    ObjectStorageError,
    TransientDataLoadError,
    TransientObjectStorageError,
    is_transient_object_storage_error,
)


def test_transient_data_fetch_error_is_both():
    err = TransientDataFetchError("msg", source="src")
    assert isinstance(err, DataFetchError)
    assert isinstance(err, TransientError)


def test_plain_data_fetch_error_is_not_transient():
    err = DataFetchError("msg", source="src")
    assert not isinstance(err, TransientError)


def test_transient_data_load_error_is_both():
    err = TransientDataLoadError("msg", destination="dst")
    assert isinstance(err, DataLoadError)
    assert isinstance(err, TransientError)


def test_transient_object_storage_error_is_all_three():
    err = TransientObjectStorageError("msg", destination="dst")
    assert isinstance(err, ObjectStorageError)
    assert isinstance(err, DataLoadError)
    assert isinstance(err, TransientError)


def test_plain_object_storage_error_is_not_transient():
    err = ObjectStorageError("msg", destination="dst")
    assert not isinstance(err, TransientError)


def test_is_transient_object_storage_error_matches_builtin_connection_and_timeout():
    assert is_transient_object_storage_error(ConnectionError("refused"))
    assert is_transient_object_storage_error(TimeoutError("timed out"))


def test_is_transient_object_storage_error_rejects_unrelated_errors():
    assert not is_transient_object_storage_error(ValueError("bad config"))
    assert not is_transient_object_storage_error(RuntimeError("permission denied"))
