# File: rlam_airflow_framework/data_fetchers/factory.py
"""
Factory for resolving a ``data_source.type`` string to a DataFetcher instance.

Adding a new source = add a fetcher class + register it; call sites
(e.g. ``taskflow_tasks.ingest_data``) never branch on source type themselves.
"""

from rlam_airflow_framework.data_fetchers.base import DataFetcher, FETCHER_REGISTRY


def get_data_fetcher(source_type: str) -> DataFetcher:
    """
    Resolve a ``data_source.type`` to its DataFetcher instance.

    Raises:
        ValueError: If no fetcher is registered for ``source_type``.
    """
    fetcher_cls = FETCHER_REGISTRY.get(source_type)
    if fetcher_cls is None:
        raise ValueError(f"Unsupported source type: {source_type}")
    return fetcher_cls()
