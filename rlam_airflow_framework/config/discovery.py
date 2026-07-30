"""
    Discover all directories containing configuration files.

    Checks:
    1. DAG bundle storage path for GitDagBundle/other bundles
    2. Traditional /opt/airflow/config/data_sources

    Returns:
        List of directory paths containing config files
    """
import os
import structlog
from typing import List

logger = structlog.get_logger(__name__)


def discover_config_directories(config_dir: str) -> List[str]:
    
    directories = []

    # Check bundle storage path first (for GitDagBundle support)
    bundle_storage = os.getenv("AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_STORAGE_PATH")
    if bundle_storage and os.path.exists(bundle_storage):
        # Bundle storage may contain multiple bundles as subdirectories
        try:
            for entry in os.listdir(bundle_storage):
                entry_path = os.path.join(bundle_storage, entry)
                if os.path.isdir(entry_path):
                    # Check if this bundle directory contains YAML configs directly
                    if any(
                        f.endswith((".yaml", ".yml"))
                        for f in os.listdir(entry_path)
                        if os.path.isfile(os.path.join(entry_path, f))
                    ):
                        directories.append(entry_path)
                        logger.info(f"Found bundle config directory: {entry_path}")
                    # Also check for nested data_sources directory
                    nested_data_sources = os.path.join(entry_path, "data_sources")
                    if os.path.exists(nested_data_sources) and os.path.isdir(
                        nested_data_sources
                    ):
                        directories.append(nested_data_sources)
                        logger.info(
                            f"Found nested data_sources in bundle: {nested_data_sources}"
                        )
        except Exception as e:
            logger.warning(f"Error scanning bundle storage {bundle_storage}: {e}")

    # Check traditional data_sources directory
    data_sources_dir = os.path.join(config_dir, "data_sources")
    if os.path.exists(data_sources_dir) and data_sources_dir not in directories:
        directories.append(data_sources_dir)
        logger.info(f"Found traditional data_sources directory: {data_sources_dir}")

    return directories
