"""Compute MD5 hash of a file for change detection."""
import yaml
import os
import hashlib
import structlog
from typing import Dict, Any, Optional
from rlam_airflow_framework.config.errors import ConfigLoadError

logger = structlog.get_logger(__name__)


def compute_file_hash(filepath: str) -> str:
    
    try:
        with open(filepath, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()
    except Exception as e:
        logger.warning(f"Failed to compute hash for {filepath}: {e}")
        return ""


def load_yaml_file(filepath: str) -> Optional[Dict[str, Any]]:
    """
    Load a single YAML file with proper error handling.

    Args:
        filepath: Path to the YAML file

    Returns:
        Parsed YAML content, or None if file is empty

    Raises:
        ConfigLoadError: If the file cannot be read or parsed
    """
    if not filepath:
        raise ConfigLoadError("Filepath cannot be None or empty")

    if not os.path.exists(filepath):
        raise ConfigLoadError(f"File not found: {filepath}", config_file=filepath)

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = yaml.safe_load(f)

        # yaml.safe_load returns None for empty files
        if content is None:
            logger.warning(f"YAML file is empty or contains only comments: {filepath}")
            return None

        return content

    except yaml.YAMLError as e:
        logger.error(f"YAML parsing error in {filepath}: {e}")
        raise ConfigLoadError(
            f"Invalid YAML syntax: {str(e)}", config_file=filepath, original_error=e
        ) from e

    except IOError as e:
        logger.error(f"IO error reading {filepath}: {e}")
        raise ConfigLoadError(
            f"Cannot read file: {str(e)}", config_file=filepath, original_error=e
        ) from e
