# File: dags/utils/config_loader.py
"""
Configuration loader that validates and processes YAML configs.

Provides:
- YAML configuration loading with validation
- Schema-based validation support
- Proper null/empty handling for yaml.safe_load
- Comprehensive logging for troubleshooting
- File change tracking for hot-reload support
"""

import yaml
import os
import hashlib
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Import tenant context for multi-tenancy support
try:
    from utils.tenant_context import TenantContext, TenantValidationError
except ImportError:
    # Fallback for testing or when running outside DAGs context
    TenantContext = None
    TenantValidationError = Exception


class ConfigLoadError(Exception):
    """Custom exception for configuration loading errors."""

    def __init__(
        self,
        message: str,
        config_file: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        self.config_file = config_file
        self.original_error = original_error
        file_context = f" in {config_file}" if config_file else ""
        super().__init__(f"Configuration error{file_context}: {message}")


class ConfigLoader:
    """
    Loads and validates data source configurations using external schema files.

    Features:
    - Proper handling of None results from yaml.safe_load
    - File hash tracking for change detection
    - Comprehensive validation with clear error messages
    """

    def __init__(self, config_dir: str = "/opt/airflow/config"):
        """
        Initialize the ConfigLoader.

        Args:
            config_dir: Base directory for configuration files

        Raises:
            ValueError: If config_dir is None or empty
        """
        if not config_dir:
            raise ValueError("config_dir cannot be None or empty")

        self.config_dir = config_dir
        self.schemas_dir = os.path.join(config_dir, "schemas")

        # Track file hashes for change detection
        self._file_hashes: Dict[str, str] = {}
        self._last_load_time: Optional[datetime] = None

        # Load validation config and schemas
        self.validation_config = self._load_validation_config()
        self.schemas = self._load_schema_files()

        # Initialize tenant context for multi-tenancy support
        self._tenant_context: Optional[TenantContext] = None
        self._global_settings: Optional[Dict[str, Any]] = None

        logger.info(f"ConfigLoader initialized with config_dir: {config_dir}")

    def _compute_file_hash(self, filepath: str) -> str:
        """Compute MD5 hash of a file for change detection."""
        try:
            with open(filepath, "rb") as f:
                return hashlib.md5(f.read()).hexdigest()
        except Exception as e:
            logger.warning(f"Failed to compute hash for {filepath}: {e}")
            return ""

    def _load_validation_config(self) -> Dict[str, Any]:
        """
        Load validation configuration.

        Returns:
            Dict with validation settings, or defaults if file not found
        """
        validation_config_path = os.path.join(self.config_dir, "validation_config.yaml")

        if os.path.exists(validation_config_path):
            try:
                config = self._load_yaml_file(validation_config_path)
                if config:  # Check for None from yaml.safe_load
                    logger.info("Loaded validation configuration from file")
                    return config
                else:
                    logger.warning(
                        f"Validation config file is empty: {validation_config_path}"
                    )
            except ConfigLoadError as e:
                logger.warning(f"Error loading validation config, using defaults: {e}")
            except Exception as e:
                logger.warning(f"Unexpected error loading validation config: {e}")

        # Default validation config
        logger.info("Using default validation configuration")
        return {
            "validation": {
                "enabled": True,
                "on_failure": "warn",
                "use_schema_files": True,
                "allow_unknown_fields": True,
                "log_validation_details": True,
            }
        }

    def _load_schema_files(self) -> Dict[str, Any]:
        """
        Load schema files from the schemas directory.

        Returns:
            Dict mapping schema names to their contents
        """
        schemas: Dict[str, Any] = {}

        if not os.path.exists(self.schemas_dir):
            logger.warning(f"Schemas directory not found: {self.schemas_dir}")
            return schemas

        schema_files = {
            "data_source": "data_source_schema.yaml",
            "transformation": "transformation_schema.yaml",
            "enrichment": "enrichment_schema.yaml",
        }

        for schema_name, filename in schema_files.items():
            filepath = os.path.join(self.schemas_dir, filename)
            if os.path.exists(filepath):
                try:
                    schema_content = self._load_yaml_file(filepath)
                    if schema_content:  # Check for None from yaml.safe_load
                        schemas[schema_name] = schema_content
                        logger.info(f"Loaded {schema_name} schema from {filename}")
                    else:
                        logger.warning(f"Schema file is empty: {filename}")
                except ConfigLoadError as e:
                    logger.warning(f"Error loading schema {filename}: {e}")
                except Exception as e:
                    logger.warning(f"Unexpected error loading schema {filename}: {e}")
            else:
                logger.debug(f"Schema file not found (optional): {filename}")

        return schemas

    def _load_yaml_file(self, filepath: str) -> Optional[Dict[str, Any]]:
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
                logger.warning(
                    f"YAML file is empty or contains only comments: {filepath}"
                )
                return None

            # Store hash for change detection
            self._file_hashes[filepath] = self._compute_file_hash(filepath)

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

    def load_data_source_configs(self) -> List[Dict[str, Any]]:
        """
        Load all data source configurations from YAML files.
        
        Supports DAG Bundles by searching multiple locations:
        1. Bundle storage path (AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_STORAGE_PATH)
        2. Traditional config_dir/data_sources

        Returns:
            List of valid configuration dictionaries
        """
        configs: List[Dict[str, Any]] = []
        
        # Discover config directories from bundle storage and traditional paths
        config_directories = self._discover_config_directories()
        
        if not config_directories:
            logger.warning("No configuration directories found")
            return configs

        # Load configs from all discovered directories
        for config_dir_path in config_directories:
            logger.info(f"Scanning for configs in: {config_dir_path}")
            configs.extend(self._load_configs_from_directory(config_dir_path))

        logger.info(f"Total configs loaded: {len(configs)}")
        return configs

    def _discover_config_directories(self) -> List[str]:
        """
        Discover all directories containing configuration files.
        
        Checks:
        1. DAG bundle storage path for GitDagBundle/other bundles
        2. Traditional /opt/airflow/config/data_sources
        
        Returns:
            List of directory paths containing config files
        """
        import os
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
                        if any(f.endswith(('.yaml', '.yml')) for f in os.listdir(entry_path) if os.path.isfile(os.path.join(entry_path, f))):
                            directories.append(entry_path)
                            logger.info(f"Found bundle config directory: {entry_path}")
                        # Also check for nested data_sources directory
                        nested_data_sources = os.path.join(entry_path, "data_sources")
                        if os.path.exists(nested_data_sources) and os.path.isdir(nested_data_sources):
                            directories.append(nested_data_sources)
                            logger.info(f"Found nested data_sources in bundle: {nested_data_sources}")
            except Exception as e:
                logger.warning(f"Error scanning bundle storage {bundle_storage}: {e}")
        
        # Check traditional data_sources directory
        data_sources_dir = os.path.join(self.config_dir, "data_sources")
        if os.path.exists(data_sources_dir) and data_sources_dir not in directories:
            directories.append(data_sources_dir)
            logger.info(f"Found traditional data_sources directory: {data_sources_dir}")
        
        return directories

    def _load_configs_from_directory(self, directory: str) -> List[Dict[str, Any]]:
        """
        Load configuration files from a specific directory.
        
        Args:
            directory: Path to directory containing YAML config files
            
        Returns:
            List of valid configuration dictionaries
        """
        configs: List[Dict[str, Any]] = []

        if not os.path.exists(directory):
            logger.warning(f"Config directory not found: {directory}")
            return configs

        # Get list of config files
        try:
            config_files = [
                f for f in os.listdir(directory) if f.endswith((".yaml", ".yml"))
            ]
        except OSError as e:
            logger.error(f"Cannot list directory {directory}: {e}")
            return configs

        if not config_files:
            logger.debug(f"No YAML files found in {directory}")
            return configs

        logger.info(f"Found {len(config_files)} config files in {directory}")

        logger.info(f"Found {len(config_files)} config files in {directory}")

        for filename in config_files:
            filepath = os.path.join(directory, filename)
            try:
                config = self._load_yaml_file(filepath)

                # Skip empty files
                if config is None:
                    logger.warning(f"Skipping empty config file: {filename}")
                    continue

                # Validate configuration
                if self._validate_config(config, filename):
                    configs.append(config)
                    logger.info(f"Successfully loaded config: {filename}")
                else:
                    logger.warning(f"Validation failed for config: {filename}")

            except ConfigLoadError as e:
                logger.error(f"Error loading config {filename}: {e}")
                # Continue loading other configs instead of stopping
                continue
            except Exception as e:
                logger.error(
                    f"Unexpected error loading config {filename}: {e}", exc_info=True
                )
                continue

        self._last_load_time = datetime.now()
        logger.info(f"Loaded {len(configs)} valid configurations")

        return configs

    def load_global_settings(self) -> Dict[str, Any]:
        """
        Load global settings configuration.

        Returns:
            Dict with global settings
        """
        global_config_path = os.path.join(self.config_dir, "global_settings.yaml")

        if os.path.exists(global_config_path):
            try:
                config = self._load_yaml_file(global_config_path)
                if config:
                    logger.info("Loaded global settings from file")
                    self._global_settings = config
                    self._init_tenant_context(config)
                    return config
            except ConfigLoadError as e:
                logger.warning(f"Error loading global settings, using defaults: {e}")

        # Return defaults if no global config or empty file
        logger.info("Using default global settings")
        default_settings = {
            "default_settings": {
                "retry_count": 3,
                "timeout": 30,
                "email_on_failure": True,
                "email_on_retry": False,
            },
            "tenants": {
                "shared_services": {
                    "description": "Default shared tenant",
                    "owner": "data-team",
                    "pool": "default_pool",
                    "connections": {},
                    "kafka": {"topic_prefix": "shared"},
                    "tags": ["shared"]
                }
            }
        }
        self._global_settings = default_settings
        self._init_tenant_context(default_settings)
        return default_settings

    def _init_tenant_context(self, global_settings: Dict[str, Any]) -> None:
        """
        Initialize tenant context from global settings.

        Args:
            global_settings: Global settings containing tenant definitions
        """
        if TenantContext is not None:
            self._tenant_context = TenantContext(global_settings)
            tenant_count = len(self._tenant_context.get_defined_tenants())
            logger.info(f"Initialized TenantContext with {tenant_count} tenants")
        else:
            logger.warning("TenantContext not available - multi-tenancy disabled")

    def get_tenant_context(self) -> Optional[TenantContext]:
        """
        Get the tenant context for multi-tenancy operations.

        Returns:
            TenantContext instance or None if not initialized
        """
        if self._tenant_context is None and self._global_settings is None:
            # Lazy load global settings to initialize tenant context
            self.load_global_settings()
        return self._tenant_context

    def _validate_config(self, config: Dict[str, Any], filename: str) -> bool:
        """
        Validate configuration against schema including mandatory tenant validation.

        Args:
            config: The configuration dictionary to validate
            filename: Filename for error messages

        Returns:
            True if valid, False otherwise
        """
        if not config:
            logger.error(f"Empty configuration in {filename}")
            return False

        if not isinstance(config, dict):
            logger.error(
                f"Configuration must be a dictionary in {filename}, got {type(config).__name__}"
            )
            return False

        # =========================================================================
        # MULTI-TENANCY VALIDATION (MANDATORY)
        # =========================================================================
        # All pipelines MUST specify metadata.tenant
        if not self._validate_tenant(config, filename):
            return False

        # Check for required top-level keys
        if "data_source" not in config:
            logger.error(f"Missing 'data_source' in {filename}")
            return False

        data_source = config["data_source"]
        if not isinstance(data_source, dict):
            logger.error(f"'data_source' must be a dictionary in {filename}")
            return False

        if "name" not in data_source or not data_source["name"]:
            logger.error(f"Missing or empty 'name' in data_source config in {filename}")
            return False

        if "destination" not in config:
            logger.error(f"Missing 'destination' in {filename}")
            return False

        if not config["destination"]:
            logger.error(f"'destination' cannot be empty in {filename}")
            return False

        logger.debug(f"Validation passed for {filename}")
        return True

    def _validate_tenant(self, config: Dict[str, Any], filename: str) -> bool:
        """
        Validate that a pipeline config has a valid tenant.

        All pipelines MUST belong to a tenant - there are no tenant-less pipelines.

        Args:
            config: The configuration dictionary to validate
            filename: Filename for error messages

        Returns:
            True if tenant is valid, False otherwise
        """
        # Check for metadata section
        metadata = config.get("metadata")
        if not metadata:
            logger.error(
                f"Missing 'metadata' section in {filename}. "
                f"All pipelines must specify metadata.tenant for multi-tenancy."
            )
            return False

        if not isinstance(metadata, dict):
            logger.error(f"'metadata' must be a dictionary in {filename}")
            return False

        # Check for tenant field
        tenant_id = metadata.get("tenant")
        if not tenant_id:
            logger.error(
                f"Missing 'metadata.tenant' in {filename}. "
                f"All pipelines must belong to a tenant."
            )
            return False

        # Validate tenant exists in registry
        if self._tenant_context is not None:
            try:
                self._tenant_context.validate_tenant(config, filename)
            except TenantValidationError as e:
                logger.error(str(e))
                return False
        else:
            # If TenantContext not available, log warning but allow
            logger.warning(
                f"TenantContext not initialized - skipping tenant registry validation for {filename}"
            )

        logger.debug(f"Tenant validation passed for {filename}: tenant={tenant_id}")
        return True

    def has_configs_changed(self) -> bool:
        """
        Check if any configuration files have changed since last load.

        Returns:
            True if any config file has changed
        """
        if not self._file_hashes:
            return True  # No files loaded yet

        for filepath, old_hash in self._file_hashes.items():
            if os.path.exists(filepath):
                current_hash = self._compute_file_hash(filepath)
                if current_hash != old_hash:
                    logger.info(f"Config file changed: {filepath}")
                    return True
            else:
                # File was deleted
                logger.info(f"Config file deleted: {filepath}")
                return True

        # Check for new files in data_sources directory
        data_sources_dir = os.path.join(self.config_dir, "data_sources")
        if os.path.exists(data_sources_dir):
            current_files = set(
                os.path.join(data_sources_dir, f)
                for f in os.listdir(data_sources_dir)
                if f.endswith((".yaml", ".yml"))
            )
            tracked_files = set(
                fp for fp in self._file_hashes.keys() if fp.startswith(data_sources_dir)
            )

            if current_files != tracked_files:
                logger.info("New config files detected")
                return True

        return False

    def get_last_load_time(self) -> Optional[datetime]:
        """Get the timestamp of the last successful config load."""
        return self._last_load_time

    def reload_if_changed(self) -> tuple[bool, List[Dict[str, Any]]]:
        """
        Reload configurations if any have changed.

        This method supports hot-reload functionality by checking
        for file changes and reloading only when necessary.

        Returns:
            Tuple of (changed: bool, configs: List[Dict])
        """
        if self.has_configs_changed():
            logger.info("Configuration changes detected, reloading...")

            # Clear cached hashes to force full reload
            self._file_hashes.clear()

            # Reload schemas in case they changed
            self.schemas = self._load_schema_files()

            # Reload all data source configs
            configs = self.load_data_source_configs()

            logger.info(f"Reloaded {len(configs)} configurations")
            return True, configs

        return False, []

    def get_file_hashes(self) -> Dict[str, str]:
        """
        Get current file hashes for external tracking.

        Returns:
            Dict mapping filepath to MD5 hash
        """
        return dict(self._file_hashes)

    def get_tracked_files(self) -> List[str]:
        """
        Get list of tracked configuration files.

        Returns:
            List of file paths being tracked
        """
        return list(self._file_hashes.keys())
