import os
from datetime import datetime
from typing import Dict, Any, List, Optional, TYPE_CHECKING
import structlog

from rlam_airflow_framework.config.errors import ConfigLoadError
from rlam_airflow_framework.config.discovery import discover_config_directories
from rlam_airflow_framework.config.reader import load_yaml_file, compute_file_hash
from rlam_airflow_framework.config.validator import ConfigValidator

if TYPE_CHECKING:
    from rlam_airflow_framework.tenant_context import TenantContext
else:
    try:
        from rlam_airflow_framework.tenant_context import TenantContext
    except ImportError:
        TenantContext = None

logger = structlog.get_logger(__name__)


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
        self._tenant_context: Optional["TenantContext"] = None
        self._global_settings: Optional[Dict[str, Any]] = None
        self._validator = ConfigValidator()

        logger.info(f"ConfigLoader initialized with config_dir: {config_dir}")

    def _load_validation_config(self) -> Dict[str, Any]:
        """Load validation configuration."""
        validation_config_path = os.path.join(self.config_dir, "validation_config.yaml")

        if os.path.exists(validation_config_path):
            try:
                config = load_yaml_file(validation_config_path)
                if config:
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
        """Load schema files from the schemas directory."""
        schemas: Dict[str, Any] = {}

        if not os.path.exists(self.schemas_dir):
            logger.warning(f"Schemas directory not found: {self.schemas_dir}")
            return schemas

        schema_files = {
            "data_source": "data_source_schema.yaml",
            "transformation": "transformation_schema.yaml",
        }

        for schema_name, filename in schema_files.items():
            filepath = os.path.join(self.schemas_dir, filename)
            if os.path.exists(filepath):
                try:
                    schema_content = load_yaml_file(filepath)
                    if schema_content:
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

    def load_data_source_configs(self) -> List[Dict[str, Any]]:
        """
        Load all data source configurations from YAML files.
        """
        configs: List[Dict[str, Any]] = []

        config_directories = discover_config_directories(self.config_dir)

        if not config_directories:
            logger.warning("No configuration directories found")
            return configs

        for config_dir_path in config_directories:
            logger.info(f"Scanning for configs in: {config_dir_path}")
            configs.extend(self._load_configs_from_directory(config_dir_path))

        logger.info(f"Total configs loaded: {len(configs)}")
        return configs

    def _load_configs_from_directory(self, directory: str) -> List[Dict[str, Any]]:
        """Load configuration files from a specific directory."""
        configs: List[Dict[str, Any]] = []

        if not os.path.exists(directory):
            logger.warning(f"Config directory not found: {directory}")
            return configs

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

        for filename in config_files:
            filepath = os.path.join(directory, filename)
            try:
                config = load_yaml_file(filepath)

                if config is None:
                    logger.warning(f"Skipping empty config file: {filename}")
                    continue

                # Track hash
                self._file_hashes[filepath] = compute_file_hash(filepath)

                if self._validator.validate_config(config, filename):
                    configs.append(config)
                    logger.info(f"Successfully loaded config: {filename}")
                else:
                    logger.warning(f"Validation failed for config: {filename}")

            except ConfigLoadError as e:
                logger.error(f"Error loading config {filename}: {e}")
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
        """Load global settings configuration."""
        global_config_path = os.path.join(self.config_dir, "global_settings.yaml")

        if os.path.exists(global_config_path):
            try:
                config = load_yaml_file(global_config_path)
                if config:
                    logger.info("Loaded global settings from file")
                    self._global_settings = config
                    self._init_tenant_context(config)
                    return config
            except ConfigLoadError as e:
                logger.warning(f"Error loading global settings, using defaults: {e}")

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
                    "tags": ["shared"],
                }
            },
        }
        self._global_settings = default_settings
        self._init_tenant_context(default_settings)
        return default_settings

    def _init_tenant_context(self, global_settings: Dict[str, Any]) -> None:
        """Initialize tenant context from global settings."""
        if TenantContext is not None:
            self._tenant_context = TenantContext(global_settings)
            self._validator = ConfigValidator(tenant_context=self._tenant_context)
            tenant_count = len(self._tenant_context.get_defined_tenants())
            logger.info(f"Initialized TenantContext with {tenant_count} tenants")
        else:
            logger.warning("TenantContext not available - multi-tenancy disabled")

    def get_tenant_context(self) -> Optional["TenantContext"]:
        """Get the tenant context for multi-tenancy operations."""
        if self._tenant_context is None and self._global_settings is None:
            self.load_global_settings()
        return self._tenant_context

    def has_configs_changed(self) -> bool:
        """Check if any configuration files have changed since last load."""
        if not self._file_hashes:
            return True

        for filepath, old_hash in self._file_hashes.items():
            if os.path.exists(filepath):
                current_hash = compute_file_hash(filepath)
                if current_hash != old_hash:
                    logger.info(f"Config file changed: {filepath}")
                    return True
            else:
                logger.info(f"Config file deleted: {filepath}")
                return True

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

    def reload_if_changed(self) -> tuple[bool, List[Dict[str, Any]]]:
        """Reload configurations if any have changed."""
        if self.has_configs_changed():
            logger.info("Configuration changes detected, reloading...")
            self._file_hashes.clear()
            self.schemas = self._load_schema_files()
            configs = self.load_data_source_configs()
            logger.info(f"Reloaded {len(configs)} configurations")
            return True, configs

        return False, []

    def get_file_hashes(self) -> Dict[str, str]:
        """Get current file hashes for external tracking."""
        return dict(self._file_hashes)

    def get_tracked_files(self) -> List[str]:
        """Get list of tracked configuration files."""
        return list(self._file_hashes.keys())
