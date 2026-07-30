"""Validates configuration dictionaries against business rules."""
import structlog
from typing import Dict, Any, Optional, TYPE_CHECKING

logger = structlog.get_logger(__name__)

if TYPE_CHECKING:
    from rlam_airflow_framework.tenant_context import (
        TenantContext,
        TenantValidationError,
    )
else:
    try:
        from rlam_airflow_framework.tenant_context import (
            TenantContext,
            TenantValidationError,
        )
    except ImportError:
        TenantContext = None
        TenantValidationError = Exception


class ConfigValidator:
    

    def __init__(self, tenant_context: Optional["TenantContext"] = None):
        self.tenant_context = tenant_context

    def validate_config(self, config: Dict[str, Any], filename: str) -> bool:
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
        if not self.validate_tenant(config, filename):
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

    def validate_tenant(self, config: Dict[str, Any], filename: str) -> bool:
        """
        Validate that a pipeline config has a valid tenant.

        Args:
            config: The configuration dictionary to validate
            filename: Filename for error messages

        Returns:
            True if tenant is valid, False otherwise
        """
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

        tenant_id = metadata.get("tenant")
        if not tenant_id:
            logger.error(
                f"Missing 'metadata.tenant' in {filename}. "
                f"All pipelines must belong to a tenant."
            )
            return False

        if self.tenant_context is not None:
            try:
                self.tenant_context.validate_tenant(config, filename)
            except TenantValidationError as e:
                logger.error(str(e))
                return False
        else:
            logger.warning(
                f"TenantContext not initialized - skipping tenant registry validation for {filename}"
            )

        logger.debug(f"Tenant validation passed for {filename}: tenant={tenant_id}")
        return True
