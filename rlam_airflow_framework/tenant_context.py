# File: dags/utils/tenant_context.py
"""
Multi-tenancy context management for ConfigDrivenDataPipeline.

Provides:
- Tenant validation (all pipelines MUST belong to a tenant)
- Connection ID resolution with tenant-based mapping
- Kafka topic namespacing with tenant prefixes
- Tenant configuration retrieval for DAG factory

Resolution Priority for Connections:
1. Explicit connection_id in destination config
2. Tenant-based mapping from global_settings
3. Environment variable
4. Hardcoded default
"""

import os
import structlog
from typing import Dict, Any, Optional, List

log = structlog.get_logger(__name__)


class TenantValidationError(Exception):
    """Raised when tenant validation fails."""

    def __init__(self, message: str, config_name: Optional[str] = None):
        self.config_name = config_name
        context = f" in config '{config_name}'" if config_name else ""
        super().__init__(f"Tenant validation error{context}: {message}")


class TenantContext:
    """
    Manages tenant context for multi-tenant pipeline execution.
    
    All pipelines must specify a tenant via metadata.tenant field.
    Tenant configuration is loaded from global_settings.yaml.
    """

    # Default connection IDs (fallback when tenant mapping not found)
    DEFAULT_CONNECTIONS = {
        "snowflake": os.getenv("SNOWFLAKE_CONN_ID", "snowflake-default"),
        "azure_data_lake": os.getenv("AZURE_DATA_LAKE_CONN_ID", "azure_data_lake_default"),
        "azure_blob": os.getenv("AZURE_BLOB_CONN_ID", "azure_blob_default"),
    }

    # Mapping from destination type to connection type
    DEST_TYPE_TO_CONN_TYPE = {
        "snowflake_table": "snowflake",
        "snowflake_stage": "snowflake",
        "azure_data_lake": "azure_data_lake",
        "azure_blob": "azure_blob",
    }

    def __init__(self, global_settings: Dict[str, Any]):
        """
        Initialize TenantContext with global settings.

        Args:
            global_settings: Global settings dict containing tenants configuration
        """
        self.global_settings = global_settings
        self.tenants = global_settings.get("tenants", {})
        
        if not self.tenants:
            log.warning("No tenants defined in global_settings - using empty tenant registry")

    def get_defined_tenants(self) -> List[str]:
        """Get list of all defined tenant IDs."""
        return list(self.tenants.keys())

    def validate_tenant(
        self, 
        config: Dict[str, Any], 
        config_name: Optional[str] = None
    ) -> str:
        """
        Validate that a pipeline config has a valid tenant.

        Args:
            config: Pipeline configuration dictionary
            config_name: Optional config filename for error messages

        Returns:
            The validated tenant ID

        Raises:
            TenantValidationError: If tenant is missing or invalid
        """
        # Check for metadata section
        metadata = config.get("metadata", {})
        if not metadata:
            raise TenantValidationError(
                "Missing 'metadata' section. All pipelines must specify metadata.tenant",
                config_name
            )

        # Check for tenant field
        tenant_id = metadata.get("tenant")
        if not tenant_id:
            raise TenantValidationError(
                "Missing 'metadata.tenant' field. All pipelines must belong to a tenant",
                config_name
            )

        # Validate tenant exists in registry
        if tenant_id not in self.tenants:
            available_tenants = ", ".join(self.get_defined_tenants()) or "none defined"
            raise TenantValidationError(
                f"Unknown tenant '{tenant_id}'. Available tenants: {available_tenants}",
                config_name
            )

        log.debug("Tenant validation passed", tenant=tenant_id, config=config_name)
        return tenant_id

    def get_tenant_config(self, tenant_id: str) -> Dict[str, Any]:
        """
        Get full configuration for a tenant.

        Args:
            tenant_id: The tenant identifier

        Returns:
            Tenant configuration dictionary

        Raises:
            TenantValidationError: If tenant not found
        """
        if tenant_id not in self.tenants:
            raise TenantValidationError(f"Tenant '{tenant_id}' not found in registry")
        
        return self.tenants[tenant_id]

    def get_tenant_owner(self, tenant_id: str) -> str:
        """Get the owner for a tenant (for DAG default_args.owner)."""
        tenant_config = self.get_tenant_config(tenant_id)
        return tenant_config.get("owner", "data-team")

    def get_tenant_pool(self, tenant_id: str) -> Optional[str]:
        """Get the pool for a tenant (for task execution isolation)."""
        tenant_config = self.get_tenant_config(tenant_id)
        return tenant_config.get("pool")

    def get_tenant_tags(self, tenant_id: str) -> List[str]:
        """
        Get tags for a tenant (for DAG tagging and UI filtering).
        
        Always includes 'tenant:{tenant_id}' tag plus any custom tenant tags.
        """
        tenant_config = self.get_tenant_config(tenant_id)
        custom_tags = tenant_config.get("tags", [])
        
        # Always include tenant identifier tag
        tenant_tag = f"tenant:{tenant_id}"
        
        return [tenant_tag] + custom_tags

    def resolve_connection_id(
        self,
        dest_config: Dict[str, Any],
        dest_type: str,
        tenant_id: str,
    ) -> str:
        """
        Resolve connection ID with 4-tier priority.

        Priority:
        1. Explicit connection_id in destination config
        2. Tenant-based mapping from global_settings
        3. Environment variable
        4. Hardcoded default

        Args:
            dest_config: Destination configuration from pipeline YAML
            dest_type: Destination type (snowflake_table, azure_blob, etc.)
            tenant_id: Tenant identifier

        Returns:
            Resolved connection ID
        """
        # Priority 1: Explicit connection_id in config
        if "connection_id" in dest_config:
            conn_id = dest_config["connection_id"]
            log.debug(
                "Using explicit connection_id from config",
                connection_id=conn_id,
                dest_type=dest_type,
                tenant=tenant_id
            )
            return conn_id

        # Priority 2: Tenant-based mapping
        conn_type = self.DEST_TYPE_TO_CONN_TYPE.get(dest_type)
        if conn_type and tenant_id in self.tenants:
            tenant_connections = self.tenants[tenant_id].get("connections", {})
            if conn_type in tenant_connections:
                conn_id = tenant_connections[conn_type]
                log.debug(
                    "Using tenant connection mapping",
                    connection_id=conn_id,
                    conn_type=conn_type,
                    tenant=tenant_id
                )
                return conn_id

        # Priority 3 & 4: Environment variable or hardcoded default
        conn_id = self.DEFAULT_CONNECTIONS.get(conn_type, "default_connection")
        log.debug(
            "Using default connection",
            connection_id=conn_id,
            conn_type=conn_type,
            tenant=tenant_id
        )
        return conn_id

    def resolve_kafka_topic(
        self,
        topic: str,
        tenant_id: str,
    ) -> str:
        """
        Resolve Kafka topic with tenant namespace prefix.

        Format: {tenant_prefix}.{topic}
        Example: team_alpha.market_data

        Args:
            topic: Base topic name from pipeline config
            tenant_id: Tenant identifier

        Returns:
            Namespaced topic name
        """
        if not topic:
            raise ValueError("Kafka topic cannot be None or empty")

        tenant_config = self.get_tenant_config(tenant_id)
        kafka_config = tenant_config.get("kafka", {})
        topic_prefix = kafka_config.get("topic_prefix", tenant_id)

        # Apply namespace prefix
        namespaced_topic = f"{topic_prefix}.{topic}"
        
        log.debug(
            "Resolved Kafka topic with tenant namespace",
            original_topic=topic,
            namespaced_topic=namespaced_topic,
            tenant=tenant_id
        )
        
        return namespaced_topic

    def get_dag_id(self, source_name: str, tenant_id: str) -> str:
        """
        Generate tenant-prefixed DAG ID.

        Format: {tenant_id}_{source_name}
        Example: team_alpha_market_data

        Args:
            source_name: Data source name from pipeline config
            tenant_id: Tenant identifier

        Returns:
            Tenant-prefixed DAG ID
        """
        return f"{tenant_id}_{source_name}"


def create_tenant_context(global_settings: Dict[str, Any]) -> TenantContext:
    """
    Factory function to create TenantContext instance.

    Args:
        global_settings: Global settings dictionary

    Returns:
        Configured TenantContext instance
    """
    return TenantContext(global_settings)
