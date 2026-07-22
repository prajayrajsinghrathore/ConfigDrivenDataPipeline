"""
RLAM Airflow Framework - Configuration-Driven Data Pipeline

This package provides utilities for building configuration-driven data pipelines
in Apache Airflow 3.x with features like:
- Multi-tenancy support
- DAG Bundles compatibility
- Kafka event publishing with bundle version tracking
- Data quality validation with quarantine handling
- Formula engine for transformations
- HITL (Human-in-the-Loop) approval workflows

Usage:
    from rlam_airflow_framework import ConfigLoader, DAGFactoryV2
"""

__version__ = "1.0.0"
__author__ = "RLAM Data Platform Team"

# Export commonly used classes for convenience
from rlam_airflow_framework.config_loader import ConfigLoader, ConfigLoadError
from rlam_airflow_framework.dag_factory_v2 import DAGFactoryV2
from rlam_airflow_framework.tenant_context import TenantContext, TenantValidationError

__all__ = [
    "ConfigLoader",
    "ConfigLoadError",
    "DAGFactoryV2",
    "TenantContext",
    "TenantValidationError",
    "__version__",
    "__author__",
]
