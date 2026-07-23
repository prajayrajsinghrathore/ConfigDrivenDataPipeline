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

# Windows compatibility patch (inert on Linux/macOS): Airflow 3.3.0 calls
# os.register_at_fork unconditionally at import (airflow.sdk _shared stats), which
# does not exist on Windows. Stub it for local development.
import os

if not hasattr(os, "register_at_fork"):
    # Pre-import concurrent.futures.thread BEFORE stubbing: its module body takes a
    # POSIX-only branch when os.register_at_fork exists and crashes on Windows
    # (`_thread.lock` has no `_at_fork_reinit`). Importing it first caches the module
    # so later `from concurrent.futures import ThreadPoolExecutor` keeps working.
    import concurrent.futures.thread  # noqa: F401

    os.register_at_fork = lambda *args, **kwargs: None

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
