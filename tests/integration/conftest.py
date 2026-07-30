# File: tests/integration/conftest.py
"""
Integration test configuration.

Registers the `shared_services` tenant used by the integration fixtures so the
tests are self-contained: they must pass regardless of which tenants the
environment's config/global_settings.yaml declares (host runs fall back to the
code defaults; in-container runs see the real mounted file, which does not
define `shared_services`).
"""

import pytest


@pytest.fixture(autouse=True)
def register_integration_tenant(monkeypatch):
    """Inject the `shared_services` tenant into global settings (see unit conftest)."""
    from rlam_airflow_framework.config import ConfigLoader

    original_load_global = ConfigLoader.load_global_settings

    def load_global_with_test_tenant(self):
        settings = original_load_global(self)
        if "tenants" not in settings:
            settings["tenants"] = {}
        settings["tenants"].setdefault(
            "shared_services",
            {
                "pool": "default_pool",
                "slots": 16,
                "connection_prefix": "shared_",
            },
        )
        return settings

    monkeypatch.setattr(
        ConfigLoader, "load_global_settings", load_global_with_test_tenant
    )
