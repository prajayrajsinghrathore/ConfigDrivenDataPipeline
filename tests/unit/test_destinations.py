# File: tests/unit/test_destinations.py
"""
Unit tests for the destination loader strategy registry.

These lock in the Open/Closed dispatch: loaders are resolved polymorphically by
config ``type``, unknown types fail clearly, and a brand-new sink can be added
by registering a subclass without editing any existing code.
"""

import pandas as pd
import pytest

from rlam_airflow_framework.destinations import (
    DestinationLoader,
    DestinationRegistry,
    LoadContext,
    PrintLogsLoader,
    SnowflakeTableLoader,
)


def _ctx(**kwargs):
    defaults = dict(topic="t", correlation_id="cid", dest_label="primary")
    defaults.update(kwargs)
    return LoadContext(**defaults)


def test_registry_resolves_registered_type_polymorphically():
    registry = DestinationRegistry()
    loader = PrintLogsLoader()
    registry.register(loader)

    resolved = registry.get("print_logs")
    assert resolved is loader
    assert isinstance(resolved, DestinationLoader)


def test_registry_unknown_type_raises_with_supported_list():
    registry = DestinationRegistry()
    registry.register(SnowflakeTableLoader())

    with pytest.raises(ValueError, match="Unsupported destination type: mystery"):
        registry.get("mystery")


def test_register_overrides_same_type():
    registry = DestinationRegistry()
    first, second = PrintLogsLoader(), PrintLogsLoader()
    registry.register(first)
    registry.register(second)
    assert registry.get("print_logs") is second


def test_print_logs_loader_returns_summary():
    df = pd.DataFrame({"a": [1, 2, 3]})
    result = PrintLogsLoader().load(df, {"type": "print_logs"}, _ctx())
    assert result == "Printed 3 rows to logs"


def test_custom_loader_added_without_touching_existing_code():
    """OCP: a new sink is a new subclass implementing _write + one register() call."""

    class EchoLoader(DestinationLoader):
        dest_type = "echo"

        def _write(self, df, dest_config, ctx):
            return f"echo:{dest_config.get('message', '')}:{len(df)}"

    registry = DestinationRegistry()
    registry.register(EchoLoader())

    df = pd.DataFrame({"x": [1, 2]})
    loader = registry.get("echo")
    assert loader.load(df, {"type": "echo", "message": "hi"}, _ctx()) == "echo:hi:2"
    assert "echo" in registry.supported_types()


def test_abstract_base_cannot_be_instantiated():
    with pytest.raises(TypeError):
        DestinationLoader()  # type: ignore[abstract]
