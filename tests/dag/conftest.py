# File: tests/dag/conftest.py
"""
DAG-lane guard: this lane needs REAL Airflow.

The unit lane (tests/unit/conftest.py) installs MagicMock stand-ins for
airflow.* in sys.modules at import time. If the dag lane runs in the same
pytest process (e.g. `pytest tests/unit tests/dag`), those mocks are still
active and DagBag/factory tests would exercise mocks instead of Airflow.

Rather than the old fragile sys.modules save/restore dance (which corrupted
per-class registries like PyYAML's constructor table), we skip loudly and tell
the operator to run the lane separately:

    pytest tests/unit tests/contract -q   # mocked lane
    pytest tests/dag -q                   # real-Airflow lane (own process)
"""

import sys
from unittest.mock import MagicMock

import pytest

# Install the Windows os.register_at_fork shim BEFORE any airflow import in
# this lane (DagBag fixtures import airflow.models directly): the shim lives in
# rlam_airflow_framework.__init__ and is inert on Linux/macOS. Without it, the
# first `from airflow.models import DagBag` on Windows dies in
# airflow.sdk._shared observability stats and the fixtures mis-report
# "Airflow not installed".
import rlam_airflow_framework  # noqa: E402,F401  (side effect import)


def pytest_collection_modifyitems(config, items):
    if isinstance(sys.modules.get("airflow"), MagicMock):
        skip = pytest.mark.skip(
            reason=(
                "unit-lane Airflow mocks are active in this process — "
                "run `pytest tests/dag` in its own invocation"
            )
        )
        for item in items:
            if "tests/dag" in str(item.fspath).replace("\\", "/"):
                item.add_marker(skip)
