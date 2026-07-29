# File: tests/unit/conftest.py
"""
Unit test specific configuration.

This conftest.py applies only to unit tests and provides Airflow mocking
to allow testing without requiring a full Airflow installation.

Mocks provided:
- airflow.sdk (decorators, asset, deadline, serde for XCom)
- airflow.decorators, airflow.operators
- airflow.providers.snowflake, airflow.providers.standard (for ApprovalOperator)
- airflow.callbacks, airflow.task, airflow.utils (for module loading)
"""

import sys
from typing import Any, cast
from unittest.mock import MagicMock

# (plugins/ folder removed 2026-07-23 — deadline_callbacks now lives in the framework package)

# =============================================================================
# AIRFLOW MOCKING - Only for unit tests
# =============================================================================

# Create comprehensive Airflow mock
airflow_mock = MagicMock()
airflow_mock.sdk = MagicMock()

def mock_dag_decorator(*args, **kwargs):
    def decorator(func):
        mock_dag = MagicMock()
        mock_dag.dag_id = kwargs.get("dag_id")
        mock_dag.default_args = kwargs.get("default_args", {})
        mock_dag.partial_kwargs = kwargs
        
        # Return mock_dag when decorator wrapper is called
        return lambda *a, **kw: mock_dag
    return decorator

airflow_mock.sdk.dag = mock_dag_decorator
airflow_mock.sdk.DAG = MagicMock()

class _MockTaskDecorator:
    """Mock ``@task`` decorator that returns the function unchanged.

    Supports ``@task``, ``@task(...)``, ``@task.branch``, ``@task.sensor``,
    ``@task.python``, and any future sub-decorator via ``__getattr__``.
    """

    def __call__(self, func=None, **kwargs):
        if func is None:
            # Called with arguments: @task(retries=3) or @task.branch(do_xcom_push=False)
            def wrapper(f):
                return f
            return wrapper
        # Called without arguments: @task
        return func

    def __getattr__(self, _name: str) -> "_MockTaskDecorator":
        # @task.branch, @task.sensor, @task.python all behave identically
        return self

mock_task_decorator = _MockTaskDecorator()

airflow_mock.sdk.task = mock_task_decorator
airflow_mock.sdk.get_current_context = MagicMock()
airflow_mock.sdk.PokeReturnValue = MagicMock()
airflow_mock.sdk.ObjectStoragePath = MagicMock()
airflow_mock.sdk.CronPartitionTimetable = MagicMock()
airflow_mock.sdk.PartitionedAssetTimetable = MagicMock()
airflow_mock.sdk.PartitionedAtRuntime = MagicMock()
airflow_mock.sdk.RollupMapper = MagicMock()
airflow_mock.sdk.FanOutMapper = MagicMock()
airflow_mock.sdk.FixedKeyMapper = MagicMock()
airflow_mock.sdk.IdentityMapper = MagicMock()
airflow_mock.sdk.StartOfDayMapper = MagicMock()
airflow_mock.sdk.StartOfWeekMapper = MagicMock()
airflow_mock.sdk.StartOfMonthMapper = MagicMock()
airflow_mock.sdk.StartOfQuarterMapper = MagicMock()
airflow_mock.sdk.StartOfYearMapper = MagicMock()
airflow_mock.sdk.DayWindow = MagicMock()
airflow_mock.sdk.WeekWindow = MagicMock()
airflow_mock.sdk.MonthWindow = MagicMock()
airflow_mock.sdk.QuarterWindow = MagicMock()
airflow_mock.sdk.YearWindow = MagicMock()
airflow_mock.sdk.WaitForAll = MagicMock()
airflow_mock.sdk.MinimumCount = MagicMock()
airflow_mock.sdk.definitions = MagicMock()
airflow_mock.sdk.definitions.asset = MagicMock()
airflow_mock.sdk.definitions.asset.Asset = MagicMock()
airflow_mock.sdk.definitions.deadline = MagicMock()
airflow_mock.sdk.definitions.deadline.AsyncCallback = MagicMock()
airflow_mock.sdk.definitions.deadline.SyncCallback = MagicMock()
airflow_mock.sdk.definitions.deadline.DeadlineAlert = MagicMock()
airflow_mock.sdk.definitions.deadline.DeadlineReference = MagicMock()
airflow_mock.sdk.SyncCallback = MagicMock()
airflow_mock.sdk.AsyncCallback = MagicMock()
airflow_mock.sdk.definitions.retry_policy = MagicMock()
airflow_mock.sdk.execution_time = MagicMock()
airflow_mock.sdk.execution_time.context = MagicMock()
airflow_mock.sdk.execution_time.context.NEVER_EXPIRE = "never_expire"
airflow_mock.decorators = MagicMock()
airflow_mock.operators = MagicMock()
airflow_mock.operators.python = MagicMock()
airflow_mock.providers = MagicMock()
airflow_mock.providers.snowflake = MagicMock()
airflow_mock.providers.snowflake.hooks = MagicMock()
airflow_mock.providers.snowflake.hooks.snowflake = MagicMock()
airflow_mock.providers.snowflake.hooks.snowflake.SnowflakeHook = MagicMock()
airflow_mock.providers.standard = MagicMock()
airflow_mock.providers.standard.operators = MagicMock()
airflow_mock.providers.standard.operators.hitl = MagicMock()
airflow_mock.callbacks = MagicMock()
airflow_mock.callbacks.callback_requests = MagicMock()
airflow_mock.sdk.serde = MagicMock()
airflow_mock.sdk.serde.U = MagicMock()
airflow_mock.sdk.serde.register = MagicMock()
airflow_mock.task = MagicMock()
airflow_mock.task.priority_strategy = MagicMock()
airflow_mock.utils = MagicMock()
airflow_mock.utils.module_loading = MagicMock()
airflow_mock.configuration = MagicMock()
airflow_mock.configuration.conf = MagicMock()
airflow_mock.utils.email = MagicMock()
airflow_mock.utils.email.send_email = MagicMock()

# Mock qualname function to return the fully qualified class name
def mock_qualname(obj):
    """Mock implementation of airflow.utils.module_loading.qualname."""
    # Get the class of the object if it's an instance
    if not isinstance(obj, type):
        obj = obj.__class__
    
    if hasattr(obj, '__module__') and hasattr(obj, '__name__'):
        return f"{obj.__module__}.{obj.__name__}"
    return str(obj)

airflow_mock.utils.module_loading.qualname = mock_qualname

# Register all mocks in sys.modules
sys.modules["airflow"] = airflow_mock
sys.modules["airflow.sdk"] = airflow_mock.sdk
sys.modules["airflow.sdk.dag"] = cast(Any, airflow_mock.sdk.dag)
sys.modules["airflow.sdk.definitions"] = airflow_mock.sdk.definitions
sys.modules["airflow.sdk.definitions.asset"] = airflow_mock.sdk.definitions.asset
sys.modules["airflow.sdk.definitions.deadline"] = airflow_mock.sdk.definitions.deadline
sys.modules["airflow.sdk.definitions.callback"] = airflow_mock.sdk.definitions.callback
sys.modules["airflow.sdk.definitions.context"] = airflow_mock.sdk.definitions.context
sys.modules["airflow.sdk.definitions.retry_policy"] = airflow_mock.sdk.definitions.retry_policy
sys.modules["airflow.decorators"] = airflow_mock.decorators
sys.modules["airflow.operators"] = airflow_mock.operators
sys.modules["airflow.operators.python"] = airflow_mock.operators.python
sys.modules["airflow.providers"] = airflow_mock.providers
sys.modules["airflow.providers.snowflake"] = airflow_mock.providers.snowflake
sys.modules["airflow.providers.snowflake.hooks"] = airflow_mock.providers.snowflake.hooks
sys.modules["airflow.providers.snowflake.hooks.snowflake"] = airflow_mock.providers.snowflake.hooks.snowflake
sys.modules["airflow.providers.standard"] = airflow_mock.providers.standard
sys.modules["airflow.providers.standard.operators"] = airflow_mock.providers.standard.operators
sys.modules["airflow.providers.standard.operators.hitl"] = airflow_mock.providers.standard.operators.hitl
sys.modules["airflow.callbacks"] = airflow_mock.callbacks
sys.modules["airflow.callbacks.callback_requests"] = airflow_mock.callbacks.callback_requests
sys.modules["airflow.sdk.serde"] = airflow_mock.sdk.serde
sys.modules["airflow.task"] = airflow_mock.task
sys.modules["airflow.task.priority_strategy"] = airflow_mock.task.priority_strategy
sys.modules["airflow.utils"] = airflow_mock.utils
sys.modules["airflow.utils.module_loading"] = airflow_mock.utils.module_loading
sys.modules["airflow.configuration"] = airflow_mock.configuration
sys.modules["airflow.utils.email"] = airflow_mock.utils.email
sys.modules["airflow.sdk.execution_time"] = airflow_mock.sdk.execution_time
sys.modules["airflow.sdk.execution_time.context"] = airflow_mock.sdk.execution_time.context

import pytest  # noqa: E402 - must import after airflow modules are mocked above

@pytest.fixture(autouse=True)
def mock_config_loader(monkeypatch):
    from rlam_airflow_framework.config import ConfigLoader
    
    original_load_global = ConfigLoader.load_global_settings
    
    def mock_load_global(self):
        settings = original_load_global(self)
        if "tenants" not in settings:
            settings["tenants"] = {}
        settings["tenants"]["test"] = {
            "pool": "test_pool",
            "slots": 10,
            "connection_prefix": "test_"
        }
        return settings
        
    monkeypatch.setattr(ConfigLoader, "load_global_settings", mock_load_global)
