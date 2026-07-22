# File: tests/unit/conftest.py
"""
Unit test specific configuration.

This conftest.py applies only to unit tests and provides Airflow mocking
to allow testing without requiring a full Airflow installation.
"""

import sys
from unittest.mock import MagicMock

# =============================================================================
# AIRFLOW MOCKING - Only for unit tests
# =============================================================================

# Create comprehensive Airflow mock
airflow_mock = MagicMock()
airflow_mock.sdk = MagicMock()
airflow_mock.sdk.dag = MagicMock()
airflow_mock.sdk.DAG = MagicMock()

# Make @task decorator a pass-through function
def mock_task_decorator(func=None, **kwargs):
    """Mock @task decorator that returns function unchanged."""
    if func is None:
        # Decorator called with arguments: @task.sensor(poke_interval=60)
        def wrapper(f):
            return f
        return wrapper
    # Decorator called without arguments: @task
    return func

# Add attributes to mock_task_decorator for @task.branch, @task.sensor, etc.
mock_task_decorator.branch = mock_task_decorator
mock_task_decorator.sensor = mock_task_decorator
mock_task_decorator.python = mock_task_decorator

airflow_mock.sdk.task = mock_task_decorator
airflow_mock.sdk.get_current_context = MagicMock()
airflow_mock.sdk.PokeReturnValue = MagicMock()
airflow_mock.sdk.ObjectStoragePath = MagicMock()
airflow_mock.sdk.definitions = MagicMock()
airflow_mock.sdk.definitions.asset = MagicMock()
airflow_mock.sdk.definitions.asset.Asset = MagicMock()
airflow_mock.sdk.definitions.deadline = MagicMock()
airflow_mock.sdk.definitions.deadline.AsyncCallback = MagicMock()
airflow_mock.sdk.definitions.deadline.DeadlineAlert = MagicMock()
airflow_mock.sdk.definitions.deadline.DeadlineReference = MagicMock()
airflow_mock.decorators = MagicMock()
airflow_mock.operators = MagicMock()
airflow_mock.operators.python = MagicMock()
airflow_mock.hooks = MagicMock()
airflow_mock.hooks.base = MagicMock()
airflow_mock.hooks.base.BaseHook = MagicMock()
airflow_mock.providers = MagicMock()
airflow_mock.providers.snowflake = MagicMock()
airflow_mock.providers.snowflake.hooks = MagicMock()
airflow_mock.providers.snowflake.hooks.snowflake = MagicMock()
airflow_mock.providers.snowflake.hooks.snowflake.SnowflakeHook = MagicMock()
airflow_mock.callbacks = MagicMock()
airflow_mock.callbacks.callback_requests = MagicMock()
airflow_mock.serialization = MagicMock()
airflow_mock.serialization.serialized_objects = MagicMock()
airflow_mock.task = MagicMock()
airflow_mock.task.priority_strategy = MagicMock()
airflow_mock.utils = MagicMock()
airflow_mock.utils.module_loading = MagicMock()

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
sys.modules["airflow.sdk.dag"] = airflow_mock.sdk.dag
sys.modules["airflow.sdk.definitions"] = airflow_mock.sdk.definitions
sys.modules["airflow.sdk.definitions.asset"] = airflow_mock.sdk.definitions.asset
sys.modules["airflow.sdk.definitions.deadline"] = airflow_mock.sdk.definitions.deadline
sys.modules["airflow.decorators"] = airflow_mock.decorators
sys.modules["airflow.operators"] = airflow_mock.operators
sys.modules["airflow.operators.python"] = airflow_mock.operators.python
sys.modules["airflow.hooks"] = airflow_mock.hooks
sys.modules["airflow.hooks.base"] = airflow_mock.hooks.base
sys.modules["airflow.providers"] = airflow_mock.providers
sys.modules["airflow.providers.snowflake"] = airflow_mock.providers.snowflake
sys.modules["airflow.providers.snowflake.hooks"] = airflow_mock.providers.snowflake.hooks
sys.modules["airflow.providers.snowflake.hooks.snowflake"] = airflow_mock.providers.snowflake.hooks.snowflake
sys.modules["airflow.callbacks"] = airflow_mock.callbacks
sys.modules["airflow.callbacks.callback_requests"] = airflow_mock.callbacks.callback_requests
sys.modules["airflow.serialization"] = airflow_mock.serialization
sys.modules["airflow.serialization.serialized_objects"] = airflow_mock.serialization.serialized_objects
sys.modules["airflow.task"] = airflow_mock.task
sys.modules["airflow.task.priority_strategy"] = airflow_mock.task.priority_strategy
sys.modules["airflow.utils"] = airflow_mock.utils
sys.modules["airflow.utils.module_loading"] = airflow_mock.utils.module_loading
