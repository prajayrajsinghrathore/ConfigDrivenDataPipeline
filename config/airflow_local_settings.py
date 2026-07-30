"""
Airflow Local Settings - Logging Configuration

This file configures custom logging behavior for the RLAM Airflow pipeline.
It integrates structlog for structured JSON logging compatible with log aggregation tools.

Place this file in PYTHONPATH (e.g., /opt/airflow/config/) and configure:
[logging]
logging_config_class = airflow_local_settings.LOGGING_CONFIG
"""

import os
from copy import deepcopy
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
import structlog


# Determine environment (dev vs production)
ENV = os.getenv("AIRFLOW_ENV", "dev")
IS_PRODUCTION = ENV == "production"

# Start with Airflow's default logging configuration
LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)

# Configure structlog processors
# In dev: Use colored console output for readability
# In production: Use JSON for log aggregation (Azure Monitor, Prometheus, etc.)
if IS_PRODUCTION:
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),  # JSON for production
    ]
else:
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.dev.ConsoleRenderer(
            exception_formatter=structlog.dev.plain_traceback
        ),  # Colored output for dev without rich box-drawing characters
    ]

# Configure structlog
structlog.configure(
    processors=processors,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

# Custom logger configurations for RLAM framework components
# Set specific log levels for different modules
LOGGING_CONFIG["loggers"]["rlam_airflow_framework"] = {
    "handlers": ["task"],
    "level": "INFO",
    "propagate": True,
}

LOGGING_CONFIG["loggers"]["rlam_airflow_framework.data_quality"] = {
    "handlers": ["task"],
    "level": "DEBUG" if not IS_PRODUCTION else "INFO",  # Verbose in dev
    "propagate": True,
}

LOGGING_CONFIG["loggers"]["rlam_airflow_framework.formula_engine"] = {
    "handlers": ["task"],
    "level": "DEBUG" if not IS_PRODUCTION else "INFO",
    "propagate": True,
}

LOGGING_CONFIG["loggers"]["rlam_airflow_framework.kafka_publisher"] = {
    "handlers": ["task"],
    "level": "INFO",
    "propagate": False,  # Don't propagate to avoid duplicate Kafka logs
}

# Reduce noise from third-party libraries in production
if IS_PRODUCTION:
    LOGGING_CONFIG["loggers"]["snowflake.connector"] = {
        "handlers": ["task"],
        "level": "WARNING",
        "propagate": False,
    }

    LOGGING_CONFIG["loggers"]["confluent_kafka"] = {
        "handlers": ["task"],
        "level": "ERROR",  # Very noisy otherwise
        "propagate": False,
    }

    LOGGING_CONFIG["loggers"]["azure"] = {
        "handlers": ["task"],
        "level": "WARNING",
        "propagate": False,
    }

# Limit task log file size (100MB per file, keep 2 rotations)
if "task" in LOGGING_CONFIG["handlers"]:
    LOGGING_CONFIG["handlers"]["task"]["max_bytes"] = 104857600  # 100MB
    LOGGING_CONFIG["handlers"]["task"]["backup_count"] = 2

# Remote task log handler (required by Airflow 3.x)
# Set to None to disable remote logging (configured via airflow.cfg instead)
REMOTE_TASK_LOG = None

# ==================================================================================
# Custom XCom Serializers
# ==================================================================================
# Register custom serializers for polars DataFrames and other objects
# This allows DataFrames to be passed between tasks via XCom

# Import the custom DataFrame serializer
from rlam_airflow_framework import serializers  # noqa: E402
import airflow.sdk.serde  # noqa: E402

# Register the DataFrame serializer module manually
airflow.sdk.serde._serializers["polars.DataFrame"] = serializers
airflow.sdk.serde._deserializers["polars.DataFrame"] = serializers

print("[airflow_local_settings] Registered custom DataFrame serializer")

print(f"[airflow_local_settings] Loaded custom logging config for environment: {ENV}")
print(
    f"[airflow_local_settings] Structlog output format: {'JSON' if IS_PRODUCTION else 'Console'}"
)
