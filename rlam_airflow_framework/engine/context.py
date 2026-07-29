# File: rlam_airflow_framework/engine/context.py
"""
ExecutionContext model.
Provides a strictly typed context for Airflow pipeline execution, metrics, and logging.
"""

from dataclasses import dataclass
import structlog


@dataclass(frozen=True)
class ExecutionContext:
    correlation_id: str
    pipeline_id: str
    task_id: str
    attempt_number: int
    logger: structlog.stdlib.BoundLogger
