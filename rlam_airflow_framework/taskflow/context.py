# File: rlam_airflow_framework/taskflow/context.py
"""
Task execution context — encapsulates Airflow runtime metadata.

Replaces the 5-line boilerplate (dag_id, task_id, run_id, correlation_id)
that was copy-pasted into every @task function in ``taskflow_tasks.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, cast

from airflow.sdk import get_current_context


@dataclass(frozen=True)
class TaskExecutionContext:
    """
    Immutable snapshot of Airflow task-instance metadata.

    Build via :meth:`from_airflow` inside a running ``@task`` function,
    or construct directly in tests.
    """

    dag_id: str
    task_id: str
    run_id: str
    correlation_id: str
    raw_context: Dict[str, Any]

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_airflow(cls) -> "TaskExecutionContext":
        """
        Create a context from the live Airflow execution environment.

        Calls ``get_current_context()`` internally — must only be invoked
        inside a running task.
        """
        ctx = cast(Dict[str, Any], get_current_context())
        dag_id = ctx["dag"].dag_id
        task_id = ctx["task"].task_id
        run_id = ctx["run_id"]
        return cls(
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            correlation_id=f"{dag_id}_{run_id}_{task_id}",
            raw_context=ctx,
        )
