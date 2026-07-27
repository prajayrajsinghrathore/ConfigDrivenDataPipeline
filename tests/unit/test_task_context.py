# File: tests/unit/test_task_context.py
"""
Unit tests for TaskExecutionContext.
"""

from unittest.mock import MagicMock, patch

import pytest

from rlam_airflow_framework.taskflow.context import TaskExecutionContext


class TestTaskExecutionContext:
    """Test TaskExecutionContext construction and factory."""

    def test_direct_construction(self):
        """Test creating a context with explicit values."""
        ctx = TaskExecutionContext(
            dag_id="my_dag",
            task_id="my_task",
            run_id="run_001",
            correlation_id="my_dag_run_001_my_task",
            raw_context={"dag": MagicMock(), "run_id": "run_001"},
        )

        assert ctx.dag_id == "my_dag"
        assert ctx.task_id == "my_task"
        assert ctx.run_id == "run_001"
        assert ctx.correlation_id == "my_dag_run_001_my_task"

    def test_frozen_dataclass(self):
        """Test that the dataclass is immutable."""
        ctx = TaskExecutionContext(
            dag_id="d",
            task_id="t",
            run_id="r",
            correlation_id="d_r_t",
            raw_context={},
        )
        with pytest.raises(AttributeError):
            ctx.dag_id = "other"  # type: ignore[misc]

    @patch("rlam_airflow_framework.taskflow.context.get_current_context")
    def test_from_airflow(self, mock_get_ctx):
        """Test factory method extracts values from Airflow context."""
        mock_get_ctx.return_value = {
            "dag": MagicMock(dag_id="test_dag"),
            "task": MagicMock(task_id="ingest"),
            "run_id": "manual__2025-01-01",
        }

        ctx = TaskExecutionContext.from_airflow()

        assert ctx.dag_id == "test_dag"
        assert ctx.task_id == "ingest"
        assert ctx.run_id == "manual__2025-01-01"
        assert ctx.correlation_id == "test_dag_manual__2025-01-01_ingest"

    @patch("rlam_airflow_framework.taskflow.context.get_current_context")
    def test_from_airflow_preserves_raw_context(self, mock_get_ctx):
        """Test that the raw_context dict is available for downstream access."""
        raw = {
            "dag": MagicMock(dag_id="d"),
            "task": MagicMock(task_id="t"),
            "run_id": "r",
            "partition_date": "2025-06-01",
        }
        mock_get_ctx.return_value = raw

        ctx = TaskExecutionContext.from_airflow()

        assert ctx.raw_context["partition_date"] == "2025-06-01"

    @patch("rlam_airflow_framework.taskflow.context.get_current_context")
    def test_correlation_id_format(self, mock_get_ctx):
        """Correlation ID should be dag_id + run_id + task_id."""
        mock_get_ctx.return_value = {
            "dag": MagicMock(dag_id="A"),
            "task": MagicMock(task_id="C"),
            "run_id": "B",
        }

        ctx = TaskExecutionContext.from_airflow()

        assert ctx.correlation_id == "A_B_C"
