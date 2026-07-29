# File: tests/unit/test_deadline_callbacks.py
"""
Unit tests for rlam_airflow_framework/deadline_callbacks.py context handling.

The payload fixture below mirrors the REAL Airflow 3.3.0 SyncCallback context
observed on the live stack (2026-07-23): `dag_run` is a plain dict and
`deadline.deadline_time` is an ISO-8601 string with a trailing 'Z'. The
notifiers must handle that shape (and the attribute-style objects older tests
pass) without raising.

Import note: under the unit-lane mocks `airflow.sdk.BaseNotifier` is a
MagicMock, and subclassing a MagicMock produces a MagicMock class — the real
notifier code would never execute. So this module loads deadline_callbacks a
second time, under a distinct module name, with a real stub BaseNotifier
temporarily installed on the airflow.sdk mock.
"""

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import pytest

MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "rlam_airflow_framework"
    / "deadline_callbacks.py"
)


class _StubBaseNotifier:
    """Minimal real base class standing in for airflow.sdk.BaseNotifier."""

    def __init__(self, **kwargs):
        pass


@pytest.fixture(scope="module")
def dc():
    """deadline_callbacks loaded with a real (stub) BaseNotifier base class."""
    sdk_mock = cast(Any, sys.modules["airflow.sdk"])
    original_base = sdk_mock.BaseNotifier
    sdk_mock.BaseNotifier = _StubBaseNotifier
    try:
        spec = importlib.util.spec_from_file_location(
            "deadline_callbacks_real", MODULE_PATH
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        yield module
    finally:
        sdk_mock.BaseNotifier = original_base
        sys.modules.pop("deadline_callbacks_real", None)


REAL_CALLBACK_CONTEXT = {
    "dag_run": {
        "dag_run_id": "manual__2026-07-23T13:25:09.040865+00:00",
        "dag_id": "sandbox_deadline_probe",
        "logical_date": None,
        "queued_at": "2026-07-23T13:25:09.360833Z",
        "run_type": "manual",
        "state": "running",
        "partition_key": None,
        "partition_date": None,
    },
    "deadline": {
        "id": "019f8f26-c0e9-76c0-b03f-17dc79a21b00",
        "deadline_time": "2026-07-23T13:26:09.360833Z",
    },
}


class TestDeadlineContextAdapter:
    """DeadlineContext normalizes the callback payload once at the boundary."""

    def test_real_dict_payload(self, dc):
        ctx = dc.DeadlineContext.from_context(dict(REAL_CALLBACK_CONTEXT))
        assert ctx.dag_id == "sandbox_deadline_probe"
        assert ctx.deadline_time == datetime(
            2026, 7, 23, 13, 26, 9, 360833, tzinfo=timezone.utc
        )
        assert ctx.reference == "dagrun_queued"

    def test_object_payload(self, dc):
        ctx = dc.DeadlineContext.from_context(
            {"dag_run": SimpleNamespace(dag_id="my_dag"), "deadline": {}}
        )
        assert ctx.dag_id == "my_dag"
        assert ctx.deadline_time is None

    def test_missing_everything(self, dc):
        ctx = dc.DeadlineContext.from_context({})
        assert ctx.dag_id == "unknown"
        assert ctx.logical_date is None
        assert ctx.deadline_time is None

    def test_datetime_passthrough_and_garbage(self, dc):
        now = datetime.now(timezone.utc)
        assert dc.DeadlineContext._parse_datetime(now) is now
        assert dc.DeadlineContext._parse_datetime(None) is None
        assert dc.DeadlineContext._parse_datetime("not-a-date") is None


class TestCompositeNotifierRealPayload:
    def test_notify_with_real_dict_payload_publishes_kafka(self, dc):
        notifier = dc.CompositeDeadlineNotifier(
            topic="pipeline-alerts",
            message="Pipeline sandbox_deadline_probe missed deadline of 1 minutes",
        )
        with patch.object(
            dc._plugin_kafka_publisher, "publish_event", return_value=True
        ) as publish:
            # Must not raise on the real dict-shaped payload
            notifier.notify(dict(REAL_CALLBACK_CONTEXT))

        publish.assert_called_once()
        kwargs = publish.call_args.kwargs
        assert kwargs["dag_id"] == "sandbox_deadline_probe"
        assert kwargs["topic"] == "pipeline-alerts"
        assert kwargs["event_type"] == "deadline_missed"
        assert kwargs["metadata"]["alert_type"] == "deadline_missed"
        # deadline_time parsed from the ISO string -> late_by computed
        assert kwargs["metadata"]["late_by_seconds"] is not None

    def test_notify_email_disabled_by_default(self, dc):
        notifier = dc.CompositeDeadlineNotifier(topic="pipeline-alerts")
        with patch.object(
            dc._plugin_kafka_publisher, "publish_event", return_value=True
        ), patch.object(notifier, "_email_notifier") as email:
            notifier.notify(dict(REAL_CALLBACK_CONTEXT))
        email.notify.assert_not_called()
