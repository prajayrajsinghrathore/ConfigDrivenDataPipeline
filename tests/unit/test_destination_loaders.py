# File: tests/unit/test_destination_loaders.py
"""
Characterization tests for the destination loader strategies.

These pin the *observable* behavior of each loader (return summaries, validation
errors, empty-DataFrame handling, and the parameterized Snowflake insert), so
the underlying implementation (see ``destinations/`` and
``destinations/primitives.py``) can evolve without regressions.

Snowflake hooks are patched locally per test (patch-where-used) for isolation;
local-file tests use a real temp filesystem.
"""

import time

import pandas as pd
import pytest
from snowflake.connector.errors import OperationalError
from unittest.mock import patch

from rlam_airflow_framework.destinations import (
    LoadContext,
    LocalFileLoader,
    ObjectStorageLoader,
    SnowflakeStageLoader,
    SnowflakeTableLoader,
    StoredProcedureLoader,
)
from rlam_airflow_framework.destinations.object_storage import ObjectStoragePath
from rlam_airflow_framework.destinations.primitives import (
    DataLoadError,
    ObjectStorageError,
    TransientDataLoadError,
    TransientObjectStorageError,
)

# Patch the Snowflake hook where each loader module imports it (patch-where-used).
# Local patching (vs. the process-wide conftest mock) isolates these tests from
# cross-test state bleed on the shared MagicMock.
_HOOK_TABLE = "rlam_airflow_framework.destinations.snowflake_table.SnowflakeHook"
_HOOK_STAGE = "rlam_airflow_framework.destinations.snowflake_stage.SnowflakeHook"
_HOOK_PROC = "rlam_airflow_framework.destinations.stored_procedure.SnowflakeHook"


def _ctx(**kwargs):
    defaults = dict(topic="pipeline-events", correlation_id="cid-123", dest_label="primary")
    defaults.update(kwargs)
    return LoadContext(**defaults)


# ---------------------------------------------------------------------------
# LocalFileLoader (real filesystem)
# ---------------------------------------------------------------------------


class TestLocalFileLoader:
    @pytest.fixture(autouse=True)
    def _sandbox_root(self, tmp_path, monkeypatch):
        # dest_config["path"] is resolved relative to LOCAL_FILE_OUTPUT_ROOT
        # (see local_file.py's sandboxing fix); point it at the test's
        # tmp_path so relative paths below still land somewhere writable.
        monkeypatch.setattr(
            "rlam_airflow_framework.destinations.local_file.LOCAL_FILE_OUTPUT_ROOT",
            tmp_path,
        )

    def test_writes_parquet_and_returns_timestamped_path(self):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

        result = LocalFileLoader().load(df, {"type": "local_file", "path": "out.parquet"}, _ctx())

        assert result.endswith(".parquet")
        assert "out" in result  # timestamp inserted before the extension
        written = pd.read_parquet(result)
        assert written.equals(df)

    def test_writes_csv(self):
        df = pd.DataFrame({"a": [1]})
        result = LocalFileLoader().load(
            df, {"type": "local_file", "path": "out.csv", "format": "csv"}, _ctx()
        )
        assert result.endswith(".csv")
        assert pd.read_csv(result).equals(df)

    def test_empty_dataframe_short_circuits(self):
        result = LocalFileLoader().load(
            pd.DataFrame(), {"type": "local_file", "path": "x.parquet"}, _ctx()
        )
        assert "empty DataFrame" in result

    @pytest.mark.parametrize(
        "malicious_path",
        [
            "../../../../opt/airflow/airflow.cfg",
            "subdir/../../escape.txt",
            "C:/Windows/win.ini",
            "sub/C:/evil.txt",
        ],
    )
    def test_rejects_path_traversal_and_drive_letter_escapes(self, malicious_path):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(DataLoadError):
            LocalFileLoader().load(
                df, {"type": "local_file", "path": malicious_path}, _ctx()
            )


# ---------------------------------------------------------------------------
# SnowflakeTableLoader (mocked hook)
# ---------------------------------------------------------------------------


class TestSnowflakeTableLoader:
    @patch(_HOOK_TABLE)
    def test_happy_path_uses_parameterized_executemany(self, mock_hook_cls):
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        cfg = {"type": "snowflake_table", "table": "MYSCHEMA.MYTABLE"}

        result = SnowflakeTableLoader().load(df, cfg, _ctx())

        assert "Loaded 2 rows" in result
        assert "MYTABLE" in result
        # The injection-safe path binds values via executemany (never string-built SQL)
        cursor = mock_hook_cls.return_value.get_conn.return_value.cursor.return_value
        assert cursor.executemany.called

    def test_invalid_table_name_rejected(self):
        df = pd.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="Invalid table name"):
            SnowflakeTableLoader().load(
                df, {"type": "snowflake_table", "table": "bad; DROP TABLE x"}, _ctx()
            )

    def test_invalid_column_name_rejected(self):
        df = pd.DataFrame({"ok": [1], "bad col": [2]})
        with pytest.raises(ValueError, match="Invalid column"):
            SnowflakeTableLoader().load(
                df, {"type": "snowflake_table", "table": "S.T"}, _ctx()
            )

    def test_empty_dataframe_short_circuits(self):
        result = SnowflakeTableLoader().load(
            pd.DataFrame(), {"type": "snowflake_table", "table": "S.T"}, _ctx()
        )
        assert "empty DataFrame" in result

    @patch(_HOOK_TABLE)
    def test_persistent_transient_error_exhausts_retries_as_transient(
        self, mock_hook_cls, monkeypatch
    ):
        # Skip tenacity's real exponential backoff between attempts.
        monkeypatch.setattr(time, "sleep", lambda _: None)

        cursor = mock_hook_cls.return_value.get_conn.return_value.cursor.return_value
        cursor.executemany.side_effect = OperationalError(msg="Connection timeout")

        df = pd.DataFrame({"id": [1]})
        with pytest.raises(TransientDataLoadError):
            SnowflakeTableLoader().load(
                df, {"type": "snowflake_table", "table": "S.T"}, _ctx()
            )


# ---------------------------------------------------------------------------
# ObjectStorageLoader
# ---------------------------------------------------------------------------


class TestObjectStorageLoader:
    def test_requires_uri_or_path_and_conn(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="uri.*or.*path"):
            ObjectStorageLoader().load(df, {"type": "object_storage"}, _ctx())

    def test_empty_dataframe_short_circuits(self):
        result = ObjectStorageLoader().load(
            pd.DataFrame(), {"type": "object_storage", "uri": "az://c@conn/x.parquet"}, _ctx()
        )
        assert "empty DataFrame" in result

    def test_connection_error_during_upload_is_transient(self):
        write_target = ObjectStoragePath.return_value.parent.__truediv__.return_value  # pyright: ignore[reportAttributeAccessIssue]
        write_target.write_bytes.side_effect = ConnectionError("refused")
        try:
            df = pd.DataFrame({"a": [1]})
            with pytest.raises(TransientObjectStorageError):
                ObjectStorageLoader().load(
                    df, {"type": "object_storage", "uri": "az://c@conn/x.parquet"}, _ctx()
                )
        finally:
            write_target.write_bytes.side_effect = None

    def test_unexpected_error_during_upload_is_deterministic(self):
        write_target = ObjectStoragePath.return_value.parent.__truediv__.return_value  # pyright: ignore[reportAttributeAccessIssue]
        write_target.write_bytes.side_effect = RuntimeError("permission denied")
        try:
            df = pd.DataFrame({"a": [1]})
            with pytest.raises(ObjectStorageError) as exc_info:
                ObjectStorageLoader().load(
                    df, {"type": "object_storage", "uri": "az://c@conn/x.parquet"}, _ctx()
                )
            assert not isinstance(exc_info.value, TransientObjectStorageError)
        finally:
            write_target.write_bytes.side_effect = None


# ---------------------------------------------------------------------------
# StoredProcedureLoader (mocked hook)
# ---------------------------------------------------------------------------


class TestStoredProcedureLoader:
    @patch(_HOOK_PROC)
    def test_calls_procedure_and_returns_summary(self, mock_hook_cls):
        df = pd.DataFrame({"a": [1]})
        cfg = {
            "type": "stored_procedure",
            "procedure": "ANALYTICS.SP_AGG",
            "capture_result": False,
        }
        result = StoredProcedureLoader().load(df, cfg, _ctx())

        assert "SP_AGG" in result
        assert mock_hook_cls.return_value.run.called

    @patch(_HOOK_PROC)
    def test_persistent_transient_error_exhausts_retries_as_transient(
        self, mock_hook_cls, monkeypatch
    ):
        monkeypatch.setattr(time, "sleep", lambda _: None)
        mock_hook_cls.return_value.run.side_effect = OperationalError(
            msg="Connection timeout"
        )

        df = pd.DataFrame({"a": [1]})
        cfg = {
            "type": "stored_procedure",
            "procedure": "ANALYTICS.SP_AGG",
            "capture_result": False,
        }
        with pytest.raises(TransientDataLoadError):
            StoredProcedureLoader().load(df, cfg, _ctx())

    def test_invalid_procedure_name_rejected(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="procedure name"):
            StoredProcedureLoader().load(
                df, {"type": "stored_procedure", "procedure": "bad; DROP"}, _ctx()
            )


# ---------------------------------------------------------------------------
# SnowflakeStageLoader (mocked hook)
# ---------------------------------------------------------------------------


class TestSnowflakeStageLoader:
    @patch(_HOOK_STAGE)
    def test_puts_file_to_stage_and_returns_summary(self, mock_hook_cls):
        df = pd.DataFrame({"a": [1]})
        cfg = {"type": "snowflake_stage", "stage_name": "MY_STAGE", "file_name": "data.csv"}
        result = SnowflakeStageLoader().load(df, cfg, _ctx())

        assert "MY_STAGE" in result
        assert mock_hook_cls.return_value.run.called

    @patch(_HOOK_STAGE)
    def test_transient_put_error_is_transient(self, mock_hook_cls):
        mock_hook_cls.return_value.run.side_effect = OperationalError(
            msg="Connection timeout"
        )
        df = pd.DataFrame({"a": [1]})
        cfg = {"type": "snowflake_stage", "stage_name": "MY_STAGE", "file_name": "data.csv"}
        with pytest.raises(TransientDataLoadError):
            SnowflakeStageLoader().load(df, cfg, _ctx())

    @patch(_HOOK_STAGE)
    def test_deterministic_put_error_is_not_transient(self, mock_hook_cls):
        mock_hook_cls.return_value.run.side_effect = RuntimeError("syntax error")
        df = pd.DataFrame({"a": [1]})
        cfg = {"type": "snowflake_stage", "stage_name": "MY_STAGE", "file_name": "data.csv"}
        with pytest.raises(Exception) as exc_info:
            SnowflakeStageLoader().load(df, cfg, _ctx())
        assert not isinstance(exc_info.value, TransientDataLoadError)

    def test_invalid_stage_name_rejected(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="stage name"):
            SnowflakeStageLoader().load(
                df, {"type": "snowflake_stage", "stage_name": "bad; DROP", "file_name": "f.csv"}, _ctx()
            )
