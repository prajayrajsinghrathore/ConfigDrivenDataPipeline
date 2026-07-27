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

import pandas as pd
import pytest
from unittest.mock import patch

from rlam_airflow_framework.destinations import (
    LoadContext,
    LocalFileLoader,
    ObjectStorageLoader,
    SnowflakeStageLoader,
    SnowflakeTableLoader,
    StoredProcedureLoader,
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
    def test_writes_parquet_and_returns_timestamped_path(self, tmp_path):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        target = tmp_path / "out.parquet"

        result = LocalFileLoader().load(df, {"type": "local_file", "path": str(target)}, _ctx())

        assert result.endswith(".parquet")
        assert target.stem in result  # timestamp inserted before the extension
        written = pd.read_parquet(result)
        assert written.equals(df)

    def test_writes_csv(self, tmp_path):
        df = pd.DataFrame({"a": [1]})
        result = LocalFileLoader().load(
            df, {"type": "local_file", "path": str(tmp_path / "out.csv"), "format": "csv"}, _ctx()
        )
        assert result.endswith(".csv")
        assert pd.read_csv(result).equals(df)

    def test_empty_dataframe_short_circuits(self, tmp_path):
        result = LocalFileLoader().load(
            pd.DataFrame(), {"type": "local_file", "path": str(tmp_path / "x.parquet")}, _ctx()
        )
        assert "empty DataFrame" in result


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

    def test_invalid_stage_name_rejected(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="stage name"):
            SnowflakeStageLoader().load(
                df, {"type": "snowflake_stage", "stage_name": "bad; DROP", "file_name": "f.csv"}, _ctx()
            )
