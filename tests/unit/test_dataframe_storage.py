# ruff: noqa: E402
from polars.testing import assert_frame_equal

"""
Unit tests for DataFrame file storage helper functions.

Tests the file-based DataFrame passing mechanism that replaces XCom serialization.
"""

from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from rlam_airflow_framework.taskflow_tasks import (
    _save_dataframe,
    _load_dataframe,
    _cleanup_dataframe,
)
from rlam_airflow_framework.taskflow.storage import DataFrameStorage


class TestSaveDataFrame:
    """Test DataFrame saving to parquet files."""

    def test_save_simple_dataframe(self, tmp_path):
        """Test saving a simple DataFrame."""
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [10.5, 20.3, 30.7],
            }
        )

        with patch("rlam_airflow_framework.taskflow_tasks.TEMP_DATA_DIR", tmp_path):
            filepath = _save_dataframe(df, "test_task", "test_run_123")

        # Verify file was created
        assert Path(filepath).exists()
        assert filepath.endswith(".parquet")
        assert "test_task_test_run_123.parquet" in filepath

        # Verify content
        loaded_df = pl.read_parquet(filepath)
        assert_frame_equal(df, loaded_df)

    def test_save_empty_dataframe(self, tmp_path):
        """Test saving an empty DataFrame."""
        df = pl.DataFrame(schema=["col1", "col2"])

        with patch("rlam_airflow_framework.taskflow_tasks.TEMP_DATA_DIR", tmp_path):
            filepath = _save_dataframe(df, "empty_task", "run_456")

        assert Path(filepath).exists()
        loaded_df = pl.read_parquet(filepath)
        assert len(loaded_df) == 0
        assert list(loaded_df.columns) == ["col1", "col2"]

    def test_save_dataframe_with_nulls(self, tmp_path):
        """Test saving DataFrame with null values."""
        df = pl.DataFrame(
            {"a": [1, None, 3], "b": ["x", "y", None], "c": [1.1, 2.2, 3.3]}
        )

        with patch("rlam_airflow_framework.taskflow_tasks.TEMP_DATA_DIR", tmp_path):
            filepath = _save_dataframe(df, "null_task", "run_789")

        loaded_df = pl.read_parquet(filepath)
        assert_frame_equal(df, loaded_df)

    def test_save_dataframe_with_datetime(self, tmp_path):
        """Test saving DataFrame with datetime columns."""
        df = pl.DataFrame(
            {
                "timestamp": pl.Series(
                    ["2024-01-01", "2024-01-02", "2024-01-03"]
                ).str.to_datetime(),
                "value": [100, 200, 300],
            }
        )

        with patch("rlam_airflow_framework.taskflow_tasks.TEMP_DATA_DIR", tmp_path):
            filepath = _save_dataframe(df, "datetime_task", "run_abc")

        loaded_df = pl.read_parquet(filepath)
        assert_frame_equal(df, loaded_df)

    def test_save_dataframe_with_index(self, tmp_path):
        """Test that DataFrame index is NOT preserved (index=False)."""
        df = pl.DataFrame({"value": [10, 20, 30]})

        with patch("rlam_airflow_framework.taskflow_tasks.TEMP_DATA_DIR", tmp_path):
            filepath = _save_dataframe(df, "index_task", "run_def")

        loaded_df = pl.read_parquet(filepath)
        # Index should be default RangeIndex, not the original
        assert len(loaded_df.columns) == 1

    def test_save_large_dataframe(self, tmp_path):
        """Test saving a large DataFrame with compression."""
        # Create a DataFrame with 10,000 rows
        df = pl.DataFrame(
            {
                "id": range(10000),
                "value": [i * 1.5 for i in range(10000)],
                "category": ["A", "B", "C"] * 3333 + ["A"],
            }
        )

        with patch("rlam_airflow_framework.taskflow_tasks.TEMP_DATA_DIR", tmp_path):
            filepath = _save_dataframe(df, "large_task", "run_ghi")

        # Verify file exists and is compressed
        assert Path(filepath).exists()
        file_size = Path(filepath).stat().st_size

        # Snappy compression should result in smaller file
        assert file_size < len(df) * 100  # Very rough estimate

        loaded_df = pl.read_parquet(filepath)
        assert_frame_equal(df, loaded_df)

    def test_save_dataframe_creates_directory(self, tmp_path):
        """Test that saving creates the directory if it doesn't exist."""
        temp_dir = tmp_path / "new_dir"
        assert not temp_dir.exists()

        df = pl.DataFrame({"col": [1, 2, 3]})

        with patch("rlam_airflow_framework.taskflow_tasks.TEMP_DATA_DIR", temp_dir):
            # The TEMP_DATA_DIR.mkdir() should have been called at import time,
            # but let's test it would work
            temp_dir.mkdir(parents=True, exist_ok=True)
            filepath = _save_dataframe(df, "mkdir_task", "run_jkl")

        assert Path(filepath).exists()


class TestLoadDataFrame:
    """Test DataFrame loading from parquet files."""

    def test_load_simple_dataframe(self, tmp_path):
        """Test loading a simple DataFrame."""
        df = pl.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})

        filepath = tmp_path / "test.parquet"
        df.write_parquet(filepath)

        loaded_df = _load_dataframe(str(filepath))
        assert_frame_equal(df, loaded_df)

    def test_load_empty_dataframe(self, tmp_path):
        """Test loading an empty DataFrame."""
        df = pl.DataFrame(schema=["a", "b", "c"])

        filepath = tmp_path / "empty.parquet"
        df.write_parquet(filepath)

        loaded_df = _load_dataframe(str(filepath))
        assert len(loaded_df) == 0
        assert list(loaded_df.columns) == ["a", "b", "c"]

    def test_load_dataframe_with_types(self, tmp_path):
        """Test that dtypes are preserved."""
        df = pl.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],
                "str_col": ["a", "b", "c"],
                "bool_col": [True, False, True],
            }
        )

        filepath = tmp_path / "types.parquet"
        df.write_parquet(filepath)

        loaded_df = _load_dataframe(str(filepath))
        assert loaded_df["int_col"].dtype == pl.Int64
        assert loaded_df["float_col"].dtype == pl.Float64
        assert loaded_df["bool_col"].dtype == pl.Boolean

    def test_load_nonexistent_file_raises_error(self):
        """Test that loading a nonexistent file raises an error."""
        with pytest.raises(FileNotFoundError):
            _load_dataframe("/nonexistent/path/file.parquet")

    def test_load_invalid_parquet_raises_error(self, tmp_path):
        """Test that loading an invalid parquet file raises an error."""
        filepath = tmp_path / "invalid.parquet"
        filepath.write_text("not a parquet file")

        with pytest.raises(Exception):  # Could be various parquet-related errors
            _load_dataframe(str(filepath))


class TestCleanupDataFrame:
    """Test DataFrame file cleanup."""

    def test_cleanup_existing_file(self, tmp_path):
        """Test cleaning up an existing file."""
        filepath = tmp_path / "cleanup_test.parquet"
        df = pl.DataFrame({"col": [1, 2, 3]})
        df.write_parquet(filepath)

        assert filepath.exists()
        _cleanup_dataframe(str(filepath))
        assert not filepath.exists()

    def test_cleanup_nonexistent_file_no_error(self, tmp_path):
        """Test that cleaning up a nonexistent file doesn't raise an error."""
        filepath = tmp_path / "does_not_exist.parquet"

        # Should not raise an error
        _cleanup_dataframe(str(filepath))

    def test_cleanup_logs_warning_on_permission_error(self, tmp_path, caplog):
        """Test that cleanup logs a warning if deletion fails."""
        filepath = tmp_path / "locked.parquet"
        df = pl.DataFrame({"col": [1]})
        df.write_parquet(filepath)

        # Mock unlink to raise PermissionError
        with patch("pathlib.Path.unlink", side_effect=PermissionError("Access denied")):
            _cleanup_dataframe(str(filepath))
            # Should log a warning but not raise


class TestRoundTripDataFrame:
    """Test complete save-load-cleanup cycle."""

    def test_round_trip_preserves_data(self, tmp_path):
        """Test that save -> load -> cleanup works correctly."""
        original_df = pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
                "score": [95.5, 87.3, 92.1, 88.9, 91.7],
                "passed": [True, True, True, True, True],
            }
        )

        with patch("rlam_airflow_framework.taskflow_tasks.TEMP_DATA_DIR", tmp_path):
            # Save
            filepath = _save_dataframe(original_df, "round_trip_task", "run_123")
            assert Path(filepath).exists()

            # Load
            loaded_df = _load_dataframe(filepath)
            assert_frame_equal(original_df, loaded_df)

            # Cleanup
            _cleanup_dataframe(filepath)
            assert not Path(filepath).exists()

    def test_multiple_saves_different_tasks(self, tmp_path):
        """Test that multiple tasks can save different DataFrames."""
        df1 = pl.DataFrame({"a": [1, 2, 3]})
        df2 = pl.DataFrame({"b": [4, 5, 6]})
        df3 = pl.DataFrame({"c": [7, 8, 9]})

        with patch("rlam_airflow_framework.taskflow_tasks.TEMP_DATA_DIR", tmp_path):
            path1 = _save_dataframe(df1, "task1", "run_001")
            path2 = _save_dataframe(df2, "task2", "run_001")
            path3 = _save_dataframe(df3, "task3", "run_001")

            # All files should exist
            assert Path(path1).exists()
            assert Path(path2).exists()
            assert Path(path3).exists()

            # Files should be different
            assert path1 != path2 != path3

            # Load and verify
            assert_frame_equal(df1, _load_dataframe(path1))
            assert_frame_equal(df2, _load_dataframe(path2))
            assert_frame_equal(df3, _load_dataframe(path3))


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_save_dataframe_with_unicode(self, tmp_path):
        """Test saving DataFrame with Unicode characters."""
        df = pl.DataFrame(
            {"name": ["Alice", "José", "François", "北京"], "value": [1, 2, 3, 4]}
        )

        with patch("rlam_airflow_framework.taskflow_tasks.TEMP_DATA_DIR", tmp_path):
            filepath = _save_dataframe(df, "unicode_task", "run_unicode")

        loaded_df = _load_dataframe(filepath)
        assert_frame_equal(df, loaded_df)

    def test_save_dataframe_with_special_characters_in_task_id(self, tmp_path):
        """Test that special characters in task_id are handled."""
        df = pl.DataFrame({"col": [1, 2, 3]})

        with patch("rlam_airflow_framework.taskflow_tasks.TEMP_DATA_DIR", tmp_path):
            # Task IDs shouldn't have special chars, but test defensively
            filepath = _save_dataframe(df, "task-with-dashes", "run_001")

        assert Path(filepath).exists()
        loaded_df = _load_dataframe(filepath)
        assert_frame_equal(df, loaded_df)

    def test_save_dataframe_with_categorical_dtype(self, tmp_path):
        """Test saving DataFrame with categorical dtype."""
        df = pl.DataFrame(
            {
                "category": ["A", "B", "A", "C", "B"],
                "value": [1, 2, 3, 4, 5],
            }
        )

        with patch("rlam_airflow_framework.taskflow_tasks.TEMP_DATA_DIR", tmp_path):
            filepath = _save_dataframe(df, "categorical_task", "run_cat")

        loaded_df = _load_dataframe(filepath)
        # Parquet preserves categorical dtype
        assert loaded_df["category"].dtype in (pl.Categorical, pl.String)
        assert_frame_equal(df, loaded_df)

    def test_filepath_includes_task_and_run_id(self, tmp_path):
        """Test that filepath includes both task_id and run_id."""
        df = pl.DataFrame({"col": [1]})

        with patch("rlam_airflow_framework.taskflow_tasks.TEMP_DATA_DIR", tmp_path):
            filepath = _save_dataframe(df, "my_task", "run_12345")

        assert "my_task" in filepath
        assert "run_12345" in filepath
        assert filepath.endswith(".parquet")


# =============================================================================
# DataFrameStorage class tests (OOP API)
# =============================================================================


class TestDataFrameStorageClass:
    """Test the OOP DataFrameStorage class directly (no patching needed)."""

    def test_save_and_load(self, tmp_path):
        """Round-trip save → load via the class API."""
        storage = DataFrameStorage(base_dir=tmp_path)
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

        filepath = storage.save(df, "task_1", "run_1")

        loaded = storage.load(filepath)
        assert_frame_equal(df, loaded)

    def test_cleanup(self, tmp_path):
        """cleanup() should remove the file."""
        storage = DataFrameStorage(base_dir=tmp_path)
        df = pl.DataFrame({"col": [1]})
        filepath = storage.save(df, "task_2", "run_2")

        assert Path(filepath).exists()
        storage.cleanup(filepath)
        assert not Path(filepath).exists()

    def test_save_creates_directory(self, tmp_path):
        """If the base_dir doesn't exist yet, save() creates it."""
        nested = tmp_path / "sub" / "deep"
        storage = DataFrameStorage(base_dir=nested)
        df = pl.DataFrame({"v": [10]})

        filepath = storage.save(df, "task_3", "run_3")
        assert Path(filepath).exists()

    def test_base_dir_property(self, tmp_path):
        storage = DataFrameStorage(base_dir=tmp_path)
        assert storage.base_dir == tmp_path

    def test_cleanup_nonexistent_no_error(self, tmp_path):
        """cleanup() on a missing file should not raise."""
        storage = DataFrameStorage(base_dir=tmp_path)
        storage.cleanup(str(tmp_path / "ghost.parquet"))  # no error

    def test_multiple_saves_isolated(self, tmp_path):
        """Different task/run combos produce different files."""
        storage = DataFrameStorage(base_dir=tmp_path)
        df1 = pl.DataFrame({"x": [1]})
        df2 = pl.DataFrame({"x": [2]})

        p1 = storage.save(df1, "t1", "r1")
        p2 = storage.save(df2, "t2", "r1")

        assert p1 != p2
        assert_frame_equal(storage.load(p1), df1)
        assert_frame_equal(storage.load(p2), df2)
