# File: tests/unit/test_partition_resolver.py
"""
Unit tests for PartitionInfo.
"""

from datetime import date


from rlam_airflow_framework.taskflow.partition import PartitionInfo


class TestPartitionResolve:
    """Test PartitionInfo.resolve() factory."""

    def test_disabled_partition(self):
        """When partition.enabled is false, value should be None."""
        config = {"partition": {"enabled": False, "column": "date"}}
        p = PartitionInfo.resolve(config, {})

        assert p.enabled is False
        assert p.column == "date"
        assert p.value is None

    def test_no_partition_section(self):
        """When partition section is absent, defaults to disabled."""
        p = PartitionInfo.resolve({}, {})

        assert p.enabled is False
        assert p.column is None
        assert p.value is None

    def test_partition_date_with_strftime(self):
        """A datetime-like partition_date should be formatted as YYYY-MM-DD."""
        config = {"partition": {"enabled": True, "column": "trade_date"}}
        context = {"partition_date": date(2025, 7, 15)}

        p = PartitionInfo.resolve(config, context)

        assert p.enabled is True
        assert p.column == "trade_date"
        assert p.value == "2025-07-15"

    def test_partition_date_string(self):
        """A plain-string partition_date should be used as-is."""
        config = {"partition": {"enabled": True, "column": "date"}}
        context = {"partition_date": "2025-07-15"}

        p = PartitionInfo.resolve(config, context)

        assert p.value == "2025-07-15"

    def test_partition_key_fallback(self):
        """When no partition_date, partition_key is used."""
        config = {"partition": {"enabled": True, "column": "region"}}
        context = {"partition_key": "EMEA"}

        p = PartitionInfo.resolve(config, context)

        assert p.value == "EMEA"

    def test_partition_date_takes_precedence(self):
        """partition_date wins over partition_key when both present."""
        config = {"partition": {"enabled": True, "column": "date"}}
        context = {"partition_date": "2025-07-15", "partition_key": "EMEA"}

        p = PartitionInfo.resolve(config, context)

        assert p.value == "2025-07-15"

    def test_enabled_but_no_context_values(self):
        """Partition enabled but no date/key in context gives None value."""
        config = {"partition": {"enabled": True, "column": "date"}}
        p = PartitionInfo.resolve(config, {})

        assert p.enabled is True
        assert p.value is None


class TestPartitionMetadata:
    """Test PartitionInfo.metadata property."""

    def test_metadata_with_value(self):
        p = PartitionInfo(enabled=True, column="date", value="2025-07-15")
        assert p.metadata == {"partition_key": "2025-07-15"}

    def test_metadata_without_value(self):
        p = PartitionInfo(enabled=True, column="date", value=None)
        assert p.metadata == {}


class TestScopePath:
    """Test PartitionInfo.scope_path()."""

    def test_inserts_partition_folder(self):
        p = PartitionInfo(enabled=True, column="date", value="2025-07-15")
        result = p.scope_path("s3://bucket/table/data.parquet")
        assert result == "s3://bucket/table/date=2025-07-15/data.parquet"

    def test_no_op_when_disabled(self):
        p = PartitionInfo(enabled=False, column="date", value="2025-07-15")
        assert p.scope_path("s3://bucket/data.parquet") == "s3://bucket/data.parquet"

    def test_no_op_when_value_missing(self):
        p = PartitionInfo(enabled=True, column="date", value=None)
        assert p.scope_path("s3://bucket/data.parquet") == "s3://bucket/data.parquet"

    def test_no_op_when_already_partitioned(self):
        p = PartitionInfo(enabled=True, column="date", value="2025-07-15")
        path = "s3://bucket/date=2025-07-15/data.parquet"
        assert p.scope_path(path) == path

    def test_no_op_when_value_in_path(self):
        p = PartitionInfo(enabled=True, column="date", value="2025-07-15")
        path = "s3://bucket/2025-07-15/data.parquet"
        assert p.scope_path(path) == path

    def test_bare_filename(self):
        """When there's no slash, partition segment is prepended."""
        p = PartitionInfo(enabled=True, column="region", value="EMEA")
        assert p.scope_path("data.parquet") == "region=EMEA/data.parquet"

    def test_empty_path(self):
        p = PartitionInfo(enabled=True, column="date", value="2025-07-15")
        assert p.scope_path("") == ""


class TestAdjustDestPaths:
    """Test PartitionInfo.adjust_dest_paths()."""

    def test_adjusts_object_storage_uri_and_path(self):
        p = PartitionInfo(enabled=True, column="date", value="2025-07-15")
        dest = {
            "primary": {
                "type": "object_storage",
                "uri": "s3://bucket/table/data.parquet",
                "path": "gs://bucket/table/data.parquet",
            }
        }
        p.adjust_dest_paths(dest)
        assert "date=2025-07-15" in dest["primary"]["uri"]
        assert "date=2025-07-15" in dest["primary"]["path"]

    def test_adjusts_local_file_path(self):
        p = PartitionInfo(enabled=True, column="date", value="2025-07-15")
        dest = {
            "primary": {"type": "local_file", "path": "/data/output.csv"}
        }
        p.adjust_dest_paths(dest)
        assert "date=2025-07-15" in dest["primary"]["path"]

    def test_skips_missing_destinations(self):
        """No error when primary/backup/archive are absent."""
        p = PartitionInfo(enabled=True, column="date", value="2025-07-15")
        dest = {}
        p.adjust_dest_paths(dest)  # Should not raise

    def test_no_op_when_disabled(self):
        p = PartitionInfo(enabled=False, column="date", value="2025-07-15")
        dest = {
            "primary": {"type": "local_file", "path": "/data/output.csv"}
        }
        p.adjust_dest_paths(dest)
        assert dest["primary"]["path"] == "/data/output.csv"
