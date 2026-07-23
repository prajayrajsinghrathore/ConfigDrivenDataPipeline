# File: tests/unit/test_taskflow_tasks.py
"""
Unit tests for taskflow_tasks module.

Tests cover:
- ingest_data() with rest_api source type
- ingest_data() with sftp source type  
- Error handling for unsupported source types
- URL extraction from config
- Response format handling
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pathlib import Path


@pytest.fixture
def mock_temp_dir(tmp_path):
    """Mock TEMP_DATA_DIR to use pytest's tmp_path."""
    with patch("rlam_airflow_framework.taskflow_tasks.TEMP_DATA_DIR", tmp_path):
        yield tmp_path


@pytest.fixture
def mock_airflow_context():
    """Mock Airflow task context."""
    return {
        "dag": MagicMock(dag_id="test_dag"),
        "task": MagicMock(task_id="test_task"),
        "run_id": "test_run_123",
    }


@pytest.fixture
def rest_api_config():
    """Sample REST API configuration."""
    return {
        "data_source": {
            "name": "test_api",
            "type": "rest_api",
            "endpoint": "https://api.example.com/data",
            "method": "GET",
            "response_format": "json",
        },
        "destination": {
            "type": "local_file",
            "path": "/tmp/output.json",
        },
    }


@pytest.fixture
def sftp_config():
    """Sample SFTP configuration."""
    return {
        "data_source": {
            "name": "test_sftp",
            "type": "sftp",
            "connection_id": "sftp_default",
            "remote_path": "/data/test.csv",
        },
        "destination": {
            "type": "local_file",
            "path": "/tmp/output.csv",
        },
    }


class TestIngestDataRestApi:
    """Test ingest_data function with REST API source."""

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("rlam_airflow_framework.taskflow_tasks.fetch_http_data")
    @patch("rlam_airflow_framework.taskflow_tasks.kafka_publisher")
    def test_ingest_rest_api_success(
        self, mock_kafka, mock_fetch_http, mock_context, rest_api_config, mock_airflow_context, mock_temp_dir
    ):
        """Test successful REST API data ingestion."""
        from rlam_airflow_framework.taskflow_tasks import ingest_data

        # Setup mocks
        mock_context.return_value = mock_airflow_context
        test_data = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        mock_fetch_http.return_value = test_data

        # Execute
        result = ingest_data(rest_api_config)

        # Verify - ingest_data now returns file path, not DataFrame
        assert isinstance(result, str)
        assert result.endswith(".parquet")
        assert Path(result).exists()
        
        # Load and verify the saved DataFrame
        saved_df = pd.read_parquet(result)
        assert len(saved_df) == 3
        assert list(saved_df.columns) == ["id", "value"]

        # Verify fetch_http_data was called with correct parameters
        mock_fetch_http.assert_called_once()
        call_args = mock_fetch_http.call_args
        assert call_args.kwargs["url"] == "https://api.example.com/data"
        assert call_args.kwargs["format"] == "json"
        assert "correlation_id" in call_args.kwargs

        # Verify Kafka events were published
        assert mock_kafka.publish_pipeline_event.call_count == 2  # start and success

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("rlam_airflow_framework.taskflow_tasks.fetch_http_data")
    @patch("rlam_airflow_framework.taskflow_tasks.kafka_publisher")
    def test_ingest_rest_api_with_csv_format(
        self, mock_kafka, mock_fetch_http, mock_context, rest_api_config, mock_airflow_context, mock_temp_dir
    ):
        """Test REST API ingestion with CSV format."""
        from rlam_airflow_framework.taskflow_tasks import ingest_data

        # Modify config for CSV
        rest_api_config["data_source"]["response_format"] = "csv"

        # Setup mocks
        mock_context.return_value = mock_airflow_context
        test_data = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        mock_fetch_http.return_value = test_data

        # Execute
        result = ingest_data(rest_api_config)

        # Verify - ingest_data now returns file path
        assert isinstance(result, str)
        assert result.endswith(".parquet")
        assert Path(result).exists()
        
        # Verify format parameter
        call_args = mock_fetch_http.call_args
        assert call_args.kwargs["format"] == "csv"

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("rlam_airflow_framework.taskflow_tasks.fetch_http_data")
    @patch("rlam_airflow_framework.taskflow_tasks.kafka_publisher")
    def test_ingest_rest_api_default_format(
        self, mock_kafka, mock_fetch_http, mock_context, rest_api_config, mock_airflow_context, mock_temp_dir
    ):
        """Test REST API ingestion defaults to json when format not specified."""
        from rlam_airflow_framework.taskflow_tasks import ingest_data

        # Remove response_format from config
        del rest_api_config["data_source"]["response_format"]

        # Setup mocks
        mock_context.return_value = mock_airflow_context
        test_data = pd.DataFrame({"data": [1, 2, 3]})
        mock_fetch_http.return_value = test_data

        # Execute
        result = ingest_data(rest_api_config)

        # Verify - ingest_data now returns file path
        assert isinstance(result, str)
        assert result.endswith(".parquet")
        assert Path(result).exists()
        
        # Verify default format is json
        call_args = mock_fetch_http.call_args
        assert call_args.kwargs["format"] == "json"

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("rlam_airflow_framework.taskflow_tasks.fetch_http_data")
    @patch("rlam_airflow_framework.taskflow_tasks.kafka_publisher")
    def test_ingest_rest_api_empty_dataframe(
        self, mock_kafka, mock_fetch_http, mock_context, rest_api_config, mock_airflow_context, mock_temp_dir
    ):
        """Test handling of empty DataFrame from REST API."""
        from rlam_airflow_framework.taskflow_tasks import ingest_data

        # Setup mocks
        mock_context.return_value = mock_airflow_context
        mock_fetch_http.return_value = pd.DataFrame()

        # Execute
        result = ingest_data(rest_api_config)

        # Verify - ingest_data now returns file path, even for empty DataFrame
        assert isinstance(result, str)
        assert result.endswith(".parquet")
        assert Path(result).exists()
        
        # Load and verify the saved DataFrame is empty
        saved_df = pd.read_parquet(result)
        assert len(saved_df) == 0

        # Success event should still be published
        assert mock_kafka.publish_pipeline_event.call_count == 2


class TestIngestDataSftp:
    """Test ingest_data function with SFTP source."""

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("rlam_airflow_framework.taskflow_tasks.fetch_sftp_data")
    @patch("rlam_airflow_framework.taskflow_tasks.kafka_publisher")
    def test_ingest_sftp_success(
        self, mock_kafka, mock_fetch_sftp, mock_context, sftp_config, mock_airflow_context, mock_temp_dir
    ):
        """Test successful SFTP data ingestion."""
        from rlam_airflow_framework.taskflow_tasks import ingest_data

        # Setup mocks
        mock_context.return_value = mock_airflow_context
        test_data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
        mock_fetch_sftp.return_value = test_data

        # Execute
        result = ingest_data(sftp_config)

        # Verify - ingest_data now returns file path
        assert isinstance(result, str)
        assert result.endswith(".parquet")
        assert Path(result).exists()
        
        # Load and verify the saved DataFrame
        saved_df = pd.read_parquet(result)
        assert len(saved_df) == 2
        
        # Verify fetch_sftp_data was called
        mock_fetch_sftp.assert_called_once()


class TestIncrementalWatermarkFiltering:
    """Regression tests for the 3B.4 watermark-corruption bug: pd.to_datetime()
    silently converts integer watermark columns to epoch-nanosecond timestamps
    (id=5 -> 1970-01-01T00:00:00.000000005), which then poisoned the stored
    watermark. Numeric comparison must win and the column must not be mutated."""

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("rlam_airflow_framework.taskflow_tasks.fetch_http_data")
    @patch("rlam_airflow_framework.taskflow_tasks.kafka_publisher")
    def test_integer_watermark_filters_numerically_without_mutation(
        self, mock_kafka, mock_fetch_http, mock_context, rest_api_config, mock_airflow_context, mock_temp_dir
    ):
        import sys

        from rlam_airflow_framework.taskflow_tasks import ingest_data

        rest_api_config["incremental"] = {
            "enabled": True,
            "watermark_column": "id",
            "initial_watermark": "0",
        }

        mock_context.return_value = mock_airflow_context
        mock_fetch_http.return_value = pd.DataFrame(
            {"id": [1, 2, 3, 4, 5], "v": ["a", "b", "c", "d", "e"]}
        )

        sdk_mock = sys.modules["airflow.sdk"]
        original_variable = sdk_mock.Variable
        sdk_mock.Variable = MagicMock()
        # Stored watermark "3" -> only ids 4 and 5 should survive
        sdk_mock.Variable.get.return_value = "3"
        try:
            result = ingest_data(rest_api_config)
        finally:
            sdk_mock.Variable = original_variable

        saved_df = pd.read_parquet(result)
        assert list(saved_df["id"]) == [4, 5]
        # The column must remain integer — NOT coerced to datetime
        assert pd.api.types.is_integer_dtype(saved_df["id"]), saved_df["id"].dtype


class TestIngestDataErrorHandling:
    """Test error handling in ingest_data function."""

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("rlam_airflow_framework.taskflow_tasks.kafka_publisher")
    def test_unsupported_source_type(
        self, mock_kafka, mock_context, rest_api_config, mock_airflow_context
    ):
        """Test error handling for unsupported source type."""
        from rlam_airflow_framework.taskflow_tasks import ingest_data

        # Setup mocks
        mock_context.return_value = mock_airflow_context
        
        # Change to unsupported type
        rest_api_config["data_source"]["type"] = "database"

        # Execute and verify exception
        with pytest.raises(ValueError, match="Unsupported source type: database"):
            ingest_data(rest_api_config)

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("rlam_airflow_framework.taskflow_tasks.fetch_http_data")
    @patch("rlam_airflow_framework.taskflow_tasks.kafka_publisher")
    def test_fetch_failure_publishes_error_event(
        self, mock_kafka, mock_fetch_http, mock_context, rest_api_config, mock_airflow_context
    ):
        """Test that fetch failures publish error events to Kafka."""
        from rlam_airflow_framework.taskflow_tasks import ingest_data

        # Setup mocks
        mock_context.return_value = mock_airflow_context
        mock_fetch_http.side_effect = Exception("Network error")

        # Execute and verify exception is raised
        with pytest.raises(Exception, match="Network error"):
            ingest_data(rest_api_config)

        # Verify error event was published
        error_calls = [
            call for call in mock_kafka.publish_pipeline_event.call_args_list
            if call.kwargs.get("event_type") == "ingestion_failed"
        ]
        assert len(error_calls) == 1

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("rlam_airflow_framework.taskflow_tasks.kafka_publisher")
    def test_missing_endpoint_in_config(
        self, mock_kafka, mock_context, rest_api_config, mock_airflow_context
    ):
        """Test error when endpoint is missing from REST API config."""
        from rlam_airflow_framework.taskflow_tasks import ingest_data

        # Setup mocks
        mock_context.return_value = mock_airflow_context
        
        # Remove endpoint
        del rest_api_config["data_source"]["endpoint"]

        # Execute and verify KeyError is raised
        with pytest.raises(KeyError, match="endpoint"):
            ingest_data(rest_api_config)


class TestConfigValidation:
    """Test configuration validation and parameter extraction."""

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("rlam_airflow_framework.taskflow_tasks.fetch_http_data")
    @patch("rlam_airflow_framework.taskflow_tasks.kafka_publisher")
    def test_url_extraction_from_config(
        self, mock_kafka, mock_fetch_http, mock_context, mock_airflow_context, mock_temp_dir
    ):
        """Test that URL is correctly extracted from endpoint field."""
        from rlam_airflow_framework.taskflow_tasks import ingest_data

        config = {
            "data_source": {
                "name": "test",
                "type": "rest_api",
                "endpoint": "https://example.com/api/v1/data",
                "response_format": "json",
            }
        }

        mock_context.return_value = mock_airflow_context
        mock_fetch_http.return_value = pd.DataFrame({"test": [1]})

        ingest_data(config)

        # Verify URL parameter
        call_args = mock_fetch_http.call_args
        assert call_args.kwargs["url"] == "https://example.com/api/v1/data"

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("rlam_airflow_framework.taskflow_tasks.fetch_http_data")
    @patch("rlam_airflow_framework.taskflow_tasks.kafka_publisher")
    def test_correlation_id_generation(
        self, mock_kafka, mock_fetch_http, mock_context, rest_api_config, mock_airflow_context, mock_temp_dir
    ):
        """Test that correlation_id is properly generated and passed."""
        from rlam_airflow_framework.taskflow_tasks import ingest_data

        mock_context.return_value = mock_airflow_context
        mock_fetch_http.return_value = pd.DataFrame({"data": [1]})

        ingest_data(rest_api_config)

        # Verify correlation_id is generated and passed
        call_args = mock_fetch_http.call_args
        correlation_id = call_args.kwargs["correlation_id"]
        assert correlation_id is not None
        assert "test_dag" in correlation_id
        assert "test_run_123" in correlation_id
        assert "test_task" in correlation_id

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("airflow.sdk.Variable")
    def test_ingest_data_incremental_watermark_filtering(self, mock_variable, mock_context, tmp_path):
        from unittest.mock import MagicMock
        mock_context.return_value = {
            "dag": MagicMock(dag_id="test_dag"),
            "task": MagicMock(task_id="test_task"),
            "run_id": "test_run_123"
        }
        mock_variable.get.return_value = "2024-01-05T00:00:00"
        
        config = {
            "metadata": {"dag_id": "test_dag", "run_id": "test_run_123", "task_id": "test_task"},
            "data_source": {
                "name": "test",
                "type": "rest_api",
                "endpoint": "https://example.com/api/v1/data?ts={{ watermark }}",
                "response_format": "json"
            },
            "incremental": {
                "enabled": True,
                "watermark_column": "updated_at",
                "initial_watermark": "2024-01-01T00:00:00"
            }
        }
        
        with patch("rlam_airflow_framework.taskflow_tasks.fetch_http_data") as mock_fetch:
            import pandas as pd
            df = pd.DataFrame({"updated_at": ["2024-01-06T00:00:00"], "id": [1]})
            mock_fetch.return_value = df
            
            from rlam_airflow_framework.taskflow_tasks import ingest_data
            ingest_data(config)
            
            # The watermark from the Variable should be used
            mock_fetch.assert_called_once()
            called_url = mock_fetch.call_args.kwargs.get("url")
            assert "2024-01-05T00:00:00" in called_url

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("airflow.sdk.Variable")
    def test_load_data_advances_watermark(self, mock_variable, mock_context, tmp_path):
        from unittest.mock import MagicMock
        mock_context.return_value = {
            "dag": MagicMock(dag_id="test_dag"),
            "task": MagicMock(task_id="test_task"),
            "run_id": "test_run_123"
        }
        from rlam_airflow_framework.taskflow_tasks import load_data
        
        import pandas as pd
        # Create a test dataframe with watermark column
        df = pd.DataFrame({
            "updated_at": [
                pd.Timestamp("2024-01-01T00:00:00"),
                pd.Timestamp("2024-01-10T00:00:00")
            ],
            "amount": [100, 200]
        })
        
        df_path = str(tmp_path / "test_load_watermark_test_run_123.parquet")
        df.to_parquet(df_path)
        
        config = {
            "metadata": {"dag_id": "test_dag", "run_id": "test_run_123", "task_id": "test_task", "data_path": df_path},
            "destination": {
                "primary": {"type": "print_logs"}
            },
            "incremental": {
                "enabled": True,
                "watermark_column": "updated_at"
            }
        }
        
        res = load_data(df_path, config)
        
        assert "Primary: Printed 2 rows to logs" in res
        
        # Verify watermark was updated
        assert mock_variable.set.call_count == 2
        calls = mock_variable.set.call_args_list
        # The first call sets the watermark
        assert calls[0].args[0] == "test_dag.high_watermark"
        assert "2024-01-10T00:00:00" in calls[0].args[1]
        # The second call sets first_run_completed
        assert calls[1].args[0] == "test_dag.first_run_completed"
        assert calls[1].args[1] == "true"


class TestWatermarkSafetyAndPartitionEvents:
    """Unit coverage for behavior previously only verified manually in the
    3B.4 live e2e (2026-07-23): the watermark retry-safety invariant, the
    strict-mode posture, and partition_key threading into Kafka events."""

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("airflow.sdk.Variable")
    def test_failed_load_does_not_advance_watermark(self, mock_variable, mock_context, tmp_path):
        """3A.2 invariant: a load failure must leave the stored watermark untouched."""
        from rlam_airflow_framework.taskflow_tasks import load_data

        mock_context.return_value = {
            "dag": MagicMock(dag_id="test_dag"),
            "task": MagicMock(task_id="test_task"),
            "run_id": "test_run_123",
        }

        df = pd.DataFrame({"id": [9, 10], "v": ["i", "j"]})
        df_path = str(tmp_path / "wm_fail_test.parquet")
        df.to_parquet(df_path)

        config = {
            "destination": {"primary": {"type": "print_logs"}},
            "incremental": {"enabled": True, "watermark_column": "id"},
        }

        with patch(
            "rlam_airflow_framework.taskflow_tasks._load_to_destination",
            side_effect=RuntimeError("destination unavailable"),
        ), patch("rlam_airflow_framework.taskflow_tasks.kafka_publisher"):
            with pytest.raises(RuntimeError, match="destination unavailable"):
                load_data(df_path, config)

        # The Variable write happens only after ALL destinations succeed
        mock_variable.set.assert_not_called()

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("airflow.sdk.Variable")
    def test_strict_mode_raises_when_watermark_lost_after_first_run(
        self, mock_variable, mock_context, mock_temp_dir
    ):
        """3A.4: strict mode must refuse a silent full load once a first run completed."""
        from rlam_airflow_framework.taskflow_tasks import ingest_data

        mock_context.return_value = {
            "dag": MagicMock(dag_id="test_dag"),
            "task": MagicMock(task_id="test_task"),
            "run_id": "test_run_123",
        }

        def variable_get(key, default=None):
            if key == "test_dag.high_watermark":
                return None  # watermark lost
            if key == "test_dag.first_run_completed":
                return "true"  # but a first run definitely completed
            return default

        mock_variable.get.side_effect = variable_get

        config = {
            "data_source": {"name": "t", "type": "rest_api", "endpoint": "https://x/api"},
            "incremental": {
                "enabled": True,
                "watermark_column": "id",
                "initial_watermark": "0",
                "strict": True,
            },
        }

        with patch("rlam_airflow_framework.taskflow_tasks.kafka_publisher"):
            with pytest.raises(ValueError, match="strict mode"):
                ingest_data(config)

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("airflow.sdk.Variable")
    @patch("rlam_airflow_framework.taskflow_tasks.fetch_http_data")
    @patch("rlam_airflow_framework.taskflow_tasks.kafka_publisher")
    def test_non_strict_missing_watermark_full_loads_from_initial(
        self, mock_kafka, mock_fetch, mock_variable, mock_context, mock_temp_dir
    ):
        """3A.4: non-strict missing watermark falls back to a full load from initial_watermark."""
        from rlam_airflow_framework.taskflow_tasks import ingest_data

        mock_context.return_value = {
            "dag": MagicMock(dag_id="test_dag"),
            "task": MagicMock(task_id="test_task"),
            "run_id": "test_run_123",
        }
        mock_variable.get.return_value = None
        mock_fetch.return_value = pd.DataFrame({"id": [1, 2], "v": ["a", "b"]})

        config = {
            "data_source": {
                "name": "t",
                "type": "rest_api",
                "endpoint": "https://x/api?since={{ watermark }}",
            },
            "incremental": {
                "enabled": True,
                "watermark_column": "id",
                "initial_watermark": "0",
            },
        }

        result = ingest_data(config)
        assert result.endswith(".parquet")
        # Fetch used the initial watermark in the templated URL
        assert mock_fetch.call_args.kwargs["url"] == "https://x/api?since=0"

    @patch("rlam_airflow_framework.taskflow_tasks.get_current_context")
    @patch("rlam_airflow_framework.taskflow_tasks.fetch_http_data")
    @patch("rlam_airflow_framework.taskflow_tasks.kafka_publisher")
    def test_partition_key_threaded_into_kafka_event_metadata(
        self, mock_kafka, mock_fetch, mock_context, mock_temp_dir
    ):
        """2A.4.2: partitioned ingest events must carry partition_key in metadata
        (verified live 2026-07-23; this locks the contract)."""
        from rlam_airflow_framework.taskflow_tasks import ingest_data

        mock_context.return_value = {
            "dag": MagicMock(dag_id="test_dag"),
            "task": MagicMock(task_id="test_task"),
            "run_id": "test_run_123",
            "partition_key": "2026-07-22",
            "partition_date": None,
        }
        mock_fetch.return_value = pd.DataFrame({"id": [1], "event_date": ["2026-07-22"]})

        config = {
            "data_source": {"name": "t", "type": "rest_api", "endpoint": "https://x/data"},
            "partition": {"enabled": True, "column": "event_date"},
        }

        ingest_data(config)

        events = {
            c.kwargs["event_type"]: c.kwargs
            for c in mock_kafka.publish_pipeline_event.call_args_list
        }
        assert events["ingestion_started"]["metadata"] == {"partition_key": "2026-07-22"}
        completed_md = events["ingestion_completed"]["metadata"]
        assert completed_md["partition_key"] == "2026-07-22"
        assert completed_md["row_count"] == 1
        # And the fetch itself was partition-scoped via the request param
        assert mock_fetch.call_args.kwargs["params"] == {"event_date": "2026-07-22"}
