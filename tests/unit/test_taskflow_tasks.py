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
