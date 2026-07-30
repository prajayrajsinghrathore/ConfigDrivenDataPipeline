"""
Shared pytest fixtures and configuration for Config-Driven Data Pipeline tests.
"""

import os
import sys
import pytest
import polars as pl
from pathlib import Path
from unittest.mock import MagicMock

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
# No need to add dags/ since we're using the installed rlam_airflow_framework package

# NOTE (2026-07-23): the former `clean_sys_modules_for_non_unit_tests` autouse
# fixture (a per-test sys.modules save/delete/restore dance) was removed. It
# corrupted per-class registries (PyYAML constructors used by
# airflow.configuration) whenever real Airflow ran in the same process. Lanes
# with incompatible airflow.* expectations now run in separate pytest
# invocations instead:
#   pytest tests/unit tests/contract -q   # mocked lane
#   pytest tests/dag -q                   # real-Airflow lane
#   pytest tests/integration tests/e2e -q # real lane vs live stack
# tests/dag/conftest.py skips its lane loudly if unit mocks are detected.

# =============================================================================
# ENVIRONMENT SETUP
# =============================================================================


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up test environment variables."""
    os.environ.setdefault("AIRFLOW_HOME", str(PROJECT_ROOT / "tests" / "airflow_home"))
    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    yield


# =============================================================================
# PATH FIXTURES
# =============================================================================


@pytest.fixture
def project_root() -> Path:
    """Return the project root path."""
    return PROJECT_ROOT


@pytest.fixture
def config_dir(project_root) -> Path:
    """Return the config directory path."""
    return project_root / "config"


@pytest.fixture
def data_sources_dir(config_dir) -> Path:
    """Return the data sources config directory."""
    return config_dir / "data_sources"


@pytest.fixture
def schemas_dir(config_dir) -> Path:
    """Return the schemas directory."""
    return config_dir / "schemas"


# =============================================================================
# SAMPLE DATA FIXTURES
# =============================================================================


@pytest.fixture
def sample_dataframe() -> pl.DataFrame:
    """Create a sample DataFrame for testing transformations."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "price": [10.50, 20.00, 15.75, 30.25, 25.00],
            "quantity": [2, 3, 1, 4, 2],
            "category": ["A", "B", "A", "C", "B"],
            "timestamp": pl.Series(
                ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"]
            ).str.to_datetime(),
        }
    )


@pytest.fixture
def sample_dataframe_with_nulls() -> pl.DataFrame:
    """Create a sample DataFrame with null values for data quality testing."""
    return pl.DataFrame(
        {
            "id": [1, 2, None, 4, 5],
            "name": ["Alice", None, "Charlie", "David", "Eve"],
            "price": [10.50, -20.00, 15.75, None, 25.00],
            "email": [
                "alice@test.com",
                "invalid-email",
                "charlie@test.com",
                "david@test.com",
                None,
            ],
            "age": [25, 150, 30, -5, 35],  # Contains invalid ages
        }
    )


@pytest.fixture
def sample_market_data() -> pl.DataFrame:
    """Create sample market data for pipeline testing."""
    return pl.DataFrame(
        {
            "symbol": ["AAPL", "GOOGL", "MSFT", "AMZN", "META"],
            "open": [150.00, 2800.00, 300.00, 3400.00, 330.00],
            "high": [155.00, 2850.00, 305.00, 3450.00, 335.00],
            "low": [148.00, 2780.00, 298.00, 3380.00, 325.00],
            "close": [152.00, 2820.00, 302.00, 3420.00, 332.00],
            "volume": [1000000, 500000, 800000, 600000, 700000],
        }
    )


@pytest.fixture
def sample_api_response() -> dict:
    """Sample API response for testing data fetchers."""
    return {
        "status": "success",
        "data": [
            {"id": 1, "value": "test1"},
            {"id": 2, "value": "test2"},
        ],
        "metadata": {
            "total": 2,
            "page": 1,
        },
    }


# =============================================================================
# CONFIG FIXTURES
# =============================================================================


@pytest.fixture
def sample_data_source_config() -> dict:
    """Sample data source configuration for testing."""
    return {
        "name": "test_source",
        "description": "Test data source",
        "source_type": "api",
        "schedule": "@daily",
        "api": {
            "url": "https://api.example.com/data",
            "method": "GET",
            "headers": {
                "Content-Type": "application/json",
            },
            "response_type": "json",
            "data_path": "data",
        },
        "transformations": [
            {
                "type": "rename_columns",
                "mapping": {"old_name": "new_name"},
            },
            {
                "type": "add_formula_column",
                "column_name": "calculated",
                "formula": "price * quantity",
            },
        ],
        "data_quality": {
            "checks": [
                {"type": "not_null", "column": "id"},
                {"type": "positive", "column": "price"},
            ]
        },
        "output": {
            "type": "kafka",
            "topic": "test-topic",
        },
    }


@pytest.fixture
def sample_transformation_config() -> list:
    """Sample transformation configuration."""
    return [
        {"type": "rename_columns", "mapping": {"old": "new"}},
        {"type": "filter_rows", "condition": "price > 0"},
        {
            "type": "add_formula_column",
            "column_name": "total",
            "formula": "price * quantity",
        },
        {"type": "select_columns", "columns": ["id", "name", "total"]},
    ]


# =============================================================================
# MOCK FIXTURES
# =============================================================================


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka producer for testing."""
    producer = MagicMock()
    producer.send.return_value.get.return_value = MagicMock(
        topic="test-topic",
        partition=0,
        offset=1,
    )
    return producer


@pytest.fixture
def mock_http_response():
    """Mock HTTP response for testing API fetchers."""
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"data": [{"id": 1, "value": "test"}]}
    response.text = '{"data": [{"id": 1, "value": "test"}]}'
    response.raise_for_status = MagicMock()
    return response


@pytest.fixture
def mock_airflow_context():
    """Mock Airflow task context."""
    return {
        "ds": "2026-02-02",
        "ds_nodash": "20260202",
        "execution_date": "2026-02-02T00:00:00+00:00",
        "dag_run": MagicMock(conf={}),
        "task_instance": MagicMock(),
        "params": {},
    }


# =============================================================================
# EXTERNAL SERVICE MOCK FIXTURES
# =============================================================================


@pytest.fixture
def mock_snowflake_hook():
    """
    Mock Snowflake hook for testing database operations.

    Usage:
        def test_something(mock_snowflake_hook):
            mock_snowflake_hook.get_polars_df.return_value = pl.DataFrame(...)
            mock_snowflake_hook.run.return_value = None
    """
    hook = MagicMock()
    hook.get_polars_df.return_value = pl.DataFrame()
    hook.run.return_value = None
    hook.get_conn.return_value = MagicMock()
    return hook


@pytest.fixture
def mock_snowflake_hook_with_data():
    """
    Mock Snowflake hook with sample lookup data.

    Returns a hook that returns sample instrument data.
    """
    hook = MagicMock()
    hook.get_polars_df.return_value = pl.DataFrame(
        {
            "INSTRUMENT_ID": ["INS001", "INS002", "INS003"],
            "ISIN": ["US0378331005", "US5949181045", "US02079K1079"],
            "INSTRUMENT_NAME": ["Apple Inc", "Microsoft Corp", "Alphabet Inc"],
            "SECTOR": ["Technology", "Technology", "Technology"],
        }
    )
    hook.run.return_value = None
    return hook


@pytest.fixture
def mock_azure_data_lake_hook():
    """
    Mock Azure Data Lake hook for testing ADLS operations.

    Usage:
        def test_upload(mock_azure_data_lake_hook):
            # Test upload functionality
    """
    hook = MagicMock()
    hook.upload_file.return_value = None
    hook.check_for_file.return_value = True
    hook.download_file.return_value = b"file content"
    hook.list_file_names.return_value = ["file1.csv", "file2.csv"]
    return hook


@pytest.fixture
def mock_azure_blob_hook():
    """
    Mock Azure Blob Storage hook for testing blob operations.

    Usage:
        def test_blob_upload(mock_azure_blob_hook):
            # Test blob upload functionality
    """
    hook = MagicMock()
    hook.load_string.return_value = None
    hook.load_file.return_value = None
    hook.read_file.return_value = "file content"
    hook.check_for_blob.return_value = True
    hook.get_blobs_list.return_value = ["blob1", "blob2"]
    return hook


@pytest.fixture
def mock_kafka_producer_success():
    """
    Mock Kafka producer that simulates successful message publishing.

    Usage:
        def test_publish(mock_kafka_producer_success):
            producer = mock_kafka_producer_success
            # Test publishing
    """
    producer = MagicMock()
    future = MagicMock()
    future.get.return_value = MagicMock(
        topic="test-topic",
        partition=0,
        offset=1,
    )
    producer.send.return_value = future
    producer.flush.return_value = None
    producer.close.return_value = None
    return producer


@pytest.fixture
def mock_kafka_producer_failure():
    """
    Mock Kafka producer that simulates message publishing failure.

    Usage:
        def test_publish_failure(mock_kafka_producer_failure):
            producer = mock_kafka_producer_failure
            # Test error handling
    """
    producer = MagicMock()
    producer.send.side_effect = Exception("Kafka connection failed")
    return producer


@pytest.fixture
def mock_kafka_publisher():
    """
    Mock kafka_publisher module for testing DQ metrics publishing.

    Usage:
        @patch("utils.data_quality.kafka_publisher")
        def test_metrics(mock_kafka):
            # Test DQ metrics publishing
    """
    publisher = MagicMock()
    publisher.publish_quality_metrics.return_value = None
    publisher.publish_data.return_value = None
    publisher.publish_event.return_value = None
    return publisher


@pytest.fixture
def mock_sftp_client():
    """
    Mock SFTP client for testing file transfers.

    Usage:
        def test_sftp_download(mock_sftp_client):
            # Test SFTP operations
    """

    sftp = MagicMock()

    # Simulate file download
    def mock_getfo(remote_path, file_obj):
        file_obj.write(b"id,name,value\n1,test,100\n2,test2,200\n")

    sftp.getfo.side_effect = mock_getfo
    sftp.listdir.return_value = ["file1.csv", "file2.csv"]
    sftp.stat.return_value = MagicMock(st_size=1024)
    return sftp


@pytest.fixture
def mock_ssh_client(mock_sftp_client):
    """
    Mock SSH client that returns mock SFTP client.

    Usage:
        def test_ssh_connection(mock_ssh_client):
            # Test SSH/SFTP connection
    """
    ssh = MagicMock()
    ssh.open_sftp.return_value = mock_sftp_client
    ssh.connect.return_value = None
    ssh.close.return_value = None
    return ssh


@pytest.fixture
def mock_requests_get_success():
    """
    Mock requests.get for successful API calls.

    Usage:
        @patch("requests.get")
        def test_api_call(mock_get, mock_requests_get_success):
            mock_get.return_value = mock_requests_get_success
    """
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "data": [
            {"id": 1, "name": "item1", "value": 100},
            {"id": 2, "name": "item2", "value": 200},
        ],
        "metadata": {"total": 2, "page": 1},
    }
    response.text = '{"data": [{"id": 1}, {"id": 2}]}'
    response.content = b'{"data": [{"id": 1}, {"id": 2}]}'
    response.raise_for_status = MagicMock()
    response.headers = {"Content-Type": "application/json"}
    return response


@pytest.fixture
def mock_requests_get_failure():
    """
    Mock requests.get for failed API calls.

    Usage:
        @patch("requests.get")
        def test_api_failure(mock_get, mock_requests_get_failure):
            mock_get.return_value = mock_requests_get_failure
    """
    response = MagicMock()
    response.status_code = 500
    response.json.side_effect = Exception("Invalid JSON")
    response.text = "Internal Server Error"
    response.raise_for_status.side_effect = Exception("500 Server Error")
    return response


@pytest.fixture
def mock_soda_scan():
    """
    Mock Soda Core Scan for testing data quality.

    Usage:
        @patch("utils.data_quality.Scan")
        def test_dq_scan(mock_scan_class, mock_soda_scan):
            mock_scan_class.return_value = mock_soda_scan
    """
    scan = MagicMock()
    scan.set_scan_definition_name.return_value = None
    scan.set_data_source_name.return_value = None
    scan.add_polars_dataframe.return_value = None
    scan.add_sodacl_yaml_str.return_value = None
    scan.execute.return_value = None
    scan.get_scan_results.return_value = {
        "checks": [
            {"name": "row_count > 0", "outcome": "pass", "diagnostics": {}},
            {"name": "missing_count(id) = 0", "outcome": "pass", "diagnostics": {}},
        ]
    }
    return scan


@pytest.fixture
def mock_soda_scan_with_failures():
    """
    Mock Soda Core Scan that returns failed checks.
    """
    scan = MagicMock()
    scan.set_scan_definition_name.return_value = None
    scan.set_data_source_name.return_value = None
    scan.add_polars_dataframe.return_value = None
    scan.add_sodacl_yaml_str.return_value = None
    scan.execute.return_value = None
    scan.get_scan_results.return_value = {
        "checks": [
            {
                "name": "row_count > 100",
                "outcome": "fail",
                "diagnostics": {"actual": 5},
            },
            {
                "name": "missing_count(id) = 0",
                "outcome": "fail",
                "diagnostics": {"missing": 3},
            },
        ]
    }
    return scan


# =============================================================================
# DATA QUALITY TEST FIXTURES
# =============================================================================


@pytest.fixture
def sample_dq_config() -> dict:
    """Sample data quality configuration for testing."""
    return {
        "data_source": {"name": "test_source"},
        "validation": {
            "soda_checks": {
                "checks": [
                    {"type": "row_count", "min": 1},
                    {"type": "missing_count", "column": "id", "max": 0},
                    {"type": "duplicate_count", "column": "id", "max": 0},
                ]
            },
            "quality_gates": {
                "fail_threshold": 0.5,
                "warn_threshold": 0.9,
                "quarantine_invalid": True,
            },
        },
        "event": {"topic": "test-topic"},
    }


@pytest.fixture
def sample_dataframe_for_dq() -> pl.DataFrame:
    """Sample DataFrame for data quality testing with various issues."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3, None, 5],
            "name": ["Alice", "Bob", None, "David", "Eve"],
            "email": ["a@test.com", "invalid", "c@test.com", "d@test.com", None],
            "price": [10.0, -5.0, 15.0, 20.0, 25.0],  # -5.0 is invalid
            "status": ["A", "B", "X", "A", "B"],  # X might be invalid
            "timestamp": pl.Series(
                [
                    "2026-02-01T00:00:00",
                    "2026-02-01T01:00:00",
                    "2026-02-01T02:00:00",
                    "2026-02-01T03:00:00",
                    "2026-02-01T04:00:00",
                ]
            ).str.to_datetime(),
        }
    )


# =============================================================================
# CLEANUP FIXTURES
# =============================================================================


@pytest.fixture
def temp_config_file(tmp_path):
    """Create a temporary config file for testing."""

    def _create_config(content: str, filename: str = "test_config.yaml"):
        config_path = tmp_path / filename
        config_path.write_text(content)
        return config_path

    return _create_config


# =============================================================================
# INTEGRATION TEST FIXTURES (requires Docker)
# =============================================================================


@pytest.fixture(scope="session")
def docker_services_available():
    """Check if Docker services are available for integration tests."""
    import socket

    # Use 127.0.0.1 instead of localhost for Windows Docker compatibility
    services = {
        "kafka": ("127.0.0.1", 9092),
        "postgres": ("127.0.0.1", 5432),
        "redis": ("127.0.0.1", 6379),
    }

    available = {}
    for name, (host, port) in services.items():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex((host, port))
        available[name] = result == 0
        sock.close()

    return available


@pytest.fixture
def skip_without_kafka(docker_services_available):
    """Skip test if Kafka is not available."""
    if not docker_services_available.get("kafka", False):
        pytest.skip("Kafka not available")


@pytest.fixture
def skip_without_postgres(docker_services_available):
    """Skip test if PostgreSQL is not available."""
    if not docker_services_available.get("postgres", False):
        pytest.skip("PostgreSQL not available")


# =============================================================================
# AIRFLOW FIXTURES
# =============================================================================


@pytest.fixture(scope="session")
def airflow_home(tmp_path_factory):
    """Create temporary Airflow home for testing."""
    airflow_home = tmp_path_factory.mktemp("airflow_home")
    os.environ["AIRFLOW_HOME"] = str(airflow_home)
    return airflow_home


# =============================================================================
# AIRFLOW 3.1.6 FEATURES FIXTURES
# =============================================================================


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the fixtures directory path."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def airflow_330_config(fixtures_dir) -> dict:
    """
    Load the Airflow 3.1.6 features test config.

    Returns the parsed YAML config with deadline and HITL settings.
    """
    import yaml

    config_path = fixtures_dir / "example_airflow_330_features.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


@pytest.fixture
def deadline_config(airflow_330_config) -> dict:
    """Extract deadline configuration from test config."""
    return airflow_330_config.get("schedule", {}).get("deadline", {})


@pytest.fixture
def hitl_config(airflow_330_config) -> dict:
    """Extract HITL configuration from test config."""
    return (
        airflow_330_config.get("destination", {}).get("quarantine", {}).get("hitl", {})
    )


@pytest.fixture
def mock_dag_run(mocker):
    """
    Mock Airflow DagRun for deadline notifier tests.

    Provides a realistic dag_run object for callback contexts.
    """
    from datetime import datetime, timezone

    dag_run = mocker.MagicMock()
    dag_run.dag_id = "test_dag"
    dag_run.run_id = "manual__2026-02-02T00:00:00+00:00"
    dag_run.logical_date = datetime(2026, 2, 2, 0, 0, 0, tzinfo=timezone.utc)
    dag_run.queued_at = datetime(2026, 2, 2, 0, 0, 0, tzinfo=timezone.utc)
    dag_run.start_date = datetime(2026, 2, 2, 0, 0, 30, tzinfo=timezone.utc)
    return dag_run


@pytest.fixture
def mock_deadline_context(mock_dag_run):
    """
    Create a mock Airflow deadline callback context.

    This simulates the context passed to deadline notifiers.
    """
    from datetime import datetime, timezone

    deadline_time = datetime(2026, 2, 2, 0, 45, 0, tzinfo=timezone.utc)

    return {
        "dag_run": mock_dag_run,
        "deadline": {
            "deadline_time": deadline_time,
            "reference": "dagrun_queued",
            "interval_seconds": 2700,  # 45 minutes
        },
        "task_instance": None,
        "dag": None,
    }


@pytest.fixture
def sample_quarantine_records() -> pl.DataFrame:
    """
    Create sample quarantine records for HITL testing.

    Includes records with various failed checks for approval workflow testing.
    """
    import json
    from datetime import datetime, timezone

    return pl.DataFrame(
        {
            "quarantine_id": ["q1", "q2", "q3"],
            "source_pipeline": ["test_pipeline", "test_pipeline", "test_pipeline"],
            "source_table": ["TEST.DATA", "TEST.DATA", "TEST.DATA"],
            "failed_checks": [
                json.dumps(["null_check", "range_check"]),
                json.dumps(["range_check"]),
                json.dumps(["null_check"]),
            ],
            "record_data": [
                json.dumps({"id": 1, "name": None, "value": -5}),
                json.dumps({"id": 2, "name": "Bob", "value": 999999}),
                json.dumps({"id": 3, "name": None, "value": 100}),
            ],
            "quarantined_at": [
                datetime.now(timezone.utc).isoformat(),
                datetime.now(timezone.utc).isoformat(),
                datetime.now(timezone.utc).isoformat(),
            ],
            "reprocessed": [False, False, False],
            "reprocessed_at": [None, None, None],
            "approval_status": ["pending", "pending", "pending"],
            "approved_by": [None, None, None],
            "approved_at": [None, None, None],
        }
    )


@pytest.fixture
def mock_kafka_publish_success(mocker):
    """
    Mock kafka_publisher with successful publish.

    Use with mocker fixture for pytest-mock style mocking.
    """
    mock_publisher = mocker.patch("utils.kafka_publisher.kafka_publisher")
    mock_publisher.publish_pipeline_event.return_value = True
    mock_publisher.publish_data.return_value = True
    return mock_publisher


@pytest.fixture
def mock_kafka_publish_failure(mocker):
    """
    Mock kafka_publisher with failing publish.

    Use with mocker fixture for pytest-mock style mocking.
    """
    mock_publisher = mocker.patch("utils.kafka_publisher.kafka_publisher")
    mock_publisher.publish_pipeline_event.side_effect = Exception(
        "Kafka connection failed"
    )
    return mock_publisher


@pytest.fixture
def mock_email_send(mocker):
    """
    Mock Airflow email sending for deadline notifier tests.
    """
    return mocker.patch("airflow.utils.email.send_email")
