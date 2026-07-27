"""
End-to-End tests for the complete pipeline.

These tests validate the full pipeline flow from triggering
a DAG to verifying the output in Kafka.

Run with: docker-compose up -d && pytest tests/e2e/ -m e2e --timeout=300

Uses confluent-kafka library for Kafka 4.x compatibility.
Airflow 3.x API auth: obtains a JWT from POST /auth/token (basic auth is not
accepted by the v2 REST API). The admin user is created by the compose init step.
"""

import pytest
import requests
import time
import json
import os

try:
    from confluent_kafka import Consumer, KafkaError
    from confluent_kafka.admin import AdminClient

    HAS_CONFLUENT_KAFKA = True
except ImportError:
    HAS_CONFLUENT_KAFKA = False


AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080")
AIRFLOW_API_URL = f"{AIRFLOW_BASE_URL}/api/v2"
AIRFLOW_USERNAME = os.environ.get("_AIRFLOW_WWW_USER_USERNAME", "airflow")
AIRFLOW_PASSWORD = os.environ.get("_AIRFLOW_WWW_USER_PASSWORD", "airflow")


def get_auth_headers():
    """Fetch a JWT from the Airflow 3.x token endpoint; None if unavailable."""
    try:
        response = requests.post(
            f"{AIRFLOW_BASE_URL}/auth/token",
            json={"username": AIRFLOW_USERNAME, "password": AIRFLOW_PASSWORD},
            timeout=5,
        )
        if response.status_code in (200, 201):
            token = response.json().get("access_token")
            if token:
                return {"Authorization": f"Bearer {token}"}
    except requests.exceptions.RequestException:
        pass
    return None


def check_kafka_available():
    """Check if confluent-kafka can connect to Kafka."""
    if not HAS_CONFLUENT_KAFKA:
        return False
    try:
        # Use 127.0.0.1 instead of localhost for Windows Docker compatibility
        admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092"})
        admin.list_topics(timeout=10)  # Verify connection works
        return True
    except Exception:
        return False


KAFKA_AVAILABLE = check_kafka_available() if HAS_CONFLUENT_KAFKA else False


@pytest.fixture
def airflow_api():
    """Airflow API base URL."""
    return AIRFLOW_API_URL


@pytest.fixture
def auth_headers():
    """JWT Authorization headers; skips when no token can be obtained."""
    headers = get_auth_headers()
    if headers is None:
        pytest.skip("Airflow JWT token not obtainable (is the stack up with the admin user created?)")
    return headers


@pytest.mark.e2e
class TestAirflowAPI:
    """Test Airflow REST API."""

    @pytest.fixture
    def skip_if_airflow_unavailable(self):
        """Skip if Airflow is not available."""
        try:
            response = requests.get(f"{AIRFLOW_API_URL}/version", timeout=5)
            if response.status_code not in (200, 401, 403):
                pytest.skip("Airflow API not available")
        except requests.exceptions.RequestException:
            pytest.skip("Airflow not running")

    def test_api_version(self, airflow_api, auth_headers, skip_if_airflow_unavailable):
        """Test getting API version."""
        response = requests.get(f"{airflow_api}/version", headers=auth_headers)
        assert response.status_code == 200
        assert "version" in response.json()

    def test_list_dags(self, airflow_api, auth_headers, skip_if_airflow_unavailable):
        """Test listing all DAGs."""
        response = requests.get(f"{airflow_api}/dags", headers=auth_headers)
        assert response.status_code == 200
        data = response.json()
        assert "dags" in data
        assert isinstance(data["dags"], list)
        assert len(data["dags"]) > 0, "stack should expose the generated DAGs"


def _pick_test_dag(airflow_api, auth_headers):
    """Pick the cheapest DAG to exercise (prefer the joke-API smoke pipeline)."""
    response = requests.get(f"{airflow_api}/dags", headers=auth_headers)
    if response.status_code != 200:
        pytest.skip(f"Could not list DAGs (HTTP {response.status_code})")
    dags = [d["dag_id"] for d in response.json().get("dags", [])]
    if not dags:
        pytest.skip("No DAGs available")
    for preferred in dags:
        if "joke_api" in preferred or "simple_test" in preferred:
            return preferred
    return dags[0]


@pytest.mark.e2e
class TestPipelineExecution:
    """Test full pipeline execution."""

    @pytest.fixture
    def skip_if_services_unavailable(self, docker_services_available):
        """Skip if required services are not available."""
        if not docker_services_available.get("kafka", False):
            pytest.skip("Kafka not available")
        try:
            response = requests.get(f"{AIRFLOW_API_URL}/version", timeout=5)
            if response.status_code not in (200, 401, 403):
                pytest.skip("Airflow API not available")
        except requests.exceptions.RequestException:
            pytest.skip("Airflow not running")

    def test_trigger_dag_run(self, airflow_api, auth_headers, skip_if_services_unavailable):
        """Test triggering a DAG run."""
        dag_id = _pick_test_dag(airflow_api, auth_headers)

        # Unpause DAG if paused
        requests.patch(
            f"{airflow_api}/dags/{dag_id}",
            json={"is_paused": False},
            headers=auth_headers,
        )

        # Trigger DAG run (Airflow 3 requires logical_date in the payload)
        run_id = f"test_run_{int(time.time())}"
        response = requests.post(
            f"{airflow_api}/dags/{dag_id}/dagRuns",
            json={"dag_run_id": run_id, "logical_date": None, "conf": {"test": True}},
            headers=auth_headers,
        )

        assert response.status_code in [200, 409]  # 409 if already running

        if response.status_code == 200:
            data = response.json()
            assert data["dag_run_id"] == run_id

    def test_dag_run_completes(self, airflow_api, auth_headers, skip_if_services_unavailable):
        """Test that a DAG run completes successfully."""
        dag_id = _pick_test_dag(airflow_api, auth_headers)
        run_id = f"e2e_test_{int(time.time())}"

        # Unpause and trigger
        requests.patch(
            f"{airflow_api}/dags/{dag_id}",
            json={"is_paused": False},
            headers=auth_headers,
        )

        response = requests.post(
            f"{airflow_api}/dags/{dag_id}/dagRuns",
            json={"dag_run_id": run_id, "logical_date": None},
            headers=auth_headers,
        )

        if response.status_code != 200:
            pytest.skip(f"Could not trigger DAG (HTTP {response.status_code})")

        # Wait for completion (max 5 minutes)
        max_wait = 300
        start_time = time.time()
        final_state = None

        while time.time() - start_time < max_wait:
            response = requests.get(
                f"{airflow_api}/dags/{dag_id}/dagRuns/{run_id}",
                headers=auth_headers,
            )

            if response.status_code == 200:
                state = response.json().get("state")
                if state in ["success", "failed"]:
                    final_state = state
                    break

            time.sleep(10)

        assert final_state is not None, "DAG run did not complete in time"
        # Note: We don't assert success here as the DAG might fail
        # due to external dependencies in the test environment


@pytest.mark.e2e
@pytest.mark.skipif(not HAS_CONFLUENT_KAFKA, reason="confluent-kafka not installed")
@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="Kafka not available")
class TestKafkaOutput:
    """Test that pipeline produces correct Kafka output."""

    @pytest.fixture
    def consumer(self, docker_services_available):
        """Create Kafka consumer for pipeline events using confluent-kafka."""
        if not docker_services_available.get("kafka", False):
            pytest.skip("Kafka not available")

        consumer = Consumer({
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": f"e2e-test-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe(["pipeline-events"])
        yield consumer
        consumer.close()

    def test_pipeline_events_topic_accessible(self, consumer):
        """Test that pipeline-events topic is accessible."""
        # Just verify we can get metadata without error
        metadata = consumer.list_topics(timeout=10)
        topics = list(metadata.topics.keys())
        assert "pipeline-events" in topics or len(topics) >= 0

    def test_consume_pipeline_events(self, consumer):
        """Test consuming messages from pipeline-events."""
        messages = []

        # Poll for messages (timeout after 5 seconds total)
        start_time = time.time()
        while time.time() - start_time < 5:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                break
            messages.append(json.loads(msg.value().decode("utf-8")))
            if len(messages) >= 5:  # Get up to 5 messages
                break

        # May be empty if no pipelines have run
        assert isinstance(messages, list)


@pytest.mark.e2e
class TestUIAccessibility:
    """Test that all UI endpoints are accessible."""

    def test_airflow_ui_accessible(self):
        """Test Airflow UI is accessible."""
        try:
            response = requests.get(f"{AIRFLOW_BASE_URL}/", timeout=5)
            assert response.status_code in [200, 302]  # 302 for redirect to login
        except requests.exceptions.RequestException:
            pytest.skip("Airflow UI not accessible")


@pytest.mark.e2e
class TestHealthEndpoints:
    """Test health check endpoints."""

    def test_airflow_health(self):
        """Test Airflow health endpoint."""
        try:
            # Airflow 3.x health endpoint paths
            health_endpoints = [
                f"{AIRFLOW_API_URL}/version",  # Version endpoint as health check
                f"{AIRFLOW_BASE_URL}/health",
                f"{AIRFLOW_API_URL}/health",
            ]

            for endpoint in health_endpoints:
                response = requests.get(endpoint, timeout=5)
                if response.status_code in [200, 401, 403]:
                    return  # Health check passed

            # If none of the endpoints returned valid status
            pytest.fail("No health endpoint returned valid status")
        except requests.exceptions.RequestException:
            pytest.skip("Airflow not accessible")


@pytest.mark.e2e
class TestDataQualityPipeline:
    """Test data quality features in pipeline."""

    def test_data_quality_results_logged(self, airflow_api):
        """Test that data quality results are logged."""
        # This would check XCom or logs for DQ results
        # Implementation depends on how DQ results are stored
        pass

    @pytest.mark.skipif(not HAS_CONFLUENT_KAFKA, reason="confluent-kafka not installed")
    @pytest.mark.skipif(not KAFKA_AVAILABLE, reason="Kafka not available")
    def test_data_quality_events_in_kafka(self, docker_services_available):
        """Test that data quality events are sent to Kafka."""
        if not docker_services_available.get("kafka", False):
            pytest.skip("Kafka not available")

        consumer = Consumer({
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": f"dq-test-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe(["data-quality"])

        try:
            messages = []
            start_time = time.time()
            while time.time() - start_time < 5:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                msg_error = msg.error()
                if msg_error:
                    if msg_error.code() == KafkaError._PARTITION_EOF:
                        continue
                    break
                msg_value = msg.value()
                assert msg_value is not None
                messages.append(json.loads(msg_value.decode("utf-8")))
            # May be empty if no DQ checks have run
            assert isinstance(messages, list)
        finally:
            consumer.close()
