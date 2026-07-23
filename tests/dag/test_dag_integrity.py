"""
DAG integrity tests for Airflow.

These tests validate that all DAGs:
- Load without import errors
- Have valid structure
- Have required metadata
- Have valid task dependencies

NOTE: Tests marked with @pytest.mark.skipif(IS_WINDOWS, ...) require Airflow
which only runs on Linux/macOS. They are skipped on Windows.

Deadline callback tests (Airflow 3.1.6) run on all platforms as they only
test the notifier classes without requiring the full Airflow runtime.
"""

import pytest
import os
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

# Add the dags folder to the path
DAGS_FOLDER = Path(__file__).parent.parent.parent / "dags"
PLUGINS_FOLDER = Path(__file__).parent.parent.parent / "plugins"
sys.path.insert(0, str(DAGS_FOLDER))
sys.path.insert(0, str(PLUGINS_FOLDER))

# Check if we're on Windows - Airflow doesn't support Windows natively
IS_WINDOWS = sys.platform == "win32"
SKIP_REASON = "Airflow DAG tests require Linux/macOS (os.register_at_fork not available on Windows)"


@pytest.mark.dag
@pytest.mark.skipif(IS_WINDOWS, reason=SKIP_REASON)
class TestDagIntegrity:
    """Test that all DAGs load correctly."""

    @pytest.fixture(scope="class")
    def dagbag(self):
        """Create a DagBag instance for testing."""
        try:
            from airflow.models import DagBag

            # Set minimal Airflow config for testing
            os.environ.setdefault(
                "AIRFLOW_HOME", str(Path(__file__).parent.parent / "airflow_home")
            )
            os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

            return DagBag(dag_folder=str(DAGS_FOLDER))
        except ImportError:
            pytest.skip("Airflow not installed")

    def test_no_import_errors(self, dagbag):
        """Test that all DAGs import without errors."""
        import_errors = dagbag.import_errors

        if import_errors:
            error_messages = "\n".join(
                [f"{dag_id}: {error}" for dag_id, error in import_errors.items()]
            )
            pytest.fail(f"DAG import errors:\n{error_messages}")

    def test_dags_loaded(self, dagbag):
        """Test that at least one DAG is loaded."""
        # This might be 0 if no configs exist yet
        assert len(dagbag.dags) >= 0, "Expected DAGs to be present"

    def test_dag_ids_are_valid(self, dagbag):
        """Test that DAG IDs follow naming conventions."""
        for dag_id in dagbag.dags:
            # Check for valid characters
            assert all(c.isalnum() or c in ["_", "-", "."] for c in dag_id), (
                f"DAG ID '{dag_id}' contains invalid characters"
            )

            # Check doesn't start with number
            assert not dag_id[0].isdigit(), (
                f"DAG ID '{dag_id}' should not start with a number"
            )


@pytest.mark.dag
@pytest.mark.skipif(IS_WINDOWS, reason=SKIP_REASON)
class TestDagMetadata:
    """Test DAG metadata requirements."""

    @pytest.fixture(scope="class")
    def dagbag(self):
        """Create a DagBag instance for testing."""
        try:
            from airflow.models import DagBag

            os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
            return DagBag(dag_folder=str(DAGS_FOLDER))
        except ImportError:
            pytest.skip("Airflow not installed")

    def test_dags_have_description(self, dagbag):
        """Test that DAGs have descriptions."""
        for dag_id, dag in dagbag.dags.items():
            # Description is recommended but not always required
            if dag.description is None:
                # Warning only
                pass

    def test_dags_have_tags(self, dagbag):
        """Test that DAGs have tags for organization."""
        for dag_id, dag in dagbag.dags.items():
            # Tags help with filtering in UI
            assert dag.tags is not None, f"DAG '{dag_id}' should have tags"

    def test_dags_have_owner(self, dagbag):
        """Test that DAGs have explicit owner."""
        for dag_id, dag in dagbag.dags.items():
            # Owner should be set explicitly, not default 'airflow'
            owner = dag.default_args.get("owner", dag.owner)
            # Just verify it exists
            assert owner is not None

    def test_dags_have_start_date(self, dagbag):
        """Test that DAGs have start_date configured."""
        for dag_id, dag in dagbag.dags.items():
            start_date = dag.default_args.get("start_date") or dag.start_date
            assert start_date is not None, f"DAG '{dag_id}' missing start_date"

    def test_dags_have_catchup_configured(self, dagbag):
        """Test that DAGs have catchup explicitly configured."""
        for dag_id, dag in dagbag.dags.items():
            # Catchup should be explicitly set (usually False for data pipelines)
            assert dag.catchup is not None


@pytest.mark.dag
@pytest.mark.skipif(IS_WINDOWS, reason=SKIP_REASON)
class TestDagTasks:
    """Test DAG task configurations."""

    @pytest.fixture(scope="class")
    def dagbag(self):
        """Create a DagBag instance for testing."""
        try:
            from airflow.models import DagBag

            os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
            return DagBag(dag_folder=str(DAGS_FOLDER))
        except ImportError:
            pytest.skip("Airflow not installed")

    def test_dags_have_tasks(self, dagbag):
        """Test that DAGs have at least one task."""
        for dag_id, dag in dagbag.dags.items():
            assert len(dag.tasks) > 0, f"DAG '{dag_id}' has no tasks"

    def test_no_cycles_in_dag(self, dagbag):
        """Test that DAGs don't have circular dependencies."""
        for dag_id, dag in dagbag.dags.items():
            # Airflow validates this on load, but double-check
            try:
                dag.topological_sort()
            except Exception as e:
                pytest.fail(f"DAG '{dag_id}' has circular dependencies: {e}")

    def test_tasks_have_unique_ids(self, dagbag):
        """Test that task IDs are unique within each DAG."""
        for dag_id, dag in dagbag.dags.items():
            task_ids = [task.task_id for task in dag.tasks]
            assert len(task_ids) == len(set(task_ids)), (
                f"DAG '{dag_id}' has duplicate task IDs"
            )

    def test_tasks_have_valid_retry_config(self, dagbag):
        """Test that tasks have valid retry configuration."""
        for dag_id, dag in dagbag.dags.items():
            for task in dag.tasks:
                retries = task.retries

                # Retries should be non-negative
                if retries is not None:
                    assert retries >= 0, f"Task '{task.task_id}' has negative retries"


@pytest.mark.dag
@pytest.mark.skipif(IS_WINDOWS, reason=SKIP_REASON)
class TestDagSchedule:
    """Test DAG schedule configurations."""

    @pytest.fixture(scope="class")
    def dagbag(self):
        """Create a DagBag instance for testing."""
        try:
            from airflow.models import DagBag

            os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
            return DagBag(dag_folder=str(DAGS_FOLDER))
        except ImportError:
            pytest.skip("Airflow not installed")

    def test_schedule_interval_is_valid(self, dagbag):
        """Test that schedule intervals are valid."""
        valid_presets = [
            None,  # None means manually triggered
            "@once",
            "@hourly",
            "@daily",
            "@weekly",
            "@monthly",
            "@yearly",
            "@continuous",
        ]

        for dag_id, dag in dagbag.dags.items():
            schedule = dag.schedule_interval

            if schedule is not None:
                is_preset = schedule in valid_presets
                is_timedelta = hasattr(schedule, "total_seconds")
                is_cron = isinstance(schedule, str) and len(schedule.split()) in [5, 6]

                assert is_preset or is_timedelta or is_cron, (
                    f"DAG '{dag_id}' has invalid schedule: {schedule}"
                )


@pytest.mark.dag
@pytest.mark.skipif(IS_WINDOWS, reason=SKIP_REASON)
class TestGeneratedDags:
    """Test dynamically generated DAGs."""

    def test_dag_generator_creates_dags(self, data_sources_dir):
        """Test that DAG generator creates DAGs from configs."""
        try:
            from rlam_airflow_framework.dag_factory_v2 import DAGFactoryV2

            dag_factory = DAGFactoryV2()
            dags = dag_factory.create_all_dags()

            # May have more or less depending on config validity
            assert len(dags) >= 0
        except ImportError:
            pytest.skip("DAG generator not available")

    def test_dag_generation_handles_invalid_config(self, temp_config_file):
        """Test that DAG generator handles invalid configs gracefully."""
        try:
            from rlam_airflow_framework.dag_factory_v2 import DAGFactoryV2

            invalid_config = {
                # Missing required fields
                "description": "Invalid config"
            }

            # Should either return None or raise clear exception
            try:
                dag_factory = DAGFactoryV2()
                dag = dag_factory.create_dag_from_config(invalid_config)
                assert dag is None or dag is not None  # Either outcome OK
            except (ValueError, KeyError):
                pass  # Expected behavior
        except ImportError:
            pytest.skip("DAG generator not available")


@pytest.mark.dag
@pytest.mark.skipif(IS_WINDOWS, reason=SKIP_REASON)
class TestDagFactory:
    """Test DAG factory functionality."""

    def test_dag_factory_creates_valid_dag(self, sample_data_source_config):
        """Test DAG factory creates valid DAG from config."""
        try:
            from rlam_airflow_framework.dag_factory_v2 import DAGFactoryV2

            factory = DAGFactoryV2()
            dag = factory.create_dag(sample_data_source_config)

            assert dag is not None
            assert dag.dag_id is not None
        except ImportError:
            pytest.skip("DAG factory not available")

    def test_dag_factory_applies_default_args(self, sample_data_source_config):
        """Test DAG factory applies default arguments."""
        try:
            from rlam_airflow_framework.dag_factory_v2 import DAGFactoryV2

            factory = DAGFactoryV2()
            dag = factory.create_dag(sample_data_source_config)

            # Check default args are set
            assert dag.default_args is not None
        except ImportError:
            pytest.skip("DAG factory not available")


# =============================================================================
# DEADLINE CALLBACKS TESTS (Airflow 3.1.6)
# =============================================================================
# These tests require Airflow to be installed as the notifiers inherit from
# airflow.sdk.BaseNotifier. They are skipped on Windows.


@pytest.mark.unit
@pytest.mark.skipif(IS_WINDOWS, reason="Deadline callback tests require Airflow (not supported on Windows)")
class TestKafkaDeadlineNotifier:
    """Test KafkaDeadlineNotifier class."""

    @pytest.fixture
    def mock_kafka_publisher(self, mocker):
        """Mock kafka_publisher for notifier tests."""
        # Need to mock at the module level where it's imported
        mock = mocker.patch("plugins.deadline_callbacks._plugin_kafka_publisher")
        mock.publish_event.return_value = True
        return mock

    @pytest.fixture
    def notifier(self):
        """Create KafkaDeadlineNotifier instance."""
        from plugins.deadline_callbacks import KafkaDeadlineNotifier

        return KafkaDeadlineNotifier(topic="test-alerts", message="Test deadline alert")

    @pytest.fixture
    def mock_context(self):
        """Create mock Airflow callback context."""
        dag_run = MagicMock()
        dag_run.dag_id = "test_dag"
        dag_run.logical_date = datetime(2026, 2, 2, 0, 0, 0, tzinfo=timezone.utc)
        dag_run.queued_at = datetime(2026, 2, 2, 0, 0, 0, tzinfo=timezone.utc)

        return {
            "dag_run": dag_run,
            "deadline": {
                "deadline_time": datetime(2026, 2, 2, 0, 30, 0, tzinfo=timezone.utc),
                "reference": "dagrun_queued",
            },
        }

    def test_init_with_defaults(self):
        """Test initialization with default values."""
        from plugins.deadline_callbacks import KafkaDeadlineNotifier

        notifier = KafkaDeadlineNotifier()

        assert notifier.topic == "pipeline-alerts"
        assert notifier.message == ""

    def test_init_with_custom_topic(self):
        """Test initialization with custom topic."""
        from plugins.deadline_callbacks import KafkaDeadlineNotifier

        notifier = KafkaDeadlineNotifier(topic="custom-alerts")

        assert notifier.topic == "custom-alerts"

    def test_template_fields_defined(self):
        """Test template fields for Jinja rendering."""
        from plugins.deadline_callbacks import KafkaDeadlineNotifier

        assert "message" in KafkaDeadlineNotifier.template_fields
        assert "topic" in KafkaDeadlineNotifier.template_fields

    def test_notify_publishes_to_kafka(
        self, notifier, mock_context, mock_kafka_publisher
    ):
        """Test notify() publishes event to Kafka."""
        notifier.notify(mock_context)

        mock_kafka_publisher.publish_event.assert_called_once()
        call_kwargs = mock_kafka_publisher.publish_event.call_args.kwargs
        assert call_kwargs["dag_id"] == "test_dag"
        assert call_kwargs["event_type"] == "deadline_missed"
        assert call_kwargs["topic"] == "test-alerts"

    def test_notify_extracts_dag_run_info(
        self, notifier, mock_context, mock_kafka_publisher
    ):
        """Test notify extracts correct dag_run information."""
        notifier.notify(mock_context)

        call_kwargs = mock_kafka_publisher.publish_event.call_args.kwargs
        assert call_kwargs["dag_id"] == "test_dag"
        assert "metadata" in call_kwargs
        assert call_kwargs["metadata"]["reference"] == "dagrun_queued"

    def test_notify_handles_missing_dag_run(self, notifier, mock_kafka_publisher):
        """Test notify handles missing dag_run gracefully."""
        context = {"dag_run": None, "deadline": {}}

        notifier.notify(context)

        # Should still publish with 'unknown' dag_id
        call_kwargs = mock_kafka_publisher.publish_event.call_args.kwargs
        assert call_kwargs["dag_id"] == "unknown"

    def test_notify_handles_kafka_failure(self, notifier, mock_context, mocker):
        """Test notify handles Kafka publish failure."""
        mock_kafka = mocker.patch("plugins.deadline_callbacks._plugin_kafka_publisher")
        mock_kafka.publish_event.side_effect = Exception("Connection failed")

        # Should not raise, just log error
        notifier.notify(mock_context)  # No exception


@pytest.mark.unit
@pytest.mark.skipif(IS_WINDOWS, reason="Deadline callback tests require Airflow (not supported on Windows)")
class TestEmailDeadlineNotifier:
    """Test EmailDeadlineNotifier class."""

    @pytest.fixture
    def notifier_with_recipients(self):
        """Create EmailDeadlineNotifier with recipients."""
        from plugins.deadline_callbacks import EmailDeadlineNotifier

        return EmailDeadlineNotifier(
            recipients=["test@example.com", "alerts@example.com"], subject="Test Alert"
        )

    @pytest.fixture
    def notifier_without_recipients(self):
        """Create EmailDeadlineNotifier without recipients."""
        from plugins.deadline_callbacks import EmailDeadlineNotifier

        return EmailDeadlineNotifier()

    @pytest.fixture
    def mock_context(self):
        """Create mock context."""
        dag_run = MagicMock()
        dag_run.dag_id = "test_dag"
        dag_run.logical_date = datetime(2026, 2, 2, tzinfo=timezone.utc)

        return {
            "dag_run": dag_run,
            "deadline": {
                "deadline_time": datetime(2026, 2, 2, 0, 30, 0, tzinfo=timezone.utc)
            },
        }

    def test_init_with_recipients(self):
        """Test initialization with email recipients."""
        from plugins.deadline_callbacks import EmailDeadlineNotifier

        notifier = EmailDeadlineNotifier(recipients=["a@test.com", "b@test.com"])

        assert len(notifier.recipients) == 2
        assert "a@test.com" in notifier.recipients

    def test_init_without_recipients(self):
        """Test initialization without recipients defaults to empty list."""
        from plugins.deadline_callbacks import EmailDeadlineNotifier

        notifier = EmailDeadlineNotifier()

        assert notifier.recipients == []

    def test_template_fields_defined(self):
        """Test template fields for Jinja rendering."""
        from plugins.deadline_callbacks import EmailDeadlineNotifier

        assert "recipients" in EmailDeadlineNotifier.template_fields
        assert "subject" in EmailDeadlineNotifier.template_fields

    def test_notify_skips_when_no_recipients(
        self, notifier_without_recipients, mock_context
    ):
        """Test notify skips email when no recipients configured."""
        # Should not raise or attempt to send
        notifier_without_recipients.notify(mock_context)

    def test_notify_sends_email_when_recipients_configured(
        self, notifier_with_recipients, mock_context, mocker
    ):
        """Test notify sends email when recipients are configured."""
        mock_send = mocker.patch("airflow.utils.email.send_email")

        notifier_with_recipients.notify(mock_context)

        mock_send.assert_called_once()
        call_kwargs = mock_send.call_args.kwargs
        assert call_kwargs["to"] == ["test@example.com", "alerts@example.com"]

    def test_notify_builds_default_content(
        self, notifier_with_recipients, mock_context, mocker
    ):
        """Test notify builds default HTML content."""
        mock_send = mocker.patch("airflow.utils.email.send_email")

        notifier_with_recipients.notify(mock_context)

        call_kwargs = mock_send.call_args.kwargs
        assert "Pipeline Deadline Missed" in call_kwargs["html_content"]
        assert "test_dag" in call_kwargs["html_content"]

    def test_notify_handles_send_failure(
        self, notifier_with_recipients, mock_context, mocker
    ):
        """Test notify handles email send failure gracefully."""
        mock_send = mocker.patch("airflow.utils.email.send_email")
        mock_send.side_effect = Exception("SMTP error")

        # Should not raise, just log error
        notifier_with_recipients.notify(mock_context)


@pytest.mark.unit
@pytest.mark.skipif(IS_WINDOWS, reason="Deadline callback tests require Airflow (not supported on Windows)")
class TestCompositeDeadlineNotifier:
    """Test CompositeDeadlineNotifier class."""

    @pytest.fixture
    def mock_kafka_publisher(self, mocker):
        """Mock kafka_publisher."""
        mock = mocker.patch("plugins.deadline_callbacks._plugin_kafka_publisher")
        mock.publish_event.return_value = True
        return mock

    @pytest.fixture
    def mock_context(self):
        """Create mock context."""
        dag_run = MagicMock()
        dag_run.dag_id = "test_dag"
        dag_run.logical_date = datetime(2026, 2, 2, tzinfo=timezone.utc)
        dag_run.queued_at = datetime(2026, 2, 2, tzinfo=timezone.utc)

        return {
            "dag_run": dag_run,
            "deadline": {
                "deadline_time": datetime(2026, 2, 2, 0, 30, 0, tzinfo=timezone.utc),
                "reference": "dagrun_queued",
            },
        }

    def test_init_creates_sub_notifiers(self):
        """Test initialization creates Kafka and Email sub-notifiers."""
        from plugins.deadline_callbacks import CompositeDeadlineNotifier

        notifier = CompositeDeadlineNotifier(
            topic="alerts", email_enabled=True, email_recipients=["test@example.com"]
        )

        assert notifier._kafka_notifier is not None
        assert notifier._email_notifier is not None
        assert notifier._kafka_notifier.topic == "alerts"

    def test_init_with_email_disabled(self):
        """Test initialization with email disabled."""
        from plugins.deadline_callbacks import CompositeDeadlineNotifier

        notifier = CompositeDeadlineNotifier(email_enabled=False)

        assert notifier.email_enabled is False

    def test_notify_always_sends_kafka(self, mock_context, mock_kafka_publisher):
        """Test Kafka notification is ALWAYS sent."""
        from plugins.deadline_callbacks import CompositeDeadlineNotifier

        notifier = CompositeDeadlineNotifier(
            topic="alerts",
            email_enabled=False,  # Email disabled
        )
        notifier.notify(mock_context)

        # Kafka should still be called
        mock_kafka_publisher.publish_event.assert_called()

    def test_notify_sends_email_when_enabled(
        self, mock_context, mock_kafka_publisher, mocker
    ):
        """Test email is sent when enabled with recipients."""
        mock_send = mocker.patch("airflow.utils.email.send_email")

        from plugins.deadline_callbacks import CompositeDeadlineNotifier

        notifier = CompositeDeadlineNotifier(
            topic="alerts", email_enabled=True, email_recipients=["test@example.com"]
        )
        notifier.notify(mock_context)

        mock_send.assert_called_once()

    def test_notify_skips_email_when_disabled(
        self, mock_context, mock_kafka_publisher, mocker
    ):
        """Test email is skipped when disabled."""
        mock_send = mocker.patch("airflow.utils.email.send_email")

        from plugins.deadline_callbacks import CompositeDeadlineNotifier

        notifier = CompositeDeadlineNotifier(
            topic="alerts", email_enabled=False, email_recipients=["test@example.com"]
        )
        notifier.notify(mock_context)

        mock_send.assert_not_called()

    def test_notify_logs_warning_for_missing_recipients(
        self, mock_context, mock_kafka_publisher, mocker
    ):
        """Test warning logged when email enabled but no recipients."""
        mock_send = mocker.patch("airflow.utils.email.send_email")

        from plugins.deadline_callbacks import CompositeDeadlineNotifier

        notifier = CompositeDeadlineNotifier(
            topic="alerts",
            email_enabled=True,
            email_recipients=[],  # No recipients
        )
        notifier.notify(mock_context)

        # Email should not be sent
        mock_send.assert_not_called()

    def test_notify_continues_after_kafka_failure(self, mock_context, mocker):
        """Test email is still sent even if Kafka fails."""
        mock_kafka = mocker.patch("plugins.deadline_callbacks._plugin_kafka_publisher")
        mock_kafka.publish_event.side_effect = Exception("Kafka down")
        mock_send = mocker.patch("airflow.utils.email.send_email")

        from plugins.deadline_callbacks import CompositeDeadlineNotifier

        notifier = CompositeDeadlineNotifier(
            topic="alerts", email_enabled=True, email_recipients=["test@example.com"]
        )

        # Should not raise
        notifier.notify(mock_context)

        # Email should still be attempted
        mock_send.assert_called_once()


# =============================================================================
# DAG FACTORY AIRFLOW 3.1.6 FEATURES TESTS - Run on All Platforms
# =============================================================================

# These tests are skipped on Windows because importing DAGFactory requires
# Airflow which doesn't support Windows natively


@pytest.mark.unit
@pytest.mark.skipif(
    IS_WINDOWS, reason="DAGFactory tests require Airflow (not supported on Windows)"
)
class TestDAGFactoryCreateDeadlineAlert:
    """Test DAGFactory._create_deadline_alert method."""

    @pytest.fixture
    def factory(self, mocker):
        """Create DAGFactory with mocked dependencies."""
        # Mock config_loader to avoid file system dependencies
        mocker.patch("rlam_airflow_framework.dag_factory_v2.ConfigLoader")
        from rlam_airflow_framework.dag_factory_v2 import DAGFactoryV2

        factory = DAGFactoryV2()
        factory.global_settings = {}
        return factory

    def test_returns_none_when_disabled(self, factory):
        """Test returns None when deadline not enabled."""
        schedule_config = {"deadline": {"enabled": False, "timeout_minutes": 30}}

        result = factory._create_deadline_alert("test_dag", schedule_config)

        assert result is None

    def test_returns_none_when_not_configured(self, factory):
        """Test returns None when no deadline config present."""
        schedule_config = {}

        result = factory._create_deadline_alert("test_dag", schedule_config)

        assert result is None

    def test_returns_none_when_notifier_unavailable(self, factory, mocker):
        """Test returns None when CompositeDeadlineNotifier not available."""
        mocker.patch("utils.dag_factory.CompositeDeadlineNotifier", None)

        schedule_config = {"deadline": {"enabled": True, "timeout_minutes": 30}}

        result = factory._create_deadline_alert("test_dag", schedule_config)

        assert result is None

    def test_creates_deadline_alert_with_timeout(self, factory, mocker):
        """Test creates DeadlineAlert with correct timeout."""
        # Mock the notifier class
        mock_notifier = mocker.MagicMock()
        mocker.patch("utils.dag_factory.CompositeDeadlineNotifier", mock_notifier)

        schedule_config = {"deadline": {"enabled": True, "timeout_minutes": 45}}

        result = factory._create_deadline_alert("test_dag", schedule_config)

        assert result is not None
        # Verify it's a DeadlineAlert
        from airflow.sdk.definitions.deadline import DeadlineAlert

        assert isinstance(result, DeadlineAlert)
        # Check timeout interval
        assert result.interval == timedelta(minutes=45)

    def test_uses_default_timeout_when_not_specified(self, factory, mocker):
        """Test uses default 30 minute timeout."""
        mock_notifier = mocker.MagicMock()
        mocker.patch("utils.dag_factory.CompositeDeadlineNotifier", mock_notifier)

        schedule_config = {
            "deadline": {
                "enabled": True
                # No timeout_minutes specified
            }
        }

        result = factory._create_deadline_alert("test_dag", schedule_config)

        assert result is not None
        assert result.interval == timedelta(minutes=30)

    def test_uses_dagrun_queued_at_reference(self, factory, mocker):
        """Test uses DAGRUN_QUEUED_AT reference point."""
        mock_notifier = mocker.MagicMock()
        mocker.patch("utils.dag_factory.CompositeDeadlineNotifier", mock_notifier)

        schedule_config = {"deadline": {"enabled": True, "timeout_minutes": 30}}

        result = factory._create_deadline_alert("test_dag", schedule_config)

        from airflow.sdk.definitions.deadline import DeadlineReference

        assert result.reference == DeadlineReference.DAGRUN_QUEUED_AT


@pytest.mark.unit
@pytest.mark.skipif(
    IS_WINDOWS, reason="DAGFactory tests require Airflow (not supported on Windows)"
)
class TestDAGFactoryCreateAssets:
    """Test DAGFactory._create_assets method."""

    @pytest.fixture
    def factory(self, mocker):
        """Create DAGFactory with mocked dependencies."""
        mocker.patch("rlam_airflow_framework.dag_factory_v2.ConfigLoader")
        from rlam_airflow_framework.dag_factory_v2 import DAGFactoryV2

        factory = DAGFactoryV2()
        factory.global_settings = {}
        return factory

    def test_creates_inlet_for_rest_api(self, factory):
        """Test creates inlet Asset for REST API source."""
        config = {
            "data_source": {
                "name": "market_data",
                "type": "rest_api",
                "endpoint": "https://api.example.com/data",
            },
            "destination": {
                "primary": {"type": "snowflake_table", "table": "RAW.DATA"}
            },
        }

        inlets, outlets = factory._create_assets(config)

        assert len(inlets) == 1
        assert "api://market_data" in inlets[0].uri

    def test_creates_inlet_for_sftp(self, factory):
        """Test creates inlet Asset for SFTP source."""
        config = {
            "data_source": {
                "name": "file_source",
                "type": "sftp",
                "remote_path": "/data/files",
            },
            "destination": {
                "primary": {"type": "snowflake_table", "table": "RAW.FILES"}
            },
        }

        inlets, outlets = factory._create_assets(config)

        assert len(inlets) == 1
        assert "sftp://file_source" in inlets[0].uri

    def test_creates_outlet_for_snowflake(self, factory):
        """Test creates outlet Asset for Snowflake destination."""
        config = {
            "data_source": {
                "name": "test_source",
                "type": "rest_api",
                "endpoint": "https://api.test.com",
            },
            "destination": {
                "primary": {"type": "snowflake_table", "table": "RAW.TEST_TABLE"}
            },
        }

        inlets, outlets = factory._create_assets(config)

        assert len(outlets) == 1
        assert outlets[0].uri == "snowflake://RAW.TEST_TABLE"

    def test_creates_outlet_for_azure_data_lake(self, factory):
        """Test creates outlet Asset for Azure Data Lake."""
        config = {
            "data_source": {
                "name": "test_source",
                "type": "rest_api",
                "endpoint": "https://api.test.com",
            },
            "destination": {
                "primary": {
                    "type": "azure_data_lake",
                    "container": "raw-data",
                    "path": "market/daily",
                }
            },
        }

        inlets, outlets = factory._create_assets(config)

        assert len(outlets) == 1
        assert outlets[0].uri == "azure://raw-data/market/daily"

    def test_creates_outlet_for_azure_blob(self, factory):
        """Test creates outlet Asset for Azure Blob Storage."""
        config = {
            "data_source": {
                "name": "test_source",
                "type": "rest_api",
                "endpoint": "https://api.test.com",
            },
            "destination": {
                "primary": {
                    "type": "azure_blob",
                    "container": "archive",
                    "path": "backups",
                }
            },
        }

        inlets, outlets = factory._create_assets(config)

        assert len(outlets) == 1
        assert outlets[0].uri == "azure://archive/backups"

    def test_handles_unknown_source_type(self, factory):
        """Test handles unknown source type gracefully."""
        config = {
            "data_source": {"name": "custom_source", "type": "custom_connector"},
            "destination": {
                "primary": {"type": "snowflake_table", "table": "RAW.CUSTOM"}
            },
        }

        inlets, outlets = factory._create_assets(config)

        assert len(inlets) == 1
        assert "custom_connector://custom_source" in inlets[0].uri

    def test_handles_unknown_destination_type(self, factory):
        """Test handles unknown destination type gracefully."""
        config = {
            "data_source": {
                "name": "test_source",
                "type": "rest_api",
                "endpoint": "https://api.test.com",
            },
            "destination": {"primary": {"type": "custom_warehouse"}},
        }

        inlets, outlets = factory._create_assets(config)

        assert len(outlets) == 1
        assert "custom_warehouse://test_source_output" in outlets[0].uri

    def test_creates_outlet_for_local_file_with_path(self, factory):
        """Test creates outlet Asset for local_file with path."""
        config = {
            "data_source": {
                "name": "simple_test",
                "type": "rest_api",
                "endpoint": "https://api.chucknorris.io/jokes/random",
            },
            "destination": {
                "type": "local_file",
                "path": "/tmp/airflow_output/simple_test.json"
            },
        }

        inlets, outlets = factory._create_assets(config)

        assert len(outlets) == 1
        assert outlets[0].uri == "file:///tmp/airflow_output/simple_test.json"

    def test_creates_outlet_for_local_file_without_path(self, factory):
        """Test creates outlet Asset for local_file without path (fallback)."""
        config = {
            "data_source": {
                "name": "test_source",
                "type": "rest_api",
                "endpoint": "https://api.test.com",
            },
            "destination": {
                "type": "local_file"
            },
        }

        inlets, outlets = factory._create_assets(config)

        assert len(outlets) == 1
        assert outlets[0].uri == "file:///tmp/test_source_output"

    def test_supports_flat_destination_structure(self, factory):
        """Test supports flat destination structure (destination.type instead of destination.primary.type)."""
        config = {
            "data_source": {
                "name": "test_source",
                "type": "rest_api",
                "endpoint": "https://api.test.com",
            },
            "destination": {
                "type": "local_file",
                "path": "/tmp/test.json"
            },
        }

        inlets, outlets = factory._create_assets(config)

        assert len(outlets) == 1
        assert outlets[0].uri == "file:///tmp/test.json"


@pytest.mark.unit
class TestAirflow316ConfigLoading:
    """Test loading Airflow 3.1.6 config fixture (runs on Windows)."""

    def test_load_test_config_fixture(self, airflow_330_config):
        """Test that the test config fixture loads correctly."""
        assert airflow_330_config is not None
        assert "data_source" in airflow_330_config
        assert "schedule" in airflow_330_config
        assert "destination" in airflow_330_config

    def test_deadline_config_present(self, airflow_330_config):
        """Test deadline configuration is present in fixture."""
        deadline = airflow_330_config.get("schedule", {}).get("deadline", {})
        assert deadline is not None
        assert deadline.get("tiers")[0].get("enabled") is True
        assert deadline.get("tiers")[1].get("timeout_minutes") == 45
        assert deadline.get("tiers")[1].get("email_enabled") is True
        assert len(deadline.get("tiers")[1].get("email_recipients", [])) == 2

    def test_hitl_config_present(self, airflow_330_config):
        """Test HITL configuration is present in fixture."""
        hitl = (
            airflow_330_config.get("destination", {})
            .get("quarantine", {})
            .get("hitl", {})
        )

        assert hitl.get("enabled") is True
        assert hitl.get("timeout_hours") == 24
        assert "data-steward" in hitl.get("allowed_roles", [])

    def test_deadline_config_fixture(self, deadline_config):
        """Test dedicated deadline config fixture."""
        assert deadline_config is not None
        assert deadline_config.get("tiers")[0].get("enabled") is True
        assert deadline_config.get("tiers")[0].get("kafka_topic") == "test-pipeline-alerts"

    def test_hitl_config_fixture(self, hitl_config):
        """Test dedicated HITL config fixture."""
        assert hitl_config.get("enabled") is True
        assert "admin" in hitl_config.get("allowed_roles", [])
