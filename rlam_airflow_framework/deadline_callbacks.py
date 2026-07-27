# File: rlam_airflow_framework/deadline_callbacks.py
"""
Airflow 3.3.0 Deadline Alert Notifiers.

Provides custom notifiers for deadline alerts:
- KafkaDeadlineNotifier: Always publishes deadline violations to Kafka
- EmailDeadlineNotifier: Optional email notifications based on config
- CompositeDeadlineNotifier: Combines Kafka (always) + Email (configurable)

These are plain notifier classes the DAG factory instantiates directly — NOT
an Airflow plugin (no AirflowPlugin registration). They live in the framework
package (moved out of a misleading `plugins/` folder 2026-07-23) so they ship
baked into the image, with no volume-mount / sys.path dependency.

Usage in DAG:
    from airflow.sdk.definitions.deadline import AsyncCallback, DeadlineAlert, DeadlineReference
    from rlam_airflow_framework.deadline_callbacks import CompositeDeadlineNotifier

    deadline = DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=30),
        callback=AsyncCallback(
            CompositeDeadlineNotifier,
            kwargs={
                "topic": "pipeline-alerts",
                "email_enabled": True,
                "email_recipients": ["alerts@company.com"]
            }
        )
    )
"""

import json
import os
import structlog
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, cast

from airflow.sdk import BaseNotifier
# Task SDK Connection: resolves via the execution API in the SyncCallback/worker
# context (3.3.0 #65269). airflow.models is DB-isolated there and must not be used.
from airflow.sdk import Connection
from airflow.sdk.definitions.context import Context

# Configure structlog
log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class DeadlineContext:
    """Typed view of the deadline-callback context, normalized ONCE at the boundary.

    Verified against the real Airflow 3.3.0 SyncCallback payload (2026-07-23):
    `context['dag_run']` arrives as a plain dict (keys: dag_id, logical_date,
    queued_at, ...) and `deadline.deadline_time` as an ISO-8601 string with a
    trailing 'Z'. Unit tests and older call sites may pass attribute-style
    objects and datetimes instead. All that tolerance lives HERE — notifier
    internals only ever see this dataclass.
    """

    dag_id: str
    logical_date: Any
    queued_at: Any
    deadline_time: Optional[datetime]
    reference: str

    @classmethod
    def from_context(cls, context: Context) -> "DeadlineContext":
        # The real SyncCallback deadline payload isn't shaped like the declared
        # Context TypedDict (it carries a "deadline" key Context doesn't have,
        # and "dag_run" arrives as a plain dict) — treat it as an untyped dict here.
        raw = cast(Dict[str, Any], context)
        dag_run = raw.get("dag_run")
        deadline_info = raw.get("deadline") or {}
        return cls(
            dag_id=cls._field(dag_run, "dag_id") or "unknown",
            logical_date=cls._field(dag_run, "logical_date"),
            queued_at=cls._field(dag_run, "queued_at"),
            deadline_time=cls._parse_datetime(deadline_info.get("deadline_time")),
            reference=deadline_info.get("reference", "dagrun_queued"),
        )

    @staticmethod
    def _field(dag_run: Any, field: str) -> Any:
        if dag_run is None:
            return None
        if isinstance(dag_run, dict):
            return dag_run.get(field)
        return getattr(dag_run, field, None)

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        if value is None or isinstance(value, datetime):
            return value
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except (ValueError, TypeError):
            log.warning("Could not parse deadline_time", value=str(value))
            return None

# =============================================================================
# CENTRALIZED TIMEOUT CONFIGURATION
# =============================================================================
# Environment variables for Kafka timeouts (align with global_settings.yaml)
PLUGIN_KAFKA_SOCKET_TIMEOUT_MS = int(os.getenv("TIMEOUT_KAFKA_SOCKET", "5")) * 1000
PLUGIN_KAFKA_MESSAGE_TIMEOUT_MS = int(os.getenv("TIMEOUT_KAFKA_MESSAGE", "10")) * 1000
PLUGIN_KAFKA_FLUSH_TIMEOUT = int(os.getenv("TIMEOUT_KAFKA_FLUSH", "10"))


# =============================================================================
# EMBEDDED KAFKA PUBLISHER (Self-contained for plugin use)
# =============================================================================


class PluginKafkaPublisher:
    """
    Lightweight Kafka publisher for plugin use.
    
    This is a simplified version embedded in the plugin to avoid
    import dependencies on dags/utils. Uses confluent-kafka for
    Kafka 4.x compatibility.
    """

    _instance: Optional["PluginKafkaPublisher"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "PluginKafkaPublisher":
        """Singleton pattern for publisher instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
            
        self._producer = None
        self._producer_lock = threading.Lock()
        self._delivery_result = None
        self._initialized = True

    def _delivery_callback(self, err, msg):
        """Callback for message delivery reports."""
        if err is not None:
            self._delivery_result = {"success": False, "error": str(err)}
            log.error("Message delivery failed", error=str(err))
        else:
            self._delivery_result = {"success": True, "topic": msg.topic(), "partition": msg.partition()}

    def _get_producer(self):
        """Lazy initialize Kafka producer using confluent-kafka."""
        if self._producer is None:
            with self._producer_lock:
                if self._producer is None:
                    try:
                        from confluent_kafka import Producer
                        
                        # Fetch Kafka configuration from Airflow Connection
                        try:
                            conn = Connection.get("kafka_default")
                            bootstrap_servers = conn.extra_dejson.get("bootstrap.servers", "kafka:29092")
                            security_protocol = conn.extra_dejson.get("security.protocol", "PLAINTEXT")
                        except Exception as e:
                            log.warning("Could not fetch kafka_default connection, using defaults", error=str(e))
                            bootstrap_servers = "kafka:29092"
                            security_protocol = "PLAINTEXT"
                        
                        config = {
                            "bootstrap.servers": bootstrap_servers,
                            "security.protocol": security_protocol,
                            "acks": "all",
                            "retries": 3,
                            "socket.timeout.ms": PLUGIN_KAFKA_SOCKET_TIMEOUT_MS,
                            "message.timeout.ms": PLUGIN_KAFKA_MESSAGE_TIMEOUT_MS,
                        }
                        
                        self._producer = Producer(config)
                        log.info(
                            "Plugin Kafka producer initialized (confluent-kafka)",
                            bootstrap_servers=bootstrap_servers,
                        )
                    except Exception as e:
                        log.error("Failed to initialize Kafka producer", error=str(e))
                        return None
        return self._producer

    def publish_event(
        self,
        topic: str,
        event_type: str,
        dag_id: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Publish an event to Kafka.

        Args:
            topic: Kafka topic
            event_type: Type of event (e.g., "deadline_missed")
            dag_id: DAG identifier
            message: Event message
            metadata: Additional metadata

        Returns:
            True if published successfully, False otherwise
        """
        producer = self._get_producer()
        if producer is None:
            log.warning("Kafka producer not available, skipping publish")
            return False

        event = {
            "event_type": event_type,
            "dag_id": dag_id,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "airflow_deadline_notifier",
            "metadata": metadata or {},
        }

        try:
            # Reset delivery result
            self._delivery_result = None
            
            # Produce message with callback
            producer.produce(
                topic=topic,
                key=dag_id.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                callback=self._delivery_callback,
            )
            
            # Flush and wait for delivery
            producer.flush(timeout=PLUGIN_KAFKA_FLUSH_TIMEOUT)
            
            if self._delivery_result and self._delivery_result.get("success"):
                log.info("Published event to Kafka", topic=topic, dag_id=dag_id)
                return True
            else:
                error_msg = self._delivery_result.get("error", "Unknown error") if self._delivery_result else "No delivery result"
                log.error("Failed to publish to Kafka", topic=topic, error=error_msg)
                return False
        except Exception as e:
            log.error("Failed to publish to Kafka", topic=topic, error=str(e))
            return False


# Singleton instance
_plugin_kafka_publisher = PluginKafkaPublisher()


# =============================================================================
# DEADLINE NOTIFIERS
# =============================================================================


class KafkaDeadlineNotifier(BaseNotifier):
    """
    Notifier that publishes deadline violations to Kafka.

    This notifier is ALWAYS active - Kafka events are published
    for all deadline violations regardless of configuration.

    Attributes:
        template_fields: Fields that support Jinja templating
        topic: Kafka topic for deadline alerts (default: "pipeline-alerts")
        message: Custom message template
    """

    template_fields = ("message", "topic")

    def __init__(self, topic: str = "pipeline-alerts", message: str = "", **kwargs):
        """
        Initialize Kafka deadline notifier.

        Args:
            topic: Kafka topic for deadline alerts
            message: Custom message (supports Jinja templating)
        """
        super().__init__(**kwargs)
        self.topic = topic
        self.message = message

    def notify(self, context: Context) -> None:
        """
        Publish deadline violation event to Kafka.

        Args:
            context: Airflow callback context containing dag_run, task, etc.
        """
        ctx = DeadlineContext.from_context(context)
        dag_id = ctx.dag_id
        logical_date = ctx.logical_date
        queued_at = ctx.queued_at
        deadline_time = ctx.deadline_time
        reference = ctx.reference

        # Calculate how late the run is
        late_by_seconds = None
        if deadline_time and datetime.now(timezone.utc) > deadline_time:
            late_by_seconds = (
                datetime.now(timezone.utc) - deadline_time
            ).total_seconds()

        # Build event metadata
        metadata = {
            "deadline_time": str(deadline_time) if deadline_time else None,
            "reference": reference,
            "queued_at": str(queued_at) if queued_at else None,
            "logical_date": str(logical_date) if logical_date else None,
            "late_by_seconds": late_by_seconds,
            "alert_type": "deadline_missed",
        }

        # Determine message
        alert_message = self.message or f"Pipeline {dag_id} missed deadline"

        log.info(
            "Publishing deadline alert to Kafka",
            dag_id=dag_id,
            topic=self.topic,
            late_by_seconds=late_by_seconds,
        )

        try:
            success = _plugin_kafka_publisher.publish_event(
                topic=self.topic,
                event_type="deadline_missed",
                dag_id=dag_id,
                message=alert_message,
                metadata=metadata,
            )

            if success:
                log.info(
                    "Successfully published deadline alert",
                    dag_id=dag_id,
                    topic=self.topic,
                )
            else:
                log.warning(
                    "Failed to publish deadline alert to Kafka",
                    dag_id=dag_id,
                    topic=self.topic,
                )

        except Exception as e:
            log.error(
                "Error publishing deadline alert to Kafka", dag_id=dag_id, error=str(e)
            )


class EmailDeadlineNotifier(BaseNotifier):
    """
    Notifier that sends deadline violation emails.

    Only sends emails when explicitly enabled via configuration.
    Uses Airflow's built-in email functionality.

    Attributes:
        template_fields: Fields that support Jinja templating
        recipients: List of email recipients
        subject: Email subject line
        html_content: Email body content
    """

    template_fields = ("recipients", "subject", "html_content")

    def __init__(
        self,
        recipients: Optional[List[str]] = None,
        subject: str = "",
        html_content: str = "",
        **kwargs,
    ):
        """
        Initialize email deadline notifier.

        Args:
            recipients: List of email addresses
            subject: Email subject (supports Jinja templating)
            html_content: Email body (supports Jinja templating)
        """
        super().__init__(**kwargs)
        self.recipients = recipients or []
        self.subject = subject
        self.html_content = html_content

    def notify(self, context: Context) -> None:
        """
        Send deadline violation email.

        Args:
            context: Airflow callback context
        """
        if not self.recipients:
            log.warning("No email recipients configured for deadline alert")
            return

        from airflow.utils.email import send_email

        ctx = DeadlineContext.from_context(context)
        dag_id = ctx.dag_id
        logical_date = ctx.logical_date
        deadline_time = ctx.deadline_time

        # Build default subject and content if not provided
        subject = self.subject or f"🚨 Deadline Alert: {dag_id}"

        if not self.html_content:
            html_content = f"""
            <h2>Pipeline Deadline Missed</h2>
            <p><strong>DAG:</strong> {dag_id}</p>
            <p><strong>Logical Date:</strong> {logical_date}</p>
            <p><strong>Deadline:</strong> {deadline_time}</p>
            <p><strong>Alert Time:</strong> {datetime.now(timezone.utc).isoformat()}</p>
            <p>Please investigate the pipeline execution.</p>
            """
        else:
            html_content = self.html_content

        log.info(
            "Sending deadline alert email", dag_id=dag_id, recipients=self.recipients
        )

        try:
            send_email(to=self.recipients, subject=subject, html_content=html_content)
            log.info(
                "Successfully sent deadline alert email",
                dag_id=dag_id,
                recipients=self.recipients,
            )
        except Exception as e:
            log.error(
                "Failed to send deadline alert email", dag_id=dag_id, error=str(e)
            )


class CompositeDeadlineNotifier(BaseNotifier):
    """
    Composite notifier that combines Kafka (always) and Email (configurable).

    Kafka alerts are ALWAYS sent for all deadline violations.
    Email alerts are only sent when email_enabled=True.

    This is the recommended notifier for production use as it ensures
    all deadline violations are captured in Kafka for downstream processing
    while allowing optional email notifications for critical pipelines.

    Attributes:
        template_fields: Fields that support Jinja templating
        topic: Kafka topic for alerts
        message: Alert message
        email_enabled: Whether to send email notifications
        email_recipients: List of email recipients
        email_subject: Email subject line
    """

    template_fields = ("message", "topic", "email_recipients", "email_subject")

    def __init__(
        self,
        topic: str = "pipeline-alerts",
        message: str = "",
        email_enabled: bool = False,
        email_recipients: Optional[List[str]] = None,
        email_subject: str = "",
        **kwargs,
    ):
        """
        Initialize composite deadline notifier.

        Args:
            topic: Kafka topic for deadline alerts
            message: Alert message (supports Jinja templating)
            email_enabled: Whether to send email notifications
            email_recipients: List of email addresses
            email_subject: Email subject line
        """
        super().__init__(**kwargs)
        self.topic = topic
        self.message = message
        self.email_enabled = email_enabled
        self.email_recipients = email_recipients or []
        self.email_subject = email_subject

        # Initialize sub-notifiers
        self._kafka_notifier = KafkaDeadlineNotifier(topic=topic, message=message)
        self._email_notifier = EmailDeadlineNotifier(
            recipients=email_recipients, subject=email_subject
        )

    def notify(self, context: Context) -> None:
        """
        Send deadline notifications via Kafka (always) and Email (if enabled).

        Args:
            context: Airflow callback context
        """
        dag_id = DeadlineContext.from_context(context).dag_id

        log.info(
            "Processing deadline alert", dag_id=dag_id, email_enabled=self.email_enabled
        )

        # ALWAYS send Kafka notification
        try:
            self._kafka_notifier.notify(context)
        except Exception as e:
            log.error("Kafka notification failed", dag_id=dag_id, error=str(e))

        # Conditionally send email notification
        if self.email_enabled and self.email_recipients:
            try:
                self._email_notifier.notify(context)
            except Exception as e:
                log.error("Email notification failed", dag_id=dag_id, error=str(e))
        elif self.email_enabled and not self.email_recipients:
            log.warning("Email enabled but no recipients configured", dag_id=dag_id)
