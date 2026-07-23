"""
Kafka publisher utilities for sending pipeline events.

Provides thread-safe Kafka publishing with:
- Double-checked locking for singleton producer initialization
- Circuit breaker pattern for resilience
- Structlog for structured logging (Airflow 3.1.6)
- Proper error handling and metrics
- Multi-tenancy support with topic namespacing
- Centralized timeout configuration support

Uses confluent-kafka library for Kafka 4.x (KRaft mode) compatibility.
"""

from __future__ import annotations

import json
import structlog
import threading
from datetime import datetime, timezone
from typing import Dict, Any, Optional, TYPE_CHECKING
from confluent_kafka import Producer, KafkaException
import os
import time
from enum import Enum

if TYPE_CHECKING:
    from rlam_airflow_framework.tenant_context import TenantContext

log = structlog.get_logger(__name__)

# =============================================================================
# CENTRALIZED TIMEOUT CONFIGURATION
# =============================================================================
# Environment variables take precedence, then fall back to defaults.
# These align with config/global_settings.yaml timeouts.kafka section.

KAFKA_REQUEST_TIMEOUT_MS = int(os.getenv("TIMEOUT_KAFKA_REQUEST", "30")) * 1000  # Convert to ms
KAFKA_FLUSH_TIMEOUT = int(os.getenv("TIMEOUT_KAFKA_FLUSH", "10"))
KAFKA_SOCKET_TIMEOUT_MS = int(os.getenv("TIMEOUT_KAFKA_SOCKET", "5")) * 1000  # Convert to ms
KAFKA_MESSAGE_TIMEOUT_MS = int(os.getenv("TIMEOUT_KAFKA_MESSAGE", "10")) * 1000  # Convert to ms
KAFKA_CIRCUIT_RECOVERY_TIMEOUT = float(os.getenv("TIMEOUT_KAFKA_CIRCUIT_RECOVERY", os.getenv("KAFKA_CIRCUIT_RECOVERY_TIMEOUT", "30.0")))

# Import tenant context for multi-tenancy support
try:
    from rlam_airflow_framework.tenant_context import TenantContext
except ImportError:
    TenantContext = None


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """
    Simple circuit breaker implementation for Kafka publishing.

    Prevents cascading failures by stopping requests when the service is unhealthy.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = KAFKA_CIRCUIT_RECOVERY_TIMEOUT,
        half_open_max_calls: int = 3,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state, potentially transitioning from OPEN to HALF_OPEN."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if (
                    time.time() - (self._last_failure_time or 0)
                    >= self.recovery_timeout
                ):
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    log.info("Circuit breaker transitioning to HALF_OPEN")
            return self._state

    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls += 1
                if self._half_open_calls >= self.half_open_max_calls:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    log.info("Circuit breaker CLOSED after successful recovery")
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    def record_failure(self) -> None:
        """Record a failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                log.warning("Circuit breaker OPEN after half-open failure")
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitState.OPEN
                    log.warning(
                        "Circuit breaker OPEN after failures",
                        failure_count=self._failure_count,
                    )

    def allow_request(self) -> bool:
        """Check if request should be allowed."""
        current_state = self.state  # This may trigger state transition

        if current_state == CircuitState.CLOSED:
            return True
        elif current_state == CircuitState.HALF_OPEN:
            return True
        else:  # OPEN
            return False


class KafkaEventPublisher:
    """
    Thread-safe publisher for sending pipeline events to Kafka.

    Features:
    - Double-checked locking for producer initialization
    - Circuit breaker for resilience
    - Correlation ID support for distributed tracing
    - Multi-tenancy support with topic namespacing
    
    Uses confluent-kafka for Kafka 4.x (KRaft mode) compatibility.
    """

    # Class-level lock for singleton initialization
    _init_lock = threading.Lock()

    def __init__(self, tenant_context: Optional["TenantContext"] = None):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.topic_market_data = os.getenv("KAFKA_TOPIC_MARKET_DATA", "Market_Data")
        self.security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
        self._producer: Optional[Producer] = None
        self._producer_lock = threading.Lock()

        # Multi-tenancy support
        self._tenant_context = tenant_context

        # DAG Bundle versioning support
        self.bundle_name = os.getenv("AIRFLOW_DAG_BUNDLE_NAME", "local_dag_bundle")
        self.bundle_version = os.getenv("AIRFLOW_DAG_BUNDLE_VERSION", "unknown")

        # Circuit breaker for resilience
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=int(os.getenv("KAFKA_CIRCUIT_FAILURE_THRESHOLD", "5")),
            recovery_timeout=float(os.getenv("KAFKA_CIRCUIT_RECOVERY_TIMEOUT", "30.0")),
        )

        # Metrics
        self._metrics = {
            "messages_sent": 0,
            "messages_failed": 0,
            "last_success_time": None,
            "last_failure_time": None,
        }
        self._metrics_lock = threading.Lock()
        
        # Delivery callback results
        self._delivery_result: Optional[Dict[str, Any]] = None
        self._delivery_event = threading.Event()

    def _delivery_callback(self, err, msg):
        """Callback for message delivery reports."""
        if err is not None:
            self._delivery_result = {"success": False, "error": str(err)}
            log.error("Message delivery failed", error=str(err))
        else:
            self._delivery_result = {
                "success": True,
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
            }
            log.debug(
                "Message delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )
        self._delivery_event.set()

    def _get_producer(self) -> Producer:
        """
        Get or create Kafka producer using double-checked locking for thread safety.

        Returns:
            confluent_kafka.Producer instance

        Raises:
            RuntimeError: If producer cannot be created
        """
        # First check without lock (fast path)
        if self._producer is not None:
            return self._producer

        # Double-checked locking
        with self._producer_lock:
            # Check again inside lock
            if self._producer is not None:
                return self._producer

            try:
                log.info(
                    "Initializing Kafka producer (confluent-kafka)",
                    bootstrap_servers=self.bootstrap_servers,
                )

                config = {
                    "bootstrap.servers": self.bootstrap_servers,
                    "security.protocol": self.security_protocol,
                    "acks": "all",
                    "retries": 3,
                    "retry.backoff.ms": 1000,
                    "request.timeout.ms": KAFKA_REQUEST_TIMEOUT_MS,
                    "socket.timeout.ms": KAFKA_SOCKET_TIMEOUT_MS,
                    "message.timeout.ms": KAFKA_MESSAGE_TIMEOUT_MS,
                    "enable.idempotence": True,
                    "max.in.flight.requests.per.connection": 1,
                }

                self._producer = Producer(config)

                log.info(
                    "Kafka producer initialized successfully (confluent-kafka)",
                    bootstrap_servers=self.bootstrap_servers,
                )
                return self._producer

            except KafkaException as e:
                log.error("Kafka error initializing producer", error=str(e))
                raise RuntimeError(f"Failed to initialize Kafka producer: {e}") from e
            except Exception as e:
                log.error("Unexpected error initializing Kafka producer", error=str(e))
                raise RuntimeError(f"Failed to initialize Kafka producer: {e}") from e

    def _update_metrics(self, success: bool) -> None:
        """Update internal metrics."""
        with self._metrics_lock:
            if success:
                self._metrics["messages_sent"] += 1
                self._metrics["last_success_time"] = datetime.now(timezone.utc)
            else:
                self._metrics["messages_failed"] += 1
                self._metrics["last_failure_time"] = datetime.now(timezone.utc)

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics snapshot."""
        with self._metrics_lock:
            return {**self._metrics, "circuit_state": self._circuit_breaker.state.value}

    def set_tenant_context(self, tenant_context: "TenantContext") -> None:
        """
        Set tenant context for topic namespacing.

        Args:
            tenant_context: TenantContext instance for resolving tenant-specific topics
        """
        self._tenant_context = tenant_context
        log.info("Tenant context set for Kafka publisher")

    def resolve_topic(
        self,
        topic: str,
        tenant_id: Optional[str] = None,
    ) -> str:
        """
        Resolve Kafka topic with optional tenant namespace prefix.

        If tenant_id is provided and tenant_context is available, the topic
        will be namespaced as: {tenant_prefix}.{topic}

        Args:
            topic: Base topic name
            tenant_id: Optional tenant identifier for namespacing

        Returns:
            Resolved (potentially namespaced) topic name
        """
        if not topic:
            raise ValueError("Topic cannot be None or empty")

        # If no tenant_id or no tenant_context, return topic as-is
        if not tenant_id or not self._tenant_context:
            return topic

        try:
            namespaced_topic = self._tenant_context.resolve_kafka_topic(topic, tenant_id)
            log.debug(
                "Resolved Kafka topic with tenant namespace",
                original_topic=topic,
                namespaced_topic=namespaced_topic,
                tenant=tenant_id
            )
            return namespaced_topic
        except Exception as e:
            log.warning(
                "Failed to resolve tenant topic, using original",
                topic=topic,
                tenant=tenant_id,
                error=str(e)
            )
            return topic

    def _validate_required_params(self, **params) -> None:
        """Validate that required parameters are not None or empty."""
        for name, value in params.items():
            if value is None or (isinstance(value, str) and not value.strip()):
                raise ValueError(f"Required parameter '{name}' cannot be None or empty")

    def publish_data(
        self,
        dag_id: str,
        data: Any,
        topic: str,
        status: str = "success",
        correlation_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
    ) -> bool:
        """
        Publish data to Kafka with circuit breaker protection and optional tenant namespacing.

        Args:
            dag_id: Airflow DAG ID
            data: Data to be published
            topic: Kafka topic (will be namespaced if tenant_id provided)
            status: Status of the operation ('success', 'failure', 'warning')
            correlation_id: Optional ID for distributed tracing
            tenant_id: Optional tenant ID for topic namespacing

        Returns:
            bool: True if successfully published, False otherwise
        """
        trace_id = correlation_id or f"data-{int(time.time() * 1000)}"

        # Validate inputs
        try:
            self._validate_required_params(dag_id=dag_id, topic=topic)
        except ValueError as e:
            log.error("Validation error", trace_id=trace_id, error=str(e))
            return False

        # Resolve topic with tenant namespace if applicable
        resolved_topic = self.resolve_topic(topic, tenant_id)

        # Check circuit breaker
        if not self._circuit_breaker.allow_request():
            log.warning(
                "Circuit breaker OPEN, rejecting publish request", trace_id=trace_id
            )
            return False

        try:
            producer = self._get_producer()
            message_key = f"{dag_id}"

            message_value = json.dumps({
                "data": data,
                "status": status,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "correlation_id": trace_id,
                "tenant_id": tenant_id,  # Include tenant in message for downstream consumers
            }, default=str)

            # Reset delivery event and result
            self._delivery_event.clear()
            self._delivery_result = None

            # Produce message with callback
            producer.produce(
                topic=resolved_topic,
                key=message_key.encode("utf-8") if message_key else None,
                value=message_value.encode("utf-8"),
                callback=self._delivery_callback,
            )

            # Flush and wait for delivery
            producer.flush(timeout=KAFKA_FLUSH_TIMEOUT)

            # Check delivery result
            if self._delivery_result and self._delivery_result.get("success"):
                self._circuit_breaker.record_success()
                self._update_metrics(success=True)
                log.debug(
                    "Successfully published data",
                    trace_id=trace_id,
                    topic=resolved_topic,
                    tenant=tenant_id
                )
                return True
            else:
                error_msg = self._delivery_result.get("error", "Unknown error") if self._delivery_result else "No delivery result"
                log.error("Failed to publish data", trace_id=trace_id, error=error_msg)
                self._circuit_breaker.record_failure()
                self._update_metrics(success=False)
                return False

        except KafkaException as e:
            log.error("Kafka error publishing data", trace_id=trace_id, error=str(e))
            self._circuit_breaker.record_failure()
            self._update_metrics(success=False)
            return False

        except Exception as e:
            log.error(
                "Unexpected error publishing data", trace_id=trace_id, error=str(e)
            )
            self._circuit_breaker.record_failure()
            self._update_metrics(success=False)
            return False

    def publish_data_load_event(
        self,
        dag_id: str,
        task_id: str,
        data_source: str,
        destination: str,
        row_count: int,
        file_size_bytes: Optional[int] = None,
        execution_date: Optional[datetime] = None,
        status: str = "success",
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Publish a data load event to Kafka

        Args:
            dag_id: Airflow DAG ID
            task_id: Airflow Task ID
            data_source: Source of the data (e.g., 'rest_api', 'sftp')
            destination: Destination of the data (e.g., 'snowflake_table', 'azure_blob')
            row_count: Number of rows processed
            file_size_bytes: Size of data in bytes (optional)
            execution_date: When the task was executed
            status: Status of the operation ('success', 'failure', 'warning')
            error_message: Error message if status is 'failure'
            metadata: Additional metadata about the operation

        Returns:
            bool: True if successfully published, False otherwise
        """
        try:
            producer = self._get_producer()

            # Create event payload
            # Enrich metadata with bundle versioning
            enriched_metadata = metadata or {}
            enriched_metadata.update({
                "bundle_name": self.bundle_name,
                "bundle_version": self.bundle_version,
            })

            event = {
                "event_type": "data_load",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "dag_id": dag_id,
                "task_id": task_id,
                "data_source": {"type": data_source, "name": data_source},
                "destination": {"type": destination, "name": destination},
                "metrics": {"row_count": row_count, "file_size_bytes": file_size_bytes},
                "status": status,
                "error_message": error_message,
                "metadata": enriched_metadata,
            }

            # Create message key using dag_id and task_id
            message_key = f"{dag_id}_{task_id}"

            # Reset delivery event and result
            self._delivery_event.clear()
            self._delivery_result = None

            # Produce message with callback
            producer.produce(
                topic=self.topic_market_data,
                key=message_key.encode("utf-8"),
                value=json.dumps(event, default=str).encode("utf-8"),
                callback=self._delivery_callback,
            )

            # Flush and wait for delivery
            producer.flush(timeout=KAFKA_FLUSH_TIMEOUT)

            if self._delivery_result and self._delivery_result.get("success"):
                log.info(
                    "Successfully published data load event to Kafka",
                    topic=self._delivery_result.get("topic"),
                    partition=self._delivery_result.get("partition"),
                    offset=self._delivery_result.get("offset"),
                    dag_id=dag_id,
                    rows=row_count,
                )
                return True
            else:
                error_msg = self._delivery_result.get("error", "Unknown error") if self._delivery_result else "No delivery result"
                log.error("Failed to publish data load event", error=error_msg)
                return False

        except KafkaException as e:
            log.error("Kafka error publishing data load event", error=str(e))
            return False
        except Exception as e:
            log.error("Unexpected error publishing data load event", error=str(e))
            return False

    def publish_pipeline_event(
        self,
        dag_id: str,
        task_id: str,
        event_type: str,
        status: str,
        message: str,
        execution_date: str,
        topic: str,
        metadata: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
    ) -> bool:
        """
        Publish a general pipeline event to Kafka

        Args:
            dag_id: Airflow DAG ID
            task_id: Airflow Task ID
            event_type: Type of event (e.g., 'task_start', 'task_success', 'task_failure')
            status: Status of the event ('success', 'failure', 'warning')
            message: Message describing the event
            execution_date: When the event occurred
            topic: Kafka topic to publish to (will be namespaced if tenant_id provided)
            metadata: Additional metadata about the event
            tenant_id: Optional tenant ID for topic namespacing

        Returns:
            bool: True if successfully published, False otherwise
        """
        try:
            producer = self._get_producer()

            # Resolve topic with tenant namespace if applicable
            resolved_topic = self.resolve_topic(topic, tenant_id)

            # Enrich metadata with bundle versioning
            enriched_metadata = metadata or {}
            enriched_metadata.update({
                "bundle_name": self.bundle_name,
                "bundle_version": self.bundle_version,
            })

            # Create event payload
            event = {
                "event_type": event_type,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "dag_id": dag_id,
                "task_id": task_id,
                "execution_date": execution_date,
                "status": status,
                "message": message,
                "metadata": enriched_metadata,
                "tenant_id": tenant_id,  # Include tenant in message
            }

            # Create message key
            message_key = f"{dag_id}_{task_id}_{event_type}"

            # Reset delivery event and result
            self._delivery_event.clear()
            self._delivery_result = None

            # Produce message with callback
            producer.produce(
                topic=resolved_topic,
                key=message_key.encode("utf-8"),
                value=json.dumps(event, default=str).encode("utf-8"),
                callback=self._delivery_callback,
            )

            # Flush and wait for delivery
            producer.flush(timeout=KAFKA_FLUSH_TIMEOUT)

            if self._delivery_result and self._delivery_result.get("success"):
                log.info(
                    "Successfully published pipeline event to Kafka",
                    topic=self._delivery_result.get("topic"),
                    event_type=event_type,
                    dag_id=dag_id,
                    tenant=tenant_id,
                )
                return True
            else:
                error_msg = self._delivery_result.get("error", "Unknown error") if self._delivery_result else "No delivery result"
                log.error("Failed to publish pipeline event", error=error_msg)
                return False

        except Exception as e:
            log.error("Error publishing pipeline event", error=str(e))
            return False

    def publish_data_quality_event(
        self,
        dag_id: str,
        task_id: str,
        quality_check: str,
        result: str,
        details: Dict[str, Any],
        execution_date: Optional[datetime] = None,
    ) -> bool:
        """
        Publish a data quality event to Kafka

        Args:
            dag_id: Airflow DAG ID
            task_id: Airflow Task ID
            quality_check: Name/type of quality check performed
            result: Result of the check ('passed', 'failed', 'warning')
            details: Detailed results of the quality check
            execution_date: When the check was performed

        Returns:
            bool: True if successfully published, False otherwise
        """
        try:
            producer = self._get_producer()

            # Create event payload with bundle metadata
            event = {
                "event_type": "data_quality_check",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "dag_id": dag_id,
                "task_id": task_id,
                "execution_date": execution_date.isoformat()
                if hasattr(execution_date, "isoformat")
                else str(execution_date)
                if execution_date
                else None,
                "quality_check": quality_check,
                "result": result,
                "details": details,
                "metadata": {
                    "bundle_name": self.bundle_name,
                    "bundle_version": self.bundle_version,
                },
            }

            # Create message key
            message_key = f"{dag_id}_{task_id}_quality"

            # Reset delivery event and result
            self._delivery_event.clear()
            self._delivery_result = None

            # Produce message with callback
            producer.produce(
                topic="data-quality",
                key=message_key.encode("utf-8"),
                value=json.dumps(event, default=str).encode("utf-8"),
                callback=self._delivery_callback,
            )

            # Flush and wait for delivery
            producer.flush(timeout=KAFKA_FLUSH_TIMEOUT)

            if self._delivery_result and self._delivery_result.get("success"):
                log.info(
                    "Successfully published data quality event to Kafka",
                    check=quality_check,
                    result=result,
                    dag_id=dag_id,
                )
                return True
            else:
                error_msg = self._delivery_result.get("error", "Unknown error") if self._delivery_result else "No delivery result"
                log.error("Failed to publish data quality event", error=error_msg)
                return False

        except Exception as e:
            log.error("Error publishing data quality event", error=str(e))
            return False

    def close(self):
        """Close the Kafka producer"""
        if self._producer:
            try:
                self._producer.flush()
                log.info("Kafka producer closed")
            except Exception as e:
                log.error("Error closing Kafka producer", error=str(e))


# Global instance
kafka_publisher = KafkaEventPublisher()
