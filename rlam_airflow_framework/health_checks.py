# File: dags/utils/health_checks.py
"""
Health check utilities for pre-flight validation of external services.

Provides:
- KafkaHealthSensor: Airflow sensor to verify Kafka connectivity before pipeline execution
- Utility functions for manual health checks

This module only provides health checks for globally configured infrastructure (Kafka).
Pipeline-specific connections (Snowflake, Azure, SFTP) are validated at task execution time
since their connection IDs are determined per-pipeline configuration.
"""

import os
from typing import Optional, Dict, Any
import structlog

try:
    from airflow.sdk import task
    from airflow.sdk import PokeReturnValue
    AIRFLOW_AVAILABLE = True
except ImportError:
    # For testing without Airflow
    task = None
    PokeReturnValue = None
    AIRFLOW_AVAILABLE = False

try:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient
    KAFKA_AVAILABLE = True
except ImportError:
    Producer = None
    AdminClient = None
    KAFKA_AVAILABLE = False

log = structlog.get_logger(__name__)

# Load timeout configuration from global settings or environment
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
DEFAULT_POKE_INTERVAL = int(os.getenv("TIMEOUT_HEALTH_KAFKA_POKE", "5"))
DEFAULT_TIMEOUT = int(os.getenv("TIMEOUT_HEALTH_KAFKA_TIMEOUT", "60"))
DEFAULT_SOCKET_TIMEOUT = int(os.getenv("TIMEOUT_KAFKA_SOCKET", "5")) * 1000  # Convert to ms


class KafkaHealthCheckError(Exception):
    """Raised when Kafka health check fails."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.details = details or {}
        super().__init__(message)


def check_kafka_health(
    bootstrap_servers: Optional[str] = None,
    timeout_ms: int = DEFAULT_SOCKET_TIMEOUT,
) -> Dict[str, Any]:
    """
    Check Kafka cluster health by requesting broker metadata.
    
    This is a lightweight check that verifies:
    - Network connectivity to Kafka brokers
    - Broker is responding to metadata requests
    - At least one broker is available
    
    Args:
        bootstrap_servers: Kafka bootstrap servers (default: from env KAFKA_BOOTSTRAP_SERVERS)
        timeout_ms: Timeout for the metadata request in milliseconds
        
    Returns:
        Dict with health check results:
        - healthy: bool indicating if Kafka is reachable
        - brokers: number of brokers found
        - topics: number of topics found (optional)
        - error: error message if unhealthy
        
    Raises:
        KafkaHealthCheckError: If Kafka libraries are not available
    """
    if not KAFKA_AVAILABLE:
        raise KafkaHealthCheckError(
            "confluent-kafka library not available",
            details={"library": "confluent-kafka", "installed": False}
        )
    
    servers = bootstrap_servers or DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    trace_id = f"kafka-health-{int(__import__('time').time() * 1000)}"
    
    log.info("Starting Kafka health check", trace_id=trace_id, bootstrap_servers=servers)
    
    try:
        # Use AdminClient for metadata request (lighter than Producer)
        admin_config = {
            "bootstrap.servers": servers,
            "socket.timeout.ms": timeout_ms,
            "request.timeout.ms": timeout_ms,
        }
        
        admin = AdminClient(admin_config)
        
        # Request cluster metadata - this validates connectivity
        cluster_metadata = admin.list_topics(timeout=timeout_ms / 1000)
        
        broker_count = len(cluster_metadata.brokers)
        topic_count = len(cluster_metadata.topics)
        
        if broker_count == 0:
            log.warning("Kafka health check: no brokers found", trace_id=trace_id)
            return {
                "healthy": False,
                "brokers": 0,
                "topics": topic_count,
                "error": "No brokers available in cluster",
                "bootstrap_servers": servers,
            }
        
        log.info(
            "Kafka health check passed",
            trace_id=trace_id,
            brokers=broker_count,
            topics=topic_count,
        )
        
        return {
            "healthy": True,
            "brokers": broker_count,
            "topics": topic_count,
            "error": None,
            "bootstrap_servers": servers,
        }
        
    except Exception as e:
        error_msg = str(e)
        log.error(
            "Kafka health check failed",
            trace_id=trace_id,
            error=error_msg,
            bootstrap_servers=servers,
        )
        
        return {
            "healthy": False,
            "brokers": 0,
            "topics": 0,
            "error": error_msg,
            "bootstrap_servers": servers,
        }


# =============================================================================
# Airflow 3.x TaskFlow API Sensor
# =============================================================================

if AIRFLOW_AVAILABLE and task is not None:
    @task.sensor(poke_interval=DEFAULT_POKE_INTERVAL, timeout=DEFAULT_TIMEOUT, mode="reschedule")
    def wait_for_kafka_health(bootstrap_servers: Optional[str] = None) -> PokeReturnValue:
        """
        Modern @task.sensor for Kafka health check (Airflow 3.x).
        
        Waits for Kafka to be healthy before proceeding with pipeline execution.
        Uses 'reschedule' mode to free up worker slots while waiting.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers (default: from env)
            
        Returns:
            PokeReturnValue with health check result
            
        Example usage in TaskFlow DAG:
            @dag(...)
            def my_pipeline():
                kafka_ready = wait_for_kafka_health()
                data = ingest_data(config)
                
                kafka_ready >> data
        """
        servers = bootstrap_servers or DEFAULT_KAFKA_BOOTSTRAP_SERVERS
        
        log.info("Checking Kafka health", bootstrap_servers=servers)
        
        result = check_kafka_health(
            bootstrap_servers=servers,
            timeout_ms=DEFAULT_SOCKET_TIMEOUT,
        )
        
        if result["healthy"]:
            log.info(
                "Kafka health check succeeded",
                brokers=result["brokers"],
                topics=result["topics"],
            )
        else:
            log.warning(
                "Kafka health check failed, will retry",
                error=result["error"],
            )
        
        return PokeReturnValue(
            is_done=result["healthy"],
            xcom_value=result,
        )
