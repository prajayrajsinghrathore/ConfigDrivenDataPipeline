# File: rlam_airflow_framework/taskflow/events.py
"""
EventPublisher interface and implementations for decoupled observability.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union
from datetime import datetime

from rlam_airflow_framework.kafka_publisher import kafka_publisher


class EventPublisher(ABC):
    """
    Interface for publishing pipeline observability events.
    """

    @abstractmethod
    def publish_pipeline_event(
        self,
        dag_id: str,
        task_id: str,
        event_type: str,
        status: str,
        message: str,
        execution_date: Union[str, datetime],
        topic: str,
        metadata: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    def publish_data(
        self,
        dag_id: str,
        data: Any,
        topic: str,
        status: str = "success",
        correlation_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    def flush(self, timeout: Optional[int] = None) -> None:
        raise NotImplementedError


class KafkaEventPublisher(EventPublisher):
    """
    Publishes events to Kafka via the legacy rlam_airflow_framework.kafka_publisher.
    """

    def publish_pipeline_event(
        self,
        dag_id: str,
        task_id: str,
        event_type: str,
        status: str,
        message: str,
        execution_date: Union[str, datetime],
        topic: str,
        metadata: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
    ) -> bool:
        return kafka_publisher.publish_pipeline_event(
            dag_id=dag_id,
            task_id=task_id,
            event_type=event_type,
            status=status,
            message=message,
            execution_date=execution_date,
            topic=topic,
            metadata=metadata,
            tenant_id=tenant_id,
        )

    def publish_data(
        self,
        dag_id: str,
        data: Any,
        topic: str,
        status: str = "success",
        correlation_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        return kafka_publisher.publish_data(
            dag_id=dag_id,
            data=data,
            topic=topic,
            status=status,
            correlation_id=correlation_id,
            tenant_id=tenant_id,
            metadata=metadata,
        )

    def flush(self, timeout: Optional[int] = None) -> None:
        kafka_publisher.flush(timeout=timeout)


def get_event_publisher() -> EventPublisher:
    """
    Factory method to get the configured EventPublisher.
    Returns the Kafka implementation by default.
    """
    return KafkaEventPublisher()
