"""
Kafka publisher utilities for sending pipeline events
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os

logger = logging.getLogger(__name__)

class KafkaEventPublisher:
    """Publisher for sending pipeline events to Kafka"""
    
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        self.topic_market_data = os.getenv('KAFKA_TOPIC_MARKET_DATA', 'Market_Data')
        self.security_protocol = os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
        self.producer = None
        
    def _get_producer(self) -> KafkaProducer:
        """Get or create Kafka producer"""
        if self.producer is None:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    security_protocol=self.security_protocol,
                    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    retries=3,
                    retry_backoff_ms=1000,
                    request_timeout_ms=30000,
                    acks='all'
                )
                logger.info(f"Kafka producer initialized for {self.bootstrap_servers}")
            except Exception as e:
                logger.error(f"Failed to initialize Kafka producer: {e}")
                raise
        return self.producer
    
    def publish_data(
        self,
        dag_id: str,
        data: str,
        topic: str,
        status: str = "success"
    ) -> bool:
        """
        Publish data to Kafka
        Args:
            dag_id: Airflow DAG ID
            data: Data to be published (e.g., JSON string)
            status: Status of the operation ('success', 'failure', 'warning')

        """
        try:
            producer = self._get_producer()
            message_key = f"{dag_id}"
            producer.send(
                topic=topic,
                key=message_key,
                value={
                    "data": data,
                    "status": status,
                    "timestamp": datetime.now(timezone.utc),
                }
            )
            return True
        except Exception as e:
            logger.error(f"Failed to publish data to Kafka: {e}")
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
        metadata: Optional[Dict[str, Any]] = None
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
            event = {
                "event_type": "data_load",
                "timestamp": datetime.utcnow().isoformat(),
                "dag_id": dag_id,
                "task_id": task_id,
                #"execution_date": execution_date if hasattr(execution_date, 'isoformat') else str(execution_date) if execution_date else None,
                "data_source": {
                    "type": data_source,
                    "name": data_source
                },
                "destination": {
                    "type": destination,
                    "name": destination
                },
                "metrics": {
                    "row_count": row_count,
                    "file_size_bytes": file_size_bytes
                },
                "status": status,
                "error_message": error_message,
                "metadata": metadata or {}
            }
            
            # Create message key using dag_id and task_id
            message_key = f"{dag_id}_{task_id}"
            
            # Send to Kafka
            future = producer.send(
                topic=self.topic_market_data,
                key=message_key,
                value=event
            )
            
            # Wait for the message to be sent (with timeout)
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Successfully published data load event to Kafka: "
                f"topic={record_metadata.topic}, "
                f"partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}, "
                f"dag_id={dag_id}, "
                f"rows={row_count}"
            )
            
            return True
            
        except KafkaError as e:
            logger.error(f"Kafka error publishing data load event: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error publishing data load event: {e}")
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
        metadata: Optional[Dict[str, Any]] = None
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
            topic: Kafka topic to publish to
            metadata: Additional metadata about the event
            
        Returns:
            bool: True if successfully published, False otherwise
        """
        try:
            producer = self._get_producer()
            
            # Create event payload
            event = {
                "event_type": event_type,
                "timestamp": datetime.utcnow().isoformat(),
                "dag_id": dag_id,
                "task_id": task_id,
                "execution_date": execution_date,
                "status": status,
                "message": message,
                "metadata": metadata or {}
            }
            
            # Create message key
            message_key = f"{dag_id}_{task_id}_{event_type}"
            
            # Send to pipeline-events topic
            futureRecordMetadata = producer.send(
                topic=topic,
                key=message_key,
                value=event
            )
            
            # Wait for the message to be sent
            record_metadata = futureRecordMetadata.get(timeout=10)
            
            logger.info(
                f"Successfully published pipeline event to Kafka: "
                f"topic={record_metadata.topic}, "
                f"event_type={event_type}, "
                f"dag_id={dag_id}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error publishing pipeline event: {e}")
            return False
    
    def publish_data_quality_event(
        self,
        dag_id: str,
        task_id: str,
        quality_check: str,
        result: str,
        details: Dict[str, Any],
        execution_date: Optional[datetime] = None
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
            
            # Create event payload
            event = {
                "event_type": "data_quality_check",
                "timestamp": datetime.utcnow().isoformat(),
                "dag_id": dag_id,
                "task_id": task_id,
                "execution_date": execution_date.isoformat() if hasattr(execution_date, 'isoformat') else str(execution_date) if execution_date else None,
                "quality_check": quality_check,
                "result": result,
                "details": details
            }
            
            # Create message key
            message_key = f"{dag_id}_{task_id}_quality"
            
            # Send to data-quality topic
            future = producer.send(
                topic='data-quality',
                key=message_key,
                value=event
            )
            
            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Successfully published data quality event to Kafka: "
                f"check={quality_check}, "
                f"result={result}, "
                f"dag_id={dag_id}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error publishing data quality event: {e}")
            return False
    
    def close(self):
        """Close the Kafka producer"""
        if self.producer:
            try:
                self.producer.flush()
                self.producer.close()
                logger.info("Kafka producer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")

# Global instance
kafka_publisher = KafkaEventPublisher()