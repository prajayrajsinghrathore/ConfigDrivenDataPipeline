# File: rlam_airflow_framework/data_quality/checker.py
"""
DataQualityChecker: the DQ facade.

Picks the right ValidationEngine (Soda / basic-registry / legacy) for the
configured pipeline, delegates evaluation to it, and handles the concerns
that apply regardless of which engine ran: DQ metrics publishing to Kafka.
"""

import uuid
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timezone
import structlog

from rlam_airflow_framework.kafka_publisher import kafka_publisher
from rlam_airflow_framework.data_quality.engines import (
    SODA_AVAILABLE,
    SODA_AVAILABLE,
    SodaEngine,
        BasicEngine,
    LegacyEngine,
)

log = structlog.get_logger(__name__)


class DataQualityChecker:
    """
    Data Quality checker facade.

    Supports:
    - SodaCL check definitions from YAML config
    - Quality gates with fail/warn thresholds
    - Quarantine routing for invalid records (see quarantine.py)
    - DQ metrics publishing to Kafka
    """

    def __init__(self, config: Dict[str, Any], dag_id: str, task_id: str):
        """
        Initialize the data quality checker.

        Args:
            config: The full pipeline configuration containing validation settings
            dag_id: The DAG identifier for metrics
            task_id: The task identifier for metrics
        """
        self.config = config
        self.dag_id = dag_id
        self.task_id = task_id
        self.validation_config = config.get("validation", {})
        self.soda_checks = self.validation_config.get("soda_checks", {})
        self.quality_gates = self.validation_config.get("quality_gates", {})

        # Get data source name for context
        self.source_name = config.get("data_source", {}).get("name", "unknown")

        # Kafka topic for DQ events
        event_config = config.get("event", {})
        self.dq_topic = event_config.get("topic", "data-quality") + "_dq_metrics"

    def run_checks(
        self, df_path: str, destination_table: Optional[str] = None
    ) -> Tuple[str, str, Dict[str, Any]]:
        """
        Run data quality checks on a DataFrame.

        Args:
            df_path: Input Parquet file path to validate
            destination_table: Optional destination table name for context

        Returns:
            Tuple of:
                - valid_df_path: Path to valid records
                - invalid_df_path: Path to invalid records (for quarantine)
                - results: Dictionary with DQ metrics and check results
        """
        import os
        if not os.path.exists(df_path):
            log.warning("File does not exist for data quality checks")
            return df_path, "", self._empty_results()

        results = {
            "scan_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": self.source_name,
            "destination_table": destination_table,
            "total_rows": 0,
            "checks": [],
            "passed": 0,
            "failed": 0,
            "warnings": 0,
            "pass_rate": 1.0,
            "status": "passed",
        }

        engine = self._select_engine()
        results["total_rows"] = 0 # Will be populated by engine

        # Dual validation fallback logic
        if isinstance(engine, SodaEngine):
            log.info("Running Soda 4 Contract Validation on Parquet directly")
            valid_df_path, invalid_df_path, results = engine.run_on_path(df_path, results)
        else:
            log.info("Running Legacy Validation Engine (materializing Parquet to Pandas)")
            import duckdb
            df = duckdb.read_parquet(df_path).df()
            results["total_rows"] = len(df)
            
            valid_df, invalid_df, results = engine.run(df, results)
            
            # For legacy, we just return the original df_path as valid, 
            # and ignore quarantine split for now, since we are moving away from Pandas.
            # In production, dual-validation phase shouldn't rely on splitting 
            # because the goal is dropping Pandas.
            valid_df_path = df_path if len(valid_df) > 0 else ""
            invalid_df_path = ""
            
        self._publish_dq_metrics(results)

        return valid_df_path, invalid_df_path, results

    def _select_engine(self):
        """
        Factory: pick the ValidationEngine for this pipeline's configuration.
        """
        if not self.soda_checks:
            log.info("No Soda checks configured, using legacy validation rules")
            return LegacyEngine(
                self.source_name,
                self.quality_gates,
                self.config.get("validation_rules", []),
            )

        if SODA_AVAILABLE:
            return SodaEngine(self.source_name, self.quality_gates, self.soda_checks)
            
        if SODA_AVAILABLE:
            return SodaEngine(self.source_name, self.quality_gates, self.soda_checks)

        log.warning(
            "Soda Core not available", install_cmd="pip install soda-core-pandas"
        )
        return BasicEngine(self.source_name, self.quality_gates, self.soda_checks)

    def _empty_results(self) -> Dict[str, Any]:
        """Return empty results structure."""
        return {
            "scan_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": self.source_name,
            "total_rows": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "checks": [],
            "passed": 0,
            "failed": 0,
            "warnings": 0,
            "pass_rate": 1.0,
            "status": "skipped",
        }

    def _publish_dq_metrics(self, results: Dict[str, Any]) -> None:
        """
        Publish DQ metrics to Kafka.
        """
        try:
            kafka_publisher.publish_pipeline_event(
                dag_id=self.dag_id,
                task_id=self.task_id,
                event_type="data_quality_metrics",
                status=results.get("status", "unknown"),
                message=(
                    f"DQ scan for {self.source_name}: "
                    f"{results.get('passed', 0)} passed, "
                    f"{results.get('failed', 0)} failed, "
                    f"{results.get('warnings', 0)} warnings"
                ),
                execution_date=results.get(
                    "timestamp", datetime.now(timezone.utc).isoformat()
                ),
                topic=self.dq_topic,
                metadata=results,
            )
        except Exception as e:
            log.warning("Failed to publish DQ metrics to Kafka", error=str(e))

    def should_fail_pipeline(self, results: Dict[str, Any]) -> bool:
        """
        Determine if the pipeline should fail based on DQ results.
        """
        return results.get("status") == "failed"

    def should_quarantine(self) -> bool:
        """
        Check if quarantine is enabled in config.
        """
        return self.quality_gates.get("quarantine_invalid", False)


def run_data_quality_checks(
    df_path: str,
    config: Dict[str, Any],
    dag_id: str,
    task_id: str,
    destination_table: Optional[str] = None,
) -> Tuple[str, str, Dict[str, Any]]:
    """
    Convenience function to run data quality checks.

    Args:
        df_path: Input Parquet file path
        config: Pipeline configuration
        dag_id: DAG identifier
        task_id: Task identifier
        destination_table: Optional destination table name

    Returns:
        Tuple of (valid_df_path, invalid_df_path, results)
    """
    checker = DataQualityChecker(config, dag_id, task_id)
    return checker.run_checks(df_path, destination_table)

