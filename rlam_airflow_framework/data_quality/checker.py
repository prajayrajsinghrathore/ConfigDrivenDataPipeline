# File: rlam_airflow_framework/data_quality/checker.py
"""
DataQualityChecker: the DQ facade.

Soda 4 is the only supported validation engine. When a pipeline configures
``soda_checks``, delegates evaluation to ``SodaEngine`` (streams the Parquet
file through DuckDB, no materialization); otherwise validation is
skipped. Handles the concerns that apply regardless: DQ metrics publishing
to Kafka.
"""

import uuid
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timezone
import structlog

from rlam_airflow_framework.kafka_publisher import kafka_publisher
from rlam_airflow_framework.data_quality.engines import (
    SODA_AVAILABLE,
    SodaEngine,
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
        results["total_rows"] = 0  # Will be populated by engine

        if engine is None:
            log.info(
                "No soda_checks configured (or Soda 4 unavailable) - "
                "skipping data quality validation"
            )
            import duckdb

            total_rows_res = duckdb.execute(
                f"SELECT COUNT(*) FROM read_parquet('{df_path}')"
            ).fetchone()
            total_rows = total_rows_res[0] if total_rows_res else 0
            results["status"] = "skipped"
            results["total_rows"] = total_rows
            self._publish_dq_metrics(results)
            return (df_path if total_rows > 0 else ""), "", results

        log.info("Running Soda 4 Contract Validation on Parquet directly")
        valid_df_path, invalid_df_path, results = engine.run_on_path(df_path, results)

        self._publish_dq_metrics(results)

        return valid_df_path, invalid_df_path, results

    def _select_engine(self) -> Optional[SodaEngine]:
        """
        Factory: pick the ValidationEngine for this pipeline's configuration.

        Soda 4 is the only supported engine. Returns None (validation
        skipped) when no ``soda_checks`` are configured or the soda-core /
        soda-duckdb packages aren't installed, rather than falling back to
        a hand-rolled re-implementation of the same checks.
        """
        if not self.soda_checks:
            return None

        if not SODA_AVAILABLE:
            log.warning(
                "Soda 4 not available - install soda-core and soda-duckdb "
                "to run configured soda_checks",
            )
            return None

        return SodaEngine(self.source_name, self.quality_gates, self.soda_checks)

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
