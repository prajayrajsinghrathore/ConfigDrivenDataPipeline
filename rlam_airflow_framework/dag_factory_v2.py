# File: rlam_airflow_framework/dag_factory_v2.py
"""
DAG factory V2 using Airflow 3.x TaskFlow API and @dag decorator.

Creates data pipelines with:
- @dag decorator for cleaner syntax
- TaskFlow tasks (@task, @task.sensor, @task.branch)
- HITL operators (ApprovalOperator, HITLOperator) for human approvals
- Asset lineage tracking
- Deadline alerts with Kafka notifications
- Multi-tenancy support
- Conditional HITL approval workflow

Note: HITL functionality uses operator classes (ApprovalOperator, HITLOperator, etc.)
not task decorators. See Airflow 3.1.6 HITL documentation.
"""

from airflow.sdk import dag, DAG
from airflow.sdk.definitions.asset import Asset
from datetime import timedelta
from typing import Dict, Any, List, Optional, cast
import structlog
import urllib.parse

# Import HITL operators for human approval workflows

from rlam_airflow_framework.config import ConfigLoader

# Import tenant context for multi-tenancy support
# Tenant context shim removed for clean break

# Import deadline notifiers (framework package — shipped in the image)
from rlam_airflow_framework.factory.schedule_resolver import ScheduleResolver
from rlam_airflow_framework.factory.task_builder import TaskBuilder

log = structlog.get_logger(__name__)


class DAGFactoryV2:
    """
    Factory class that creates TaskFlow-based DAGs from configurations.

    Airflow 3.x Features:
    - @dag decorator for DAG definition
    - TaskFlow tasks with automatic XCom
    - @task.sensor for Kafka health checks
    - ApprovalOperator for quarantine approval (HITL)
    - @task.branch for conditional routing
    - Asset-based lineage tracking
    - Deadline alerts
    - Multi-tenancy support
    """

    def __init__(self, config_loader: Optional[ConfigLoader] = None):
        # Inject the config loader so the factory depends on the abstraction, not
        # a hard-wired instance - lets tests supply a stub/fake loader.
        self.config_loader = config_loader or ConfigLoader()
        self.global_settings = self.config_loader.load_global_settings()

        # Initialize tenant context for multi-tenancy
        self.tenant_context = self.config_loader.get_tenant_context()
        if self.tenant_context:
            log.info(
                "DAGFactoryV2 initialized with multi-tenancy support",
                tenants=self.tenant_context.get_defined_tenants(),
            )
        else:
            log.warning("DAGFactoryV2 initialized without multi-tenancy support")

        # Detect DAG bundle metadata from environment or bundle storage path
        self.bundle_name = self._detect_bundle_name()
        self.bundle_version = self._detect_bundle_version()
        log.info(
            "DAG bundle detected",
            bundle_name=self.bundle_name,
            bundle_version=self.bundle_version,
        )

    def create_dag_from_config(self, config: Dict[str, Any]) -> DAG:
        """
        Create a TaskFlow DAG from configuration using @dag decorator.

        Orchestrates the focused collaborators below (schedule resolution,
        partition planning, max-active-runs policy, and task-graph building);
        each is independently testable.

        Args:
            config: Pipeline configuration from YAML

        Returns:
            Instantiated DAG
        """
        data_source = config["data_source"]
        schedule_config = config.get("schedule", {})
        metadata = config.get("metadata", {})

        # Multi-tenancy: every pipeline must declare its tenant
        tenant_id = metadata.get("tenant")
        if not tenant_id:
            raise ValueError(
                f"Missing metadata.tenant for data source '{data_source.get('name', 'unknown')}'. "
                f"All pipelines must belong to a tenant."
            )

        # Reject pipelines that reference another tenant's connection_id
        # before the DAG is even built (see resolve_connection_id's
        # cross-tenant guard in tenant_context.py).
        self._validate_pipeline_connections(config, tenant_id)

        source_name = data_source["name"]
        dag_id = self._resolve_dag_id(source_name, tenant_id)

        # Resolve the cross-cutting DAG settings via focused collaborators
        is_partitioned = config.get("partition", {}).get("enabled", False)
        timezone = self._get_timezone(schedule_config, dag_id)
        schedule = ScheduleResolver.resolve_schedule(
            schedule_config, dag_id, timezone, is_partitioned
        )
        tags = self._resolve_tags(tenant_id, schedule_config)
        default_args = self._create_default_args(tenant_id, metadata, schedule_config)
        pool = self._resolve_pool(tenant_id)
        tenant_pool_slots = self._get_tenant_pool_slots(tenant_id, pool)
        deadline = TaskBuilder.create_deadline_alert(
            dag_id, schedule_config, tenant_id, self._resolve_kafka_bootstrap_servers
        )
        inlets, outlets = self._create_assets(config)

        # Partition planning may replace the schedule interval with a timetable
        schedule_interval, is_partitioned, max_fan_out = (
            ScheduleResolver.plan_partitioning(
                config, schedule, inlets, dag_id, source_name, tenant_pool_slots
            )
        )
        max_active_runs = ScheduleResolver.compute_max_active_runs(
            schedule_config, is_partitioned, tenant_pool_slots, dag_id
        )

        dag_kwargs = {
            "dag_id": dag_id,
            "description": f"Data integration pipeline for {source_name} (tenant: {tenant_id})",
            "schedule": schedule_interval,
            "start_date": schedule.start_date,
            "end_date": schedule.end_date,
            "catchup": schedule.catchup,
            "tags": tags,
            "max_active_runs": max_active_runs,
            "default_args": default_args,
            "deadline": deadline,
            # max_fan_out is surfaced here so create_all_dags can aggregate
            # per-tenant declared concurrency for admission control without
            # re-deriving partition config.
            "params": {
                "tenant_id": tenant_id,
                "tenant_pool": pool,
                "max_fan_out": max_fan_out,
            },
        }

        if "rerun_with_latest_version" in metadata:
            dag_kwargs["rerun_with_latest_version"] = metadata[
                "rerun_with_latest_version"
            ]

        @dag(**dag_kwargs)
        def create_pipeline():
            """TaskFlow pipeline definition (see TaskBuilder.build_task_graph)."""
            TaskBuilder.build_task_graph(
                config,
                pool,
                inlets,
                outlets,
                dag_id,
                tenant_id,
                self._resolve_kafka_bootstrap_servers,
            )

        # cast: the @dag stub types the call as the wrapped function's return
        # (None), but it returns the built DAG at runtime
        pipeline_dag = cast(DAG, create_pipeline())

        log.info(
            "Created TaskFlow DAG with multi-tenancy",
            dag_id=dag_id,
            tenant=tenant_id,
            pool=pool,
            tags=tags,
        )

        return pipeline_dag

    def _validate_pipeline_connections(
        self, config: Dict[str, Any], tenant_id: str
    ) -> None:
        """
        Validate that every connection_id declared anywhere in this
        pipeline's YAML — source, transformation lookups, and destinations —
        belongs to this tenant.

        Fetcher/transformer/destination classes all read ``connection_id``
        straight off their config dict, so an unvalidated YAML could
        reference another tenant's registered connection (as a source to
        read from, a Snowflake lookup to join against, or a destination to
        write to) and gain access to its warehouse. Raises here at
        DAG-parse time so a spoofed config fails loudly instead of quietly
        reading/writing another tenant's data at runtime.
        """
        if not self.tenant_context:
            return

        blocks = []

        data_source = config.get("data_source", {})
        if isinstance(data_source, dict) and "connection_id" in data_source:
            blocks.append((data_source.get("type", "data_source"), data_source))

        for step in config.get("transformations", []):
            if isinstance(step, dict) and "connection_id" in step:
                blocks.append((step.get("type", "transformation"), step))

        destination = config.get("destination", {})
        if "connection_id" in destination or "type" in destination:
            blocks.append((destination.get("type", "unknown"), destination))
        for key in ("primary", "secondary", "quarantine"):
            block = destination.get(key)
            if isinstance(block, dict):
                blocks.append((block.get("type", key), block))

        for block_type, block_config in blocks:
            # Raises TenantValidationError if connection_id belongs to
            # a different tenant.
            self.tenant_context.resolve_connection_id(
                block_config, block_type, tenant_id
            )

    def _get_timezone(self, schedule_config: Dict[str, Any], dag_id: str) -> str:
        """
        Get timezone for DAG scheduling with fallback to global default.

        Args:
            schedule_config: Schedule configuration from YAML
            dag_id: DAG identifier for logging

        Returns:
            IANA timezone string (e.g., "UTC", "America/New_York")
        """
        # Check for pipeline-specific timezone
        pipeline_tz = schedule_config.get("timezone")

        if pipeline_tz:
            log.info(
                "Using pipeline-specific timezone",
                dag_id=dag_id,
                timezone=pipeline_tz,
                source="pipeline_config",
            )

            # Log warning for non-UTC timezones about DST implications
            if pipeline_tz.upper() != "UTC":
                log.info(
                    "WARNING: NON-UTC TIMEZONE DETECTED - Pipeline is exposed to Daylight Saving Time transitions",
                    dag_id=dag_id,
                    timezone=pipeline_tz,
                    dst_impact="Cron schedules will adjust for DST; timedelta schedules will not",
                    recommendation="Consider using UTC for predictable execution times",
                )

            return pipeline_tz

        # Fallback to global default timezone
        global_tz = self.global_settings.get("default_settings", {}).get(
            "default_timezone", "UTC"
        )

        log.info(
            "Using global default timezone (pipeline config did not specify timezone)",
            dag_id=dag_id,
            timezone=global_tz,
            source="global_settings",
        )

        return global_tz

    def _resolve_dag_id(self, source_name: str, tenant_id: str) -> str:
        """Generate the tenant-prefixed DAG id."""
        if self.tenant_context:
            return self.tenant_context.get_dag_id(source_name, tenant_id)
        return f"{tenant_id}_{source_name}"

    def _resolve_tags(
        self, tenant_id: str, schedule_config: Dict[str, Any]
    ) -> List[str]:
        """Merge (and deduplicate) tenant tags with schedule-configured tags."""
        config_tags = schedule_config.get("tags", [])
        if self.tenant_context:
            tenant_tags = self.tenant_context.get_tenant_tags(tenant_id)
        else:
            tenant_tags = [f"tenant:{tenant_id}"]
        return list(set(tenant_tags + config_tags))

    def _resolve_pool(self, tenant_id: str) -> Optional[str]:
        """Get the tenant pool for resource isolation, if multi-tenancy is on."""
        if self.tenant_context:
            return self.tenant_context.get_tenant_pool(tenant_id)
        return None

    def _get_tenant_pool_slots(
        self, tenant_id: str, pool: Optional[str]
    ) -> Optional[int]:
        """Look up the declared slot count for a tenant's pool, if any."""
        if not pool:
            return None
        tenants_config = self.global_settings.get("tenants", {})
        tenant_config = tenants_config.get(tenant_id, {})
        return tenant_config.get("slots")

    def _resolve_kafka_bootstrap_servers(
        self, event_config: Dict[str, Any], tenant_id: str
    ) -> Optional[str]:
        """
        Resolve Kafka bootstrap servers with 3-tier priority:
        1. Explicit event.bootstrap_servers in the pipeline config
        2. Tenant's kafka.bootstrap_servers in global_settings.yaml
        3. None (health_checks falls back to the global KAFKA_BOOTSTRAP_SERVERS)

        Every tenant shares one Kafka cluster today, so tiers 1-2 are unused
        in practice - this only matters once a tenant or pipeline needs to
        point at a different cluster.
        """
        if "bootstrap_servers" in event_config:
            return event_config["bootstrap_servers"]
        if self.tenant_context:
            return self.tenant_context.get_tenant_kafka_bootstrap_servers(tenant_id)
        return None

    def _create_assets(self, config: Dict[str, Any]) -> tuple[List[Asset], List[Asset]]:
        """
        Create Asset definitions for data lineage tracking.

        Assets enable Airflow's Asset API to track data dependencies
        and provide lineage information via the scheduled_dags API.

        Args:
            config: Pipeline configuration

        Returns:
            Tuple of (inlets, outlets) Asset lists
        """
        inlets = []
        outlets = []

        data_source = config.get("data_source", {})
        destination = config.get("destination", {})

        # Create inlet asset for data source
        source_name = data_source.get("name", "unknown")
        source_type = data_source.get("type", "unknown")

        if source_type == "rest_api":
            endpoint = data_source.get("endpoint", "")
            inlet_uri = f"api://{source_name}/{endpoint.replace('https://', '').replace('http://', '').split('/')[0]}"
        elif source_type == "sftp":
            remote_path = data_source.get("remote_path", "")
            inlet_uri = f"sftp://{source_name}/{remote_path}"
        else:
            inlet_uri = f"{source_type}://{source_name}"

        inlets.append(Asset(uri=inlet_uri))

        # Create outlet asset for destination
        # Support both flat structure (destination.type) and nested (destination.primary.type)
        primary_dest = destination.get("primary", destination)
        dest_type = primary_dest.get("type", "unknown")

        if dest_type == "snowflake_table":
            # AIP-60 snowflake URI: snowflake://<account>/<database>/<schema>/<table>.
            # The snowflake provider's URI normalizer hard-rejects anything else at
            # Asset construction (i.e. at DAG-parse time), so a bare
            # f"snowflake://{table}" broke every snowflake-sink pipeline on real
            # Airflow 3.3 (caught 2026-07-23 when the dag lane ran unmocked).
            # Explicit fields win; dotted table refs (DB.SCHEMA.TABLE) fill the
            # rest; placeholders keep parse-time construction DB-free.
            table_ref = str(primary_dest.get("table", "unknown"))
            parts = table_ref.split(".")
            table = parts[-1]
            schema = primary_dest.get("schema") or (
                parts[-2] if len(parts) > 1 else "default"
            )
            database = primary_dest.get("database") or (
                parts[-3] if len(parts) > 2 else "default"
            )
            account = (
                primary_dest.get("account") or primary_dest.get("conn_id") or "default"
            )
            account_safe = urllib.parse.quote(account, safe="")
            database_safe = urllib.parse.quote(database, safe="")
            schema_safe = urllib.parse.quote(schema, safe="")
            table_safe = urllib.parse.quote(table, safe="")
            outlet_uri = (
                f"snowflake://{account_safe}/{database_safe}/{schema_safe}/{table_safe}"
            )
        elif dest_type == "object_storage":
            uri = primary_dest.get("uri", "")
            path = primary_dest.get("path", "")
            storage_path = uri or path
            outlet_uri = (
                storage_path
                if storage_path
                else f"object_storage://{source_name}_output"
            )
        elif dest_type == "local_file":
            path = primary_dest.get("path", "")
            outlet_uri = (
                f"file://{path}" if path else f"file:///tmp/{source_name}_output"
            )
        else:
            outlet_uri = f"{dest_type}://{source_name}_output"

        outlets.append(Asset(uri=outlet_uri))

        log.debug(
            "Created assets for lineage",
            source=source_name,
            inlets=[a.uri for a in inlets],
            outlets=[a.uri for a in outlets],
        )

        return inlets, outlets

    def create_all_dags(self) -> List[DAG]:
        """Create all DAGs from configuration files."""
        configs = self.config_loader.load_data_source_configs()
        dags = []

        for config in configs:
            try:
                # Enrich config with bundle metadata for tasks
                config["_bundle_metadata"] = {
                    "bundle_name": self.bundle_name,
                    "bundle_version": self.bundle_version,
                }

                dag_instance = self.create_dag_from_config(config)
                dags.append(dag_instance)
                log.info("Created TaskFlow DAG", dag_id=dag_instance.dag_id)
            except Exception as e:
                source_name = config.get("data_source", {}).get("name", "unknown")
                log.error(
                    "Error creating TaskFlow DAG", source=source_name, error=str(e)
                )

        self._check_tenant_admission(dags)
        return dags

    def _check_tenant_admission(self, dags: List[DAG]) -> None:
        """
        Warn when a tenant's aggregate declared concurrency across all its
        pipelines heavily oversubscribes its pool.

        create_all_dags is the only place with visibility across every
        tenant's pipelines at once, so it's where cross-DAG oversubscription
        (invisible to any single DAG's own config) can actually be caught.
        Warns rather than raises so one misconfigured pipeline doesn't take
        down a tenant's otherwise-healthy DAGs; the equivalent hard-fail
        check runs at PR time in tests/unit/test_tenant_quotas.py.
        """
        multiplier = self.global_settings.get("platform", {}).get(
            "max_tenant_oversubscription", 5
        )
        tenants_config = self.global_settings.get("tenants", {})

        aggregate_by_tenant: Dict[str, int] = {}
        for dag_instance in dags:
            tenant_id = dag_instance.params.get("tenant_id")
            if not tenant_id:
                continue
            max_fan_out = dag_instance.params.get("max_fan_out") or 1
            max_active_runs = dag_instance.max_active_runs or 1
            aggregate_by_tenant[tenant_id] = (
                aggregate_by_tenant.get(tenant_id, 0) + max_active_runs * max_fan_out
            )

        for tenant_id, aggregate in aggregate_by_tenant.items():
            tenant_pool_slots = tenants_config.get(tenant_id, {}).get("slots")
            if tenant_pool_slots is None:
                continue
            threshold = tenant_pool_slots * multiplier
            if aggregate > threshold:
                log.warning(
                    f"Tenant '{tenant_id}' aggregate declared concurrency "
                    f"({aggregate}) exceeds {multiplier}x its pool size "
                    f"({tenant_pool_slots} slots, threshold {threshold}). "
                    f"Pipelines will queue heavily; reduce max_active_runs/"
                    f"max_fan_out or request more pool slots.",
                    tenant_id=tenant_id,
                )

    def _detect_bundle_name(self) -> str:
        """
        Detect DAG bundle name from environment or infer from paths.

        Returns:
            Bundle name (e.g., 'operations_configs', 'local_dag_bundle')
        """
        import os

        # Check environment variable first (set by Airflow DAG processor)
        bundle_name = os.getenv("AIRFLOW_DAG_BUNDLE_NAME")
        if bundle_name:
            return bundle_name

        # Fallback to detecting from config path
        config_path = os.getenv(
            "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_STORAGE_PATH", "/opt/airflow/dags"
        )
        if "bundle_storage" in config_path:
            # Extract bundle name from path like /opt/airflow/bundle_storage/operations_configs
            parts = config_path.rstrip("/").split("/")
            if len(parts) > 0:
                return parts[-1]

        return "local_dag_bundle"

    def _detect_bundle_version(self) -> str:
        """
        Detect DAG bundle version from environment, Git, or bundle metadata.

        Returns:
            Bundle version (Git tag/commit SHA or 'unknown')
        """
        import os
        import subprocess

        # Check environment variable first (set by CI/CD or Airflow)
        bundle_version = os.getenv("AIRFLOW_DAG_BUNDLE_VERSION")
        if bundle_version:
            return bundle_version

        # Try to detect Git commit SHA from bundle storage path
        bundle_storage = os.getenv("AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_STORAGE_PATH")
        if bundle_storage and os.path.isdir(bundle_storage):
            try:
                result = subprocess.run(
                    ["git", "rev-parse", "--short", "HEAD"],
                    cwd=bundle_storage,
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0:
                    return result.stdout.strip()
            except Exception as e:
                log.debug(
                    "Could not detect Git version from bundle storage", error=str(e)
                )

        # Fallback to checking local dags directory
        try:
            dags_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=dags_dir,
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception as e:
            log.debug("Could not detect Git version from dags directory", error=str(e))

        return "unknown"

    def _create_default_args(
        self,
        tenant_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        schedule_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create default arguments for DAGs with tenant owner mapping and bundle metadata."""
        defaults = self.global_settings.get("default_settings", {})

        # Determine owner: metadata override > tenant owner > default
        owner = "data-team"  # Ultimate fallback
        if self.tenant_context:
            owner = self.tenant_context.get_tenant_owner(tenant_id)
        if metadata and metadata.get("owner"):
            owner = metadata["owner"]  # Explicit override in metadata

        default_args = {
            "owner": owner,
            "retries": defaults.get("retry_count", 3),
            "retry_delay": timedelta(minutes=5),
            "email_on_failure": defaults.get("email_on_failure", True),
            "email_on_retry": defaults.get("email_on_retry", False),
            # Add bundle metadata for observability in task logs
            "bundle_name": self.bundle_name,
            "bundle_version": self.bundle_version,
        }

        # Configure retry policy
        schedule = schedule_config or {}
        retry_config = schedule.get("retry", {})

        # Numeric backoff
        if "exponential_backoff" in retry_config:
            default_args["retry_exponential_backoff"] = retry_config[
                "exponential_backoff"
            ]

        if "retry_delay_seconds" in retry_config:
            default_args["retry_delay"] = timedelta(
                seconds=retry_config["retry_delay_seconds"]
            )

        if "max_retries" in retry_config:
            default_args["retries"] = retry_config["max_retries"]

        # Apply transient retry policy if configured
        if retry_config.get("policy") == "transient":
            from rlam_airflow_framework.utils.retry_policy import (
                build_transient_retry_policy,
            )

            default_args["retry_policy"] = build_transient_retry_policy(retry_config)

        return default_args
