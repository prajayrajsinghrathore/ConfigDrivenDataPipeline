# File: dags/utils/dag_factory_v2.py
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

This replaces the legacy DAGFactory with operator-based tasks.

Note: HITL functionality uses operator classes (ApprovalOperator, HITLOperator, etc.)
not task decorators. See Airflow 3.1.6 HITL documentation.
"""

from airflow.sdk import (
    dag,
    DAG,
    AssetAll,
    CronPartitionTimetable,
    PartitionedAssetTimetable,
    PartitionedAtRuntime,
    RollupMapper,
    FanOutMapper,
    FixedKeyMapper,
    IdentityMapper,
    DayWindow,
    WeekWindow,
    MonthWindow,
    QuarterWindow,
    YearWindow,
    WaitForAll,
    MinimumCount,
    StartOfDayMapper,
    StartOfWeekMapper,
    StartOfMonthMapper,
    StartOfQuarterMapper,
    StartOfYearMapper,
)
from airflow.sdk.definitions.asset import Asset
from airflow.sdk.definitions.callback import SyncCallback
from airflow.sdk.definitions.deadline import (
    DeadlineAlert,
    DeadlineReference,
)
from datetime import timedelta
from typing import Dict, Any, List, Optional, cast
import pendulum
import structlog

# Import HITL operators for human approval workflows
    
from rlam_airflow_framework.config_loader import ConfigLoader
from rlam_airflow_framework.taskflow_tasks import (
    ingest_data,
    transform_data,
    validate_data_quality,
    route_dq_results,
    quarantine_invalid_data,
    prepare_hitl_approval_context,
    process_approval_decision,
    load_data,
)
from rlam_airflow_framework.health_checks import wait_for_kafka_health

# Import tenant context for multi-tenancy support
# Tenant context shim removed for clean break

# Import deadline notifiers
from deadline_callbacks import CompositeDeadlineNotifier

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

    def __init__(self):
        self.config_loader = ConfigLoader()
        self.global_settings = self.config_loader.load_global_settings()
        
        # Initialize tenant context for multi-tenancy
        self.tenant_context = self.config_loader.get_tenant_context()
        if self.tenant_context:
            log.info(
                "DAGFactoryV2 initialized with multi-tenancy support",
                tenants=self.tenant_context.get_defined_tenants()
            )
        else:
            log.warning("DAGFactoryV2 initialized without multi-tenancy support")

        # Detect DAG bundle metadata from environment or bundle storage path
        self.bundle_name = self._detect_bundle_name()
        self.bundle_version = self._detect_bundle_version()
        log.info(
            "DAG bundle detected",
            bundle_name=self.bundle_name,
            bundle_version=self.bundle_version
        )

    def create_dag_from_config(self, config: Dict[str, Any]) -> DAG:
        """
        Create a TaskFlow DAG from configuration using @dag decorator.
        
        Args:
            config: Pipeline configuration from YAML
            
        Returns:
            Instantiated DAG
        """
        data_source = config["data_source"]
        schedule_config = config.get("schedule", {})
        metadata = config.get("metadata", {})

        # =======================================================================
        # MULTI-TENANCY: Extract tenant and apply tenant-specific configuration
        # =======================================================================
        tenant_id = metadata.get("tenant")
        if not tenant_id:
            raise ValueError(
                f"Missing metadata.tenant for data source '{data_source.get('name', 'unknown')}'. "
                f"All pipelines must belong to a tenant."
            )

        # Generate tenant-prefixed DAG ID
        source_name = data_source['name']
        if self.tenant_context:
            dag_id = self.tenant_context.get_dag_id(source_name, tenant_id)
        else:
            dag_id = f"{tenant_id}_{source_name}"

        # Parse schedule configuration with timezone awareness
        timezone = self._get_timezone(schedule_config, dag_id)
        schedule_interval = schedule_config.get("interval", "@daily")
        start_date = self._parse_datetime_with_timezone(
            schedule_config.get("start_date", "2024-01-01"),
            timezone,
            dag_id,
            "start_date"
        )
        end_date = None
        if schedule_config.get("end_date"):
            end_date = self._parse_datetime_with_timezone(
                schedule_config["end_date"],
                timezone,
                dag_id,
                "end_date"
            )
        catchup = schedule_config.get("catchup", False)
        
        # Merge tenant tags with schedule tags
        config_tags = schedule_config.get("tags", [])
        if self.tenant_context:
            tenant_tags = self.tenant_context.get_tenant_tags(tenant_id)
        else:
            tenant_tags = [f"tenant:{tenant_id}"]
        tags = list(set(tenant_tags + config_tags))  # Deduplicate

        # Create default arguments
        default_args = self._create_default_args(tenant_id, metadata, schedule_config)

        # Get tenant pool for resource isolation
        pool = None
        if self.tenant_context:
            pool = self.tenant_context.get_tenant_pool(tenant_id)

        # Build deadline alert if configured
        deadline = self._create_deadline_alert(dag_id, schedule_config)

        # Build asset definitions for lineage tracking
        inlets, outlets = self._create_assets(config)

        # Partitioning configuration resolution
        partition_config = config.get("partition", {})
        is_partitioned = partition_config.get("enabled", False)
        
        # Incremental configuration resolution
        incremental_config = config.get("incremental", {})
        is_incremental = incremental_config.get("enabled", False)
        
        if is_partitioned and is_incremental:
            raise ValueError(
                f"Configuration error for '{source_name}': "
                f"Combining 'partition' and 'incremental' is not supported as it causes race conditions on the global watermark."
            )
        
        if is_partitioned:
            granularity = partition_config.get("granularity", "day")
            mapper_type = partition_config.get("mapper", "fan_out")
            wait_policy_type = partition_config.get("wait_policy", "wait_for_all")
            min_count = partition_config.get("minimum_count", 1)
            max_fan_out = partition_config.get("max_fan_out", 64)
            
            # Global limit capping
            from airflow.configuration import conf
            try:
                global_max_keys = conf.getint("scheduler", "partition_mapper_max_downstream_keys", fallback=None)
            except Exception:
                global_max_keys = None
                
            if isinstance(global_max_keys, int) and max_fan_out > global_max_keys:
                log.warning(
                    f"max_fan_out {max_fan_out} is bounded by global partition_mapper_max_downstream_keys {global_max_keys}.",
                    dag_id=dag_id
                )
                max_fan_out = global_max_keys
                
            # Maps
            window_classes = {
                "day": DayWindow,
                "week": WeekWindow,
                "month": MonthWindow,
                "quarter": QuarterWindow,
                "year": YearWindow,
            }
            mapper_classes = {
                "day": StartOfDayMapper,
                "week": StartOfWeekMapper,
                "month": StartOfMonthMapper,
                "quarter": StartOfQuarterMapper,
                "year": StartOfYearMapper,
            }
            
            window_cls = window_classes.get(granularity, DayWindow)
            upstream_mapper_cls = mapper_classes.get(granularity, StartOfDayMapper)
            
            if wait_policy_type == "minimum_count":
                wait_policy = MinimumCount(min_count)
            else:
                wait_policy = WaitForAll()
                
            if mapper_type == "rollup":
                mapper = RollupMapper(
                    upstream_mapper=upstream_mapper_cls(),
                    window=window_cls(),
                    wait_policy=wait_policy,
                    max_downstream_keys=max_fan_out
                )
            elif mapper_type == "fan_out":
                mapper = FanOutMapper(
                    upstream_mapper=upstream_mapper_cls(),
                    window=window_cls(),
                    max_downstream_keys=max_fan_out
                )
            elif mapper_type == "fixed_key":
                mapper = FixedKeyMapper(
                    downstream_key="fixed_key",
                    max_downstream_keys=max_fan_out
                )
            else:
                mapper = IdentityMapper()
                
            if partition_config.get("runtime_assigned"):
                schedule_interval = PartitionedAtRuntime()
            else:
                is_time_schedule = False
                if isinstance(schedule_interval, str):
                    schedule_interval_clean = schedule_interval.strip().lower()
                    if schedule_interval_clean.startswith("@") or len(schedule_interval.split()) in [5, 6]:
                        is_time_schedule = True
                        
                if is_time_schedule:
                    if "mapper" in partition_config or "wait_policy" in partition_config:
                        raise ValueError(
                            f"Configuration error for '{source_name}': "
                            f"Cron schedules cannot be combined with 'mapper' or 'wait_policy'."
                        )
                    cron_map = {
                        "@hourly": "0 * * * *",
                        "@daily": "0 0 * * *",
                        "@weekly": "0 0 * * 0",
                        "@monthly": "0 0 1 * *",
                        "@yearly": "0 0 1 1 *",
                    }
                    cron_str = cron_map.get(schedule_interval, schedule_interval)
                    schedule_interval = CronPartitionTimetable(cron_str, timezone=timezone)
                else:
                    schedule_interval = PartitionedAssetTimetable(
                        assets=inlets[0] if len(inlets) == 1 else AssetAll(*inlets),
                        default_partition_mapper=mapper
                    )
                    
        # Determine max_active_runs
        explicit_max_active_runs = schedule_config.get("max_active_runs")
        if not is_partitioned:
            max_active_runs = 1
        else:
            tenant_pool_slots = None
            if pool:
                # Retrieve from global settings directly
                tenants_config = self.global_settings.get("tenants", {})
                tenant_config = tenants_config.get(tenant_id, {})
                tenant_pool_slots = tenant_config.get("slots")
                
            if tenant_pool_slots is not None:
                default_runs = min(8, max(1, tenant_pool_slots // 2))
            else:
                default_runs = 8
                
            if explicit_max_active_runs is not None:
                if tenant_pool_slots is not None and explicit_max_active_runs > tenant_pool_slots:
                    log.warning(
                        f"Explicit max_active_runs {explicit_max_active_runs} exceeds tenant pool slots {tenant_pool_slots}. Clamping to {tenant_pool_slots}.",
                        dag_id=dag_id
                    )
                    max_active_runs = tenant_pool_slots
                else:
                    max_active_runs = explicit_max_active_runs
            else:
                max_active_runs = default_runs

        # =======================================================================
        # CREATE DAG USING @dag DECORATOR
        # =======================================================================
        
        dag_kwargs = {
            "dag_id": dag_id,
            "description": f"Data integration pipeline for {source_name} (tenant: {tenant_id})",
            "schedule": schedule_interval,
            "start_date": start_date,
            "end_date": end_date,
            "catchup": catchup,
            "tags": tags,
            "max_active_runs": max_active_runs,
            "default_args": default_args,
            "deadline": deadline,
            "params": {"tenant_id": tenant_id, "tenant_pool": pool},
        }

        if "rerun_with_latest_version" in metadata:
            dag_kwargs["rerun_with_latest_version"] = metadata["rerun_with_latest_version"]
        
        @dag(**dag_kwargs)
        def create_pipeline():
            """
            TaskFlow pipeline definition.
            
            Flow:
            1. [Kafka health check] (if pipeline publishes to Kafka)
            2. Ingest data from source
            3. [Transform data] (if transformation configured)
            4. [Validate data quality] (if validation configured)
               - Route to either load or quarantine based on DQ results
               - If quarantine and HITL enabled: wait for approval
            5. Load data to destination(s)
            """
            
            # Task 0: Kafka Health Check (conditional)
            event_config = config.get("event", {})
            has_kafka_destination = bool(event_config.get("topic"))
            
            kafka_health_task = None
            if has_kafka_destination:
                kafka_health_task = wait_for_kafka_health.override(
                    task_id="check_kafka_health",
                    pool=pool,
                )()
                log.debug(
                    "Added Kafka health sensor to DAG",
                    dag_id=dag_id,
                    topic=event_config.get("topic"),
                )
            
            # Task 1: Data Ingestion (with inlet assets for lineage)
            ingest_task = ingest_data.override(
                task_id="fetch_source_data",
                inlets=inlets or [],
                pool=pool,
            )(config)
            
            # Task 2: Data Transformation (conditional)
            has_transformation = (
                "transformation" in config
                or "validation_rules" in config
                or "enrichment" in config
            )
            
            if has_transformation:
                # cast: TaskFlow passes an XComArg placeholder that resolves to the
                # declared type (str path) at runtime.
                transform_task = transform_data.override(
                    task_id="apply_transformations",
                    pool=pool,
                )(cast(str, ingest_task), config)
                current_df = transform_task
            else:
                current_df = ingest_task
            
            # Task 3: Data Quality Validation (conditional)
            validation_config = config.get("validation", {})
            has_validation = (
                "soda_checks" in validation_config
                or "quality_gates" in validation_config
            )
            
            if has_validation:
                # Run DQ checks - returns (valid_df, invalid_df, dq_results)
                # cast: the call returns a PlainXComArg (supports __getitem__ for
                # dict key access) but is typed as the base XComArg.
                dq_task = cast(Any, validate_data_quality.override(
                    task_id="run_quality_checks",
                    pool=pool,
                )(cast(str, current_df), config))

                valid_df = dq_task["valid_path"]
                invalid_df = dq_task["invalid_path"]
                dq_results = dq_task["results"]
                
                # Check if quarantine is enabled
                quality_gates = validation_config.get("quality_gates", {})
                should_quarantine = quality_gates.get("quarantine_invalid", False)
                
                if should_quarantine:
                    # Conditional routing based on DQ results
                    router = route_dq_results.override(
                        task_id="evaluate_quality_gates",
                        pool=pool,
                    )(dq_results, config)
                    
                    # Branch 1: Load valid data directly
                    load_valid_task = load_data.override(
                        task_id="load_valid_data",
                        outlets=outlets or [],
                        pool=pool,
                    )(valid_df, config)
                    
                    # Branch 2: Quarantine invalid data
                    quarantine_task = quarantine_invalid_data.override(
                        task_id="quarantine_invalid_data",
                        pool=pool,
                    )(invalid_df, dq_results, config)
                    
                    # Check if HITL approval is enabled
                    quarantine_config = config.get("destination", {}).get("quarantine", {})
                    hitl_config = quarantine_config.get("hitl", {})
                    hitl_enabled = hitl_config.get("enabled", False)
                    
                    if hitl_enabled:
                        from airflow.providers.standard.operators.hitl import ApprovalOperator
                        from airflow.sdk import Param
                        from datetime import timedelta
                        
                        timeout_hours = hitl_config.get("timeout_hours", 24)
                        
                        # 1. Prepare context
                        approval_context = prepare_hitl_approval_context.override(
                            task_id="prepare_hitl_approval_context",
                            pool=pool,
                        )(cast(str, quarantine_task), config)
                        
                        # 2. Instantiate ApprovalOperator
                        hitl_approval_task = ApprovalOperator(
                            task_id="quarantine_approval",
                            subject="Quarantine Release Approval for {{ dag.dag_id }}",
                            body="""
## Quarantine Summary
- **Total Records**: {{ ti.xcom_pull(task_ids='prepare_hitl_approval_context')['quarantine_summary']['total_records'] }}
- **Failed Checks**: {{ ti.xcom_pull(task_ids='prepare_hitl_approval_context')['quarantine_summary']['failed_checks'] | join(', ') }}
- **Quarantine Time**: {{ ti.xcom_pull(task_ids='prepare_hitl_approval_context')['quarantine_summary']['quarantine_time'] }}

Please review the quarantined records.
""",
                            params={
                                "notes": Param(type="string", default="", description="Approval Notes")
                            },
                            response_timeout=timedelta(hours=timeout_hours),
                            pool=pool,
                        )
                        
                        # 3. Process the decision
                        process_decision_task = process_approval_decision.override(
                            task_id="process_approval_decision",
                            pool=pool,
                        )(
                            approval_result=cast(Dict[str, Any], hitl_approval_task.output),
                            quarantine_df_path=cast(str, quarantine_task),
                            config=config,
                        )
                        
                        # 4. Load quarantine records
                        load_quarantine_task = load_data.override(
                            task_id="load_quarantine_records",
                            pool=pool,
                        )(cast(str, process_decision_task), config)
                        
                        # Wire dependencies
                        router >> [load_valid_task, quarantine_task]
                        quarantine_task >> approval_context >> hitl_approval_task >> process_decision_task >> load_quarantine_task
                    else:
                        # Auto-approve quarantine (no HITL)
                        load_quarantine_task = load_data.override(
                            task_id="load_quarantine_records",
                            pool=pool,
                        )(cast(str, quarantine_task), config)
                        
                        # Wire dependencies
                        router >> [load_valid_task, quarantine_task]
                        quarantine_task >> load_quarantine_task
                else:
                    # No quarantine - load all valid data
                    load_data.override(
                        task_id="load_data",
                        outlets=outlets or [],
                        pool=pool,
                    )(valid_df, config)
            else:
                # No validation - load data directly
                load_data.override(
                    task_id="load_data",
                    outlets=outlets or [],
                    pool=pool,
                )(cast(str, current_df), config)
            
            # Wire Kafka health check to ingestion
            if kafka_health_task:
                kafka_health_task >> ingest_task

        # Instantiate the DAG
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

    def _create_deadline_alert(
        self, dag_id: str, schedule_config: Dict[str, Any]
    ) -> Optional[List[DeadlineAlert]]:
        """
        Create deadline alert configuration from schedule config.

        Deadline alerts notify when a DAG run is late relative to when it was queued.
        Kafka alerts are ALWAYS sent; email is configurable per-pipeline.

        Args:
            dag_id: DAG identifier for alert messages
            schedule_config: Schedule configuration from YAML

        Returns:
            List of DeadlineAlerts or None if not configured
        """
        deadline_config = schedule_config.get("deadline", {})

        if not deadline_config:
            return None
            
        # Support both single config (back-compat) or tiers array
        tiers = deadline_config.get("tiers", [deadline_config])

        alerts: List[DeadlineAlert] = []
        for tier in tiers:
            if not tier.get("enabled", False):
                continue

            if CompositeDeadlineNotifier is None:
                log.warning(
                    "Deadline alerts disabled - CompositeDeadlineNotifier not available",
                    dag_id=dag_id,
                )
                continue

            # Get timeout in minutes (default 30)
            timeout_minutes = tier.get("timeout_minutes", 30)

            # Email configuration (Kafka is always on)
            email_enabled = tier.get("email_enabled", False)
            email_recipients = tier.get("email_recipients", [])

            # Get Kafka topic for alerts
            kafka_topic = tier.get("kafka_topic", "pipeline-alerts")
            
            # Reference
            ref_str = tier.get("reference", "queued_at").upper()
            reference = DeadlineReference.AVERAGE_RUNTIME() if "AVERAGE" in ref_str else DeadlineReference.DAGRUN_QUEUED_AT

            log.info(
                "Creating deadline alert tier",
                dag_id=dag_id,
                timeout_minutes=timeout_minutes,
                email_enabled=email_enabled,
                kafka_topic=kafka_topic,
                reference=ref_str,
            )

            alerts.append(
                DeadlineAlert(
                    reference=reference,
                    interval=timedelta(minutes=timeout_minutes),
                    callback=SyncCallback(
                        CompositeDeadlineNotifier,
                        kwargs={
                            "topic": kafka_topic,
                            "message": f"Pipeline {dag_id} missed deadline of {timeout_minutes} minutes",
                            "email_enabled": email_enabled,
                            "email_recipients": email_recipients,
                            "email_subject": f"🚨 Deadline Alert: {dag_id}",
                        },
                    ),
                )
            )
            
        return alerts if alerts else None

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
            outlet_uri = f"snowflake://{account}/{database}/{schema}/{table}"
        elif dest_type == "object_storage":
            uri = primary_dest.get("uri", "")
            path = primary_dest.get("path", "")
            storage_path = uri or path
            outlet_uri = storage_path if storage_path else f"object_storage://{source_name}_output"
        elif dest_type == "local_file":
            path = primary_dest.get("path", "")
            outlet_uri = f"file://{path}" if path else f"file:///tmp/{source_name}_output"
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
                source="pipeline_config"
            )
            
            # Log warning for non-UTC timezones about DST implications
            if pipeline_tz.upper() != "UTC":
                log.info(
                    "⚠️  NON-UTC TIMEZONE DETECTED - Pipeline is exposed to Daylight Saving Time transitions",
                    dag_id=dag_id,
                    timezone=pipeline_tz,
                    dst_impact="Cron schedules will adjust for DST; timedelta schedules will not",
                    recommendation="Consider using UTC for predictable execution times"
                )
            
            return pipeline_tz
        
        # Fallback to global default timezone
        global_tz = self.global_settings.get("default_settings", {}).get("default_timezone", "UTC")
        
        log.info(
            "Using global default timezone (pipeline config did not specify timezone)",
            dag_id=dag_id,
            timezone=global_tz,
            source="global_settings"
        )
        
        return global_tz
    
    def _parse_datetime_with_timezone(
        self, 
        date_string: str, 
        timezone: str, 
        dag_id: str,
        field_name: str
    ) -> pendulum.DateTime:
        """
        Parse date string into timezone-aware pendulum DateTime.
        
        Args:
            date_string: Date in YYYY-MM-DD format
            timezone: IANA timezone name
            dag_id: DAG identifier for logging
            field_name: Name of the field being parsed (for error messages)
            
        Returns:
            Timezone-aware pendulum DateTime object
            
        Raises:
            ValueError: If date string is invalid or timezone is unknown
        """
        try:
            # Parse date string and apply timezone
            # pendulum.parse handles YYYY-MM-DD format and sets time to midnight
            dt = pendulum.parse(date_string, tz=timezone)

            # pendulum.parse can return Date/Time/Duration for non-datetime strings
            if not isinstance(dt, pendulum.DateTime):
                raise ValueError(
                    f"Expected a datetime but got {type(dt).__name__}"
                )

            log.debug(
                f"Parsed {field_name} with timezone",
                dag_id=dag_id,
                field=field_name,
                date_string=date_string,
                timezone=timezone,
                parsed_datetime=str(dt),
                is_dst=dt.is_dst()
            )

            return dt
            
        except Exception as e:
            log.error(
                f"Failed to parse {field_name} with timezone",
                dag_id=dag_id,
                field=field_name,
                date_string=date_string,
                timezone=timezone,
                error=str(e)
            )
            raise ValueError(
                f"Invalid {field_name} '{date_string}' for timezone '{timezone}': {e}"
            )

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
                log.error("Error creating TaskFlow DAG", source=source_name, error=str(e))

        return dags

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
        config_path = os.getenv("AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_STORAGE_PATH", "/opt/airflow/dags")
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
                    timeout=5
                )
                if result.returncode == 0:
                    return result.stdout.strip()
            except Exception as e:
                log.debug("Could not detect Git version from bundle storage", error=str(e))
        
        # Fallback to checking local dags directory
        try:
            dags_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=dags_dir,
                capture_output=True,
                text=True,
                timeout=5
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
            default_args["retry_exponential_backoff"] = retry_config["exponential_backoff"]
            
        if "retry_delay_seconds" in retry_config:
            default_args["retry_delay"] = timedelta(seconds=retry_config["retry_delay_seconds"])
            
        if "max_retries" in retry_config:
            default_args["retries"] = retry_config["max_retries"]
            
        # Apply transient retry policy if configured
        if retry_config.get("policy") == "transient":
            from rlam_airflow_framework.retry_policy import build_transient_retry_policy
            default_args["retry_policy"] = build_transient_retry_policy(retry_config)
        
        return default_args
