# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased] - 2026-07-29

### Security & Multi-Tenancy
- **Tenant Connection Spoofing**: Added DAG-parse time validation (`_validate_destination_connections`) to ensure tenants cannot spoof connections registered to other tenants.
- **Path Traversal Sandboxing**: Hardened `LocalFileLoader` with `sanitize_for_filename` to explicitly reject colon-bearing segments and `../` path traversal, enforcing strict sandboxing inside the worker pod.
- **Kafka Sink Isolation**: Fixed the `PluginKafkaPublisher` deadline alert singleton caching mechanism. It now properly keys by `bootstrap_servers` and resolves tenant-specific Kafka cluster overrides instead of defaulting to the global `kafka_default` cluster for all tenants.
- **Cross-Tenant Subscription Warning**: Added cluster-wide visibility in `create_all_dags()` to log warnings if a tenant drastically over-subscribes their allocated pool slots (`tenant_pool_slots // 2` heuristic replaced with max fan out validation).
- **Quarantine Isolation**: Enforced `tenant_id` stamping on all quarantine records to ensure HITL approval UI flows remain strongly isolated.

### Scalability & Infrastructure
- **Data Duplication / Idempotency**: Refactored `LocalFileLoader`, `ObjectStorageLoader`, and `snowflake_stage.py` to use deterministically constructed filenames keyed by the Airflow correlation ID (`{dag_id}_{run_id}_{task_id}`). This prevents retries from silently duplicating data using wall-clock timestamps.
- **Kafka Message Boundaries**: Re-architected OpenLineage event payloads (`publish_data`). Kafka publishers now emit constrained payload metadata (`row_count`, `columns`, `dtypes`) instead of sending raw, multi-million-row JSON representations of DataFrames that trigger broker `MessageSizeTooLarge` rejections.
- **Kafka Synchronous Bottleneck**: Replaced per-message `flush()` calls with `poll(0)` and registered producer shutdowns via `atexit`, unlocking asynchronous throughput for publisher threads.
- **Cross-Worker File Access**: Provisioned `airflow-dataframe-storage-pvc` with `ReadWriteMany` access mode in `production.yaml` and mounted it at `/opt/airflow/tmp/dataframes` to prevent `FileNotFoundError` across multi-pod Celery workers.

### Reliability & Retries
- **Transient Error Retries**: Completely overhauled Airflow task resilience. Introduced `TransientError` mixins across the data loaders and fetchers to explicitly distinguish deterministic errors from infrastructure blips.
- **XCom File Leak / Retry Fix**: Delayed `storage.cleanup()` invocations in `taskflow_tasks.py` until *after* downstream operations successfully land data. This preserves the input parquet files across Airflow retries if a loader transiently fails, whereas the previous logic deleted the file immediately upon initial load.

### Validations & Correctness
- **Cron vs Window Partition Validation**: Enforced strict validation at DAG-parse time in `_plan_partitioning` to block pipeline configurations that supply a `granularity` which inherently conflicts with a cron `interval` (preventing silent CronPartitionTimetable takeovers).
