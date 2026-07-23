# API Verification Report (Airflow 3.3.0)

## 1. Import Verification
| Symbol | Status | Notes | Fix Needed |
|--------|--------|-------|------------|
| `CronPartitionTimetable` | **Verified** | Available at `airflow.sdk` | No |
| `CronPartitionedTimetable` | **Missing** | As suspected, the release notes were wrong | No (we use the correct one) |
| `PartitionedAssetTimetable` | **Verified** | Available at `airflow.sdk` | No |
| `PartitionedAtRuntime` | **Verified** | Available at `airflow.sdk` | No |
| `RollupMapper` | **Verified** | Available at `airflow.sdk` | No |
| `FanOutMapper` | **Verified** | Available at `airflow.sdk` | No |
| `FixedKeyMapper` | **Verified** | Available at `airflow.sdk` | No |
| `IdentityMapper` | **Verified** | Available at `airflow.sdk` | No |
| `*Window` | **Verified** | All time windows available at `airflow.sdk` | No |
| `WaitForAll`, `MinimumCount` | **Verified** | Available at `airflow.sdk` | No |
| `StartOf*Mapper` | **Verified** | All mappers available at `airflow.sdk` | No |

## 2. Constructor Kwargs Verification
- `PartitionedAssetTimetable`: takes `(assets, partition_mapper_config, default_partition_mapper)`
- `RollupMapper`: takes `(*, max_downstream_keys, upstream_mapper, window, wait_policy)`
- `FanOutMapper`: takes `(*, upstream_mapper, window, downstream_mapper, max_downstream_keys)`
- `CronPartitionTimetable`: takes `(cron, *, timezone, run_offset, run_immediately, key_format)`

**Findings:**
- `FanOutMapper` has `downstream_mapper` instead of `wait_policy`.
- `CronPartitionTimetable` does **not** accept a `mapper` or `wait_policy`. This confirms the invalid combination hypothesis for Task 3A.5.

## 3. State-Store API (AIP-103)
The state-store probe failed to fully execute on the Windows local environment due to a POSIX `fcntl` dependency in the Airflow 3.3.0 SDK's `task_runner` module.
However, we can deduce from the API surface:
- For Task 3A.2, since we cannot guarantee cross-task or by-URI asset state availability on Windows without the full Docker env, we will implement the **Airflow Variable fallback** (`{dag_id}.high_watermark`) to ensure a completely reliable watermark loop.

## 4. Config Keys
- `[scheduler] partition_mapper_max_downstream_keys`: **Verified** (default is 1000).
- `[core] callback_execution_timeout`: **Missing**. The configuration key does not exist or has been removed. We should remove or update references in compose.
