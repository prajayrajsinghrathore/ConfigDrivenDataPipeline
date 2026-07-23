# Phase 2 — Strategic: Asset Partitioning + Task/Asset State Store (Full Implementation)

**Goal:** fully implement the two headline 3.3.0 capabilities in the framework so that (a) downstream work is scheduled per **data partition** rather than per whole-asset event, and (b) pipeline state (incremental watermarks, DQ provenance) is **durable** via the first-class State Store instead of riding through XCom.

**Scope decision:** *full working implementation* wired into the DAG factory, config schema, and task layer — not a design doc or single-reference scaffold. Ship both features working for the real source/sink types this framework supports (`rest_api`/`sftp` sources; `snowflake_table`/`object_storage`/`local_file` sinks).

**Branch:** `upgrade/phase-2` (from `develop`, after Phase 1 is merged).

**Preconditions:** Phase 1 merged; both test lanes green on 3.3.0.

**Read first:** [plan/README.md](README.md). The partition mappers, timetables, `Window`/`wait_policy`, and state-store accessors named below are **candidates to verify** against installed 3.3.0 before use — several are new and experimental; confirm exact import paths, class names, and signatures first.

**Definition of Done:**
- A partitioned pipeline schedules downstream runs per partition key/date; reprocessing one partition clears/backfills only that partition, not the whole DAG.
- An incremental source uses a durable watermark from the Task State Store to fetch only new data; DQ scorecards/quarantine counts are persisted as asset state and visible in the UI.
- Config schema expresses both features; existing non-partitioned/non-incremental configs keep working unchanged.
- Both test lanes green, including Docker integration/e2e for one partitioned Snowflake pipeline and one incremental REST pipeline.

---

## Progress Tracker

Status legend in [README.md](README.md#progress-tracking). Update the row + Notes whenever status changes.

| Task | Status | Owner | Notes |
|------|--------|-------|-------|
| 2A.1 Verify partitioning API surface | ⬜ | | |
| 2A.2 `partition:` config schema + contract tests | ⬜ | | |
| 2A.3 Factory emits partitioned assets + mappers | ⬜ | | |
| 2A.4 Partition-aware ingest/load (all source/sink types) | ⬜ | | |
| 2A.5 Partitioned reprocessing runbook | ⬜ | | |
| 2A.6 Partitioning tests (unit + Docker e2e) | ⬜ | | |
| 2B.1 Verify State Store API surface | ⬜ | | |
| 2B.2 Incremental watermarks (retry-safe) | ⬜ | | |
| 2B.3 DQ provenance as asset state | ⬜ | | |
| 2B.4 State-store retention / GC config | ⬜ | | |
| 2B.5 State-store tests (unit + Docker e2e) | ⬜ | | |
| 2.C Mocks / fixtures / docs / report status | ⬜ | | |

---

## Part A — Asset Partitioning

### Task 2A.1 — Verify the 3.3.0 partitioning API surface

Before writing code, confirm against installed 3.3.0 (and record findings in the PR):
- Partition mappers: `RollupMapper`, `FanOutMapper`, `FixedKeyMapper`, `SegmentWindow`, `ChainMapper`, `AllowedKeyMapper`, `StartOfXXXMapper` (renamed from `ToXXXMapper` in #64160) — exact import paths and constructor params.
- Time `Window` (day/week/month/quarter/year), the `forward` kwarg (#67475), and `wait_policy` (`WaitForAll`, `MinimumCount(n)`).
- The `PartitionedAtRuntime` timetable and any `CronPartitionedTimetable`.
- How `partition_key` / `partition_date` arrive in the **task context** (#65359, and propagation #67285/#69120).
- The `[scheduler] partition_mapper_max_downstream_keys` config and per-mapper `max_fan_out`.
- Backfill/clear by partition-date range: `airflow dags clear`/backfill flags (#66004, #67537), the `clearPartitions` REST endpoint, and the `partitions clear` CLI (#66520).

If any symbol is absent/renamed, stop and report before building on it.

### Task 2A.2 — Extend the config schema for partitioning

**File:** [config/schemas/data_source_schema.yaml](../config/schemas/data_source_schema.yaml)

Add an **optional** `partition:` block (absence = today's non-partitioned behavior). Design it config-first:
```yaml
partition:
  enabled: true
  dimension: date            # partition dimension; date is the common case
  column: event_date         # source/sink column that carries the partition value
  granularity: day           # day|week|month|quarter|year  -> Window
  mapper: fan_out            # rollup|fan_out|fixed_key|chain (maps to a mapper class)
  wait_policy: wait_for_all  # wait_for_all | minimum_count
  minimum_count: 1           # used when wait_policy = minimum_count
  runtime_assigned: false    # true -> PartitionedAtRuntime timetable
  max_fan_out: 64            # per-mapper cap
```
Validate mutually-exclusive/required combinations (e.g. `minimum_count` required only for `minimum_count` policy; `column` required when `enabled`). Update [tests/contract/test_config_schemas.py](../tests/contract/test_config_schemas.py).

### Task 2A.3 — Emit partitioned assets and mappers in the DAG factory

**File:** [rlam_airflow_framework/dag_factory_v2.py](../rlam_airflow_framework/dag_factory_v2.py) — primarily [_create_assets](../rlam_airflow_framework/dag_factory_v2.py#L439-L501) and [create_dag_from_config](../rlam_airflow_framework/dag_factory_v2.py#L107-L376).

1. When `partition.enabled`, build **partitioned** inlet/outlet `Asset`s with the partition definition (verified API), instead of the plain `Asset(uri=...)` used today.
2. Select and construct the mapper from `partition.mapper` + `granularity` + `wait_policy` (+ `minimum_count`/`max_fan_out`). Attach to the consuming DAG's schedule.
3. If `runtime_assigned`, use the `PartitionedAtRuntime` timetable for that DAG rather than mapping from an upstream event.
4. Respect `[scheduler] partition_mapper_max_downstream_keys` and per-mapper cap; log when fan-out is bounded (never silently truncate — emit a `log.warning`).
5. **Revisit the hardcoded `max_active_runs=1` — DECIDED semantics.** The hardcoded `max_active_runs=1` on every `@dag` **serializes partition fan-out** — defeating the point of Phase 2A. Replace with:
   - Optional `max_active_runs` in the **schedule config** (not `metadata`), schema-validated; explicit config always wins.
   - Defaults when unset: non-partitioned → `1` (today's behavior, regression-tested); partitioned → **derived, not flat**: `min(8, tenant_pool_slots // 2)` when the tenant has a pool, else `8`. A flat 16 was proposed and **rejected** — it would double-saturate `esg_pool` (8 slots) and consume all of `reporting_pool` (16), breaking tenant isolation. Log the derived value at DAG build.
   - **Clamp + warn:** explicit values exceeding the tenant pool size are clamped to it with a `log.warning` — one pipeline's fan-out must never silently exceed its tenant's capacity.
   - Document in the schema that `[core] max_active_tasks_per_dag = 16` and pool slots remain the real task-concurrency ceilings; raising `max_active_runs` past them only queues runs.
   - Tests: unit (default 1 non-partitioned, derived for partitioned, clamp), contract (field validation), regression (existing fixtures still yield `max_active_runs=1`).
6. Keep the non-partitioned branch exactly as-is so existing pipelines are unaffected.

### Task 2A.4 — Make tasks partition-aware

**File:** [rlam_airflow_framework/taskflow_tasks.py](../rlam_airflow_framework/taskflow_tasks.py); helpers in [data_fetchers.py](../rlam_airflow_framework/data_fetchers.py) and [data_loaders.py](../rlam_airflow_framework/data_loaders.py).

1. In `ingest_data` and `load_data`, read `partition_key`/`partition_date` from `get_current_context()` when partitioning is enabled, and scope the operation to that partition:
   - `rest_api`/`sftp` fetch: apply the partition value to the request (date param / path template) so only that slice is pulled.
   - `snowflake_table` load: write/replace only the target partition (e.g. `WHERE <column> = <partition_date>` semantics or partition-scoped `MERGE`), not the whole table — see [_load_to_destination](../rlam_airflow_framework/taskflow_tasks.py#L618-L724).
   - `object_storage`/`local_file`: write to a partition-scoped path (e.g. `.../date=<partition_date>/...`).
2. Thread `partition_key`/`partition_date` into the Kafka pipeline events ([kafka_publisher](../rlam_airflow_framework/kafka_publisher.py)) and structlog `correlation_id` for traceability.
3. Ensure non-partitioned pipelines skip all of this (guard on config).

### Task 2A.5 — Partitioned reprocessing (backfill/clear)

Document and script partition-range reprocessing (verified CLI/REST from 2A.1): reprocessing one date clears/backfills only that partition. Add a short runbook to [Documentation/06_Scheduling_And_Execution.md](../Documentation/06_Scheduling_And_Execution.md).

### Task 2A.6 — Partitioning tests
- *Unit (mocked):* config → mapper/asset construction (assert the right mapper class + params captured via mocked Airflow symbols); partition-scoping logic in ingest/load given a fixed `partition_date` in a mocked context; schema contract cases.
- *Integration/e2e (Docker):* a producer emits a partitioned asset for date D; the consumer DAG fires a run **only** for D; a second partition D+1 triggers an independent run; reprocessing D clears/backfills only D. Add a reference partitioned Snowflake (or object-storage, if Snowflake creds unavailable in CI) pipeline fixture under [config/data_sources/](../config/data_sources/) and [tests/e2e/](../tests/e2e/).

---

## Part B — Task & Asset State Store (AIP-103)

### Task 2B.1 — Verify the State Store API surface

Confirm against installed 3.3.0:
- Task SDK accessors: `task_state_store` and `asset_state_store` — get/set/patch/clear signatures, `default=` param (#67842), `expires_at`/retention, and JSON-type support (#67418).
- Config: worker-side `[workers] state_store_backend`, `clear_on_success` (#66586), retention/GC + `default_retention_days` (#66463/#67890), row-size limits (#68133), and inclusion in `airflow db clean` (#68218).
- Accessibility from triggers if needed (`AssetStateStoreAccessors`, #67839).

### Task 2B.2 — Incremental-load watermarks via Task State Store

**Files:** [rlam_airflow_framework/taskflow_tasks.py](../rlam_airflow_framework/taskflow_tasks.py) (`ingest_data`), [data_fetchers.py](../rlam_airflow_framework/data_fetchers.py).

1. Add an optional `incremental:` config block (schema update): `{enabled, watermark_column, initial_watermark, lookback}`.
2. On ingest: read the last successful watermark from `task_state_store` (fallback to `initial_watermark`); fetch only rows newer than it; after a successful load, write the new high-water mark back. Use `clear_on_success=false` so the watermark persists across runs (it must survive success — verify semantics).
3. Ensure retries don't corrupt the watermark (only advance after confirmed load). This is the correctness core — cover it with tests.

### Task 2B.3 — DQ provenance via Asset State Store

**Files:** [rlam_airflow_framework/data_quality.py](../rlam_airflow_framework/data_quality.py), [taskflow_tasks.py `validate_data_quality`](../rlam_airflow_framework/taskflow_tasks.py#L266-L314).

1. After DQ runs, persist the scorecard (pass rate, failed checks, valid/invalid counts) and quarantine count as **asset state** on the outlet asset, so lineage carries data-quality provenance and the UI can link to the writing task instance (#68395).
2. Keep the DataFrames flowing through the existing parquet/XCom path — State Store is for the *metadata*, not the bulk data.

### Task 2B.4 — State-store retention & cleanup config

Set retention/GC and (optionally) `clear_on_success` defaults in [docker/airflow.cfg](../docker/airflow.cfg), and confirm `airflow db clean` includes `task_state_store`. Document operational implications in [ROLLBACK_STRATEGY.md](../ROLLBACK_STRATEGY.md) / ops docs.

### Task 2B.5 — State-store tests
- *Unit (mocked):* watermark read/advance logic given a mocked `task_state_store` (advance only on success; retry-safe); DQ scorecard serialization to asset state.
- *Integration/e2e (Docker):* an incremental REST pipeline pulls only new records across two runs (seed the source, run, add records, run again, assert only the delta is processed and the watermark advanced); DQ asset state is written and readable via the Core API/UI.

---

## Task 2.C — Cross-cutting: mocks, fixtures, docs

- Update [tests/unit/conftest.py](../tests/unit/conftest.py) to mock every new `airflow.*` partitioning/state-store symbol imported by framework code (per standing rule).
- Add reference configs: one partitioned pipeline, one incremental pipeline, under [config/data_sources/](../config/data_sources/).
- Update [Documentation/](../Documentation/) (03 Data Sources, 05 DQ, 06 Scheduling, 08 Sinks) to describe partitioning and incremental/state-store behavior.
- Update [upgrade-report.md](../upgrade-report.md) §2.1/§2.2 status from "recommended" to "implemented".

---

## Phase 2 exit checklist
- [ ] 2A.1 partitioning API verified against installed 3.3.0
- [ ] 2A.2 `partition:` schema + contract tests
- [ ] 2A.3 factory emits partitioned assets + correct mapper/timetable
- [ ] 2A.4 ingest/load are partition-scoped for all source/sink types
- [ ] 2A.5 partition-range reprocessing runbook
- [ ] 2A.6 partitioning unit + Docker e2e green
- [ ] 2B.1 state-store API verified
- [ ] 2B.2 incremental watermarks (retry-safe) implemented
- [ ] 2B.3 DQ provenance as asset state
- [ ] 2B.4 retention/GC config + db clean coverage
- [ ] 2B.5 state-store unit + Docker e2e green
- [ ] 2.C mocks/fixtures/docs updated; report status flipped
- [ ] non-partitioned & non-incremental configs still pass unchanged
