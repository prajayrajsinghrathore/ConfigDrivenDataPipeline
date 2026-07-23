# Airflow Upgrade Report: 3.1.6 → 3.3.0

**Project:** ConfigDrivenDataPipeline (RLAM config-driven Airflow framework)
**Current version:** `apache-airflow==3.1.6` / `apache-airflow-task-sdk==1.1.6`
**Target version:** `apache-airflow==3.3.0`
**Upgrade path crossed:** 3.1.7 → 3.1.8 → **3.2.0** → 3.2.1 → 3.2.2 → **3.3.0**
**Date:** 2026-07-22

---

## 1. Executive Summary

This upgrade is unusually high-value for *this specific project* because the two headline features of the 3.2/3.3 line — **Asset Partitioning** and the **Task & Asset State Store (AIP-103)** — map almost one-to-one onto capabilities the framework currently hand-rolls or leaves stubbed. The framework already uses the exact building blocks 3.3.0 matures: the `@dag` TaskFlow API, `Asset` inlets/outlets for lineage ([dag_factory_v2.py](rlam_airflow_framework/dag_factory_v2.py#L439-L501)), `DeadlineAlert` with a custom Kafka/email notifier ([deadline_callbacks.py](plugins/deadline_callbacks.py)), a custom pandas-DataFrame XCom serializer ([serializers.py](rlam_airflow_framework/serializers.py)), DAG-bundle version tracking for rollback, and a hand-built multi-tenancy layer ([tenant_context.py](rlam_airflow_framework/tenant_context.py)).

**Top opportunities (detailed below):**

| # | Opportunity | Airflow feature | Impact |
|---|-------------|-----------------|--------|
| 1 | Partition-aware scheduling for partitioned Snowflake / object-storage sinks | Asset Partitioning (3.2.0 + 3.3.0) | High |
| 2 | Durable pipeline state (watermarks, DQ metrics, quarantine counts) across retries/runs | Task & Asset State Store, AIP-103 (3.3.0) | High |
| 3 | Finally implement the currently-stubbed HITL quarantine approval | `awaiting_input` state on triggerer + HITL history/UX (3.2/3.3) | High |
| 4 | Smarter retries (transient-only) instead of blanket `retries=3` | Pluggable Retry Policies, AIP-105 (3.3.0) | Medium |
| 5 | Deadline notifier reads Kafka/SMTP creds from a Connection, not env vars | SyncCallback + Connection access (3.3.0) | Medium |
| 6 | Explicit control over rollback / rerun bundle version | `rerun_with_latest_version` (3.3.0) | Medium |
| 7 | Replace custom tenant isolation with native Multi-Team (roadmap) | Multi-Team Deployments (3.2.0, expanded 3.3.0) | Medium (experimental) |
| 8 | Free performance, security, and observability gains | many | Medium |

**Migration effort:** Moderate. The framework stays on the same major version (3.x), the TaskFlow/`@dag`/Asset APIs it uses are unchanged, and no *breaking* API it depends on is removed. The real work is (a) a **SQLAlchemy 2.0** environment bump, (b) migrating two deprecated `airflow.serialization.serde` imports to `airflow.sdk.serde`, (c) validating the DB migration + rollback path, and (d) the dependency version bumps in [requirements.txt](requirements.txt). See §4.

---

## 2. High-Value Opportunities

### 2.1 Asset Partitioning — the strategic win (3.2.0 headline, greatly expanded in 3.3.0)

**What the framework does today:** every pipeline emits one outlet `Asset` and consumes one inlet `Asset` ([dag_factory_v2.py `_create_assets`](rlam_airflow_framework/dag_factory_v2.py#L439-L501)). Any update to an asset triggers *all* downstream DAGs regardless of which slice of data changed.

**What 3.3.0 offers:** downstream DAGs can be scheduled on *specific partitions* of an asset. This is designed exactly for date-partitioned Snowflake tables, date-partitioned object-storage paths, and Hive/BigQuery-style partitions — all of which this framework targets (`snowflake_table`, `object_storage`, `local_file` outlets).

Concretely available now:
- **Partition mappers**: `RollupMapper` (many-to-one), `FanOutMapper` (one-to-many), `FixedKeyMapper` + `SegmentWindow` (categorical rollup), `ChainMapper`, `AllowedKeyMapper`, composed with time `Window`s (day/week/month/quarter/year) and a `wait_policy` (`WaitForAll` / `MinimumCount(n)`).
- **`partition_key` and `partition_date` in the task context** (#65359, #90 etc.) and propagated producer→consumer (#67285, #69120).
- **`PartitionedAtRuntime` timetable** — assign partition key(s) when the run starts.
- **Backfill by partition-date range** (`airflow dags clear` / backfill, #66004, #67537) and a REST/CLI `clearPartitions` endpoint.

**Recommended adoption:** add an optional `partition:` block to the data-source config schema (e.g. `partition: {dimension: date, granularity: day}`), then in `_create_assets` emit partitioned assets and select a mapper based on config. This turns "reprocess one day" from a full-pipeline rerun into a targeted partition clear — a large efficiency and cost win for the Snowflake/ADLS sinks. **This is the single feature most worth planning a follow-up story around.**

> **STATUS (2026-07-23): IMPLEMENTED & VALIDATED.** `partition:` config block, mapper selection, and partition-scoped ingest/load shipped (Phase 2A); validated live on the Docker stack — per-partition runs with scoped fetches, `clearPartitions` reprocessing of a single partition, and `partition_key` threaded into Kafka event metadata. See [plan/phase-2 tracker](plan/phase-2-strategic-partitioning-statestore.md).

### 2.2 Task & Asset State Store (AIP-103, 3.3.0 headline)

**What the framework does today:** all inter-task state (valid/invalid DataFrames, DQ results, quarantine payloads, bundle metadata) flows through **XCom** using the custom pandas serializer ([serializers.py](rlam_airflow_framework/serializers.py)). Nothing survives cleanly across retries or across runs — e.g. an incremental "last successful watermark" has no first-class home.

**What 3.3.0 offers:** a first-class key-value **state store** for tasks (`task_state_store`) and assets (`asset_state_store`), available from the Task SDK and even from triggers (`AssetStateStoreAccessors`, #67839). Features: state survives retries/runs, per-key retention with periodic GC, optional `clear_on_success`, configurable row-size limits, DB or worker-side backend, full Core/Execution API management, and a UI (asset/task store views, #67292).

**Recommended adoption:**
- Store **incremental-load watermarks** (high-water timestamps / IDs) in `task_state_store` so `rest_api`/`sftp` sources can do true incremental pulls instead of full pulls.
- Persist **DQ scorecards and quarantine counts** as asset state so lineage carries data-quality provenance (with a UI link to the writing task instance, #68395).
- Replace ad-hoc XCom bookkeeping where the payload is small metadata rather than a DataFrame.

> **STATUS (2026-07-23): PARTIALLY IMPLEMENTED & VALIDATED — with a design deviation.** Incremental watermarks shipped and were validated live (delta-only fetch, retry-safe advance, failed-load recovery) but live in **Airflow Variables**, not the state store: the Phase 3A.2 audit found the task-state scope unsuitable for the cross-task read/write the watermark needs (see [plan/phase-3 3A.2](plan/phase-3-remediation-validation-infra.md)). **DQ provenance as asset state was NOT implemented** — dropped in the same retreat; tracked at plan item 2B.3 for re-scoping if wanted.

### 2.3 Human-in-the-Loop (HITL) — implement the currently-stubbed approval

**What the framework does today:** the quarantine-approval path is *explicitly a placeholder*. [dag_factory_v2.py:314-318](rlam_airflow_framework/dag_factory_v2.py#L314-L323) logs a warning and **falls back to auto-approve** because `ApprovalOperator` "requires instantiation in the DAG context." The config schema already models it (`quarantine.hitl.enabled/timeout_hours/allowed_roles`, see [example_airflow_316_features.yaml](tests/fixtures/example_airflow_316_features.yaml#L47-L52)).

**What 3.2/3.3 offer to make it real:**
- **`awaiting_input` task state running off the triggerer** (#68028) — HITL tasks no longer hold a worker slot while waiting for a human.
- **HITL Detail History** with a full approval/rejection audit trail (3.2.0), **improved HITL form UX** and **notification UX** (3.3.0), mapped-task-instance support (#66433).
- **`airflow dags test` now waits for HITL input** instead of looping forever (#69104) — makes the approval path testable.
- HITL Review system for the AgenticOperator (#63081).

**Recommended adoption:** replace the auto-approve fallback with a real `ApprovalOperator`/`HITLOperator` wired into the branch, using `timeout_hours` and `allowed_roles` from config. 3.3.0 is the first release where this is ergonomic and testable end-to-end.

### 2.4 Pluggable Retry Policies (AIP-105, 3.3.0) + numeric backoff (3.2.0)

**What the framework does today:** blanket `retries=3`, `retry_delay=5min` for every task ([dag_factory_v2.py `_create_default_args`](rlam_airflow_framework/dag_factory_v2.py#L710-L719)). A schema-validation failure retries just as pointlessly as a transient Snowflake throttle.

**What 3.3.0 offers:** attach a **custom retry policy** that decides *whether and when* to retry — e.g. retry only on network/API/warehouse-throttle exceptions, never on `ValueError`/validation errors. Plus 3.2.0's `retry_exponential_backoff` now accepts a **numeric multiplier** (`2.0`, `3.5`) instead of just `True`.

**Recommended adoption:** define one shared retry policy (transient-vs-permanent classification) and apply it via `default_args`; add optional `retry_exponential_backoff` to config for API sources that rate-limit. Note the REST API type change for that field (boolean → number) — see §4.

### 2.5 Deadline Alerts — maturity for the existing notifier

The framework already ships a `CompositeDeadlineNotifier` (Kafka always + optional email) via `AsyncCallback` on `DeadlineReference.DAGRUN_QUEUED_AT` ([dag_factory_v2.py:378-437](rlam_airflow_framework/dag_factory_v2.py#L378-L437)). 3.2/3.3 improve this surface directly:

- **Deadline callbacks can now access Connections and Variables** (`SyncCallback`, #65269). Today [PluginKafkaPublisher](plugins/deadline_callbacks.py#L56-L121) reads `KAFKA_BOOTSTRAP_SERVERS`/`KAFKA_SECURITY_PROTOCOL` from **env vars**; you can move Kafka + SMTP config into a managed Airflow **Connection** and read it inside the callback.
- **Multiple deadline alerts per DAG** — pass a list to `deadline=` (3.2.0). The factory currently builds a single alert; you could emit warn-at-15m + page-at-30m tiers.
- **`DeadlineReference.AVERAGE_RUNTIME`** now excludes non-successful runs so failed runs no longer skew the computed deadline (#68949) — a better reference than a fixed `timeout_minutes` for variable-latency sources.
- **New Deadlines page under Browse** + a **Dashboard Deadlines section** (#67586, #68038) for operator visibility.
- **`callback_execution_timeout`** for deadline callbacks (#66609), and a fix for **duplicate deadline-miss callbacks firing from multiple HA scheduler replicas** (#64737) — relevant if you run HA schedulers.

### 2.6 DAG-bundle version control on rerun/clear/backfill (3.3.0)

The framework detects bundle name/version for observability and rollback ([dag_factory_v2.py `_detect_bundle_version`](rlam_airflow_framework/dag_factory_v2.py#L647-L693)) and ships a [ROLLBACK_STRATEGY.md](ROLLBACK_STRATEGY.md). 3.3.0 adds the **`rerun_with_latest_version`** setting (Dag-level and `[core]`) controlling whether a cleared/rerun/backfilled run uses the *latest* bundle version or the *original* one — precedence: request param → Dag-level → `[core]` → default. This gives the rollback strategy an explicit, supported knob instead of relying on implicit behavior. Related correctness fixes you get for free: latest version resolved by **version number** not timestamp (#68389), and fixes for stale/outdated bundle execution after in-place serialized updates (#68558, #68336).

### 2.7 Native Multi-Team vs. the custom tenant layer (3.2.0, expanded 3.3.0) — *roadmap*

[tenant_context.py](rlam_airflow_framework/tenant_context.py) hand-implements tenant isolation: DAG-ID prefixing, tag namespacing, per-tenant pools, connection-ID mapping, and Kafka-topic namespacing. Airflow's native **Multi-Team** gives isolated Dags, connections, variables, pools, and executors per team, with `team_name` in the task context (#65617), team-scoped XCom (#68850), pool team ownership enforced in scheduling (#68649), and per-team metrics tags. 3.3.0 adds substantial multi-team CLI/API/triggerer support.

**Recommendation:** treat as a **roadmap item, not part of this upgrade** — Multi-Team is still flagged *experimental* in 3.2/3.3. But it could eventually retire a large amount of custom `tenant_context` code. Track it; don't migrate yet.

### 2.8 Structured XCom & observability

- **Return Pydantic models through XCom** for structured output (#67644) and the **`@result` decorator** to mark a TaskFlow task as the Dag's result (#64563) — cleaner than the current `dq_task[0]/[1]/[2]` tuple indexing in [dag_factory_v2.py:263-265](rlam_airflow_framework/dag_factory_v2.py#L263-L265).
- **Async XCom accessors** and **`aget_hook`** for async tasks (#68299, #68506); **async callables in `PythonOperator`** (3.2.0).
- **OpenTelemetry**: timer metrics now use Histograms (#64207), tagged dag-processing metrics (#62487), head sampling (#68591). Aligns with the project's existing OpenLineage/structlog observability posture.

---

## 3. "Free" Gains on Upgrade (no code changes required)

Purely from running on 3.3.0, this project inherits:

- **Performance:** faster Dag serialization (#67702, #67701), `TaskGroup.topological_sort` speedups (#67288, #67688), O(N) dag-processor file dedup (#67750), faster Dags-list/dashboard queries on large `DagRun` tables (#67721), numerous N+1 query fixes, grid-view virtualization + pagination (3.2.0, #65388). Directly benefits a framework that generates many DAGs from config.
- **Security/hardening:** path-traversal blocks in `dag_id`/`run_id` (#63296), stricter CORS (#67502), broader secret masking/redaction in logs, rendered templates, and audit logs (#68049, #68624, #67495), `hmac.compare_digest` for SimpleAuthManager (#66556), mTLS for the API client/server (#67214). Note **`allowed_deserialization_classes_regexp` now uses `re.fullmatch`** (3.2.2) — relevant because this project registers a custom serializer (see §4).
- **Stability:** many scheduler/triggerer crash-loop and race fixes, including the triggerer deadlock that silently stalled *all* deferred tasks (3.2.1/3.2.2) — important given this framework's Kafka health **sensor** and deferrable patterns.
- **Ops/infra:** optional **gunicorn API server with zero-downtime worker recycling** (3.2.0), `[core] mp_start_method` config (#68875), `[logging] json_logs` structured API logs (3.2.0) that align with the project's structlog usage, and decoupled remote-logging resolution (#67056).

---

## 4. Migration Checklist & Risks

Because the path crosses **3.2.0**, a few environment-level changes apply even though the framework's own APIs are stable.

### 4.1 Required code / config actions

| Area | Action | Where | Severity |
|------|--------|-------|----------|
| **serde import moved** | Migrate `from airflow.serialization.serde import U` → `airflow.sdk.serde`; migrate `from airflow.serialization.serde import register` | [serializers.py:16](rlam_airflow_framework/serializers.py#L16), [config/airflow_local_settings.py:125](config/airflow_local_settings.py#L125) | Warning now; **removed in Airflow 4**. Do it now. |
| **SQLAlchemy 2.0** | 3.2.0 requires SQLA 2.0 (`sqlalchemy[asyncio]>=2.0.48`). Verify custom DB code / `airflow_local_settings.py` / integration tests (`psycopg2`) | [requirements.txt](requirements.txt), [tests/integration/test_database_integration.py](tests/integration/test_database_integration.py) | Medium |
| **SMTP cert validation** | 3.2.2: `send_email` STARTTLS now validates the server cert by default. The `EmailDeadlineNotifier` uses `send_email`. If your SMTP endpoint has a self-signed cert, set `email.ssl_context = "none"` | [deadline_callbacks.py:340-370](plugins/deadline_callbacks.py#L340) | Medium |
| **Deserialization regex** | 3.2.2: `allowed_deserialization_classes_regexp` now `re.fullmatch`. If configured with a prefix pattern, append `.*`. Check for a pattern allowing the custom `pandas.DataFrame` serializer | [docker/airflow.cfg](docker/airflow.cfg) | Medium |
| **Bundle-name detection** | 3.3.0: provider example Dags become per-provider bundles; REST clients filtering `bundle_name == "dags-folder"` must update. Review `_detect_bundle_name` assumptions | [dag_factory_v2.py:623-645](rlam_airflow_framework/dag_factory_v2.py#L623-L645) | Low |
| **retry_exponential_backoff** | If exposed via config, REST API type changed boolean → **number** (3.2.0). Use numeric values | config schema | Low |
| **HITL endpoint perms** | 3.2.1: `/dags` endpoint now needs `DagRun` + `TaskInstance` + `HITL_DETAIL` read. Update custom roles if you restrict DAG-only readers | RBAC / auth config | Low |
| **Sensor arg validation** | 3.2.0: invalid `poke_interval`/`timeout` now raise `ValueError` (was `AirflowException`). The Kafka health sensor path should be checked | [health_checks.py](rlam_airflow_framework/health_checks.py) | Low |

Good news confirmed by inspection: the framework does **not** import task exceptions from `airflow.exceptions` (grep clean), so the 3.2.0 `airflow.sdk.exceptions` move requires no changes there.

### 4.2 Dependency bumps ([requirements.txt](requirements.txt#L99-L111))

```diff
- apache-airflow-task-sdk==1.1.6
- apache-airflow-core==3.1.6
- apache-airflow==3.1.6
+ apache-airflow-task-sdk==<matching 3.3.0 SDK, e.g. 1.3.x>
+ apache-airflow-core==3.3.0
+ apache-airflow==3.3.0
```
Then re-pin the providers (`microsoft-azure`, `cncf-kubernetes`, `snowflake`, `ftp`, `http`, `ssh`, `apache-kafka`) to versions declaring 3.3.0 compatibility, and re-resolve. Watch the already-aggressive pins `pandas==3.0.0`, `numpy==2.4.2`, `cryptography<46` (constrained by cncf-kubernetes) — validate against the constraints file for 3.3.0. **`openlineage-airflow==1.41.0`** should be checked for a 3.3.0-compatible release.

### 4.3 Database migration & rollback

- Upgrading applies Alembic migrations (deadline_alert JSON conversion, `task_state_store` table, partition columns, indexes on `task_instance.dag_version_id` / `dag_run.created_dag_version_id`). **Back up the metadata DB first.**
- Several 3.2.x/3.3.0 fixes target **downgrade** paths (MySQL `deadline_alert.interval`, SQLite FK checks). Since the project maintains a [ROLLBACK_STRATEGY.md](ROLLBACK_STRATEGY.md), rehearse the downgrade on a staging DB before production.
- `airflow db clean` now includes the `task_state_store` table (#68218) — update retention runbooks.

### 4.4 Recommended validation

1. Bump deps, `pip install`, resolve conflicts against the 3.3.0 constraints file.
2. Run the existing suite ([tests/](tests/)) — unit, contract, DAG-integrity, integration (DB/Kafka/timezone), e2e — watching for `DeprecationWarning`/`DeprecatedImportWarning`.
3. `airflow dags reserialize` + parse-check all generated DAGs; confirm no import errors from the bundle-detection or serializer paths.
4. Smoke-test one full pipeline end-to-end (ingest → transform → DQ → quarantine → load) including a deadline-alert fire and (newly) a real HITL approval via `airflow dags test`.
5. Verify the pandas-DataFrame XCom round-trip still works after the serde import migration.

---

## 5. Suggested Phasing

- **Phase 0 — Compatibility upgrade (this PR):** dep bumps, serde import migration, SMTP/regex/config checks, DB backup + migration, full test pass. Ship 3.3.0 running the existing feature set unchanged. Inherit all §3 free gains.
- **Phase 1 — Low-risk feature adoption:** real HITL approval (§2.3), pluggable retry policy + numeric backoff (§2.4), deadline notifier reads a Connection + `rerun_with_latest_version` knob (§2.5, §2.6).
- **Phase 2 — Strategic:** incremental-load watermarks + DQ provenance via the State Store (§2.2); design partition-aware scheduling for the Snowflake/ADLS sinks (§2.1).
- **Phase 3 — Roadmap watch:** evaluate native Multi-Team to retire custom `tenant_context` once it exits experimental (§2.7).

---

*Report generated from [airflow-release-notes.txt](airflow-release-notes.txt) (3.1.6 → 3.3.0) cross-referenced against the framework source in [rlam_airflow_framework/](rlam_airflow_framework/) and [plugins/](plugins/).*
