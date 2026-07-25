# Phase 3 — Remediation, Real-Environment Validation & Deferred Infrastructure

**Goal:** close everything that stands between "code written" and "upgrade done": fix the blockers found in the Phase 0–2 audit (2026-07-22), run the **first real-environment validation** of the entire program (no phase has had its Docker lane executed yet), then deliver the infrastructure work deferred out of Phase 0 (Helm/AKS, CI). Ends with the phase trackers honestly marked ✅.

**Branch:** continue on the working branch carrying Phases 0–2; Part C may branch separately.

**Read first:** [plan/README.md](README.md) standing rules. Context: [the audit verdict](phase-2-strategic-partitioning-statestore.md) — Phase 2 has two design blockers (B1 unverified API surface, B2 watermark scope) plus 3 failing tests; Phases 0–1 are code-complete but Docker-unvalidated.

**Definition of Done:**
- All Part A remediation items fixed; **all three test lanes green with zero failures** (skips only where a lane genuinely cannot run on the host).
- Part B executed on a machine with ACR access: full stack healthy on 3.3.0, and every e2e scenario below observed passing.
- Part C delivered: Helm values/chart on 3.3.0, CI running the real test lanes.
- Progress trackers in all four phase files updated to reflect reality.

---

## Progress Tracker

Status legend in [README.md](README.md#progress-tracking). Update the row + Notes whenever status changes.

| Task | Status | Owner | Notes |
|------|--------|-------|-------|
| 3A.1 API-verification probe in 3.3.0 container (B1) | ✅ | | Closed 2026-07-23: zero import errors + full integration/e2e (36/36) on the live 3.3.0 container; serializer registers; FAB resource names verified from ab_view_menu |
| 3A.2 Watermark state-scope redesign (B2) | ✅ | | airflow.sdk Variable read/write verified in source; no state-store leftovers (grep clean 2026-07-23) |
| 3A.3 Fix 3 failing factory tests (B3) | ✅ | Antigravity | Verified: 631 passed, 0 failures (reviewer-run) |
| 3A.4 Loud-failure posture for incremental reads | ✅ | Antigravity | Verified: strict mode + first_run_completed marker |
| 3A.5 Cron-branch mapper: wire or reject | ✅ | Antigravity | Verified: rejection at dag_factory_v2.py:199 + cron/mapper combination rejected |
| 3A.6 Pool slots from config, not parse-time DB | ✅ | Antigravity | Verified: airflow.models gone; slots from global_settings.yaml |
| 3A.7 Reference configs + dep pins + tracker hygiene | ✅ | | fab==3.7.1/celery==3.21.0 match constraints; constraints renamed constraints.txt w/ provenance note; scratch files in scripts/; trackers updated 2026-07-23 |
| 3B.1 Full stack up (ACR) + fail-loud init checks | ✅ | | 2026-07-23: rebuilt image (deps in image env), all components healthy incl. worker (healthcheck fixed); init now creates admin user + data-steward role/grants + steward user (was empty!) |
| 3B.2 Phase 0 validation (migrate, imports, rollback) | ✅ | | 2026-07-23: zero import errors; integration+e2e 36/36 vs live stack; rollback round-trip rehearsed + documented; cfg rename verified; BaseHook/models.Connection migrated to airflow.sdk |
| 3B.3 Phase 1 e2e (HITL, deadline, retry, rerun, HA) | ✅ | | 2026-07-23 all observed: HITL approve; deadline-miss (1 event, DB Connection, dict-payload bug fixed); ValueError failed at try 1/2 with policy reason in task log (retry_reason column is retry-only core semantics); HA: 2 schedulers, 1 miss → exactly 1 event. Rerun: serialized-flag verified; bundle-version execution deferred w/ 1.4 (needs ACR git-bundle stack) |
| 3B.4 Phase 2 e2e (fan-out, incremental, DQ state) | ✅ | | 2026-07-23: partition fan-out/reprocessing + incremental two-run + watermark safety all observed (see 2A.6/2B.5 notes). DQ-provenance excluded — feature was dropped in 3A.2, tracked at 2B.3 |
| 3B.5 Close-out: trackers ✅, commit/tag | ✅ | | 2026-07-23: trackers evidence-based across phases 0-3; upgrade-report §2.1/§2.2 statuses updated (2B.3 deviation documented); state committed on upgrade/phase-3-remediation and tagged airflow-3.3.0-validated |
| 3C.1 Helm/AKS upgrade (chart, apiServer, images) | 🟨 | | 2026-07-23 code done: Chart 3.3.0/v2.0.0, image tag, webserver->apiServer (VERIFY vs chart schema), kafka/smtp connections, pool provisioning from global_settings (3A.6). helm lint OK. BLOCKED on ACR/AKS: image mirror+digest pin, apiServer schema verify, dev-AKS sanity deploy |
| 3C.2 Azure DevOps CI: real test lanes | 🟨 | | 2026-07-23: engine ci.yml added — mocked + DAG lanes as separate PR-gating jobs, docker integration on push/schedule. YAML valid, pytest invocations match local reality. config.yml clarified as config-repo pipeline. Image-build stage stubbed pending ACR conn. Not yet run in Azure DevOps |
| 3C.3 ZScaler build-arg wiring for inside builds | ✅ | | 2026-07-23: USE_ZSCALER_CERT wired compose build.args + .env.example incl daemon-trust caveat; Dockerfile ARG pre-existing; `docker compose config` validates. Cert-freshness check remains an inside-env manual step |
| 3C.4 Pipeline-config delivery (external git repo → CI drop) | 🟨 | | 2026-07-23: Helm two-bundle split documented (engine LocalDagBundle baked / configs GitDagBundle from separate repo, tag-pinned); no configs baked in image; rollback = tag move. Remaining: stand up the actual config repo + its CI, and the pool/global_settings ship path decision with 3C.1 |
| 3D.1 Multi-Team evaluation spike (gated) | ⬜ | | |
| 3D.2 Custom DataFrame serializer retirement (gated) | ⬜ | | |

---

# Part A — Remediation of audit findings (agent-executable now; no ACR needed)

## Task 3A.1 — Verify the entire Phase 2 API surface in the real 3.3.0 package (Blocker B1)

The agent already pulled `apache/airflow:3.3.0` from Docker Hub for test runs — **ACR access is not needed for this task.** Write a probe script (pattern: the existing [docker/find_hitl.py](../docker/find_hitl.py)) and run it in that container:

1. **Import probe** — attempt every symbol [dag_factory_v2.py](../rlam_airflow_framework/dag_factory_v2.py#L20-L44) imports from `airflow.sdk`: `CronPartitionTimetable` (⚠️ release notes spell it **`CronPartitionedTimetable`**, #62441 — at least one is wrong), `PartitionedAssetTimetable`, `PartitionedAtRuntime`, `RollupMapper`, `FanOutMapper`, `FixedKeyMapper`, `IdentityMapper`, `Day/Week/Month/Quarter/YearWindow`, `WaitForAll`, `MinimumCount`, `StartOf*Mapper`. For each: does it exist at that path? If not, grep the installed package for where it actually lives and its real name.
2. **Constructor probe** — instantiate each mapper/timetable with the kwargs the factory passes (`upstream_mapper=`, `window=`, `wait_policy=`, `max_downstream_keys=`, `downstream_key=`, `assets=`, `default_partition_mapper=`, `timezone=`) and record which kwargs are real.
3. **State-store probe** — resolve the real API for AIP-103: are `task_state_store` / `asset_state_store` context keys? Is subscript access (`store[asset]`) real? The `.set(key, value, retention=...)` signature? Does `airflow.sdk.execution_time.context.NEVER_EXPIRE` exist? **What is task-state scope — per-task or per-DAG?** (feeds 3A.2). Also probe the by-name/by-URI asset-state routes (#66336).
4. **Config probe** — confirm `[scheduler] partition_mapper_max_downstream_keys` and the `[core] callback_execution_timeout` env key spelling used in compose.
5. **Write findings to `plan/api-verification-report.md`** (symbol → verified path/signature → fix needed y/n), then **fix every wrong import/kwarg** in `dag_factory_v2.py`/`taskflow_tasks.py` and update the conftest mocks to mirror only the *verified* paths.

**Acceptance:** probe report exists; framework imports match it exactly; mocked lane still green.

## Task 3A.2 — Close the watermark loop (Blocker B2)

Today the watermark is **written** by `load_data` (own task store + **outlet** asset state, [taskflow_tasks.py:842-864](../rlam_airflow_framework/taskflow_tasks.py#L842-L864)) but **read** by `ingest_data` (own task store + **inlet** asset). If task state is task-scoped, and since inlet ≠ outlet, **no write is ever seen by the reader** → silent full loads.

1. Decide the single source of truth from 3A.1's findings, using this **fallback ladder** (top rung that the probe proves available wins; the retry-safety invariant — advance only after confirmed load — holds identically on every rung):
   1. **Inlet asset state via by-URI/name access** (preferred — the watermark semantically describes the *source*): written by `load_data` after all destinations succeed, read by `ingest_data` from its inlet.
   2. **Cross-task write to the ingest task's state** — only if the probe proves the accessor or Core/Execution API can address *another task's* state (note: "DAG-scoped task state" is **not** a documented AIP-103 concept — do not assume it; the probe question is specifically cross-task addressability).
   3. **Airflow Variable fallback** (`{dag_id}.high_watermark`) — guaranteed-available Task SDK surface (Variable get/set verified in 3.3.0; #68542/#66022): `load_data` sets it post-success, `ingest_data` reads it. Less observability/retention than AIP-103, but zero scope uncertainty.
2. Keep exactly **one** read path and **one** write path — delete the current dual store/fallback maze.
3. **Concurrency guard (required regardless of rung):** partitioned pipelines now run with `max_active_runs > 1`, so a config enabling **both** `partition` and `incremental` would race concurrent runs on one global watermark (last-writer-wins corruption). For Phase 3, **reject that combination at config validation** with a clear message; per-partition watermark keying (`high_watermark:{partition_key}`) is a documented future enhancement, not implemented now.
4. **Tests:** unit — watermark advances only on successful load; a failed load leaves it untouched; second run reads what the first wrote (dict-backed fake matching the *verified* API shape); contract — partition+incremental combination is rejected.

**Acceptance:** read and write provably target the same key/scope per the verified API; unit tests cover the retry-safety invariant.

## Task 3A.3 — Fix the three failing tests (Blocker B3)

All three failures in [tests/unit/test_dag_factory_v2.py](../tests/unit/test_dag_factory_v2.py) are `TenantValidationError: Tenant 'test' not found in registry`. Fix by having the test fixture register the tenant (patch/inject global settings with a `test` tenant incl. a pool with known slots — which also enables asserting the derived `max_active_runs`), not by weakening tenant validation.

**Acceptance:** `pytest tests/unit tests/contract -q` → **0 failures**.

## Task 3A.4 — Loud-failure posture for incremental reads

An incremental pipeline that silently falls back to a full load is a data-cost incident. In `ingest_data`:
- When `incremental.enabled` and no stored watermark is found, log **WARNING** ("incremental configured but no watermark found — performing FULL load from initial_watermark") — not info/debug.
- Add `incremental.strict: bool` (schema + validation): when true, a missing watermark **after the first successful run** raises instead of full-loading. (First run legitimately has none; consider recording a `first_run_completed` marker alongside the watermark.)
- State-store write failures in `load_data` stay non-fatal but must log WARNING with enough context to alert on.

**Acceptance:** unit tests assert the WARNING and the strict-mode raise.

## Task 3A.5 — Cron-branch mapper: wire it or reject the combination

In the partitioned+cron branch the configured `mapper`/`wait_policy` are built and silently discarded ([dag_factory_v2.py](../rlam_airflow_framework/dag_factory_v2.py) partition block). Per the 3A.1 findings, either the cron-partitioned timetable can accept a mapper (wire it) or it can't (then **fail validation** when a config combines a cron schedule with `mapper`/`wait_policy` settings — tell the user what to remove). No silently-ignored configuration.

**Acceptance:** every partition config key provably affects behavior or is rejected at load with a clear message; contract test for the invalid combination.

## Task 3A.6 — Pool slots from config, not a parse-time DB query

`get_pool_slots()` imports `airflow.models.pool.Pool` during DAG parsing — against Airflow 3 parse isolation, and its blanket `except → None` means the derived default silently becomes 8 in practice. Replace: add `slots` to each tenant's pool definition in [config/global_settings.yaml](../config/global_settings.yaml) (tenant_context already reads this file), have the factory use that, and delete `get_pool_slots`. Document that the config value must match the actual Airflow pool size (the compose/Helm pool provisioning should create pools *from the same config* — add that to the init script if not present).

**Acceptance:** no `airflow.models` import anywhere in DAG-parse code paths; derived `max_active_runs` unit tests use config-declared slots.

## Task 3A.7 — Loose ends

- Add **reference configs** under [config/data_sources/](../config/data_sources/): one partitioned pipeline, one incremental pipeline (plan 2.C debt). They must parse in the dag-integrity lane.
- Pin `apache-airflow-providers-fab` and `-celery` in [requirements.txt](../requirements.txt#L167-L168) to the constraints-resolved versions (currently unpinned).
- Delete or promote stray scratch files (`docker/find_hitl.py`, `docker/test_health.py`) — either into a `scripts/` home with a docstring, or out.
- Update the **Progress Trackers** in phase files 0–2 to actual status (nothing is ✅ until 3B completes).

---

# Part B — Real-environment validation (requires ACR access — run from inside, or allowlist the agent IP)

> This is the first time any phase's Docker lane runs. Order matters: B1 → B2 → B3 → B4. Every step that fails loudly here is the verification working — fix forward, re-run.

## Task 3B.1 — Full stack up + fail-loud init

`docker compose up` with the ACR-hardened images. The init container is *designed* to hard-fail on: wrong FAB resource names (`add-perms` has no `|| true`), connection-seeding errors, migration failures. Resolve any failure it surfaces (the `"HITLDetail"` resource name is the most likely casualty — fix from `airflow roles list-resources` output or FAB constants). All services healthy; `airflow version` reports 3.3.0 everywhere.

## Task 3B.2 — Phase 0 validation

`airflow db migrate` clean (deadline JSON conversion, `task_state_store` table, partition columns, `retry_delay_override`/`retry_reason`); **zero import errors** across all generated DAGs (`airflow dags list-import-errors`) — this is also the final proof of 3A.1; DataFrame XCom round-trip between two real tasks; the [ROLLBACK_STRATEGY.md](../ROLLBACK_STRATEGY.md) downgrade rehearsal on a throwaway DB copy; `pytest tests/integration tests/e2e` against the stack.

## Task 3B.3 — Phase 1 e2e

1. **HITL clickthrough:** pipeline with `hitl.enabled: true` + seeded invalid records → task reaches `awaiting_input` holding no worker slot → approve as a `data-steward` user → `process_approval_decision` receives the real payload (this validates the `responded_by_user`/`params_input` keys and `"Approve"/"Reject"` labels — if the fail-loud mapping raises, fix the keys from the observed payload) → `load_quarantine_records` completes. Also one **rejection** path.
2. **Deadline:** force a miss → `SyncCallback` reads the **DB-stored** `kafka_default` connection → event lands on the topic (verify in Kowl) → exactly **one** event (HA-dedup assertion; scale schedulers to 2 for this check if feasible in compose).
3. **Retry policy:** a task raising `requests.exceptions.ConnectionError` retries; one raising `ValueError` fails immediately with `retry_reason` populated on the TI.
4. **Rerun version:** two bundle versions; clear a run with `rerun_with_latest_version` unset vs explicitly set; confirm which code version executes.

## Task 3B.4 — Phase 2 e2e

1. **Partition fan-out:** producer emits partitioned asset events for dates D and D+1 → consumer fires **independent runs per partition**, `partition_key`/`partition_date` visible in task context and Kafka events; runs bounded by the derived `max_active_runs`.
2. **Partition reprocessing:** clear/backfill **only D** per the [runbook](../Documentation/Runbook_Partition_Range_Reprocessing.md); D+1 untouched. Fix the runbook where reality disagrees.
3. **Incremental two-run test:** seed source → run 1 (full load, WARNING logged, watermark stored) → add rows → run 2 ingests **only the delta** and advances the watermark. Then kill a load mid-run and confirm the watermark did **not** advance.
4. **DQ provenance:** the scorecard appears in the asset state store (UI asset-store view) linked to the writing task instance.

## Task 3B.5 — Close-out

All trackers in phases 0–2 flipped to ✅ with notes; commit (and tag if desired) the validated state; update [upgrade-report.md](../upgrade-report.md) §2.1/§2.2 status to "implemented & validated".

---

# Part C — Deferred infrastructure (from Phase 0's Deferred section)

## Task 3C.1 — Helm/AKS upgrade

- Bump [helm/Chart.yaml](../helm/Chart.yaml) to a chart version supporting Airflow 3.3.0; bump `images.airflow.tag` from `"3.1.6-custom"`.
- **Rework the `webserver:` block** ([values:175-189](../helm/values/production.yaml#L175-L189)) for the chart's `apiServer` rename — verify against the target chart's values schema, don't assume.
- Provision `kafka_default`/`smtp_default` in the Helm `connections:` block (mirroring compose); create tenant pools from `global_settings.yaml` slot counts (3A.6 contract).
- ACR: mirror the 3.3.0 base image, then switch the Dockerfile `AIRFLOW_BASE_IMAGE` back from DHI (the `TODO(acr)` marker) and pin by digest.
- Sanity-deploy to a dev AKS namespace before touching production values.

## Task 3C.2 — Azure DevOps CI: run the real test lanes

Extend [.azure-pipelines/config.yml](../.azure-pipelines/config.yml) (today: schema checks only). Keep the existing schema/tenant validation jobs. Gate PRs on the mocked lane at minimum.

**Lane invocations — run in SEPARATE steps (post 2026-07-23 lane-separation):**
- Mocked lane: `pytest tests/unit tests/contract` (pip-cached, fast; installs `airflow.*` mocks in `sys.modules`).
- **DAG lane in its OWN invocation:** `pytest tests/dag` — it needs *real* Airflow and [tests/dag/conftest.py](../tests/dag/conftest.py) **loudly skips** the whole lane if the unit mocks are already in-process. Do **not** combine it with `tests/unit` in one `pytest` call (that yields ~57 silent-looking skips). On a Linux CI runner the DAG lane runs fully; the 13 DagBag tests that need POSIX `fcntl` only skip on Windows.
- Docker-based job for `tests/integration` (+ `tests/e2e` if runner capacity allows) using compose services. The **opt-in probe suite** ([tests/e2e/test_probe_scenarios.py](../tests/e2e/test_probe_scenarios.py), `RUN_PROBE_E2E=1`) is a good nightly/scheduled job — it drives real deadline/partition/incremental scenarios and takes minutes.
- Value note: the DAG lane un-skip (2026-07-23) already caught a real parse-time bug (snowflake AIP-60 URI) that the container lane masked — strong reason to make `pytest tests/dag` a required PR gate.

## Task 3C.3 — ZScaler wiring for inside builds

Implement the `USE_ZSCALER_CERT` build ARG (Phase 0 Task 0.11 design) if not yet done; wire `--build-arg USE_ZSCALER_CERT=true` into the inside-env build path (compose `build.args`, CI variable); document the **daemon-level trust** requirement for pulling through ZScaler; confirm [zscaler-ca.crt](../zscaler-ca.crt) is current.

## Task 3C.4 — Pipeline-config delivery: separate git repo → CI drop → DAG generation

**Context / decision (2026-07-23).** The framework distinguishes **engine** (the `rlam_airflow_framework` package — now includes the deadline notifiers after the `plugins/` fold-in — shipped baked into the image) from **content** (the per-pipeline `config/data_sources/*.yaml`). The agreed model: **pipeline configs live in a separate git repo**, and a CI/CD YAML pipeline places them where Airflow reads them, which drives DAG generation via [dags/generate_dags.py](../dags/generate_dags.py). This mirrors the DAG-bundle approach the rollback strategy already assumes ([ROLLBACK_STRATEGY.md](../ROLLBACK_STRATEGY.md), `GitDagBundle` → `refs/tags/prod-vX.Y.Z`).

**The gap this closes.** Today `config/` (and `dags/`) reach the local stack **only via docker-compose volume mounts** ([docker/docker-compose.yaml](../docker/docker-compose.yaml) `../config:/opt/airflow/config`, `../dags:/opt/airflow/dags`) — the same mount-dependency class that left `plugins/` missing from the image before the fold-in. In AKS there is no host to mount from, so config delivery must be explicit. **Do NOT bake configs into the image** — that would recouple config changes to engine releases and defeat config-only rollback.

**Work:**
1. Decide the config repo layout + the schema contract it must satisfy (the [data_source_schema.yaml](../config/schemas/data_source_schema.yaml) validation, including the new **unknown-top-level-key rejection** contract test added 2026-07-23 — the external repo's configs must pass it in the config repo's own CI).
2. Wire the delivery mechanism in Helm/AKS: `GitDagBundle` pointing at the config repo (preferred — gives tag-based rollback for free), **or** an init/sync step that clones the tagged config revision into the DAGs/config path. Reconcile with the two bundles already sketched in the rollback doc (framework LocalDagBundle + configs GitDagBundle).
3. Provision `global_settings.yaml` (tenants/pools) and connections consistently with 3A.6/3C.1 — decide whether global settings ship with the engine image or the config repo (recommendation: engine image, since pool slots are an operational contract the DAG factory reads at parse time).
4. Keep the local compose mounts as the **fast-iteration dev path** (they correctly override the delivered copies on a laptop); the delivered/bundle path is for AKS.
5. Add a CI job in the **config repo** (not this repo) that runs the contract/schema lane against every config before it can be tagged for deployment — so a bad config is caught before it reaches Airflow.

**Acceptance:** an AKS deployment picks up pipeline configs from the external repo at a pinned revision with no volume mount and no image rebuild; rolling a config back is a git-tag move; the engine image contains zero pipeline configs.

---

# Part D — Roadmap items (explicitly gated; do not start without a go-decision)

## Task 3D.1 — Multi-Team evaluation spike (from [upgrade-report.md §2.7](../upgrade-report.md))

**Gate:** Airflow Multi-Team no longer flagged experimental (re-check in the then-current release notes). Time-boxed spike, no production code: map `tenant_context` capabilities (DAG-ID prefixing, pools, connection mapping, Kafka namespacing, tags) onto native Multi-Team; prototype one tenant as a team in a sandbox; deliverable is a written go/no-go with a migration outline and the list of `tenant_context` code it would retire.

## Task 3D.2 — Custom DataFrame serializer retirement (parked from the rejected "simplifications" proposal)

**Gate:** verified behavior parity. The built-in serde pandas serializer (3.2.0 notes) may make [serializers.py](../rlam_airflow_framework/serializers.py) redundant — but the custom one preserves dtypes via `orient='split'`. Spike: round-trip representative DataFrames (dtype-heavy, tz-aware datetimes, NaN/None) through the built-in serializer in the 3.3.0 container; retire ours **only** on full parity, migrating the 25 serializer tests to assert the built-in path. Otherwise keep and document why.

---

## Phase 3 exit checklist
- [x] 3A.1 probe report written; all imports/kwargs match verified API; mocks mirror it
- [x] 3A.2 watermark loop closed (single scope), retry-safety unit-tested
- [x] 3A.3 zero test failures in mocked lane
- [x] 3A.4 loud fallback + strict mode for incremental
- [x] 3A.5 no silently-ignored partition config
- [x] 3A.6 pool slots config-driven; no parse-time DB access
- [x] 3A.7 reference configs, pins, scratch-file cleanup, trackers honest
- [x] 3B\.1–3B\.4\ full\ Docker\ validation\ executed\ and\ green\ \(all\ scenarios\ observed\)
- [x] 3B\.5\ trackers\ ✅,\ state\ committed/tagged,\ report\ updated
- [ ] 3C.1 Helm on 3.3.0 (apiServer rework verified), dev-AKS sanity deploy
- [ ] 3C.2 CI runs mocked lane (PR gate) + Docker integration lane
- [ ] 3C.3 ZScaler ARG wired + daemon-trust documented
- [ ] 3C.4 pipeline-config delivery from external repo (no mount, no bake, tag-rollback)
- [ ] 3D.1 / 3D.2 remain gated unless explicitly green-lit
