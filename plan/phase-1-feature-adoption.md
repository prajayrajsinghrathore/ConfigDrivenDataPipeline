# Phase 1 — Low-Risk Feature Adoption on Airflow 3.3.0

**Goal:** turn on the 3.3.0 capabilities that the framework already models in config but does not truly implement — real HITL approval, pluggable retry policies, deadline notifications sourced from Connections (plus multi-tier alerts), and explicit rerun/rollback bundle-version control.

**Branch:** `upgrade/phase-1` (from `develop`, after Phase 0 is merged).

**Preconditions:** Phase 0 exit checklist complete. The stack runs on 3.3.0; both test lanes are green.

**Read first:** [plan/README.md](README.md) standing rules. In particular, **verify every 3.3.0 API against the installed package** — the class names below (retry-policy base class, HITL operator, deadline reference enums, config keys) are *candidates to confirm*, not facts to assume.

**Definition of Done:**
- Quarantine HITL actually pauses on the triggerer and resumes on a human decision; auto-approve is gone.
- Tasks retry only on transient failures via a pluggable policy; blanket `retries=3` is replaced.
- The deadline notifier reads Kafka/SMTP settings from an Airflow Connection, supports multiple alert tiers, and can use `AVERAGE_RUNTIME`.
- `rerun_with_latest_version` is wired from config and documented against the rollback strategy.
- Both test lanes green, including a Docker e2e that drives a real HITL approval.

---

## Progress Tracker

Status legend in [README.md](README.md#progress-tracking). Update the row + Notes whenever status changes.

| Task | Status | Owner | Notes |
|------|--------|-------|-------|
| 1.1 Real HITL quarantine approval (triggerer) | ✅ | | 2026-07-23 e2e observed: awaiting_input → approve → load_quarantine_records success on live stack |
| 1.2 Pluggable retry policy + numeric backoff | ✅ | | retry_policy.py per AIP-105; unit-covered; transient path exercised by deadline probe |
| 1.3 Deadline from Connection + tiers + AVERAGE_RUNTIME | ✅ | | 2026-07-23 e2e: deadline miss fired 1.6s late; SyncCallback read DB-stored kafka_default (kafka:29092); exactly one event on pipeline-alerts; callback success. Real payload shape (dict dag_run, ISO deadline_time) fixed + unit-locked |
| 1.4 `rerun_with_latest_version` control | 🟨 | | 2026-07-23: config → @dag kwarg → serialized DAG verified on live stack ('rerun_with_latest_version': true). Two-bundle-version execution e2e still open — needs the git-bundle stack (bundle-test compose pulls from ACR; local dags-folder bundle is unversioned) |
| 1.5 Schema / fixtures / contract-tests / docs | ✅ | | schema fields optional w/ defaults; contract tests green; example_airflow_330_features.yaml |

---

## Task 1.1 — Implement real HITL quarantine approval

Replace the Phase-0 fail-fast placeholder with a working Human-in-the-Loop approval, using 3.3.0's `awaiting_input` task state that runs off the **triggerer** (so it holds no worker slot while waiting).

**Files:** [rlam_airflow_framework/dag_factory_v2.py](../rlam_airflow_framework/dag_factory_v2.py#L293-L345), [rlam_airflow_framework/taskflow_tasks.py](../rlam_airflow_framework/taskflow_tasks.py#L399-L468), [rlam_airflow_framework/data_quality.py](../rlam_airflow_framework/data_quality.py#L649-L760).

**Steps:**
1. **Verify the operator.** Confirm the exact 3.3.0 HITL operator/decorator and its parameters (`ApprovalOperator` / `HITLOperator` / a `@task.hitl`-style decorator) and where it lives. Confirm how `timeout_hours` maps to its timeout kwarg and how `allowed_roles` maps to its assignment/authorization parameter. Do not proceed on the commented assumptions currently in the code.
2. **Wire it into the branch.** In the quarantine path, after `quarantine_invalid_data`, instantiate the real approval operator (operators cannot be `@task` decorators — instantiate in the `@dag` body). Feed it the approval context produced by [prepare_hitl_approval_context](../rlam_airflow_framework/taskflow_tasks.py#L399-L440) (`create_hitl_quarantine_approval_task` builds summary/form fields/timeout). On **approve** → `load_quarantine_records`; on **reject/reprocess** → route per [process_hitl_approval_result](../rlam_airflow_framework/data_quality.py#L714-L760) (which already returns `(df, action)`).
3. **Consume the decision.** Update `process_approval_decision` to read the operator's real output (approved_by/action/notes) instead of a synthetic dict. Preserve the existing quarantine-record metadata columns.
4. **Config/schema:** `hitl.enabled/timeout_hours/allowed_roles` already exist ([data_source_schema.yaml](../config/schemas/data_source_schema.yaml#L99-L113)); re-enable the flag in [example_airflow_316_features.yaml](../tests/fixtures/example_airflow_316_features.yaml) that Phase 0 disabled.
5. **`allowed_roles` enforcement — DECIDED: FAB access control, NOT operator `assigned_users`.** The 3.x HITL operators take `assigned_users` (a list of `HITLUser` identities: `id`/`name`) and have **no role parameter**. Do **not** map role names into `assigned_users` — entries are matched against the responding *user's identity* (FAB does not expand a role into its members), so a role name would match nobody (blocking approvals until timeout); it is also baked into the serialized DAG, while role membership is dynamic. Instead:
   - The DAG factory does **not** pass `allowed_roles` to the operator; leave `assigned_users` unset so anyone authorized via the auth manager's `is_authorized_hitl_task()` (3.1.6, #59399) can respond.
   - `allowed_roles` becomes a **provisioning contract**: for each listed role, grant that FAB role the per-DAG permissions to see/answer the HITL task on this `dag_id` (HITL_DETAIL read/edit + the DagRun/TI/HITL reads from the 3.2.1 `/dags` change). Script this in the compose-init role bootstrap (extend the existing `airflow roles create data-steward` step) — not "configure via UI". Per-DAG scoping + tenant-prefixed dag_ids yields tenant-scoped approver groups.
   - **Optional:** add a separate honest `hitl.assigned_users:` config field for pipelines that want specific named approvers, passed through verbatim as `HITLUser` entries (narrows *within* the role-authorized set).
   - **Known limitation (document it):** gating granularity is per-DAG, not per-task — acceptable here because each DAG has exactly one HITL task (quarantine approval).
   - **Verify against installed 3.3.0:** (a) semantics of empty/omitted `assigned_users` (who may respond); (b) FAB `is_authorized_hitl_task()` honors per-DAG scoping, not just a global HITL permission; (c) the exact FAB permission/resource names to grant.
   - **Forward-compatibility — Azure Entra (planned future integration):** this design survives an Entra rollout unchanged *if* Entra is integrated as **FAB + OAuth/OIDC** — Entra authenticates and supplies group claims; FAB maps groups → local roles (`AUTH_ROLES_MAPPING`, `AUTH_ROLES_SYNC_AT_LOGIN = True` in `webserver_config.py`), and the per-DAG role permissions granted here keep working with zero DAG/framework changes (this is a direct benefit of gating on roles at the auth-manager layer instead of baking user identities into `assigned_users`). Two Entra gotchas for that future work: group claims are **object IDs (GUIDs)** by default, so the roles mapping must key on what the claim actually emits; and users in >200 groups hit the **groups-overage** case (claim dropped, Graph lookup needed). If Entra is instead integrated via a **non-FAB auth manager**, the operator-level design still holds, but the `allowed_roles` provisioning contract must be re-implemented against that auth manager's authorization model.
6. **Remove** the Phase-0 `NotImplementedError`/deferral branch.

**Tests:**
- *Unit (mocked):* `prepare_hitl_approval_context` builds the correct summary/timeout; `process_approval_decision` handles approve/reject/reprocess. Mock the HITL operator in [conftest.py](../tests/unit/conftest.py) at its verified path.
- *e2e (Docker):* a pipeline with `hitl.enabled: true` reaches `awaiting_input`; drive the approval via the REST/Execution API or CLI, then assert the run completes down the approved branch. Note 3.3.0 fix #69104 makes `airflow dags test` **wait** for HITL input rather than loop — use it to script the approval.
- *Remove:* any remaining assertion of auto-approve.

**Acceptance:** real pause-and-resume approval works end-to-end in Docker; unit lane covers context/decision logic.

---

## Task 1.2 — Pluggable retry policy (AIP-105) + numeric exponential backoff

Replace the blanket `retries=3` in [_create_default_args](../rlam_airflow_framework/dag_factory_v2.py#L695-L721) with a policy that distinguishes transient from permanent failures.

**New file:** `rlam_airflow_framework/retry_policy.py`.

**Steps:**
1. **AIP-105 interface — VERIFIED from the spec** ([AIP-105_+Pluggable+Retry+Policies.doc](../AIP-105_+Pluggable+Retry+Policies.doc), Completed, released in 3.3.0). Build against this API (a one-line import smoke-test against the installed package is still worthwhile — spec and shipped code can drift — but this is no longer an open question):
   - Module **`airflow.sdk.definitions.retry_policy`**: `RetryPolicy` (ABC with `evaluate(exception, try_number, max_tries, context=None) -> RetryDecision`), `ExceptionRetryPolicy(rules: list[RetryRule], default=RetryAction.DEFAULT)`, `RetryRule` (**singular**; fields: `exception` (class | dotted-path str | list of either), `action`, `retry_delay`, `reason`, `match_subclasses=True`), `RetryAction` (`RETRY`/`FAIL`/`DEFAULT`), `RetryDecision` (with `.fail()/.retry()/.default()` classmethods).
   - Attached via `retry_policy=` on any task/operator **and via `default_args`** (explicitly supported) — so wiring through `_create_default_args` is the sanctioned path. Works with mapped tasks.
   - Composition: `retries` remains the **cap** (a policy can fail earlier, never exceed it); `retry_delay`/`retry_exponential_backoff`/`max_retry_delay` are the delay calculation used when the policy returns `DEFAULT` **or** `retry_delay=None`; `AirflowFailException` bypasses the policy entirely (explicit no-retry from task code always wins).
   - Evaluation runs **in the task worker**; serialization stores only a `has_retry_policy` bool (the policy object is never serialized — no importability requirement on the API server; 3.3.0 also fixed the retry-policy serialization version-churn bugs #69315/#69241).
   - The 3.3.0 DB migration adds `task_instance.retry_delay_override`/`retry_reason` columns — already covered by Phase 0 Task 0.8's migration run.
2. Implement `build_transient_retry_policy(...)` in `retry_policy.py` as a **factory returning an `ExceptionRetryPolicy`** (no subclass needed): transient errors (network/timeout `ConnectionError`/`TimeoutError`, HTTP 429/5xx `requests.exceptions.HTTPError`, Snowflake throttling/transient, Kafka delivery timeouts) → `RetryAction.RETRY` with **`retry_delay=None`** so the task's own delay/backoff calculation applies; deterministic errors (`builtins.ValueError`, schema/validation failures, `FormulaError` from [formula_engine.py](../rlam_airflow_framework/formula_engine.py)) → `RetryAction.FAIL` with a `reason` (it lands in task logs and the new `retry_reason` column — observability for free). Implementation details from the spec:
   - **String exception names must be dotted paths** (`"builtins.ValueError"`, not `"ValueError"`) — non-dotted strings raise `ValueError` at definition time. Config-supplied exception names must be validated/normalized accordingly.
   - `match_subclasses=True` is isinstance-based — a rule for `OSError` also catches `ConnectionError`. Order rules from specific → general, and use `match_subclasses=False` where a subclass must be treated differently.
   - Prefer passing framework-importable exception **classes** directly (e.g. `FormulaError`); dotted strings that can't resolve at parse time only warn (they may resolve on the worker).
3. Support numeric exponential backoff (3.2.0): the framework can now pass `retry_exponential_backoff=<float>`; expose it via config and default sensibly. Note the REST API type change (boolean→number) — only numeric values are valid. This composes cleanly with step 2: transient `RETRY` decisions with `retry_delay=None` inherit exactly this backoff calculation.
4. **Config/schema:** add an optional `retry:` block to the config schema ([data_source_schema.yaml](../config/schemas/data_source_schema.yaml)) — e.g. `retry: {policy: transient|fixed, max_retries: N, exponential_backoff: <float>, retry_delay_seconds: N}`. Wire it in `_create_default_args`. Keep a `fixed` option that reproduces today's behavior for pipelines that want it.
5. Update [config/global_settings.yaml](../config/global_settings.yaml) defaults if retry defaults live there.

**Tests:**
- *Unit:* the classifier returns retry/no-retry for a representative set of exceptions; config → policy wiring produces the expected `default_args`.
- *Integration (Docker):* a task that raises a transient error retries; one that raises `ValueError` fails immediately without consuming retries.

**Acceptance:** transient-only retry behavior verified in the real lane; config-driven and back-compatible via the `fixed` option.

---

## Task 1.3 — Deadline notifications from a Connection + multi-tier alerts + AVERAGE_RUNTIME

Evolve [plugins/deadline_callbacks.py](../plugins/deadline_callbacks.py) and [_create_deadline_alert](../rlam_airflow_framework/dag_factory_v2.py#L378-L437) to stop reading Kafka/SMTP config from environment variables and to use 3.3.0 deadline improvements.

**Steps:**
1. **Connection-sourced config — DECIDED: migrate the Kafka/SMTP deadline callbacks to `SyncCallback`.** Today [PluginKafkaPublisher](../plugins/deadline_callbacks.py#L56-L121) reads `KAFKA_BOOTSTRAP_SERVERS`/`KAFKA_SECURITY_PROTOCOL` from env. Move Kafka connection details (and SMTP for email) into Airflow **Connections**, read from the callback. `AsyncCallback` cannot fetch Connections without extra context plumbing; Connection/Variable access was explicitly added to **`SyncCallback`** in 3.3.0 (#65269) — use that paved road. Three riders on the decision:
   - **Adverse-selection mitigation (REVISED after verification):** `SyncCallback` runs via the **executor** (was: triggerer), so the alert competes for worker slots — and deadline misses correlate with resource pressure. Implementation verification found that **pools are not supported on callbacks**, and `executor=` only selects among *configured* executors — with this project's single `CeleryExecutor` it is a **no-op**. Revised mitigation: leave the callback **un-pinned**; keep `callback_execution_timeout` (30s) so a hung flush/send releases the slot; **document the residual risk** (a saturated Celery fleet delays the alert — acceptable: misses are rare, callbacks short, Kafka flush capped at 10s) and watch callback latency via the existing StatsD/Grafana metrics. **Escalation path (documented, not built):** if alert timeliness under load proves a problem, add a secondary `LocalExecutor` (Airflow 3 supports concurrent executors) and pin callbacks to it via `executor=` so alerts run scheduler-side — verify the param accepts a secondary executor name before adopting.
   - **Verify the callable contract:** current wiring is `AsyncCallback(CompositeDeadlineNotifier, kwargs={...})` + `BaseNotifier.notify(context)`. Verify against installed 3.3.0 that `SyncCallback` invokes the notifier the same way and that the context payload (`dag_run`, `deadline` info) has the same shape; adapt [deadline_callbacks.py](../plugins/deadline_callbacks.py) if not.
   - **Verify metadata-DB Connections specifically:** the 3.2.0 notes warned SyncCallback could not read Connections stored in the metadata DB; #65269 (3.3.0) is the fix for exactly that case — and the DB is where this project's Connections live (no secrets backend configured). The e2e test below must exercise a **DB-stored** Connection end-to-end.
   - **Seed the Connections where the stack runs.** Add the Kafka (and SMTP) Connection to the local stack so this works end-to-end and in the e2e lane: define it in the [docker/docker-compose.yaml](../docker/docker-compose.yaml) `airflow-init` step (e.g. `airflow connections add`) and in [docker/.env.example](../docker/.env.example). (Helm `connections:` provisioning is part of the deferred Helm effort — leave a TODO pointer, don't do it here.) Once creds come from a Connection, remove the now-dead `KAFKA_*` env wiring from compose to avoid two sources of truth.
2. **Multiple alert tiers.** 3.2.0 allows a **list** of `DeadlineAlert`s on `deadline=`. Extend `_create_deadline_alert` to return a list and let config define tiers (e.g. warn at 15m via Kafka only, page at 30m via Kafka+email). Update the schema `deadline:` block ([data_source_schema.yaml](../config/schemas/data_source_schema.yaml#L367-L388)) to accept either a single object (back-compat) or a `tiers:` array.
3. **Reference options.** Add support for `DeadlineReference.AVERAGE_RUNTIME` (3.3.0 now excludes non-successful runs, #68949) as an alternative to the fixed `DAGRUN_QUEUED_AT` + `timeout_minutes`. Expose via config (`deadline.reference: queued_at|average_runtime`). **Verify** the enum member name in 3.3.0.
4. **Timeout guard.** Set `[core] callback_execution_timeout` (or the per-DAG equivalent, #66609) so a slow notifier can't hang. **HA is the intended deployment** ([helm values scheduler.replicas: 2](../helm/values/production.yaml#L156)), so fix #64737 (duplicate deadline-miss callbacks across HA replicas) matters — it is handled by core, no code needed, but write a test/assertion that a single deadline miss produces exactly one Kafka event, and document the reliance on the core fix.
5. Keep the "Kafka always, email optional" contract intact.

**Tests:**
- *Unit:* config → alert-list construction (single vs tiered); notifier builds the correct Kafka payload; email path gated on `email_enabled` + recipients.
- *Integration (Docker):* create a **metadata-DB-stored** Kafka Connection (bootstrap `kafka:29092` — the in-network listener; `9092` is host-only), trigger a deadline miss, assert the `SyncCallback` notifier reads that Connection and an event lands on the topic (env vars removed), and that the callback completed within `callback_execution_timeout`.

**Acceptance:** no deadline-path credential comes from env vars; tiered alerts fire; verified in the real lane.

---

## Task 1.4 — Explicit rerun / rollback bundle-version control

3.3.0's `rerun_with_latest_version` controls whether cleared/rerun/backfilled runs use the latest bundle version or the original. This directly serves [ROLLBACK_STRATEGY.md](../ROLLBACK_STRATEGY.md) and the bundle-version detection already in [_detect_bundle_version](../rlam_airflow_framework/dag_factory_v2.py#L647-L693).

**Steps:**
1. **Verify** the exact knob names/precedence in 3.3.0: `[core] rerun_with_latest_version`, the Dag-level `rerun_with_latest_version` kwarg on `@dag`, and the request/CLI override. Confirm defaults (False for clear/rerun, True for backfill).
2. Set the `[core]` default in [docker/airflow.cfg](../docker/airflow.cfg) to the value the rollback strategy wants (record the rationale).
3. Allow per-pipeline override via config: add `rerun_with_latest_version: bool` to the schedule/metadata schema and pass it to the `@dag(...)` call in [create_dag_from_config](../rlam_airflow_framework/dag_factory_v2.py#L183-L195).
4. Update [ROLLBACK_STRATEGY.md](../ROLLBACK_STRATEGY.md) to describe exactly how an operator pins/floats bundle versions on rerun using this setting (and note the related correctness fixes #68389/#68558/#68336 that come with 3.3.0).

**Tests:**
- *Unit:* config value propagates to the `@dag` kwargs (assert via the mocked `dag` decorator capturing kwargs).
- *Integration (Docker):* with two bundle versions present, clear a run with the setting True vs False and assert which version executes.

**Acceptance:** rollback strategy is backed by a real, tested setting rather than implicit behavior.

---

## Task 1.5 — Schema, fixtures, and docs

- Extend [config/schemas/data_source_schema.yaml](../config/schemas/data_source_schema.yaml) for the new `retry:`, tiered `deadline:`, and `rerun_with_latest_version` fields; keep every addition **optional** with sane defaults so existing configs still validate.
- Update the contract tests ([tests/contract/test_config_schemas.py](../tests/contract/test_config_schemas.py)) to cover the new fields (valid + invalid cases).
- Add/adjust an example config demonstrating HITL + tiered deadlines + transient retries end-to-end (extend [example_airflow_316_features.yaml](../tests/fixtures/example_airflow_316_features.yaml) or add a new fixture; rename away from "316" since it now targets 3.3.0).
- Update [Documentation/](../Documentation/) pages for scheduling/deadlines/DQ to describe the now-real behaviors.

**Acceptance:** contract tests green; docs match implemented behavior.

---

## Phase 1 exit checklist
- [x] 1\.1\ real\ HITL\ approval\ on\ triggerer;\ auto\-approve\ fully\ gone;\ e2e\ proves\ pause/resume
- [x] 1\.2\ pluggable\ transient\ retry\ policy\ \+\ numeric\ backoff;\ config\-driven;\ `fixed`\ fallback\ kept
- [x] 1\.3\ deadline\ config\ from\ Connections;\ tiered\ alerts;\ AVERAGE_RUNTIME\ option;\ callback\ timeout
- [ ] 1.4 `rerun_with_latest_version` wired + rollback doc updated
- [x] 1\.5\ schema/fixtures/contract\-tests/docs\ updated
- [x] both\ test\ lanes\ green\ \(mocked\ unit\ \+\ Docker\ integration/e2e\)
