# Phase 0 — Compatibility Upgrade to Airflow 3.3.0 (Clean Break)

**Goal:** the framework installs, imports, parses all generated DAGs, and passes both test lanes on `apache-airflow==3.3.0`, with all deprecated/compat code removed. No new *features* are added in this phase (those are Phase 1/2); we only make the codebase correct and clean on 3.3.0.

**Branch:** `upgrade/phase-0` (from `develop`).

**Read first:** [plan/README.md](README.md) standing rules — especially *no assumptions about Airflow APIs* and *two test lanes*.

**Definition of Done (all must hold):**
- `pip install -r requirements.txt` resolves against the official Airflow 3.3.0 constraints file with no conflicts.
- `pytest tests/unit tests/contract tests/dag -q` is green (mocked lane).
- A Docker stack on 3.3.0 comes up healthy, `airflow db migrate` succeeds, all generated DAGs import with **zero import errors**, and `pytest tests/integration tests/e2e` is green (real lane).
- No remaining imports from `airflow.serialization.serde` / `airflow.serialization.serializers`; no `try/except ImportError` compat shims in framework code.
- The downgrade rehearsal in [ROLLBACK_STRATEGY.md](../ROLLBACK_STRATEGY.md) has been run once on a throwaway DB and documented.

---

## Progress Tracker

Status legend in [README.md](README.md#progress-tracking). Update the row + Notes whenever status changes.

| Task | Status | Owner | Notes |
|------|--------|-------|-------|
| 0.1 Resolve target dependency set | ✅ | | requirements.txt + pyproject vs v3-3-test constraints snapshot (renamed constraints.txt); image builds clean |
| 0.2 Migrate serde → `airflow.sdk.serde` | ✅ | | no `airflow.serialization` imports remain; serializer registers in container |
| 0.3 Remove import shims + delete dead tenant import | ✅ | | no try/except ImportError around Airflow imports; direct imports verified in container |
| 0.4 Remove HITL auto-approve stub (defer honestly) | ✅ | | superseded by real HITL (Phase 1.1); no auto-approve path remains |
| 0.5 airflow.cfg / SMTP / deserialization review | ✅ | | cfg rename `num_dag_runs_to_retain_rendered_fields` applied; FAB auth manager consistent cfg/compose; data-steward grants scripted in init (2026-07-23) |
| 0.6 Update unit-test Airflow mock registry | ✅ | | mocks mirror verified import surface; stale models/hooks mocks pruned 2026-07-23 |
| 0.7 Fix / add / remove tests (mocked lane) | ✅ | | 639 passed / 0 failed (2026-07-23); deprecation filters un-blinded |
| 0.8 Docker migrate + integration/e2e + zero import errors | ✅ | | 2026-07-23 validated on live 3.3.0 Docker stack: migrate clean, zero import errors, integration+e2e 36/36 |
| 0.9 Downgrade / rollback rehearsal | ✅ | | 2026-07-23 round-trip rehearsed on throwaway DB; documented in ROLLBACK_STRATEGY.md |
| 0.10 Documentation & comment hygiene | 🟨 | | not systematically re-audited; spot fixes applied |
| 0.11 Bump local Docker stack to 3.3.0 | ✅ | | DHI base image (py3.13); all components report 3.3.0; worker healthcheck fixed to `python -m celery` 2026-07-23 |
| 0.12 Provider compatibility + hook-usage audit | ✅ | | fab 3.7.1 / celery 3.21.0 pinned per constraints; openlineage provider package; Connection access moved to airflow.sdk 2026-07-23 |

> **Scope note (per project decision):** the **local Docker stack** ([Dockerfile](../Dockerfile), [docker/docker-compose.yaml](../docker/docker-compose.yaml)) is in scope for Phase 0 because it is the integration/e2e test lane. **Helm/AKS** ([helm/](../helm/)) and the **Azure DevOps CI pipeline** ([.azure-pipelines/config.yml](../.azure-pipelines/config.yml)) are **deferred to a separate effort** — see "Deferred" at the bottom of this file so the work is not lost.

---

## Task 0.1 — Establish the target dependency set (resolve, don't guess)

**Files:** [requirements.txt](../requirements.txt), [pyproject.toml](../pyproject.toml) (read it first — it may also declare deps/pins).

1. Determine the Python version the Docker image uses (check [Dockerfile](../Dockerfile)); pick the matching constraints file:
   `https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.<minor>.txt`.
2. Bump the three Airflow pins in [requirements.txt](../requirements.txt#L99-L111):
   - `apache-airflow==3.3.0`, `apache-airflow-core==3.3.0`, and `apache-airflow-task-sdk==<version the 3.3.0 constraints file lists>` (do **not** hardcode `1.1.6` or a guessed `1.3.x` — read it from constraints).
3. Re-pin every provider (`microsoft-azure`, `cncf-kubernetes`, `snowflake`, `ftp`, `http`, `ssh`, `apache-kafka`) and `openlineage-airflow` to the versions the 3.3.0 constraints file resolves. If a provider has no compatible release for 3.3.0, **stop and report**.
4. Re-check the aggressive third-party pins against constraints: `pandas==3.0.0`, `numpy==2.4.2`, `cryptography>=45.0.7,<46.0.0`, `pendulum>=3.0.0`, `structlog>=24.1.0`. Adjust only if constraints demand it; note any change.
5. Clean-break dependency hygiene: 3.2.0 removed PyOpenSSL from core deps (#63869). Remove `pyOpenSSL==25.3.0` from [requirements.txt](../requirements.txt#L44) unless a *direct* import of `OpenSSL` exists in the repo (grep to confirm; there is none in framework code).
6. Mirror all version changes into [pyproject.toml](../pyproject.toml) so the packaged distribution and the requirements file agree.

**Note — providers that live in the base image, not `requirements.txt`:** the deployable stack uses `FabAuthManager`, `CeleryExecutor`, and `GitDagBundle`, which need `apache-airflow-providers-fab`, `-celery`, `-git`, and `-redis`. These are **not** pinned in [requirements.txt](../requirements.txt) today (they ship in the base image). Their 3.3.0 compatibility is handled in **Task 0.12** — cross-reference, and do not consider 0.1 done until 0.12 confirms them.

**Acceptance:** `pip install -r requirements.txt -c <constraints-url>` completes cleanly in the Docker build. Capture the resolved versions in the commit message.

---

## Task 0.2 — Migrate serde imports to `airflow.sdk.serde` (clean break)

3.2.0 moved serde to `airflow.sdk.serde`; the old path is deprecated and removed in Airflow 4. Because this is a clean break, switch outright — no `try/except`.

**Files & exact edits:**
1. [rlam_airflow_framework/serializers.py:16](../rlam_airflow_framework/serializers.py#L16) — change `from airflow.serialization.serde import U` → import `U` from `airflow.sdk.serde`. **Verify** `U` still exists at that path in installed 3.3.0 (per standing rule); if the type alias was renamed, use the current name and note it.
2. [config/airflow_local_settings.py:124-133](../config/airflow_local_settings.py#L124-L133) — replace `from airflow.serialization.serde import register` with the `airflow.sdk.serde` equivalent, and **remove the surrounding `try/except`**. Registration failing should fail loudly (fail-fast) so a broken serializer is caught at startup rather than silently disabling DataFrame XCom. Keep the two `print()` breadcrumbs.
3. Grep the whole repo for any other `airflow.serialization` usage and migrate/remove.

**Acceptance:** `grep -rn "airflow.serialization" --include=*.py` returns nothing in framework/config code (test mocks are handled in Task 0.6).

---

## Task 0.3 — Remove defensive import shims and fix the latent tenant import bug

**File:** [rlam_airflow_framework/dag_factory_v2.py](../rlam_airflow_framework/dag_factory_v2.py)

1. **HITL import (lines 33-38):** replace the `try/except ImportError` around `ApprovalOperator` with a direct import. **Verify the real 3.3.0 import path** — in 3.x HITL operators are *not* guaranteed to live at `airflow.operators.hitl`; check the installed package (candidates to verify, not assume: `airflow.providers.standard.operators.hitl`, `airflow.sdk`, etc.). Delete the `HITL_AVAILABLE` flag and all branches that depend on it — Phase 1 implements the real HITL flow, but Phase 0 must leave imports honest (import what exists, at the correct path). If the operator genuinely must be imported lazily for DAG-parse performance, document why; otherwise import at module top.
2. **Latent bug — tenant import (lines 54-58):** the shim imports `from utils.tenant_context import TenantContext, TenantValidationError`, but the module actually lives at `rlam_airflow_framework.tenant_context`. The `except ImportError` currently swallows this and silently sets `TenantContext = None`. **Verified impact:** these two names are referenced *only* in this import block — nowhere in the module body (the factory gets its live tenant context from `config_loader.get_tenant_context()`). So this is **dead, misleading code with no current runtime effect**, not an active bug. **Action (clean break): delete the entire import block.** Repath it to `from rlam_airflow_framework.tenant_context import ...` *only* if you introduce a real type annotation or `except TenantValidationError` that needs the symbol — in which case remove the `try/except` and import directly.
3. **Deadline notifier import (lines 61-64):** make `from plugins.deadline_callbacks import CompositeDeadlineNotifier` direct (no shim). Confirm `plugins/` is on `PYTHONPATH` in the Docker image and in unit tests; if not, adjust the import to the packaged path.
4. Grep the repo for other `try/except ImportError` blocks guarding Airflow symbols and remove them (e.g. any in [taskflow_tasks.py](../rlam_airflow_framework/taskflow_tasks.py), [health_checks.py](../rlam_airflow_framework/health_checks.py)). Leave non-Airflow defensive `try/except` (e.g. filesystem `mkdir`) alone.

**Acceptance:** no `try/except ImportError` remains around Airflow imports; tenant context import path is correct; module imports succeed under real Airflow in Docker.

---

## Task 0.4 — Reconcile the HITL auto-approve stub with the clean break

The current quarantine branch ([dag_factory_v2.py:297-334](../rlam_airflow_framework/dag_factory_v2.py#L297-L334)) logs a warning and **auto-approves** because HITL was never wired. Full HITL is **Phase 1**. For Phase 0, do the minimum that keeps the code honest and green:

- Keep the quarantine → load path working, but **remove the misleading "HITL enabled → fall back to auto-approve" branch and its warning**. Until Phase 1, treat `hitl.enabled: true` as **not yet supported**: raise a clear `NotImplementedError` at DAG-build time (fail-fast) OR ignore the flag with a single explicit `log.info("HITL deferred to Phase 1")` — **choose fail-fast** to match the clean-break principle, unless doing so breaks the existing e2e fixtures. If [example_airflow_316_features.yaml](../tests/fixtures/example_airflow_316_features.yaml) (which sets `hitl.enabled: true`) is exercised by a test that must stay green, set that fixture's `hitl.enabled: false` for Phase 0 and add a `# TODO(phase-1): re-enable` note.
- Do **not** implement real approval here; that is Phase 1's Task 1.1.

**Acceptance:** no code path claims to do HITL while silently auto-approving. Tests updated accordingly (Task 0.7).

---

## Task 0.5 — Configuration & environment review (`airflow.cfg`, SMTP, deserialization)

**File:** [docker/airflow.cfg](../docker/airflow.cfg) and the Docker env.

1. **Deserialization allow-list:** confirmed current values are `allowed_deserialization_classes = airflow.* pandas.* rlam_airflow_framework.*` (glob) and `allowed_deserialization_classes_regexp =` (empty). The 3.2.2 `re.fullmatch` change only affects the *regexp* variant, which is empty → **no change required**, but add a one-line comment recording that this was reviewed for the upgrade.
2. **SMTP STARTTLS cert validation (3.2.2):** [EmailDeadlineNotifier](../plugins/deadline_callbacks.py#L340-L370) calls `send_email`, which now validates the server certificate by default. This cannot be auto-decided by an agent. Add an explicit `email.ssl_context` (or `[smtp]`/`[email]` section, whichever 3.3.0 uses — **verify the section name**) to `airflow.cfg` and **flag for the human**: keep the default validating context for real CA-signed SMTP; set `ssl_context = none` only if the deployment uses a self-signed SMTP server. Document the decision inline.
3. **xcom_backend:** verify `xcom_backend = airflow.sdk.execution_time.xcom.BaseXCom` (line ~348) is still the correct path in 3.3.0; update if the class moved.
4. **New-in-3.3.0 knobs to leave at defaults for now** (documented, not enabled): `[core] rerun_with_latest_version` (Phase 1), `[core] mp_start_method`, task/asset state-store settings (Phase 2). Add commented placeholders with pointers to the phase that turns them on.
5. Scan the full `airflow.cfg` (it is ~3182 lines; read the remainder beyond line 1394) for any option **renamed or removed** between 3.1.6 and 3.3.0 and reconcile. Cross-check against the "Significant Changes" of each release in [airflow-release-notes.txt](../airflow-release-notes.txt).
6. **Reconcile the auth-manager contradiction (target = FAB).** [docker/airflow.cfg:57](../docker/airflow.cfg#L57) sets `SimpleAuthManager`, but the deployable stack ([docker/docker-compose.yaml:11](../docker/docker-compose.yaml#L11)) overrides to `FabAuthManager` and creates a `data-steward` role ([compose init line ~321](../docker/docker-compose.yaml#L321)). The framework's HITL design gates on real roles, so **FAB is the target**: set `auth_manager` to `FabAuthManager` in `airflow.cfg` so config and runtime agree, and keep `SimpleAuthManager` documented only as an optional local-dev override. This requires `apache-airflow-providers-fab` at a 3.3.0-compatible version (Task 0.12).
7. **3.2.1 `/dags` permission change.** With FAB + custom roles, the `/dags` endpoint now additionally requires `DagAccessEntity.RUN` + `TASK_INSTANCE` + `HITL_DETAIL` read (#64822). Ensure the `data-steward` role (and any custom role) is granted these, or it will lose `/dags` access. Capture the role-permission setup as code/CLI in the compose init step, not just "configure via UI".

**Acceptance:** `airflow config list` inside the 3.3.0 container emits no "unknown/removed option" warnings for options this repo sets; `airflow.cfg` and `docker-compose.yaml` agree on the auth manager; the `data-steward` role has the permissions needed for `/dags` under 3.3.0.

---

## Task 0.6 — Update the unit-test Airflow mock registry for 3.3.0

**File:** [tests/unit/conftest.py](../tests/unit/conftest.py)

The unit lane fakes `airflow.*` in `sys.modules`. Clean-break import changes will break it unless updated:

1. Remove the `airflow.serialization` / `airflow.serialization.serialized_objects` mocks (lines 62-63, 100-101) **only if** nothing under test still imports them; add mocks for **`airflow.sdk.serde`** exposing `U` and `register` (needed by the migrated [serializers.py](../rlam_airflow_framework/serializers.py)).
2. Add a mock for the real HITL operator import path chosen in Task 0.3 (e.g. register `airflow.providers.standard.operators.hitl` or whatever the verified path is) so unit imports of [dag_factory_v2.py](../rlam_airflow_framework/dag_factory_v2.py) succeed.
3. Add mocks for `rlam_airflow_framework.tenant_context` symbols if the corrected direct import is evaluated at unit-import time.
4. Keep `mock_qualname` and the `@task`/`@task.branch`/`@task.sensor` pass-throughs.

**Guardrail:** the mock is a maintenance liability. Add a short module docstring listing "every `airflow.*` path this file must mock, and why" so future agents keep it in sync.

**Acceptance:** `pytest tests/unit -q` imports every framework module without `ModuleNotFoundError`.

---

## Task 0.7 — Fix, add, and remove tests

Work through the suite ([tests/](../tests/)) and reconcile with the clean-break changes:

- **Fix:** any unit/contract test that imports the migrated serde path or the old HITL import path.
- **Remove:** tests asserting the **auto-approve fallback** behavior removed in Task 0.4 (search `tests/unit/test_taskflow_tasks.py` and any dag-integrity test referencing `auto_approve_quarantine`).
- **Add:**
  - A unit test asserting `serializers.serialize`/`deserialize` round-trips a DataFrame (this already may exist as [test_serializers.py](../tests/unit/test_serializers.py) / [test_dataframe_storage.py](../tests/unit/test_dataframe_storage.py) — update, don't duplicate).
  - A contract test asserting `hitl.enabled: true` now raises the fail-fast error at DAG build (documents the Phase-0 posture).
  - A dag-integrity test ([tests/dag/test_dag_integrity.py](../tests/dag/test_dag_integrity.py)) that builds every config in [config/data_sources/](../config/data_sources/) and asserts no exceptions — under the mocked lane.
- Keep the timezone/DST tests ([test_dst_transitions.py](../tests/unit/test_dst_transitions.py), [test_schedule_timezone.py](../tests/unit/test_schedule_timezone.py)) green; pendulum behavior is unchanged but re-run to confirm.
- **Un-blind deprecation warnings during the migration.** [pytest.ini:31-34](../pytest.ini#L31-L34) currently sets `ignore::DeprecationWarning:airflow.*` / `ignore::PendingDeprecationWarning`, which hides exactly the signals this phase depends on (deprecated `airflow.serialization` imports, removed-in-4 shims). For the Phase 0 work, **temporarily remove/relax that filter** (or run `pytest -W error::DeprecationWarning` for a single audit pass) so any remaining deprecated Airflow usage surfaces as a failure. Once the clean break is verified, decide deliberately whether to restore a *narrower* ignore (do not blanket-ignore all `airflow.*` deprecations again — that reintroduces the blind spot).

**Acceptance:** `pytest tests/unit tests/contract tests/dag -q` fully green with no skips that hide real breakage; a `-W error::DeprecationWarning` audit pass over the framework imports is clean.

---

## Task 0.8 — Real-Airflow validation in Docker (integration + e2e + migrations)

**Files:** [docker/docker-compose.yaml](../docker/docker-compose.yaml), [docker/docker-compose.bundle-test.yaml](../docker/docker-compose.bundle-test.yaml), [Dockerfile](../Dockerfile).

1. Rebuild the image on the new requirements. Confirm the base image / entrypoint still valid for 3.3.0 (note: 3.2.0 removed the MySQL client from official images — this project uses Postgres/SQLite/Snowflake, so verify no image step assumed MySQL client).
2. Bring the stack up; run `airflow db migrate` and confirm all 3.2.x/3.3.0 Alembic migrations apply cleanly (deadline_alert JSON conversion, `task_state_store` table, partition columns, new indexes). **Back up the metadata DB volume before migrating.**
3. `airflow dags reserialize` and assert **zero import errors** in the UI / `airflow dags list-import-errors`.
4. Run `pytest tests/integration tests/e2e` against the live stack (DB, Kafka, timezone). Fix any real-behavior breaks the mocks hid.
5. Smoke-test one full pipeline end-to-end (the joke-API config is the cheapest: [config/data_sources/joke_api_test.yaml](../config/data_sources/joke_api_test.yaml)) via `airflow dags test`, confirming ingest → transform → DQ → load and a Kafka event emission.
6. Confirm the custom DataFrame XCom serializer registers (look for the `[airflow_local_settings] Registered custom DataFrame serializer` log) and round-trips between two real tasks.

**Acceptance:** healthy stack, zero import errors, integration+e2e green, one real pipeline run succeeds.

---

## Task 0.9 — Downgrade / rollback rehearsal

Per [ROLLBACK_STRATEGY.md](../ROLLBACK_STRATEGY.md), on a **throwaway copy** of a migrated 3.3.0 metadata DB, exercise the documented downgrade path once (several 3.2.x/3.3.0 fixes specifically target MySQL/SQLite downgrade). Record the exact commands and result in `ROLLBACK_STRATEGY.md` (update it if the 3.3.0 reality differs from what it currently claims). If downgrade is not actually supported/needed since nothing is deployed, say so explicitly in the doc rather than leaving stale guidance.

**Acceptance:** ROLLBACK_STRATEGY.md reflects verified 3.3.0 behavior.

---

## Task 0.10 — Documentation & comment hygiene

- Update stale "Airflow 3.1.6" references in docstrings/comments where they now mislead (e.g. [dag_factory_v2.py](../rlam_airflow_framework/dag_factory_v2.py) header, [deadline_callbacks.py](../plugins/deadline_callbacks.py) header, schema comments in [config/schemas/data_source_schema.yaml](../config/schemas/data_source_schema.yaml)) to "Airflow 3.3.0". Do **not** mass-rewrite prose; change only what is now inaccurate.
- Update [requirements.txt](../requirements.txt) header comment and any README badge/version string.

**Acceptance:** no comment claims a version or behavior that Phase 0 changed.

---

## Task 0.11 — Bump the local Docker stack to 3.3.0

**Files:** [Dockerfile](../Dockerfile), [docker/docker-compose.yaml](../docker/docker-compose.yaml), [docker/docker-compose.bundle-test.yaml](../docker/docker-compose.bundle-test.yaml), [docker/.env.example](../docker/.env.example).

1. **Dockerfile — switch the base image to public DHI (interim decision).** The private ACR mirror `rlmsbxdpluksacr.azurecr.io/apache/airflow` **does not yet have a 3.3.0 tag**. Per project decision, use the public **Docker Hardened Image `dhi.io/airflow:3-debian-dev`** for now; the ACR mirror will be populated with the equivalent image later and the base should switch back then.
   - The current Dockerfile hardcodes the registry + `apache/airflow` path + numeric version across **both** stages: `FROM ${ACR_REGISTRY}/apache/airflow:${AIRFLOW_VERSION}` ([Dockerfile:8](../Dockerfile#L8) and [:48](../Dockerfile#L48)). The DHI reference has a different path (`airflow`, not `apache/airflow`) and a **non-numeric tag** (`3-debian-dev`), so parameterize it cleanly: introduce a single `ARG AIRFLOW_BASE_IMAGE=dhi.io/airflow:3-debian-dev` and use `FROM ${AIRFLOW_BASE_IMAGE}` in both stages. Keep the old `ACR_REGISTRY`/`AIRFLOW_VERSION` args only if still needed elsewhere; otherwise remove them to avoid a stale second source of truth. Leave a `# TODO(acr): switch AIRFLOW_BASE_IMAGE back to rlmsbxdpluksacr.azurecr.io/... once the 3.3.0 image is mirrored` comment.
   - **Verify the exact pullable reference** `dhi.io/airflow:3-debian-dev` resolves (registry host, repo path, auth); if the real DHI reference differs, use the correct one and note it.
   - **Floating-tag caveat — reconcile with Task 0.1.** `3-debian-dev` tracks the latest Airflow 3.x, so it may resolve to 3.3.0 *or newer*. After building, run `airflow version` inside the image and **pin the constraints file in Task 0.1 to whatever exact 3.x version it reports** — the two must agree or `pip install -c` will fight the base image. For build reproducibility, prefer pinning by digest (or a specific tag) once known, rather than relying on the moving `3-debian-dev` tag long-term.
   - **DHI image-layout assumptions to verify:** the Dockerfile relies on the `apache/airflow` conventions — the `airflow` user at **uid 50000**, home `/home/airflow/.local`, a Debian base with `update-ca-certificates` and `certifi` (for the ZScaler CA injection, [Dockerfile:15-18](../Dockerfile#L15-L18)/[:62-64](../Dockerfile#L62-L64)), and pip available in the builder stage. Confirm the DHI image preserves these; the `-dev` variant should include the shell/build tooling the builder stage needs. Adjust the cert/user steps if the layout differs.
   - Verify `ARG PYTHON_VERSION=3.12` matches the Python the DHI image ships (used in the `PYTHONPATH` at [Dockerfile:83](../Dockerfile#L83)).
   - **Make the ZScaler CA injection optional via a build ARG (needed inside the internal env, inert outside).** The ZScaler cert is required *inside* the ZScaler-inspected network (it does TLS interception, so pip/Snowflake/Azure/REST calls fail without trusting its CA) but is unnecessary outside. Trusting an extra CA is additive and harmless outside, and the `update-ca-certificates`/`cat >> certifi` commands are local (no network), so this is about hygiene — not baking a corporate MITM root into images that run outside — not about a build break. Gate the trust step behind an ARG in **both** stages ([Dockerfile:17-18](../Dockerfile#L17-L18)/[:63-64](../Dockerfile#L63-L64)):
   ```dockerfile
   ARG USE_ZSCALER_CERT=false
   RUN if [ "$USE_ZSCALER_CERT" = "true" ]; then \
         update-ca-certificates \
         && cat /usr/local/share/ca-certificates/zscaler-ca.crt >> $(python -c "import certifi; print(certifi.where())"); \
       fi
   ```
   Keep the `COPY zscaler-ca.crt ...` unconditional (the file is checked into the repo, so it costs nothing and keeps inside-builds simple). Build with `--build-arg USE_ZSCALER_CERT=true` inside the internal env, and omit it (default `false`) outside. Wire the same ARG through [docker/docker-compose.yaml](../docker/docker-compose.yaml) `build.args` and the Helm image-build path (Helm build is part of the deferred effort — leave a TODO).
   - **Daemon-level trust caveat (documentation, not a Dockerfile change):** the in-image cert only helps processes *inside* the container. When you move inside, pulling the DHI base image *through* ZScaler additionally requires the **Docker daemon/host** to trust the ZScaler CA — that is host config, outside this repo. Note it in the build docs so an inside build doesn't fail at `docker pull` with a confusing TLS error.
   - **Cert freshness for inside builds:** ensure the committed [zscaler-ca.crt](../zscaler-ca.crt) is the current corporate root before inside builds; a stale cert causes TLS failures inside only.
2. **docker-compose.yaml:** the services build from the local image so they inherit the bump, but re-verify per-service correctness on 3.3.0: the `api-server`/`scheduler`/`dag-processor`/`triggerer`/`celery worker` commands, the healthcheck endpoints (e.g. `http://localhost:8080/api/v2/version`, scheduler `:8974/health`), and that `CeleryExecutor` + `FabAuthManager` + Redis/Postgres wiring still hold. Reconcile the auth manager with Task 0.5 (FAB) so compose and `airflow.cfg` agree.
3. Confirm the bundle-test compose still exercises the DAG-bundle path under 3.3.0 (relevant to the provider-example-DAGs-as-bundles change and `_detect_bundle_name`).
4. Bring the stack up and confirm `airflow version` reports 3.3.0 in every component (the init container already prints it, [compose line ~293](../docker/docker-compose.yaml#L293)).

**Acceptance:** `docker compose up` yields a healthy 3.3.0 stack (all components report 3.3.0); this is the substrate Task 0.8 runs against.

---

## Task 0.12 — Provider compatibility & hook-usage audit

**Goal:** confirm every Airflow **provider** the framework and stack rely on has a 3.3.0-compatible version, and that the framework's use of provider APIs still compiles and runs.

1. **Resolve/verify provider versions** against the 3.3.0 constraints file for: `fab`, `celery`, `git`, `redis` (base-image providers from Task 0.1's note) **and** the explicitly-pinned `microsoft-azure`, `cncf-kubernetes`, `snowflake`, `ftp`, `http`, `ssh`, `apache-kafka`. If FAB has no 3.3.0-compatible release, **stop and report** (FAB has historically lagged core — see the 3.1.8 FAB/connexion fix in the notes).
2. **OpenLineage — likely a package swap.** [requirements.txt:125](../requirements.txt#L125) pins `openlineage-airflow==1.41.0`, which is the **Airflow 2** integration. Airflow 3 uses **`apache-airflow-providers-openlineage`**. Verify against installed 3.3.0 and, if confirmed, replace the package and reconcile the Marquez/`openlineage.events` wiring in [docker/docker-compose.yaml:481-575](../docker/docker-compose.yaml#L481-L575) and [docker/marquez.yml](../docker/marquez.yml). Confirm the lineage listener/config path is the provider's, not the legacy plugin's.
3. **Hook-usage audit.** Grep and read the provider-API touch points and confirm signatures/import paths are unchanged on the new provider majors (fix any that moved):
   - `SnowflakeHook` usage in [data_loaders.py](../rlam_airflow_framework/data_loaders.py) (`load_to_snowflake`, `load_to_snowflake_stage`, `call_stored_procedure`) and [data_transformers.py](../rlam_airflow_framework/data_transformers.py) (`enrich_from_snowflake`).
   - SSH/FTP/HTTP fetch in [data_fetchers.py](../rlam_airflow_framework/data_fetchers.py).
   - `ObjectStoragePath` / object-storage load in [data_loaders.py](../rlam_airflow_framework/data_loaders.py) (`load_to_object_storage`).
   - Kafka provider vs the direct `confluent-kafka` client used in [kafka_publisher.py](../rlam_airflow_framework/kafka_publisher.py) / [plugins/deadline_callbacks.py](../plugins/deadline_callbacks.py).
   - The `@task.sensor` Kafka health check in [health_checks.py](../rlam_airflow_framework/health_checks.py) (and `PokeReturnValue` if used).
4. Add any newly-required provider import surfaces to the unit mock registry (Task 0.6).

**Acceptance:** all providers resolve on 3.3.0; OpenLineage runs via the correct package with lineage events reaching Marquez in the Docker lane; no framework module fails on a changed provider API.

---

## Deferred to a separate effort (tracked, not planned in detail here)

Per the project decision, these are **out of scope for Phase 0** but must not be forgotten:
- **Helm/AKS** ([helm/values/production.yaml](../helm/values/production.yaml), [helm/Chart.yaml](../helm/Chart.yaml)): bump image `tag: "3.1.6-custom"` → 3.3.0; **bump the chart version to one that supports Airflow 3.3.0**; and note the Airflow-3 Helm chart **renamed `webserver` → `apiServer`** — the `webserver:` block (lines ~175-189) likely needs rewriting. HA scheduler `replicas: 2` interacts with Phase 1 deadline/rerun behavior.
- **Azure DevOps CI** ([.azure-pipelines/config.yml](../.azure-pipelines/config.yml)): today it runs **only** contract/schema tests. To make "both lanes green" enforceable, add jobs that run `tests/unit` (mocked) and a Docker-based `tests/integration`/`tests/e2e` lane.

Create tickets for both before closing Phase 0.

---

## Phase 0 exit checklist
- [ ] 0.1 deps resolved against 3.3.0 constraints (requirements.txt + pyproject.toml)
- [ ] 0.2 serde migrated to `airflow.sdk.serde`
- [ ] 0.3 import shims removed; tenant import path fixed
- [ ] 0.4 auto-approve stub removed; HITL deferred honestly
- [ ] 0.5 airflow.cfg / SMTP / deserialization reviewed
- [ ] 0.6 unit mock registry updated
- [ ] 0.7 tests fixed/added/removed; mocked lane green
- [ ] 0.8 Docker migrate + integration/e2e green + zero import errors
- [ ] 0.9 rollback rehearsal documented
- [ ] 0.10 comments/docs de-staled
- [ ] 0.11 local Docker stack on 3.3.0 (all components report 3.3.0)
- [ ] 0.12 providers resolved + hook-usage audited + OpenLineage on correct package
- [ ] Deferred Helm & CI tickets created
