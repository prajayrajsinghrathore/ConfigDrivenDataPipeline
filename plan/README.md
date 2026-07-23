# Airflow 3.1.6 → 3.3.0 Upgrade — Execution Plans

These plans operationalize [upgrade-report.md](../upgrade-report.md) into agent-executable work. Read the report first for *why*; these files are the *how*.

| Phase | File | Goal | Depends on |
|-------|------|------|-----------|
| 0 | [phase-0-compatibility-upgrade.md](phase-0-compatibility-upgrade.md) | Run cleanly on 3.3.0; all existing tests green | — |
| 1 | [phase-1-feature-adoption.md](phase-1-feature-adoption.md) | Real HITL, retry policies, deadline-via-Connection, rerun-version control | Phase 0 merged |
| 2 | [phase-2-strategic-partitioning-statestore.md](phase-2-strategic-partitioning-statestore.md) | Asset Partitioning + Task/Asset State Store, fully implemented | Phase 1 merged |
| 3 | [phase-3-remediation-validation-infra.md](phase-3-remediation-validation-infra.md) | Audit remediation → first real-environment (Docker) validation → deferred Helm/CI; gated roadmap items | Phases 0–2 code-complete |

## Progress tracking

Each phase file opens with a **Progress Tracker** table — one row per task. Keep it current as the single source of truth for phase status. Status legend (used across all phases):

| Status | Meaning |
|--------|---------|
| ⬜ Not started | No work begun |
| 🟨 In progress | Actively being worked |
| 🟦 In review | Code done, under review / tests running |
| ✅ Done | Merged to the phase branch, both applicable test lanes green |
| 🟥 Blocked | Cannot proceed — see Notes (record the blocker + who/what is needed) |
| ⏭️ Skipped | Deliberately not done — Notes must justify why |

When you change a task's status, update its row's **Notes** with a one-line what/why (and a commit SHA or PR link when relevant). A phase's exit checklist at the bottom of its file is the final gate; the tracker table is the running state.

## Standing rules for every agent working these plans

1. **No assumptions about Airflow APIs.** This project is not deployed and the upgrade is intentionally *not* backward compatible, so there is no runtime to fall back on. Before using any 3.3.0 symbol (import path, class, kwarg, config key), **verify it against the actually-installed `apache-airflow==3.3.0`** — e.g. `python -c "import airflow.sdk; print(dir(airflow.sdk))"`, grep the installed package under `site-packages/airflow/`, or read the official 3.3.0 docs. If a symbol named in a plan does not exist under that exact path in 3.3.0, **stop and report** — do not guess an alternative and proceed silently.
2. **Two test lanes** (per project decision):
   - **Unit + contract** (`tests/unit`, `tests/contract`, `tests/dag`) run with the Airflow modules **mocked** ([tests/unit/conftest.py](../tests/unit/conftest.py)). Every time you add a *new* `airflow.*` import to framework code, you MUST update the mock registry in that conftest or the unit suite will break on import.
   - **Integration + e2e** (`tests/integration`, `tests/e2e`) run against a **real Airflow 3.3.0 in Docker** ([docker/docker-compose.yaml](../docker/docker-compose.yaml)). This is the only lane that validates real 3.3.0 behavior.
3. **Clean break.** Remove compatibility shims (`try/except ImportError` fallbacks, deprecated import paths, the auto-approve HITL stub) rather than preserving them. Prefer fail-fast over silent fallback.
4. **Tests are a deliverable, not an afterthought.** For each change: keep passing tests passing, add tests for new behavior, and **delete tests that assert removed behavior** (e.g. the auto-approve fallback). A phase is not done until both lanes are green.
5. **Work on a branch, commit per task, do not push or open PRs unless asked.** Current branch is `develop`; branch from it per phase (e.g. `upgrade/phase-0`).
6. **If a plan step is blocked or wrong, report back** with the specific finding rather than improvising scope.
