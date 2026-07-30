# File: tests/e2e/test_probe_scenarios.py
"""
Automated probe scenarios — repeatable versions of the 3B.3/3B.4 validations
that were first performed manually on 2026-07-23.

These are SLOW (minutes: real deadline windows, DAG runs, reparses) and mutate
stack state (Variables, DAG runs, a helper container), so they are opt-in:

    RUN_PROBE_E2E=1 pytest tests/e2e/test_probe_scenarios.py -q

Requirements:
- the docker compose stack up and healthy (apiserver on localhost:8080,
  kafka on localhost:9092 host / kafka:29092 in-network, postgres)
- docker CLI available on the host
- probe DAGs parsed: sandbox_deadline_probe, sandbox_partition_probe,
  sandbox_incremental_probe (config/data_sources/*_probe.yaml)

Covered:
- deadline miss -> exactly one deadline_missed event on pipeline-alerts
- partition fan-out (2 keys -> 2 scoped runs) + clearPartitions of a single key
- incremental two-run delta with watermark advance (and clean numeric values)
"""

import json
import os
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

import pytest
import requests

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        os.environ.get("RUN_PROBE_E2E") != "1",
        reason="probe scenarios are opt-in: set RUN_PROBE_E2E=1 (slow, mutates stack state)",
    ),
]

AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080")
API = f"{AIRFLOW_BASE_URL}/api/v2"
USERNAME = os.environ.get("_AIRFLOW_WWW_USER_USERNAME", "airflow")
PASSWORD = os.environ.get("_AIRFLOW_WWW_USER_PASSWORD", "airflow")
COMPOSE_NETWORK = os.environ.get("COMPOSE_NETWORK", "docker_default")


def _docker(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=120
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} failed: {result.stderr[:500]}")
    return result.stdout


def _scheduler(*airflow_args: str) -> str:
    # check=False: e.g. `variables delete` of a missing key exits non-zero, which is fine
    return _docker("exec", "airflow-scheduler", "airflow", *airflow_args, check=False)


def _kafka_topic_lines(topic: str) -> list:
    out = _docker(
        "exec",
        "kafka",
        "sh",
        "-c",
        f"/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 "
        f"--topic {topic} --from-beginning --timeout-ms 5000 2>/dev/null",
        check=False,
    )
    return [line for line in out.splitlines() if line.strip().startswith("{")]


@pytest.fixture(scope="module")
def headers():
    response = requests.post(
        f"{AIRFLOW_BASE_URL}/auth/token",
        json={"username": USERNAME, "password": PASSWORD},
        timeout=10,
    )
    if response.status_code not in (200, 201):
        pytest.skip(
            f"cannot obtain JWT (HTTP {response.status_code}) — is the stack up?"
        )
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


def _unpause(dag_id, headers):
    requests.patch(
        f"{API}/dags/{dag_id}", json={"is_paused": False}, headers=headers, timeout=10
    )


def _pause(dag_id, headers):
    requests.patch(
        f"{API}/dags/{dag_id}", json={"is_paused": True}, headers=headers, timeout=10
    )


def _trigger(dag_id, headers, run_id, partition_key=None):
    body = {"dag_run_id": run_id, "logical_date": None}
    if partition_key is not None:
        body["partition_key"] = partition_key
    response = requests.post(
        f"{API}/dags/{dag_id}/dagRuns", json=body, headers=headers, timeout=10
    )
    assert response.status_code == 200, (
        f"trigger failed: {response.status_code} {response.text[:200]}"
    )
    return response.json()


def _wait_run_state(dag_id, run_id, headers, want=("success", "failed"), timeout=420):
    deadline = time.time() + timeout
    while time.time() < deadline:
        response = requests.get(
            f"{API}/dags/{dag_id}/dagRuns/{run_id}", headers=headers, timeout=10
        )
        if response.status_code == 200:
            state = response.json().get("state")
            if state in want:
                return state
        time.sleep(10)
    pytest.fail(f"{dag_id}/{run_id} did not reach {want} within {timeout}s")


@pytest.fixture(scope="module")
def mock_api():
    """Start the controllable mock API (scripts/mock_api.py) on the compose network."""
    name = f"mock-api-{uuid.uuid4().hex[:8]}"
    srv_dir = Path(tempfile.mkdtemp(prefix="probe_mockapi_"))
    script = Path(__file__).resolve().parents[2] / "scripts" / "mock_api.py"
    (srv_dir / "mock_api.py").write_text(script.read_text())
    (srv_dir / "data.json").write_text(
        json.dumps([{"id": i, "v": chr(96 + i)} for i in range(1, 6)])
    )
    _docker(
        "run",
        "-d",
        "--name",
        name,
        "--network",
        COMPOSE_NETWORK,
        "--network-alias",
        "mock-api",
        "-v",
        f"{srv_dir}:/srv",
        "python:3.13-alpine",
        "python",
        "/srv/mock_api.py",
    )
    try:
        # readiness: reachable from inside the stack
        for _ in range(20):
            code = subprocess.run(
                [
                    "docker",
                    "exec",
                    "airflow-scheduler",
                    "python",
                    "-c",
                    "import urllib.request; urllib.request.urlopen('http://mock-api:8000/items?id=0', timeout=3); print('ok')",
                ],
                capture_output=True,
                text=True,
            )
            if "ok" in code.stdout:
                break
            time.sleep(2)
        else:
            pytest.fail("mock-api not reachable from the stack")
        yield srv_dir
    finally:
        _docker("rm", "-f", name, check=False)


class TestDeadlineMissProbe:
    def test_deadline_miss_emits_exactly_one_event(self, headers):
        dag_id = "sandbox_deadline_probe"
        before = sum(
            1 for line in _kafka_topic_lines("pipeline-alerts") if dag_id in line
        )

        _unpause(dag_id, headers)
        try:
            run_id = f"probe_deadline_{int(time.time())}"
            _trigger(dag_id, headers, run_id)

            # deadline fires 1 minute after queued_at; allow scheduler cadence + callback
            deadline = time.time() + 360
            after = before
            while time.time() < deadline:
                after = sum(
                    1
                    for line in _kafka_topic_lines("pipeline-alerts")
                    if dag_id in line
                )
                if after > before:
                    break
                time.sleep(15)

            assert after == before + 1, (
                f"expected exactly one new deadline event, got {after - before}"
            )
        finally:
            _pause(dag_id, headers)


class TestPartitionProbe:
    def test_fan_out_and_single_partition_reprocessing(self, headers, mock_api):
        dag_id = "sandbox_partition_probe"
        _unpause(dag_id, headers)
        try:
            stamp = int(time.time())
            keys = ["2026-07-20", "2026-07-21"]
            run_ids = {}
            for key in keys:
                run_id = f"probe_partition_{key}_{stamp}"
                data = _trigger(dag_id, headers, run_id, partition_key=key)
                assert data.get("partition_key") == key
                run_ids[key] = run_id

            for key in keys:
                assert _wait_run_state(dag_id, run_ids[key], headers) == "success"

            # untouched run's end_date snapshot
            keep = requests.get(
                f"{API}/dags/{dag_id}/dagRuns/{run_ids[keys[1]]}",
                headers=headers,
                timeout=10,
            ).json()

            # clear ONLY the first partition
            response = requests.post(
                f"{API}/dags/{dag_id}/clearPartitions",
                json={
                    "partition_key": keys[0],
                    "clear_task_instances": True,
                    "dry_run": False,
                },
                headers=headers,
                timeout=30,
            )
            assert response.status_code == 200, response.text[:200]
            assert response.json()["dag_runs_cleared"] == 1

            # cleared run re-executes to success; sibling untouched
            assert _wait_run_state(dag_id, run_ids[keys[0]], headers) == "success"
            keep_after = requests.get(
                f"{API}/dags/{dag_id}/dagRuns/{run_ids[keys[1]]}",
                headers=headers,
                timeout=10,
            ).json()
            assert keep_after["end_date"] == keep["end_date"], (
                "sibling partition was touched"
            )
        finally:
            _pause(dag_id, headers)


class TestIncrementalProbe:
    def test_two_run_delta_and_watermark_advance(self, headers, mock_api):
        dag_id = "sandbox_incremental_probe"
        # reset watermark state
        _scheduler("variables", "delete", f"{dag_id}.high_watermark")
        _scheduler("variables", "delete", f"{dag_id}.first_run_completed")
        _unpause(dag_id, headers)
        try:
            stamp = int(time.time())
            run1 = f"probe_incremental_1_{stamp}"
            _trigger(dag_id, headers, run1)
            assert _wait_run_state(dag_id, run1, headers) == "success"
            wm1 = (
                _scheduler("variables", "get", f"{dag_id}.high_watermark")
                .strip()
                .splitlines()[-1]
            )
            assert wm1 == "5", f"run1 watermark should be clean numeric 5, got {wm1!r}"

            # append the delta
            (mock_api / "data.json").write_text(
                json.dumps([{"id": i, "v": chr(96 + i)} for i in range(1, 9)])
            )
            run2 = f"probe_incremental_2_{stamp}"
            _trigger(dag_id, headers, run2)
            assert _wait_run_state(dag_id, run2, headers) == "success"
            wm2 = (
                _scheduler("variables", "get", f"{dag_id}.high_watermark")
                .strip()
                .splitlines()[-1]
            )
            assert wm2 == "8", f"run2 watermark should advance to 8, got {wm2!r}"
        finally:
            _pause(dag_id, headers)
            _scheduler("variables", "delete", f"{dag_id}.high_watermark")
            _scheduler("variables", "delete", f"{dag_id}.first_run_completed")
