"""
CI admission control: catch tenant pool oversubscription at PR time.

Mirrors the math in DAGFactoryV2._compute_max_active_runs /
_plan_partitioning / _check_tenant_admission (rlam_airflow_framework/dag_factory_v2.py),
but runs statically over every pipeline config so a developer sees the
failure in CI output instead of a DAG Processor warning after merge.
"""
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
import yaml

CONFIG_DIR = Path(__file__).parent.parent.parent / "config"
DATA_SOURCES_DIR = CONFIG_DIR / "data_sources"
GLOBAL_SETTINGS_PATH = CONFIG_DIR / "global_settings.yaml"


def declared_concurrency(config: Dict[str, Any]) -> int:
    """Worst-case concurrent pool demand a single pipeline config can declare."""
    partition_config = config.get("partition", {})
    if not partition_config.get("enabled", False):
        return 1

    max_active_runs = config.get("schedule", {}).get("max_active_runs", 1)
    max_fan_out = partition_config.get("max_fan_out", 64)
    return max_active_runs * max_fan_out


def find_oversubscribed_tenants(
    configs: List[Dict[str, Any]],
    tenants_config: Dict[str, Any],
    multiplier: float,
) -> List[str]:
    """Return one problem string per tenant whose aggregate declared
    concurrency exceeds `multiplier`x its pool size."""
    aggregate_by_tenant: Dict[str, int] = {}
    for config in configs:
        tenant_id = config.get("metadata", {}).get("tenant")
        if not tenant_id:
            continue
        aggregate_by_tenant[tenant_id] = (
            aggregate_by_tenant.get(tenant_id, 0) + declared_concurrency(config)
        )

    problems = []
    for tenant_id, aggregate in aggregate_by_tenant.items():
        tenant_pool_slots: Optional[int] = tenants_config.get(tenant_id, {}).get("slots")
        if tenant_pool_slots is None:
            continue
        threshold = tenant_pool_slots * multiplier
        if aggregate > threshold:
            problems.append(
                f"tenant '{tenant_id}': declared concurrency {aggregate} exceeds "
                f"{multiplier}x its pool size ({tenant_pool_slots} slots, "
                f"threshold {threshold})"
            )
    return problems


# --- synthetic cases: prove the check itself works, independent of what
# the repo's configs currently look like ---

def test_declared_concurrency_non_partitioned_is_one():
    assert declared_concurrency({"partition": {"enabled": False}}) == 1
    assert declared_concurrency({}) == 1


def test_declared_concurrency_partitioned_uses_fan_out_and_max_active_runs():
    config = {
        "partition": {"enabled": True, "max_fan_out": 20},
        "schedule": {"max_active_runs": 3},
    }
    assert declared_concurrency(config) == 60


def test_find_oversubscribed_tenants_flags_over_threshold():
    configs = [
        {"metadata": {"tenant": "acme"}, "partition": {"enabled": True, "max_fan_out": 64}},
        {"metadata": {"tenant": "acme"}, "partition": {"enabled": True, "max_fan_out": 64}},
    ]
    tenants_config = {"acme": {"slots": 4}}

    problems = find_oversubscribed_tenants(configs, tenants_config, multiplier=5)
    assert len(problems) == 1
    assert "acme" in problems[0]


def test_find_oversubscribed_tenants_allows_within_threshold():
    configs = [
        {"metadata": {"tenant": "acme"}, "partition": {"enabled": True, "max_fan_out": 4}},
    ]
    tenants_config = {"acme": {"slots": 4}}

    assert find_oversubscribed_tenants(configs, tenants_config, multiplier=5) == []


def test_find_oversubscribed_tenants_skips_tenants_without_declared_slots():
    configs = [
        {"metadata": {"tenant": "unmanaged"}, "partition": {"enabled": True, "max_fan_out": 999}},
    ]
    assert find_oversubscribed_tenants(configs, tenants_config={}, multiplier=5) == []


# --- integration: run the same check over the repo's real configs ---

@pytest.fixture(scope="module")
def global_settings():
    if not GLOBAL_SETTINGS_PATH.exists():
        pytest.skip("global_settings.yaml not found")
    with open(GLOBAL_SETTINGS_PATH) as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def data_source_configs():
    if not DATA_SOURCES_DIR.exists():
        pytest.skip("config/data_sources not found")
    configs = []
    for path in sorted(DATA_SOURCES_DIR.glob("*.yaml")):
        with open(path) as f:
            configs.append(yaml.safe_load(f))
    return configs


def test_repo_tenants_do_not_oversubscribe_their_pools(data_source_configs, global_settings):
    multiplier = global_settings.get("platform", {}).get("max_tenant_oversubscription", 5)
    tenants_config = global_settings.get("tenants", {})

    problems = find_oversubscribed_tenants(data_source_configs, tenants_config, multiplier)

    assert not problems, (
        "Tenant pipelines collectively over-subscribe their pool beyond the "
        f"configured {multiplier}x multiplier (config/global_settings.yaml: "
        "platform.max_tenant_oversubscription). Reduce max_active_runs/"
        "max_fan_out on the offending pipelines, or increase the tenant's "
        "slots:\n" + "\n".join(problems)
    )
