from typing import Any, cast

import pytest

import rlam_airflow_framework.dag_factory_v2 as dag_factory_v2_module
from rlam_airflow_framework.dag_factory_v2 import DAGFactoryV2

def test_hitl_enabled_does_not_raise():
    factory = DAGFactoryV2()
    
    config_dict = {
        "metadata": {"tenant": "test", "rerun_with_latest_version": True},
        "data_source": {"name": "test_src"},
        "schedule": {
            "interval": "@daily", 
            "start_date": "2024-01-01",
            "retry": {
                "policy": "transient",
                "max_retries": 5
            },
            "deadline": {
                "tiers": [
                    {"enabled": True, "timeout_minutes": 30}
                ]
            }
        },
        "destination": {
            "primary": {"type": "print_logs"},
            "quarantine": {
                "type": "print_logs",
                "hitl": {"enabled": True}
            }
        },
        "validation": {
            "soda_checks": {
                "checks": [{"type": "row_count", "min": 1}]
            },
            "quality_gates": {
                "quarantine_invalid": True
            }
        }
    }
    
    # Should not raise any exceptions
    dag = factory.create_dag_from_config(config_dict)
    
    # Verify the DAG was created successfully
    assert dag is not None
    assert dag.dag_id == "test_test_src"
    
    # Verify rerun_with_latest_version
    assert cast(Any, dag).partial_kwargs.get("rerun_with_latest_version") is True
    
    # Verify retry policy in default_args
    assert "retry_policy" in dag.default_args


def test_partitioned_dag_creation_defaults_to_one():
    factory = DAGFactoryV2()

    # max_active_runs NOT provided: defaults to 1 regardless of pool slots,
    # since a tenant's pool is shared across all its pipelines - concurrency
    # beyond 1 must be requested explicitly.
    config_dict = {
        "metadata": {"tenant": "test"},
        "data_source": {"name": "test_src"},
        "schedule": {
            "interval": "@daily",
            "start_date": "2024-01-01"
        },
        "partition": {
            "enabled": True,
            "granularity": "day"
        },
        "destination": {
            "primary": {"type": "print_logs"}
        }
    }

    dag = factory.create_dag_from_config(config_dict)
    assert dag is not None
    assert cast(Any, dag).partial_kwargs.get("max_active_runs") == 1

def test_partitioned_dag_creation_explicit_runs():
    factory = DAGFactoryV2()

    # Explicit max_active_runs provided
    config_dict = {
        "metadata": {"tenant": "test"},
        "data_source": {"name": "test_src"},
        "schedule": {
            "interval": "@daily",
            "start_date": "2024-01-01",
            "max_active_runs": 3
        },
        "partition": {
            "enabled": True,
            "granularity": "day"
        },
        "destination": {
            "primary": {"type": "print_logs"}
        }
    }

    dag = factory.create_dag_from_config(config_dict)
    assert dag is not None
    assert cast(Any, dag).partial_kwargs.get("max_active_runs") == 3

def test_partitioned_dag_explicit_runs_clamped_to_pool_slots():
    factory = DAGFactoryV2()

    # Explicit max_active_runs exceeding the tenant's pool slots (10) is
    # clamped down rather than allowed to over-subscribe the pool.
    config_dict = {
        "metadata": {"tenant": "test"},
        "data_source": {"name": "test_src"},
        "schedule": {
            "interval": "@daily",
            "start_date": "2024-01-01",
            "max_active_runs": 50
        },
        "partition": {
            "enabled": True,
            "granularity": "day"
        },
        "destination": {
            "primary": {"type": "print_logs"}
        }
    }

    dag = factory.create_dag_from_config(config_dict)
    assert dag is not None
    assert cast(Any, dag).partial_kwargs.get("max_active_runs") == 10

def test_partitioned_dag_rejects_incremental():
    factory = DAGFactoryV2()
    
    config_dict = {
        "metadata": {"tenant": "test"},
        "data_source": {"name": "test_src"},
        "schedule": {
            "interval": "@daily", 
            "start_date": "2024-01-01"
        },
        "partition": {
            "enabled": True
        },
        "incremental": {
            "enabled": True
        },
        "destination": {
            "primary": {"type": "print_logs"}
        }
    }
    
    import pytest
    with pytest.raises(ValueError, match="Combining 'partition' and 'incremental' is not supported"):
        factory.create_dag_from_config(config_dict)


def test_non_partitioned_dag_max_active_runs_is_always_1():
    factory = DAGFactoryV2()

    # Attempting to set max_active_runs on non-partitioned DAG
    config_dict = {
        "metadata": {"tenant": "test"},
        "data_source": {"name": "test_src"},
        "schedule": {
            "interval": "@daily",
            "start_date": "2024-01-01",
            "max_active_runs": 5  # Should be ignored for non-partitioned
        },
        "destination": {
            "primary": {"type": "print_logs"}
        }
    }

    dag = factory.create_dag_from_config(config_dict)
    assert dag is not None
    assert cast(Any, dag).partial_kwargs.get("max_active_runs") == 1


def test_resolve_kafka_bootstrap_servers_defaults_to_none():
    # Today every tenant shares the one global Kafka cluster: no override
    # anywhere means "use health_checks' own DEFAULT_KAFKA_BOOTSTRAP_SERVERS".
    factory = DAGFactoryV2()
    assert factory._resolve_kafka_bootstrap_servers({}, "test") is None


def test_resolve_kafka_bootstrap_servers_tenant_override():
    factory = DAGFactoryV2()
    factory.global_settings["tenants"]["test"]["kafka"] = {
        "bootstrap_servers": "tenant-cluster:9092"
    }
    assert (
        factory._resolve_kafka_bootstrap_servers({}, "test")
        == "tenant-cluster:9092"
    )


def test_resolve_kafka_bootstrap_servers_explicit_event_config_wins():
    factory = DAGFactoryV2()
    factory.global_settings["tenants"]["test"]["kafka"] = {
        "bootstrap_servers": "tenant-cluster:9092"
    }
    event_config = {"topic": "t", "bootstrap_servers": "pipeline-specific:9092"}
    assert (
        factory._resolve_kafka_bootstrap_servers(event_config, "test")
        == "pipeline-specific:9092"
    )


def test_build_task_graph_raises_clear_error_when_kafka_sensor_unavailable(monkeypatch):
    # Simulate health_checks.wait_for_kafka_health being None (e.g. Airflow's
    # task/PokeReturnValue surface unavailable) - a pipeline that declares
    # event.topic should fail with a clear error, not an AttributeError deep
    # inside .override().
    monkeypatch.setattr(dag_factory_v2_module, "wait_for_kafka_health", None)
    factory = DAGFactoryV2()

    config_dict = {
        "event": {"topic": "some-topic"},
        "destination": {"primary": {"type": "print_logs"}},
    }

    with pytest.raises(RuntimeError, match="Kafka health sensor is unavailable"):
        factory._build_task_graph(
            config_dict, pool=None, inlets=[], outlets=[], dag_id="x", tenant_id="test"
        )
