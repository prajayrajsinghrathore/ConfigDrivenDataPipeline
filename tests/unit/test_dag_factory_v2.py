from typing import Any, cast

import pytest
from rlam_airflow_framework.dag_factory_v2 import DAGFactoryV2
from rlam_airflow_framework.tenant_context import TenantContext, TenantValidationError


def test_hitl_enabled_does_not_raise():
    factory = DAGFactoryV2()

    config_dict = {
        "metadata": {"tenant": "test", "rerun_with_latest_version": True},
        "data_source": {"name": "test_src"},
        "schedule": {
            "interval": "@daily",
            "start_date": "2024-01-01",
            "retry": {"policy": "transient", "max_retries": 5},
            "deadline": {"tiers": [{"enabled": True, "timeout_minutes": 30}]},
        },
        "destination": {
            "primary": {"type": "print_logs"},
            "quarantine": {"type": "print_logs", "hitl": {"enabled": True}},
        },
        "validation": {
            "soda_checks": {"checks": [{"type": "row_count", "min": 1}]},
            "quality_gates": {"quarantine_invalid": True},
        },
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
        "schedule": {"interval": "@daily", "start_date": "2024-01-01"},
        "partition": {"enabled": True, "granularity": "day"},
        "destination": {"primary": {"type": "print_logs"}},
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
            "max_active_runs": 3,
        },
        "partition": {"enabled": True, "granularity": "day"},
        "destination": {"primary": {"type": "print_logs"}},
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
            "max_active_runs": 50,
        },
        "partition": {"enabled": True, "granularity": "day"},
        "destination": {"primary": {"type": "print_logs"}},
    }

    dag = factory.create_dag_from_config(config_dict)
    assert dag is not None
    assert cast(Any, dag).partial_kwargs.get("max_active_runs") == 10


def test_partitioned_dag_rejects_incremental():
    factory = DAGFactoryV2()

    config_dict = {
        "metadata": {"tenant": "test"},
        "data_source": {"name": "test_src"},
        "schedule": {"interval": "@daily", "start_date": "2024-01-01"},
        "partition": {"enabled": True},
        "incremental": {"enabled": True},
        "destination": {"primary": {"type": "print_logs"}},
    }

    import pytest

    with pytest.raises(
        ValueError, match="Combining 'partition' and 'incremental' is not supported"
    ):
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
            "max_active_runs": 5,  # Should be ignored for non-partitioned
        },
        "destination": {"primary": {"type": "print_logs"}},
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
    assert factory._resolve_kafka_bootstrap_servers({}, "test") == "tenant-cluster:9092"


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
    from rlam_airflow_framework.factory.task_builder import TaskBuilder
    import rlam_airflow_framework.factory.task_builder as task_builder_module

    monkeypatch.setattr(task_builder_module, "wait_for_kafka_health", None)
    factory = DAGFactoryV2()

    config_dict = {
        "event": {"topic": "some-topic"},
        "destination": {"primary": {"type": "print_logs"}},
    }

    with pytest.raises(RuntimeError, match="Kafka health sensor is unavailable"):
        TaskBuilder.build_task_graph(
            config_dict,
            pool=None,
            inlets=[],
            outlets=[],
            dag_id="x",
            tenant_id="test",
            resolve_kafka_bootstrap_servers_fn=factory._resolve_kafka_bootstrap_servers,
        )


def _factory_with_tenants(tenants):
    factory = DAGFactoryV2()
    factory.tenant_context = TenantContext({"tenants": tenants})
    return factory


def test_validate_pipeline_connections_rejects_cross_tenant_source_connection():
    # A tenant's data_source.connection_id referencing another tenant's
    # registered connection must be caught, same as for destinations -
    # otherwise a pipeline could read from another tenant's source.
    factory = _factory_with_tenants(
        {
            "acme": {},
            "globex": {"connections": {"http": "globex_http_conn"}},
        }
    )
    config = {
        "data_source": {
            "name": "src",
            "type": "http",
            "connection_id": "globex_http_conn",
        }
    }

    with pytest.raises(TenantValidationError):
        factory._validate_pipeline_connections(config, "acme")


def test_validate_pipeline_connections_rejects_cross_tenant_lookup_connection():
    # A snowflake_lookup transformation step's connection_id is just as
    # capable of reaching another tenant's warehouse as a destination is.
    factory = _factory_with_tenants(
        {
            "acme": {},
            "globex": {"connections": {"snowflake": "globex_snowflake_conn"}},
        }
    )
    config = {
        "data_source": {"name": "src"},
        "transformations": [
            {
                "type": "snowflake_lookup",
                "connection_id": "globex_snowflake_conn",
                "table": "SOME.TABLE",
                "join_on": {"id": "id"},
                "select_columns": ["id"],
            }
        ],
    }

    with pytest.raises(TenantValidationError):
        factory._validate_pipeline_connections(config, "acme")


def test_validate_pipeline_connections_allows_own_tenant_source_and_lookup():
    factory = _factory_with_tenants(
        {
            "acme": {
                "connections": {
                    "http": "acme_http_conn",
                    "snowflake": "acme_snowflake_conn",
                }
            },
        }
    )
    config = {
        "data_source": {
            "name": "src",
            "type": "http",
            "connection_id": "acme_http_conn",
        },
        "transformations": [
            {
                "type": "snowflake_lookup",
                "connection_id": "acme_snowflake_conn",
                "table": "SOME.TABLE",
                "join_on": {"id": "id"},
                "select_columns": ["id"],
            }
        ],
    }

    # Should not raise
    factory._validate_pipeline_connections(config, "acme")
