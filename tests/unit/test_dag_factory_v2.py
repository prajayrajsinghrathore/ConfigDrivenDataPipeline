from typing import Any, cast

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


def test_partitioned_dag_creation_uses_pool_slots():
    factory = DAGFactoryV2()
    
    # max_active_runs NOT provided, should be derived from pool slots (10 // 2 = 5)
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
    assert cast(Any, dag).partial_kwargs.get("max_active_runs") == 5

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
