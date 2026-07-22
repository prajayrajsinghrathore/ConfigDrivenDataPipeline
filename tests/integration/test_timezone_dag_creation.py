# File: tests/integration/test_timezone_dag_creation.py
"""
Integration tests for end-to-end timezone-aware DAG creation.

Tests cover:
- DAGs created with explicit timezone have correct start_date.tzinfo
- DAGs use global default when pipeline config omits timezone
- Multiple DAGs with different timezones coexist correctly
- Non-UTC timezone triggers INFO log warning
- Logging confirms timezone source (config vs global default)
"""

import pendulum
from unittest.mock import patch
from rlam_airflow_framework.dag_factory_v2 import DAGFactoryV2


class TestDAGCreationWithExplicitTimezone:
    """Test DAG creation when pipeline config specifies timezone."""
    
    def test_dag_with_utc_timezone(self):
        """Create DAG with UTC timezone."""
        factory = DAGFactoryV2()
        
        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {
                "name": "test_utc",
                "type": "rest_api",
                "endpoint": "https://api.example.com/data"
            },
            "destination": {
                "primary": {"type": "print_logs"}
            },
            "schedule": {
                "interval": "@daily",
                "start_date": "2024-01-01",
                "timezone": "UTC",
                "catchup": False
            }
        }
        
        dag = factory.create_dag_from_config(config)
        
        # Verify DAG was created
        assert dag is not None
        assert dag.dag_id == "shared_services_test_utc"
        
        # Verify start_date is timezone-aware and DAG has correct timezone
        assert isinstance(dag.start_date, pendulum.DateTime)
        assert str(dag.timezone) == "UTC"
    
    def test_dag_with_america_new_york_timezone(self):
        """Create DAG with America/New_York timezone."""
        factory = DAGFactoryV2()
        
        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {
                "name": "test_ny",
                "type": "rest_api",
                "endpoint": "https://api.example.com/data"
            },
            "destination": {
                "primary": {"type": "print_logs"}
            },
            "schedule": {
                "interval": "@daily",
                "start_date": "2024-01-01",
                "timezone": "America/New_York",
                "catchup": False
            }
        }
        
        dag = factory.create_dag_from_config(config)
        
        # Verify timezone
        assert str(dag.timezone) == "America/New_York"
    
    def test_dag_with_europe_london_timezone(self):
        """Create DAG with Europe/London timezone."""
        factory = DAGFactoryV2()
        
        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {
                "name": "test_london",
                "type": "rest_api",
                "endpoint": "https://api.example.com/data"
            },
            "destination": {
                "primary": {"type": "print_logs"}
            },
            "schedule": {
                "interval": "@daily",
                "start_date": "2024-01-01",
                "timezone": "Europe/London",
                "catchup": False
            }
        }
        
        dag = factory.create_dag_from_config(config)
        
        # Verify timezone
        assert str(dag.timezone) == "Europe/London"
    
    def test_dag_with_asia_tokyo_timezone(self):
        """Create DAG with Asia/Tokyo timezone (no DST)."""
        factory = DAGFactoryV2()
        
        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {
                "name": "test_tokyo",
                "type": "rest_api",
                "endpoint": "https://api.example.com/data"
            },
            "destination": {
                "primary": {"type": "print_logs"}
            },
            "schedule": {
                "interval": "@daily",
                "start_date": "2024-01-01",
                "timezone": "Asia/Tokyo",
                "catchup": False
            }
        }
        
        dag = factory.create_dag_from_config(config)
        
        # Verify timezone
        assert str(dag.timezone) == "Asia/Tokyo"


class TestDAGCreationWithGlobalDefaultTimezone:
    """Test DAG creation when pipeline config omits timezone (uses global default)."""
    
    def test_dag_uses_global_default_timezone(self):
        """DAG should use global default timezone when not specified."""
        factory = DAGFactoryV2()
        factory.global_settings = {
            "default_settings": {
                "default_timezone": "America/Chicago"
            }
        }
        
        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {
                "name": "test_no_tz",
                "type": "rest_api",
                "endpoint": "https://api.example.com/data"
            },
            "destination": {
                "primary": {"type": "print_logs"}
            },
            "schedule": {
                "interval": "@daily",
                "start_date": "2024-01-01",
                # No timezone specified
                "catchup": False
            }
        }
        
        dag = factory.create_dag_from_config(config)
        
        # Verify DAG uses global default
        assert str(dag.timezone) == "America/Chicago"
    
    @patch('rlam_airflow_framework.dag_factory_v2.log')
    def test_logs_global_default_timezone_usage(self, mock_log):
        """Should log when using global default timezone."""
        factory = DAGFactoryV2()
        factory.global_settings = {
            "default_settings": {
                "default_timezone": "UTC"
            }
        }
        
        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {
                "name": "test_log_tz",
                "type": "rest_api",
                "endpoint": "https://api.example.com/data"
            },
            "destination": {
                "primary": {"type": "print_logs"}
            },
            "schedule": {
                "interval": "@daily",
                "start_date": "2024-01-01",
                # No timezone specified
                "catchup": False
            }
        }
        
        _dag = factory.create_dag_from_config(config)
        
        # Verify log message about global default
        global_default_logs = [
            call for call in mock_log.info.call_args_list
            if "global default timezone" in str(call).lower()
        ]
        assert len(global_default_logs) > 0


class TestMultipleDAGsWithDifferentTimezones:
    """Test that multiple DAGs with different timezones coexist correctly."""
    
    def test_create_multiple_dags_different_timezones(self):
        """Create multiple DAGs with different timezones."""
        factory = DAGFactoryV2()
        
        # DAG 1: UTC
        config1 = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {
                "name": "dag1_utc",
                "type": "rest_api",
                "endpoint": "https://api.example.com/data"
            },
            "destination": {"primary": {"type": "print_logs"}},
            "schedule": {
                "interval": "@daily",
                "start_date": "2024-01-01",
                "timezone": "UTC",
                "catchup": False
            }
        }
        
        # DAG 2: New York
        config2 = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {
                "name": "dag2_ny",
                "type": "rest_api",
                "endpoint": "https://api.example.com/data"
            },
            "destination": {"primary": {"type": "print_logs"}},
            "schedule": {
                "interval": "@daily",
                "start_date": "2024-01-01",
                "timezone": "America/New_York",
                "catchup": False
            }
        }
        
        # DAG 3: Tokyo
        config3 = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {
                "name": "dag3_tokyo",
                "type": "rest_api",
                "endpoint": "https://api.example.com/data"
            },
            "destination": {"primary": {"type": "print_logs"}},
            "schedule": {
                "interval": "@daily",
                "start_date": "2024-01-01",
                "timezone": "Asia/Tokyo",
                "catchup": False
            }
        }
        
        dag1 = factory.create_dag_from_config(config1)
        dag2 = factory.create_dag_from_config(config2)
        dag3 = factory.create_dag_from_config(config3)
        
        # Verify each DAG has correct timezone
        assert str(dag1.timezone) == "UTC"
        assert str(dag2.timezone) == "America/New_York"
        assert str(dag3.timezone) == "Asia/Tokyo"
        
        # Verify different DAG IDs
        assert dag1.dag_id != dag2.dag_id
        assert dag2.dag_id != dag3.dag_id


class TestNonUTCTimezoneLogging:
    """Test INFO logging for non-UTC timezones."""
    
    @patch('rlam_airflow_framework.dag_factory_v2.log')
    def test_non_utc_timezone_triggers_warning(self, mock_log):
        """Non-UTC timezone should trigger DST warning log."""
        factory = DAGFactoryV2()
        
        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {
                "name": "test_dst_warning",
                "type": "rest_api",
                "endpoint": "https://api.example.com/data"
            },
            "destination": {"primary": {"type": "print_logs"}},
            "schedule": {
                "interval": "@daily",
                "start_date": "2024-01-01",
                "timezone": "America/New_York",
                "catchup": False
            }
        }
        
        _dag = factory.create_dag_from_config(config)
        
        # Verify DST warning was logged
        dst_warnings = [
            call for call in mock_log.info.call_args_list
            if "NON-UTC TIMEZONE DETECTED" in str(call)
        ]
        assert len(dst_warnings) > 0
    
    @patch('rlam_airflow_framework.dag_factory_v2.log')
    def test_utc_timezone_no_dst_warning(self, mock_log):
        """UTC timezone should not trigger DST warning."""
        factory = DAGFactoryV2()
        
        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {
                "name": "test_no_warning",
                "type": "rest_api",
                "endpoint": "https://api.example.com/data"
            },
            "destination": {"primary": {"type": "print_logs"}},
            "schedule": {
                "interval": "@daily",
                "start_date": "2024-01-01",
                "timezone": "UTC",
                "catchup": False
            }
        }
        
        _dag = factory.create_dag_from_config(config)
        
        # Verify no DST warning for UTC
        dst_warnings = [
            call for call in mock_log.info.call_args_list
            if "NON-UTC TIMEZONE DETECTED" in str(call)
        ]
        assert len(dst_warnings) == 0


class TestEndDateSupport:
    """Test end_date field support."""
    
    def test_dag_with_end_date(self):
        """Create DAG with end_date."""
        factory = DAGFactoryV2()
        
        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {
                "name": "test_end_date",
                "type": "rest_api",
                "endpoint": "https://api.example.com/data"
            },
            "destination": {"primary": {"type": "print_logs"}},
            "schedule": {
                "interval": "@daily",
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
                "timezone": "UTC",
                "catchup": False
            }
        }
        
        dag = factory.create_dag_from_config(config)
        
        # Verify end_date is set and timezone-aware
        assert dag.end_date is not None
        assert isinstance(dag.end_date, pendulum.DateTime)
        assert dag.end_date.timezone_name == "UTC"
        assert dag.end_date.year == 2024
        assert dag.end_date.month == 12
        assert dag.end_date.day == 31
    
    def test_dag_without_end_date(self):
        """Create DAG without end_date (should be None)."""
        factory = DAGFactoryV2()
        
        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {
                "name": "test_no_end",
                "type": "rest_api",
                "endpoint": "https://api.example.com/data"
            },
            "destination": {"primary": {"type": "print_logs"}},
            "schedule": {
                "interval": "@daily",
                "start_date": "2024-01-01",
                "timezone": "UTC",
                "catchup": False
            }
        }
        
        dag = factory.create_dag_from_config(config)
        
        # end_date should be None
        assert dag.end_date is None


class TestTimezoneSourceLogging:
    """Test logging confirms timezone source (config vs global default)."""
    
    @patch('rlam_airflow_framework.dag_factory_v2.log')
    def test_logs_pipeline_config_timezone_source(self, mock_log):
        """Should log when using pipeline-specific timezone."""
        factory = DAGFactoryV2()
        
        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {
                "name": "test_source_log",
                "type": "rest_api",
                "endpoint": "https://api.example.com/data"
            },
            "destination": {"primary": {"type": "print_logs"}},
            "schedule": {
                "interval": "@daily",
                "start_date": "2024-01-01",
                "timezone": "Europe/Paris",
                "catchup": False
            }
        }
        
        _dag = factory.create_dag_from_config(config)
        
        # Verify log shows pipeline config as source
        pipeline_tz_logs = [
            call for call in mock_log.info.call_args_list
            if "pipeline-specific timezone" in str(call).lower()
        ]
        assert len(pipeline_tz_logs) > 0
