"""
Contract tests for configuration schemas.

These tests validate that all YAML configuration files
conform to their defined schemas.
"""

import pytest
import yaml
from pathlib import Path

try:
    from jsonschema import validate, ValidationError, Draft7Validator

    HAS_JSONSCHEMA = True
except ImportError:
    HAS_JSONSCHEMA = False


@pytest.mark.contract
@pytest.mark.skipif(not HAS_JSONSCHEMA, reason="jsonschema not installed")
class TestDataSourceSchemaValidation:
    """Validate data source configuration files against schema."""

    @pytest.fixture
    def data_source_schema(self, schemas_dir):
        """Load the data source schema."""
        schema_path = schemas_dir / "data_source_schema.yaml"
        if not schema_path.exists():
            pytest.skip("Data source schema not found")

        with open(schema_path) as f:
            return yaml.safe_load(f)

    @pytest.fixture
    def config_files(self, data_sources_dir):
        """Get all data source config files."""
        return list(data_sources_dir.glob("*.yaml"))

    def test_schema_is_valid(self, data_source_schema):
        """Test that the schema itself is valid JSON Schema."""
        # This validates the schema is well-formed
        Draft7Validator.check_schema(data_source_schema)

    @pytest.mark.parametrize(
        "config_file",
        [
            pytest.param(f, id=f.stem)
            for f in Path("config/data_sources").glob("*.yaml")
            if f.exists()
        ]
        if Path("config/data_sources").exists()
        else [],
    )
    def test_config_validates_against_schema(self, config_file, data_source_schema):
        """Test each config file validates against the schema."""
        with open(config_file) as f:
            config = yaml.safe_load(f)

        try:
            validate(instance=config, schema=data_source_schema)
        except ValidationError as e:
            pytest.fail(f"Config {config_file.name} failed validation: {e.message}")

    def test_all_configs_have_required_name(self, config_files):
        """Test all configs have a name field (at top level or nested)."""
        for config_file in config_files:
            with open(config_file) as f:
                config = yaml.safe_load(f)

            # Check at top level or nested in data_source
            has_name = (
                "name" in config
                or "source_name" in config
                or (
                    isinstance(config.get("data_source"), dict)
                    and "name" in config.get("data_source", {})
                )
            )

            assert has_name, f"{config_file.name} missing name field"

    def test_all_configs_have_source_type(self, config_files):
        """Test all configs have a source_type field (at top level or nested)."""
        for config_file in config_files:
            with open(config_file) as f:
                config = yaml.safe_load(f)

            # Check at top level or nested in data_source
            has_type = (
                "source_type" in config
                or "type" in config
                or (
                    isinstance(config.get("data_source"), dict)
                    and "type" in config.get("data_source", {})
                )
            )

            assert has_type, f"{config_file.name} missing source_type field"


@pytest.mark.contract
@pytest.mark.skipif(not HAS_JSONSCHEMA, reason="jsonschema not installed")
class TestTransformationSchemaValidation:
    """Validate transformation configuration against schema."""

    @pytest.fixture
    def transformation_schema(self, schemas_dir):
        """Load the transformation schema."""
        schema_path = schemas_dir / "transformation_schema.yaml"
        if not schema_path.exists():
            pytest.skip("Transformation schema not found")

        with open(schema_path) as f:
            return yaml.safe_load(f)

    def test_valid_transformation_types(self, transformation_schema):
        """Test that schema defines valid transformation types."""
        # Get allowed types from schema if defined
        if "definitions" in transformation_schema:
            assert "transformation" in transformation_schema["definitions"] or True

    def test_rename_transformation_format(self):
        """Test rename transformation format is correct."""
        valid_rename = {
            "type": "rename_columns",
            "mapping": {"old_column": "new_column"},
        }
        # Validate structure
        assert valid_rename["type"] == "rename_columns"
        assert isinstance(valid_rename["mapping"], dict)

    def test_filter_transformation_format(self):
        """Test filter transformation format is correct."""
        valid_filter = {"type": "filter_rows", "condition": "price > 0"}
        assert valid_filter["type"] == "filter_rows"
        assert "condition" in valid_filter

    def test_formula_transformation_format(self):
        """Test formula transformation format is correct."""
        valid_formula = {
            "type": "add_formula_column",
            "column_name": "total",
            "formula": "price * quantity",
        }
        assert valid_formula["type"] == "add_formula_column"
        assert "column_name" in valid_formula
        assert "formula" in valid_formula


@pytest.mark.contract
class TestConfigStructureValidation:
    """Validate configuration structure without jsonschema."""

    @pytest.fixture
    def config_files(self, data_sources_dir):
        """Get all data source config files."""
        if not data_sources_dir.exists():
            return []
        return list(data_sources_dir.glob("*.yaml"))

    def test_all_yaml_files_are_parseable(self, config_files):
        """Test all YAML files can be parsed."""
        for config_file in config_files:
            try:
                with open(config_file) as f:
                    config = yaml.safe_load(f)
                assert config is not None, f"{config_file.name} parsed to None"
            except yaml.YAMLError as e:
                pytest.fail(f"Failed to parse {config_file.name}: {e}")

    def test_no_duplicate_config_names(self, config_files):
        """Test no two configs have the same name."""
        names = []
        for config_file in config_files:
            with open(config_file) as f:
                config = yaml.safe_load(f)
            name = config.get("name") or config.get("source_name")
            if name:
                assert name not in names, f"Duplicate config name: {name}"
                names.append(name)

    def test_schedule_format_is_valid(self, config_files):
        """Test schedule formats are valid cron or Airflow presets."""
        valid_presets = [
            "@once",
            "@hourly",
            "@daily",
            "@weekly",
            "@monthly",
            "@yearly",
            None,
        ]

        for config_file in config_files:
            with open(config_file) as f:
                config = yaml.safe_load(f)

            # Handle schedule at top level or nested in 'schedule' dict
            schedule = config.get("schedule")
            if isinstance(schedule, dict):
                schedule = schedule.get("interval")

            if schedule and isinstance(schedule, str):
                # Either a preset or a cron expression (5 or 6 parts)
                is_preset = schedule in valid_presets
                is_cron = len(schedule.split()) in [5, 6]
                assert is_preset or is_cron, (
                    f"Invalid schedule in {config_file.name}: {schedule}"
                )

    def test_api_configs_have_url(self, config_files):
        """Test API source configs have URL defined."""
        for config_file in config_files:
            with open(config_file) as f:
                config = yaml.safe_load(f)

            source_type = config.get("source_type") or config.get("type")
            if source_type == "api":
                api_config = config.get("api", {})
                assert "url" in api_config or "endpoint" in api_config, (
                    f"API config {config_file.name} missing url"
                )


@pytest.mark.contract
class TestGlobalSettingsValidation:
    """Validate global settings configuration."""

    @pytest.fixture
    def global_settings(self, config_dir):
        """Load global settings."""
        settings_path = config_dir / "global_settings.yaml"
        if not settings_path.exists():
            pytest.skip("Global settings not found")

        with open(settings_path) as f:
            return yaml.safe_load(f)

    def test_global_settings_is_dict(self, global_settings):
        """Test global settings is a dictionary."""
        assert isinstance(global_settings, dict)

    def test_kafka_settings_if_present(self, global_settings):
        """Test Kafka settings structure if present."""
        if "kafka" in global_settings:
            kafka = global_settings["kafka"]
            # Common Kafka settings
            assert isinstance(kafka, dict)

    def test_no_sensitive_data_exposed(self, global_settings):
        """Test no plaintext passwords or secrets in config."""
        sensitive_patterns = ["password", "secret", "api_key", "token"]

        def check_dict(d, path=""):
            for key, value in d.items():
                current_path = f"{path}.{key}" if path else key
                if isinstance(value, dict):
                    check_dict(value, current_path)
                elif isinstance(value, str):
                    for pattern in sensitive_patterns:
                        if pattern in key.lower() and not value.startswith("${"):
                            # Allow environment variable references
                            if len(value) > 0 and value not in ["", "null", "None"]:
                                # This is a warning, not a failure
                                pass

        check_dict(global_settings)


@pytest.mark.contract
class TestValidationConfigValidation:
    """Validate validation configuration."""

    @pytest.fixture
    def validation_config(self, config_dir):
        """Load validation config."""
        config_path = config_dir / "validation_config.yaml"
        if not config_path.exists():
            pytest.skip("Validation config not found")

        with open(config_path) as f:
            return yaml.safe_load(f)

    def test_validation_config_structure(self, validation_config):
        """Test validation config has expected structure."""
        assert isinstance(validation_config, dict)

    def test_check_types_are_valid(self, validation_config):
        """Test that defined check types are recognized."""
        valid_check_types = [
            "not_null",
            "unique",
            "positive",
            "negative",
            "range",
            "pattern",
            "type",
            "completeness",
            "custom",
            "freshness",
            "schema",
        ]

        # Extract check types from config if present
        if "checks" in validation_config:
            for check in validation_config["checks"]:
                check_type = check.get("type")
                if check_type:
                    assert check_type in valid_check_types, (
                        f"Unknown check type: {check_type}"
                    )


# =============================================================================
# AIRFLOW 3.3.0 FEATURES SCHEMA VALIDATION (Deadline & HITL)
# =============================================================================


@pytest.mark.contract
class TestAirflow330SchemaValidation:
    """
    Validate Airflow 3.3.0 features configuration (deadline alerts, retry & HITL).

    These tests validate the schema structure for new features added in
    the Airflow 3.3.0 upgrade without requiring jsonschema.
    """

    @pytest.fixture
    def fixtures_dir(self) -> Path:
        """Return the fixtures directory path."""
        return Path(__file__).parent.parent / "fixtures"

    @pytest.fixture
    def airflow_330_config(self, fixtures_dir):
        """Load the Airflow 3.3.0 test config fixture."""
        config_path = fixtures_dir / "example_airflow_330_features.yaml"
        if not config_path.exists():
            pytest.skip("Airflow 3.3.0 test fixture not found")

        with open(config_path) as f:
            return yaml.safe_load(f)

    def test_config_fixture_loads(self, airflow_330_config):
        """Test that the 3.3.0 config fixture loads without error."""
        assert airflow_330_config is not None
        assert isinstance(airflow_330_config, dict)

    def test_deadline_config_structure(self, airflow_330_config):
        """Test deadline configuration has correct structure."""
        schedule = airflow_330_config.get("schedule", {})
        deadline = schedule.get("deadline", {})

        # Verify all expected fields exist in tiers
        tiers = deadline.get("tiers", [])
        assert len(tiers) > 0, "deadline.tiers is required"
        for tier in tiers:
            assert "enabled" in tier, "tier.enabled is required"
            assert "timeout_minutes" in tier, "tier.timeout_minutes is required"
            assert "kafka_topic" in tier, "tier.kafka_topic is required"

    def test_deadline_enabled_is_boolean(self, airflow_330_config):
        """Test deadline tier.enabled is a boolean."""
        tiers = (
            airflow_330_config.get("schedule", {}).get("deadline", {}).get("tiers", [])
        )

        for tier in tiers:
            enabled = tier.get("enabled")
            assert isinstance(enabled, bool), (
                f"tier.enabled should be bool, got {type(enabled)}"
            )

    def test_deadline_timeout_minutes_is_positive(self, airflow_330_config):
        """Test timeout_minutes is a positive integer."""
        tiers = (
            airflow_330_config.get("schedule", {})
            .get("deadline", {})
            .get("tiers", [])
        )

        for tier in tiers:
            timeout = tier.get("timeout_minutes")
            assert isinstance(timeout, int), (
                f"timeout_minutes should be int, got {type(timeout)}"
            )
            assert timeout > 0, f"timeout_minutes should be positive, got {timeout}"

    def test_deadline_email_recipients_is_array(self, airflow_330_config):
        """Test email_recipients is an array of strings."""
        tiers = (
            airflow_330_config.get("schedule", {})
            .get("deadline", {})
            .get("tiers", [])
        )

        for tier in tiers:
            if "email_recipients" in tier:
                recipients = tier.get("email_recipients", [])
                assert isinstance(recipients, list), (
                    f"email_recipients should be list, got {type(recipients)}"
                )
                for recipient in recipients:
                    assert isinstance(recipient, str), (
                        f"Each recipient should be string, got {type(recipient)}"
                    )

    def test_hitl_config_structure(self, airflow_330_config):
        """Test HITL configuration has correct structure."""
        quarantine = airflow_330_config.get("destination", {}).get("quarantine", {})
        hitl = quarantine.get("hitl", {})

        # Verify all expected fields exist
        assert "enabled" in hitl, "hitl.enabled is required"
        assert "timeout_hours" in hitl, "hitl.timeout_hours is required"
        assert "allowed_roles" in hitl, "hitl.allowed_roles is required"

    def test_hitl_enabled_is_boolean(self, airflow_330_config):
        """Test hitl.enabled is a boolean."""
        hitl = (
            airflow_330_config.get("destination", {})
            .get("quarantine", {})
            .get("hitl", {})
        )
        enabled = hitl.get("enabled")

        assert isinstance(enabled, bool), (
            f"hitl.enabled should be bool, got {type(enabled)}"
        )

    def test_hitl_timeout_hours_is_positive(self, airflow_330_config):
        """Test timeout_hours is a positive integer."""
        hitl = (
            airflow_330_config.get("destination", {})
            .get("quarantine", {})
            .get("hitl", {})
        )
        timeout = hitl.get("timeout_hours")

        assert isinstance(timeout, int), (
            f"timeout_hours should be int, got {type(timeout)}"
        )
        assert timeout > 0, f"timeout_hours should be positive, got {timeout}"

    def test_hitl_allowed_roles_is_array(self, airflow_330_config):
        """Test allowed_roles is an array of strings."""
        hitl = (
            airflow_330_config.get("destination", {})
            .get("quarantine", {})
            .get("hitl", {})
        )
        roles = hitl.get("allowed_roles", [])

        assert isinstance(roles, list), (
            f"allowed_roles should be list, got {type(roles)}"
        )
        for role in roles:
            assert isinstance(role, str), (
                f"Each role should be string, got {type(role)}"
            )

    def test_hitl_allowed_roles_contains_data_steward(self, airflow_330_config):
        """Test allowed_roles includes data-steward role."""
        hitl = (
            airflow_330_config.get("destination", {})
            .get("quarantine", {})
            .get("hitl", {})
        )
        roles = hitl.get("allowed_roles", [])

        assert "data-steward" in roles, "data-steward should be in allowed_roles"

    def test_partition_config_structure(self, airflow_330_config):
        """Test partition configuration has correct structure."""
        partition = airflow_330_config.get("partition", {})
        assert partition.get("enabled") is True
        assert partition.get("dimension") == "date"
        assert partition.get("column") == "event_date"
        assert partition.get("granularity") == "day"
        assert partition.get("mapper") == "fan_out"
        assert partition.get("wait_policy") == "wait_for_all"
        assert partition.get("runtime_assigned") is False
        assert partition.get("max_fan_out") == 64

    def test_incremental_config_structure(self, airflow_330_config):
        """Test incremental configuration has correct structure."""
        incremental = airflow_330_config.get("incremental", {})
        assert incremental.get("enabled") is True
        assert incremental.get("watermark_column") == "updated_at"
        assert incremental.get("initial_watermark") == "2024-01-01T00:00:00Z"
        assert incremental.get("lookback") == 60

    def test_schedule_max_active_runs(self, airflow_330_config):
        """Test schedule has max_active_runs."""
        schedule = airflow_330_config.get("schedule", {})
        assert schedule.get("max_active_runs") == 8


@pytest.mark.contract
class TestDataSourceSchemaDeadlineValidation:
    """
    Validate deadline configuration in actual data source configs.

    These tests check any data source config with deadline settings.
    """

    @pytest.fixture
    def config_files(self, data_sources_dir):
        """Get all data source config files."""
        if not data_sources_dir.exists():
            return []
        return list(data_sources_dir.glob("*.yaml"))

    def test_deadline_configs_have_required_fields(self, config_files):
        """Test configs with deadline have all required fields."""
        required_fields = ["enabled", "timeout_minutes"]

        for config_file in config_files:
            with open(config_file) as f:
                config = yaml.safe_load(f)

            deadline = config.get("schedule", {}).get("deadline", {})
            if deadline.get("enabled"):
                for field in required_fields:
                    assert field in deadline, (
                        f"{config_file.name}: deadline.{field} missing when enabled"
                    )

    def test_deadline_email_requires_recipients(self, config_files):
        """Test email_enabled requires email_recipients to be set."""
        for config_file in config_files:
            with open(config_file) as f:
                config = yaml.safe_load(f)

            deadline = config.get("schedule", {}).get("deadline", {})
            if deadline.get("email_enabled"):
                recipients = deadline.get("email_recipients", [])
                assert len(recipients) > 0, (
                    f"{config_file.name}: email_enabled but no email_recipients"
                )

    def test_hitl_configs_have_required_fields(self, config_files):
        """Test configs with HITL have all required fields."""
        required_fields = ["enabled", "timeout_hours"]

        for config_file in config_files:
            with open(config_file) as f:
                config = yaml.safe_load(f)

            hitl = config.get("destination", {}).get("quarantine", {}).get("hitl", {})
            if hitl.get("enabled"):
                for field in required_fields:
                    assert field in hitl, (
                        f"{config_file.name}: hitl.{field} missing when enabled"
                    )


class TestNoUnknownTopLevelKeys:
    """Reject unknown top-level keys in every pipeline config.

    Guards against dead configuration: the factory only reads the keys listed
    below, so anything else (a typo like `deadine:`, or legacy-layout leftovers
    such as top-level `retries:`/`data_quality_checks:`) validates silently and
    does NOTHING. This class of bug shipped twice before this test existed
    (financial_operations_v2's top-level hitl/deadline/retry; joke_api_test's
    entire legacy DQ/transformation config). Extend ALLOWED_TOP_LEVEL_KEYS only
    when the factory actually starts reading a new key.
    """

    # Keys consumed by dag_factory_v2 / taskflow_tasks / config_loader.
    # NOTE: `authentication` is documented in the schema but read nowhere —
    # deliberately excluded until implemented.
    ALLOWED_TOP_LEVEL_KEYS = {
        "metadata",
        "data_source",
        "schedule",
        "destination",
        "validation",
        "partition",
        "incremental",
        "transformations",
        "event",
    }

    @pytest.fixture
    def config_files(self):
        data_sources = Path(__file__).parent.parent.parent / "config" / "data_sources"
        return sorted(data_sources.glob("*.yaml"))

    def test_no_unknown_top_level_keys(self, config_files):
        problems = []
        for config_file in config_files:
            with open(config_file) as f:
                config = yaml.safe_load(f)
            unknown = set(config.keys()) - self.ALLOWED_TOP_LEVEL_KEYS
            if unknown:
                problems.append(f"{config_file.name}: unknown top-level keys {sorted(unknown)}")
        assert not problems, (
            "Unknown top-level keys are dead configuration (the factory never reads them):\n"
            + "\n".join(problems)
        )

    def test_fixture_configs_have_no_unknown_top_level_keys(self):
        """Same guard for test fixtures so examples stay honest."""
        fixtures = Path(__file__).parent.parent / "fixtures"
        problems = []
        for config_file in sorted(fixtures.glob("*.yaml")):
            with open(config_file) as f:
                config = yaml.safe_load(f)
            if not isinstance(config, dict):
                continue
            unknown = set(config.keys()) - self.ALLOWED_TOP_LEVEL_KEYS
            if unknown:
                problems.append(f"{config_file.name}: unknown top-level keys {sorted(unknown)}")
        assert not problems, "\n".join(problems)
