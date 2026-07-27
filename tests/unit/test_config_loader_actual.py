from rlam_airflow_framework.config.reader import load_yaml_file
from rlam_airflow_framework.config.validator import ConfigValidator
# File: tests/unit/test_config_loader_actual.py
"""
Unit tests for the actual ConfigLoader implementation.

Tests cover:
- __init__() initialization
- _load_validation_config() with and without file
- _load_schema_files() loading from schemas directory
- load_data_source_configs() loading all configs
- _load_config_file() loading single YAML
- _validate_config() validation logic
- load_global_settings() with defaults fallback
- Edge cases: missing files, invalid YAML, Unicode
"""

import pytest
import os
from unittest.mock import patch
import yaml

# Import actual implementation
from rlam_airflow_framework.config import ConfigLoader, ConfigLoadError


class TestConfigLoaderInit:
    """Test ConfigLoader initialization."""

    def test_init_with_default_path(self):
        """Test initialization with default config directory."""
        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader()
                assert loader.config_dir == "/opt/airflow/config"

    def test_init_with_custom_path(self, tmp_path):
        """Test initialization with custom config directory."""
        config_dir = str(tmp_path / "config")
        os.makedirs(config_dir, exist_ok=True)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=config_dir)
                assert loader.config_dir == config_dir

    def test_init_sets_schemas_dir(self, tmp_path):
        """Test initialization sets schemas_dir correctly."""
        config_dir = str(tmp_path / "config")
        os.makedirs(config_dir, exist_ok=True)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=config_dir)
                assert loader.schemas_dir == os.path.join(config_dir, "schemas")


class TestConfigLoaderLoadValidationConfig:
    """Test _load_validation_config() method."""

    def test_load_validation_config_from_file(self, tmp_path):
        """Test loading validation config from existing file."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        validation_content = {
            "validation": {
                "enabled": True,
                "on_failure": "error",
                "custom_setting": "test",
            }
        }

        validation_path = config_dir / "validation_config.yaml"
        with open(validation_path, "w") as f:
            yaml.dump(validation_content, f)

        with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
            loader = ConfigLoader(config_dir=str(config_dir))

        assert loader.validation_config["validation"]["enabled"] is True
        assert loader.validation_config["validation"]["on_failure"] == "error"
        assert loader.validation_config["validation"]["custom_setting"] == "test"

    def test_load_validation_config_default_when_file_missing(self, tmp_path):
        """Test default validation config when file doesn't exist."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
            loader = ConfigLoader(config_dir=str(config_dir))

        # Should have default values
        assert loader.validation_config["validation"]["enabled"] is True
        assert loader.validation_config["validation"]["on_failure"] == "warn"

    def test_load_validation_config_default_on_error(self, tmp_path):
        """Test default validation config when file is invalid."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        validation_path = config_dir / "validation_config.yaml"
        with open(validation_path, "w") as f:
            f.write("invalid: yaml: content: [")  # Invalid YAML

        with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
            loader = ConfigLoader(config_dir=str(config_dir))

        # Should fall back to defaults
        assert loader.validation_config["validation"]["enabled"] is True


class TestConfigLoaderLoadSchemaFiles:
    """Test _load_schema_files() method."""

    def test_load_schema_files_all_present(self, tmp_path):
        """Test loading all schema files when present."""
        config_dir = tmp_path / "config"
        schemas_dir = config_dir / "schemas"
        schemas_dir.mkdir(parents=True)

        # Create schema files
        schemas = {
            "data_source_schema.yaml": {"type": "object", "properties": {"name": {}}},
            "transformation_schema.yaml": {
                "type": "object",
                "properties": {"type": {}},
            },
        }

        for filename, content in schemas.items():
            with open(schemas_dir / filename, "w") as f:
                yaml.dump(content, f)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            loader = ConfigLoader(config_dir=str(config_dir))

        assert "data_source" in loader.schemas
        assert "transformation" in loader.schemas

    def test_load_schema_files_partial(self, tmp_path):
        """Test loading when only some schema files exist."""
        config_dir = tmp_path / "config"
        schemas_dir = config_dir / "schemas"
        schemas_dir.mkdir(parents=True)

        # Only create one schema file
        with open(schemas_dir / "data_source_schema.yaml", "w") as f:
            yaml.dump({"type": "object"}, f)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            loader = ConfigLoader(config_dir=str(config_dir))

        assert "data_source" in loader.schemas
        assert "transformation" not in loader.schemas

    def test_load_schema_files_missing_directory(self, tmp_path):
        """Test loading when schemas directory doesn't exist."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        # Don't create schemas directory

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            loader = ConfigLoader(config_dir=str(config_dir))

        assert loader.schemas == {}

    def test_load_schema_files_invalid_yaml(self, tmp_path):
        """Test handling invalid YAML in schema files."""
        config_dir = tmp_path / "config"
        schemas_dir = config_dir / "schemas"
        schemas_dir.mkdir(parents=True)

        # Create invalid YAML file
        with open(schemas_dir / "data_source_schema.yaml", "w") as f:
            f.write("invalid: yaml: [")

        # Create valid YAML file
        with open(schemas_dir / "transformation_schema.yaml", "w") as f:
            yaml.dump({"type": "object"}, f)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            loader = ConfigLoader(config_dir=str(config_dir))

        # Should skip invalid file but load valid one
        assert "data_source" not in loader.schemas
        assert "transformation" in loader.schemas


class TestConfigLoaderLoadDataSourceConfigs:
    """Test load_data_source_configs() method."""

    def test_load_data_source_configs_all_valid(self, tmp_path):
        """Test loading multiple valid data source configs."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        # Create valid config files with required metadata.tenant
        config1 = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {"name": "source1", "type": "api"},
            "destination": {"primary": {"type": "local_file"}},
        }
        config2 = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {"name": "source2", "type": "sftp"},
            "destination": {"primary": {"type": "snowflake"}},
        }

        with open(data_sources_dir / "config1.yaml", "w") as f:
            yaml.dump(config1, f)
        with open(data_sources_dir / "config2.yaml", "w") as f:
            yaml.dump(config2, f)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

        configs = loader.load_data_source_configs()

        assert len(configs) == 2
        names = [c["data_source"]["name"] for c in configs]
        assert "source1" in names
        assert "source2" in names

    def test_load_data_source_configs_skips_invalid(self, tmp_path):
        """Test that invalid configs are skipped."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        # Valid config with required metadata.tenant
        valid_config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {"name": "valid_source"},
            "destination": {"primary": {}},
        }
        # Invalid config - missing data_source (and metadata)
        invalid_config = {
            "destination": {"primary": {}},
        }

        with open(data_sources_dir / "valid.yaml", "w") as f:
            yaml.dump(valid_config, f)
        with open(data_sources_dir / "invalid.yaml", "w") as f:
            yaml.dump(invalid_config, f)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

        configs = loader.load_data_source_configs()

        assert len(configs) == 1
        assert configs[0]["data_source"]["name"] == "valid_source"

    def test_load_data_source_configs_missing_directory(self, tmp_path):
        """Test loading when data_sources directory doesn't exist."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        # Don't create data_sources directory

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

        configs = loader.load_data_source_configs()
        assert configs == []

    def test_load_data_source_configs_yml_extension(self, tmp_path):
        """Test loading configs with .yml extension."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {"name": "yml_source"},
            "destination": {"primary": {}},
        }

        with open(data_sources_dir / "config.yml", "w") as f:
            yaml.dump(config, f)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

        configs = loader.load_data_source_configs()

        assert len(configs) == 1
        assert configs[0]["data_source"]["name"] == "yml_source"

    def test_load_data_source_configs_ignores_non_yaml(self, tmp_path):
        """Test that non-YAML files are ignored."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        # Create non-YAML files
        (data_sources_dir / "readme.md").write_text("# Readme")
        (data_sources_dir / "config.json").write_text('{"key": "value"}')

        # Create YAML file with required metadata.tenant
        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {"name": "yaml_source"},
            "destination": {"primary": {}},
        }
        with open(data_sources_dir / "config.yaml", "w") as f:
            yaml.dump(config, f)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

        configs = loader.load_data_source_configs()

        assert len(configs) == 1


class TestConfigLoaderLoadGlobalSettings:
    """Test load_global_settings() method."""

    def test_load_global_settings_from_file(self, tmp_path):
        """Test loading global settings from file."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        settings = {
            "default_settings": {
                "retry_count": 5,
                "timeout": 60,
                "custom_setting": "value",
            }
        }

        with open(config_dir / "global_settings.yaml", "w") as f:
            yaml.dump(settings, f)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

        result = loader.load_global_settings()

        assert result["default_settings"]["retry_count"] == 5
        assert result["default_settings"]["timeout"] == 60
        assert result["default_settings"]["custom_setting"] == "value"

    def test_load_global_settings_default_when_missing(self, tmp_path):
        """Test default global settings when file doesn't exist."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

        result = loader.load_global_settings()

        # Should return defaults
        assert result["default_settings"]["retry_count"] == 3
        assert result["default_settings"]["timeout"] == 30
        assert result["default_settings"]["email_on_failure"] is True
        assert result["default_settings"]["email_on_retry"] is False


class TestConfigLoaderLoadConfigFile:
    """Test _load_yaml_file() method."""

    def test_load_config_file_valid_yaml(self, tmp_path):
        """Test loading valid YAML file."""
        config_file = tmp_path / "config.yaml"
        content = {"key": "value", "nested": {"a": 1, "b": 2}}

        with open(config_file, "w") as f:
            yaml.dump(content, f)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(tmp_path))

        result = load_yaml_file(str(config_file))

        assert result is not None
        assert result["key"] == "value"
        assert result["nested"]["a"] == 1

    def test_load_config_file_empty_yaml(self, tmp_path):
        """Test loading empty YAML file."""
        config_file = tmp_path / "empty.yaml"
        config_file.write_text("")

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(tmp_path))

        result = load_yaml_file(str(config_file))
        assert result is None

    def test_load_config_file_not_found(self, tmp_path):
        with pytest.raises(ConfigLoadError, match="File not found"):
            load_yaml_file(str(tmp_path / "nonexistent.yaml"))


class TestConfigLoaderValidateConfig:
    @pytest.fixture
    def validator(self):
        return ConfigValidator()

    def test_validate_config_valid(self, validator):
        config = {
            "metadata": {"tenant": "test_tenant"},
            "data_source": {"name": "test_source", "type": "postgres"},
            "destination": {"type": "snowflake"},
        }
        result = validator.validate_config(config, "test.yaml")
        assert result is True

    def test_validate_config_missing_name(self, validator):
        """Test validation fails when name is missing in data_source."""
        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {"type": "api"},  # Missing 'name'
            "destination": {"primary": {}},
        }

        result = validator.validate_config(config, "test.yaml")
        assert result is False

    def test_validate_config_missing_destination(self, validator):
        """Test validation fails when destination is missing."""
        config = {
            "data_source": {"name": "test_source"},
            # Missing 'destination'
        }

        result = validator.validate_config(config, "test.yaml")
        assert result is False


class TestConfigLoaderEdgeCases:
    """Test edge cases and error handling."""

    def test_unicode_config_content(self, tmp_path):
        """Test loading config with Unicode content."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {
                "name": "unicode_source",
                "description": "Test with special chars",
            },
            "destination": {"primary": {"path": "/path/to/file"}},
        }

        # Write with explicit UTF-8 encoding
        config_file = data_sources_dir / "unicode.yaml"
        with open(config_file, "w", encoding="utf-8") as f:
            yaml.dump(config, f, allow_unicode=True)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

        configs = loader.load_data_source_configs()

        assert len(configs) == 1
        assert configs[0]["data_source"]["name"] == "unicode_source"

    def test_large_config_file(self, tmp_path):
        """Test loading large config file."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        # Create config with many entries and required metadata.tenant
        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {"name": "large_source"},
            "destination": {"primary": {}},
            "transformations": [
                {"type": f"transform_{i}", "config": {"value": i}} for i in range(100)
            ],
        }

        with open(data_sources_dir / "large.yaml", "w") as f:
            yaml.dump(config, f)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

        configs = loader.load_data_source_configs()

        assert len(configs) == 1
        assert len(configs[0]["transformations"]) == 100

    def test_deeply_nested_config(self, tmp_path):
        """Test loading deeply nested config structure."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        config = {
            "metadata": {"tenant": "shared_services"},
            "data_source": {"name": "nested_source"},
            "destination": {"primary": {}},
            "nested": {"level1": {"level2": {"level3": {"level4": {"value": "deep"}}}}},
        }

        with open(data_sources_dir / "nested.yaml", "w") as f:
            yaml.dump(config, f)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

        configs = loader.load_data_source_configs()

        assert (
            configs[0]["nested"]["level1"]["level2"]["level3"]["level4"]["value"]
            == "deep"
        )

    def test_config_with_special_yaml_types(self, tmp_path):
        """Test loading config with YAML special types."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        yaml_content = """
metadata:
  tenant: shared_services
data_source:
  name: special_source
  enabled: true
  count: 42
  rate: 3.14
  empty_value: null
  tags:
    - tag1
    - tag2
destination:
  primary: {}
"""

        with open(data_sources_dir / "special.yaml", "w") as f:
            f.write(yaml_content)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

        configs = loader.load_data_source_configs()

        assert configs[0]["data_source"]["enabled"] is True
        assert configs[0]["data_source"]["count"] == 42
        assert configs[0]["data_source"]["rate"] == 3.14
        assert configs[0]["data_source"]["empty_value"] is None
        assert configs[0]["data_source"]["tags"] == ["tag1", "tag2"]

    def test_config_file_with_yaml_anchors(self, tmp_path):
        """Test loading config with YAML anchors and aliases."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        yaml_content = """
defaults: &defaults
  retry_count: 3
  timeout: 30

metadata:
  tenant: shared_services
data_source:
  name: anchor_source
  settings:
    <<: *defaults
    custom: value
destination:
  primary: {}
"""

        with open(data_sources_dir / "anchors.yaml", "w") as f:
            f.write(yaml_content)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

        configs = loader.load_data_source_configs()

        # Anchors should be resolved
        assert configs[0]["data_source"]["settings"]["retry_count"] == 3
        assert configs[0]["data_source"]["settings"]["timeout"] == 30
        assert configs[0]["data_source"]["settings"]["custom"] == "value"

    def test_multiple_yaml_documents_in_file(self, tmp_path):
        """Test that multiple YAML documents in file raises error."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        yaml_content = """key1: value1
---
key2: value2
"""

        config_file = config_dir / "multi.yaml"
        with open(config_file, "w") as f:
            f.write(yaml_content)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

        # yaml.safe_load raises ComposerError for multiple documents
        with pytest.raises(Exception):  # yaml.composer.ComposerError
            load_yaml_file(str(config_file))


class TestConfigLoaderIntegration:
    """Integration tests using real file system."""

    def test_full_workflow(self, tmp_path):
        """Test complete config loading workflow."""
        # Set up directory structure
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        schemas_dir = config_dir / "schemas"

        data_sources_dir.mkdir(parents=True)
        schemas_dir.mkdir(parents=True)

        # Create validation config
        validation_config = {
            "validation": {
                "enabled": True,
                "on_failure": "warn",
            }
        }
        with open(config_dir / "validation_config.yaml", "w") as f:
            yaml.dump(validation_config, f)

        # Create global settings
        global_settings = {
            "default_settings": {
                "retry_count": 5,
            }
        }
        with open(config_dir / "global_settings.yaml", "w") as f:
            yaml.dump(global_settings, f)

        # Create schema file
        schema = {"type": "object"}
        with open(schemas_dir / "data_source_schema.yaml", "w") as f:
            yaml.dump(schema, f)

        # Create data source configs with required metadata.tenant
        for i in range(3):
            config = {
                "metadata": {"tenant": "shared_services"},
                "data_source": {"name": f"source_{i}"},
                "destination": {"primary": {}},
            }
            with open(data_sources_dir / f"source_{i}.yaml", "w") as f:
                yaml.dump(config, f)

        # Load and verify
        loader = ConfigLoader(config_dir=str(config_dir))

        # Verify validation config was loaded
        assert loader.validation_config["validation"]["enabled"] is True

        # Verify schemas were loaded
        assert "data_source" in loader.schemas

        # Verify data source configs were loaded
        configs = loader.load_data_source_configs()
        assert len(configs) == 3

        # Verify global settings were loaded
        settings = loader.load_global_settings()
        assert settings["default_settings"]["retry_count"] == 5
