# File: tests/unit/test_edge_cases_config_loader.py
"""
Edge case tests for ConfigLoader.

Tests cover:
- Empty files handling
- Malformed YAML syntax
- Missing required fields
- Unicode content handling
- File permission errors
- Concurrent access scenarios
- Hash change detection
- Hot-reload integration
"""

import pytest
import yaml
import threading
from unittest.mock import patch

# Import actual implementation
from rlam_airflow_framework.config_loader import ConfigLoader, ConfigLoadError


class TestConfigLoaderEmptyFiles:
    """Test handling of empty configuration files."""

    def test_empty_yaml_file_returns_none(self, tmp_path):
        """Test that empty YAML file returns None."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        # Create empty file
        empty_file = config_dir / "empty.yaml"
        empty_file.write_text("")

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                result = loader._load_yaml_file(str(empty_file))

        assert result is None

    def test_yaml_file_with_only_comments(self, tmp_path):
        """Test YAML file containing only comments returns None."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        comments_file = config_dir / "comments_only.yaml"
        comments_file.write_text("""
# This is a comment
# Another comment
# No actual content
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                result = loader._load_yaml_file(str(comments_file))

        assert result is None

    def test_yaml_file_with_only_whitespace(self, tmp_path):
        """Test YAML file containing only spaces/newlines returns None."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        # Only spaces and newlines (no tabs - YAML doesn't allow tabs)
        whitespace_file = config_dir / "whitespace.yaml"
        whitespace_file.write_text("   \n   \n   \n")

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                result = loader._load_yaml_file(str(whitespace_file))

        assert result is None

    def test_empty_data_sources_directory_returns_empty_list(self, tmp_path):
        """Test empty data_sources directory returns empty list."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir()

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                configs = loader.load_data_source_configs()

        assert configs == []

    def test_skips_empty_config_files_in_data_sources(self, tmp_path):
        """Test that empty config files are skipped during loading."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        # Create one empty and one valid config
        empty_config = data_sources_dir / "empty.yaml"
        empty_config.write_text("")

        valid_config = data_sources_dir / "valid.yaml"
        valid_config.write_text("""
metadata:
  tenant: shared_services
data_source:
  name: test_source
  type: api
destination:
  type: snowflake
  table: test_table
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                configs = loader.load_data_source_configs()

        # Should only have 1 config (the valid one)
        assert len(configs) == 1
        assert configs[0]["data_source"]["name"] == "test_source"


class TestConfigLoaderMalformedYAML:
    """Test handling of malformed YAML syntax."""

    def test_invalid_yaml_syntax_raises_error(self, tmp_path):
        """Test that invalid YAML syntax raises ConfigLoadError."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        invalid_file = config_dir / "invalid.yaml"
        invalid_file.write_text("""
data_source:
  name: test
  invalid: [unclosed bracket
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

                with pytest.raises(ConfigLoadError) as exc_info:
                    loader._load_yaml_file(str(invalid_file))

                assert "Invalid YAML syntax" in str(exc_info.value)

    def test_yaml_with_tabs_instead_of_spaces(self, tmp_path):
        """Test YAML with tab indentation (invalid)."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        tab_file = config_dir / "tabs.yaml"
        # YAML with tabs - should be handled or error gracefully
        tab_file.write_text("data_source:\n\tname: test")

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

                # Should either parse or raise ConfigLoadError
                try:
                    result = loader._load_yaml_file(str(tab_file))
                    # Some YAML parsers accept tabs
                    assert isinstance(result, dict)
                except ConfigLoadError:
                    # This is also acceptable
                    pass

    def test_yaml_with_duplicate_keys(self, tmp_path):
        """Test YAML with duplicate keys (last value wins)."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        dup_file = config_dir / "duplicate_keys.yaml"
        dup_file.write_text("""
data_source:
  name: first_name
  name: second_name
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                result = loader._load_yaml_file(str(dup_file))

        # YAML spec: last value wins for duplicate keys
        assert result["data_source"]["name"] == "second_name"

    def test_yaml_with_circular_reference_anchor(self, tmp_path):
        """Test YAML with recursive anchor references."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        circular_file = config_dir / "circular.yaml"
        circular_file.write_text("""
data_source: &source
  name: test
  ref: *source
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

                # PyYAML's safe_load should handle this
                try:
                    result = loader._load_yaml_file(str(circular_file))
                    # If it parses, it creates a reference (not truly circular in safe_load)
                    assert "data_source" in result
                except (ConfigLoadError, yaml.YAMLError):
                    # Also acceptable to reject
                    pass

    def test_continues_loading_other_configs_on_malformed(self, tmp_path):
        """Test that one malformed file doesn't stop loading others."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        # Create malformed config
        malformed = data_sources_dir / "a_malformed.yaml"
        malformed.write_text("invalid: [")

        # Create valid config
        valid = data_sources_dir / "b_valid.yaml"
        valid.write_text("""
metadata:
  tenant: shared_services
data_source:
  name: valid_source
destination:
  type: snowflake
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                configs = loader.load_data_source_configs()

        # Should still load the valid config
        assert len(configs) == 1
        assert configs[0]["data_source"]["name"] == "valid_source"


class TestConfigLoaderMissingFields:
    """Test handling of missing required fields."""

    def test_missing_data_source_key_fails_validation(self, tmp_path):
        """Test config without data_source key fails validation."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        invalid = data_sources_dir / "no_data_source.yaml"
        invalid.write_text("""
destination:
  type: snowflake
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                configs = loader.load_data_source_configs()

        # Should not include invalid config
        assert len(configs) == 0

    def test_missing_name_in_data_source_fails_validation(self, tmp_path):
        """Test config without name in data_source fails validation."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        invalid = data_sources_dir / "no_name.yaml"
        invalid.write_text("""
data_source:
  type: api
destination:
  type: snowflake
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                configs = loader.load_data_source_configs()

        assert len(configs) == 0

    def test_empty_name_in_data_source_fails_validation(self, tmp_path):
        """Test config with empty name fails validation."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        invalid = data_sources_dir / "empty_name.yaml"
        invalid.write_text("""
data_source:
  name: ""
destination:
  type: snowflake
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                configs = loader.load_data_source_configs()

        assert len(configs) == 0

    def test_missing_destination_fails_validation(self, tmp_path):
        """Test config without destination fails validation."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        invalid = data_sources_dir / "no_destination.yaml"
        invalid.write_text("""
data_source:
  name: test_source
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                configs = loader.load_data_source_configs()

        assert len(configs) == 0

    def test_null_destination_fails_validation(self, tmp_path):
        """Test config with null destination fails validation."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        invalid = data_sources_dir / "null_destination.yaml"
        invalid.write_text("""
data_source:
  name: test_source
destination: ~
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                configs = loader.load_data_source_configs()

        assert len(configs) == 0

    def test_data_source_not_dict_fails_validation(self, tmp_path):
        """Test config where data_source is not a dict fails."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        invalid = data_sources_dir / "data_source_string.yaml"
        invalid.write_text("""
data_source: "just a string"
destination:
  type: snowflake
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                configs = loader.load_data_source_configs()

        assert len(configs) == 0


class TestConfigLoaderUnicode:
    """Test handling of Unicode content in configurations."""

    def test_unicode_in_config_values(self, tmp_path):
        """Test Unicode characters in configuration values."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        unicode_config = data_sources_dir / "unicode.yaml"
        unicode_config.write_text(
            """
metadata:
  tenant: shared_services
data_source:
  name: unicode_テスト_源
  description: "数据源 - データソース - مصدر البيانات"
  emoji: "🚀📊💾"
destination:
  type: snowflake
  table: テーブル名
        """,
            encoding="utf-8",
        )

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                configs = loader.load_data_source_configs()

        assert len(configs) == 1
        assert configs[0]["data_source"]["name"] == "unicode_テスト_源"
        assert "🚀" in configs[0]["data_source"]["emoji"]

    def test_unicode_in_file_names(self, tmp_path):
        """Test loading config files with Unicode names."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        # Note: Some file systems may not support Unicode filenames
        try:
            unicode_file = data_sources_dir / "配置_конфиг.yaml"
            unicode_file.write_text(
                """
metadata:
  tenant: shared_services
data_source:
  name: unicode_filename_test
destination:
  type: snowflake
            """,
                encoding="utf-8",
            )

            with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
                with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                    loader = ConfigLoader(config_dir=str(config_dir))
                    configs = loader.load_data_source_configs()

            assert len(configs) == 1
        except (OSError, UnicodeError):
            # Skip if filesystem doesn't support Unicode filenames
            pytest.skip("Filesystem does not support Unicode filenames")

    def test_bom_in_yaml_file(self, tmp_path):
        """Test YAML file with UTF-8 BOM marker."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        bom_file = config_dir / "with_bom.yaml"
        # Write with UTF-8 BOM
        with open(bom_file, "wb") as f:
            f.write(b"\xef\xbb\xbf")  # UTF-8 BOM
            f.write(
                """
data_source:
  name: bom_test
destination:
  type: snowflake
            """.encode("utf-8")
            )

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                # PyYAML handles BOM, but we should test it
                try:
                    result = loader._load_yaml_file(str(bom_file))
                    assert "data_source" in result
                except ConfigLoadError:
                    # Some implementations may reject BOM
                    pass


class TestConfigLoaderFileErrors:
    """Test handling of file system errors."""

    def test_nonexistent_file_raises_error(self, tmp_path):
        """Test loading nonexistent file raises ConfigLoadError."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

                with pytest.raises(ConfigLoadError) as exc_info:
                    loader._load_yaml_file(str(config_dir / "nonexistent.yaml"))

                assert "File not found" in str(exc_info.value)

    def test_empty_filepath_raises_error(self, tmp_path):
        """Test empty filepath raises ConfigLoadError."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

                with pytest.raises(ConfigLoadError):
                    loader._load_yaml_file("")

    def test_none_filepath_raises_error(self, tmp_path):
        """Test None filepath raises ConfigLoadError."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

                with pytest.raises(ConfigLoadError):
                    loader._load_yaml_file(None)

    def test_nonexistent_data_sources_directory(self, tmp_path):
        """Test handling of nonexistent data_sources directory."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        # Don't create data_sources directory

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                configs = loader.load_data_source_configs()

        assert configs == []

    def test_none_config_dir_raises_error(self):
        """Test None config_dir raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            ConfigLoader(config_dir=None)

        assert "cannot be None or empty" in str(exc_info.value)

    def test_empty_config_dir_raises_error(self):
        """Test empty config_dir raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            ConfigLoader(config_dir="")

        assert "cannot be None or empty" in str(exc_info.value)


class TestConfigLoaderHashTracking:
    """Test file hash tracking for change detection."""

    def test_computes_hash_on_load(self, tmp_path):
        """Test that file hash is computed when loading."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        config_file = data_sources_dir / "test.yaml"
        config_file.write_text("""
metadata:
  tenant: shared_services
data_source:
  name: test
destination:
  type: snowflake
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                loader.load_data_source_configs()

        # Should have hash for the loaded file
        assert len(loader._file_hashes) > 0

    def test_detects_file_content_change(self, tmp_path):
        """Test detection of file content changes."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        config_file = data_sources_dir / "test.yaml"
        config_file.write_text("""
metadata:
  tenant: shared_services
data_source:
  name: original
destination:
  type: snowflake
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                loader.load_data_source_configs()

                assert not loader.has_configs_changed()

                # Modify the file
                config_file.write_text("""
metadata:
  tenant: shared_services
data_source:
  name: modified
destination:
  type: snowflake
                """)

                assert loader.has_configs_changed()

    def test_detects_new_file_added(self, tmp_path):
        """Test detection of new config file."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        config_file = data_sources_dir / "original.yaml"
        config_file.write_text("""
metadata:
  tenant: shared_services
data_source:
  name: original
destination:
  type: snowflake
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                loader.load_data_source_configs()

                # Add new file
                new_file = data_sources_dir / "new.yaml"
                new_file.write_text("""
metadata:
  tenant: shared_services
data_source:
  name: new_source
destination:
  type: snowflake
                """)

                assert loader.has_configs_changed()

    def test_detects_file_deleted(self, tmp_path):
        """Test detection of deleted config file."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        config_file = data_sources_dir / "to_delete.yaml"
        config_file.write_text("""
metadata:
  tenant: shared_services
data_source:
  name: deleteme
destination:
  type: snowflake
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                loader.load_data_source_configs()

                # Delete the file
                config_file.unlink()

                assert loader.has_configs_changed()

    def test_reload_if_changed_clears_hashes(self, tmp_path):
        """Test that reload_if_changed clears and recomputes hashes."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        config_file = data_sources_dir / "test.yaml"
        config_file.write_text("""
metadata:
  tenant: shared_services
data_source:
  name: original
destination:
  type: snowflake
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                loader.load_data_source_configs()

                # Modify and reload
                config_file.write_text("""
metadata:
  tenant: shared_services
data_source:
  name: modified
destination:
  type: snowflake
                """)

                changed, configs = loader.reload_if_changed()

                assert changed is True
                assert len(configs) == 1
                assert configs[0]["data_source"]["name"] == "modified"


class TestConfigLoaderGlobalSettings:
    """Test global settings loading."""

    def test_loads_global_settings_from_file(self, tmp_path):
        """Test loading global settings from file."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        global_settings = config_dir / "global_settings.yaml"
        global_settings.write_text("""
default_settings:
  retry_count: 5
  timeout: 60
  custom_key: custom_value
        """)

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                settings = loader.load_global_settings()

        assert settings["default_settings"]["retry_count"] == 5
        assert settings["default_settings"]["timeout"] == 60
        assert settings["default_settings"]["custom_key"] == "custom_value"

    def test_returns_defaults_when_file_missing(self, tmp_path):
        """Test default settings when file doesn't exist."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                settings = loader.load_global_settings()

        assert settings["default_settings"]["retry_count"] == 3
        assert settings["default_settings"]["timeout"] == 30

    def test_returns_defaults_when_file_empty(self, tmp_path):
        """Test default settings when file is empty."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        global_settings = config_dir / "global_settings.yaml"
        global_settings.write_text("")

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))
                settings = loader.load_global_settings()

        # Should fall back to defaults
        assert "default_settings" in settings


class TestConfigLoaderConcurrency:
    """Test thread safety and concurrent access."""

    def test_concurrent_config_loading(self, tmp_path):
        """Test concurrent config loading doesn't cause issues."""
        config_dir = tmp_path / "config"
        data_sources_dir = config_dir / "data_sources"
        data_sources_dir.mkdir(parents=True)

        # Create multiple config files
        for i in range(10):
            config_file = data_sources_dir / f"config_{i}.yaml"
            config_file.write_text(f"""
metadata:
  tenant: shared_services
data_source:
  name: source_{i}
destination:
  type: snowflake
            """)

        results = []
        errors = []

        def load_configs():
            try:
                with patch.object(
                    ConfigLoader, "_load_validation_config", return_value={}
                ):
                    with patch.object(
                        ConfigLoader, "_load_schema_files", return_value={}
                    ):
                        loader = ConfigLoader(config_dir=str(config_dir))
                        configs = loader.load_data_source_configs()
                        results.append(len(configs))
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=load_configs) for _ in range(5)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert all(r == 10 for r in results), f"Inconsistent results: {results}"


class TestConfigLoadErrorException:
    """Test ConfigLoadError exception handling."""

    def test_config_load_error_with_all_params(self):
        """Test ConfigLoadError with all parameters."""
        original = ValueError("original error")
        error = ConfigLoadError(
            "Test error message",
            config_file="/path/to/config.yaml",
            original_error=original,
        )

        assert "Test error message" in str(error)
        assert "/path/to/config.yaml" in str(error)
        assert error.config_file == "/path/to/config.yaml"
        assert error.original_error is original

    def test_config_load_error_without_file(self):
        """Test ConfigLoadError without config file."""
        error = ConfigLoadError("Test error")

        assert "Test error" in str(error)
        assert error.config_file is None

    def test_config_load_error_preserves_chain(self, tmp_path):
        """Test that ConfigLoadError preserves exception chain."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        invalid_file = config_dir / "invalid.yaml"
        invalid_file.write_text("invalid: [unclosed")

        with patch.object(ConfigLoader, "_load_validation_config", return_value={}):
            with patch.object(ConfigLoader, "_load_schema_files", return_value={}):
                loader = ConfigLoader(config_dir=str(config_dir))

                try:
                    loader._load_yaml_file(str(invalid_file))
                except ConfigLoadError as e:
                    # Should have __cause__ set
                    assert e.__cause__ is not None or e.original_error is not None
