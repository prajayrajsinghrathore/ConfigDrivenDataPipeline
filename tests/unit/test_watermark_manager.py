# File: tests/unit/test_watermark_manager.py
"""
Unit tests for WatermarkConfig and WatermarkManager.
"""

from unittest.mock import patch

import polars as pl
import pytest

from rlam_airflow_framework.taskflow.watermark import WatermarkConfig, WatermarkManager


# =============================================================================
# WatermarkConfig
# =============================================================================


class TestWatermarkConfig:
    """Test WatermarkConfig.from_config() factory."""

    def test_defaults_when_missing(self):
        cfg = WatermarkConfig.from_config({})
        assert cfg.enabled is False
        assert cfg.watermark_column is None
        assert cfg.initial_watermark is None
        assert cfg.lookback == 0
        assert cfg.strict is False

    def test_full_config(self):
        cfg = WatermarkConfig.from_config(
            {
                "incremental": {
                    "enabled": True,
                    "watermark_column": "updated_at",
                    "initial_watermark": "2024-01-01T00:00:00",
                    "lookback": 300,
                    "strict": True,
                }
            }
        )
        assert cfg.enabled is True
        assert cfg.watermark_column == "updated_at"
        assert cfg.initial_watermark == "2024-01-01T00:00:00"
        assert cfg.lookback == 300
        assert cfg.strict is True


# =============================================================================
# WatermarkManager — resolve
# =============================================================================


class TestWatermarkResolve:
    """Test WatermarkManager.resolve()."""

    def test_disabled_returns_none(self):
        cfg = WatermarkConfig(
            enabled=False,
            watermark_column=None,
            initial_watermark=None,
            lookback=0,
            strict=False,
        )
        wm = WatermarkManager("dag_1", cfg)
        assert wm.resolve() is None

    @patch("airflow.sdk.Variable")
    def test_loads_from_variable(self, mock_variable_cls):
        """When a Variable exists, it should be used as the watermark."""
        mock_variable_cls.get.side_effect = lambda key, default=None: (
            "2025-07-01T00:00:00" if "high_watermark" in key else default
        )

        cfg = WatermarkConfig(
            enabled=True,
            watermark_column="ts",
            initial_watermark="2024-01-01T00:00:00",
            lookback=0,
            strict=False,
        )
        wm = WatermarkManager("dag_1", cfg)
        result = wm.resolve()

        assert result == "2025-07-01T00:00:00"
        assert wm.adjusted_watermark == "2025-07-01T00:00:00"

    @patch("airflow.sdk.Variable")
    def test_falls_back_to_initial_watermark(self, mock_variable_cls):
        """When no Variable exists, use initial_watermark."""
        mock_variable_cls.get.return_value = None

        cfg = WatermarkConfig(
            enabled=True,
            watermark_column="ts",
            initial_watermark="2024-01-01T00:00:00",
            lookback=0,
            strict=False,
        )
        wm = WatermarkManager("dag_1", cfg)
        result = wm.resolve()

        assert result == "2024-01-01T00:00:00"

    @patch("airflow.sdk.Variable")
    def test_strict_mode_raises_on_missing_watermark(self, mock_variable_cls):
        """Strict mode with no watermark but first_run_completed should raise."""
        mock_variable_cls.get.side_effect = lambda key, default=None: (
            "true" if "first_run_completed" in key else None
        )

        cfg = WatermarkConfig(
            enabled=True,
            watermark_column="ts",
            initial_watermark=None,
            lookback=0,
            strict=True,
        )
        wm = WatermarkManager("dag_1", cfg)

        with pytest.raises(ValueError, match="strict mode"):
            wm.resolve()

    @patch("airflow.sdk.Variable")
    def test_lookback_adjustment(self, mock_variable_cls):
        """Lookback should subtract seconds from the watermark."""
        mock_variable_cls.get.side_effect = lambda key, default=None: (
            "2025-07-01T12:00:00+00:00" if "high_watermark" in key else default
        )

        cfg = WatermarkConfig(
            enabled=True,
            watermark_column="ts",
            initial_watermark=None,
            lookback=3600,  # 1 hour
            strict=False,
        )
        wm = WatermarkManager("dag_1", cfg)
        result = wm.resolve()

        # Should be 11:00 instead of 12:00
        assert result is not None
        assert "11:00:00" in result


# =============================================================================
# WatermarkManager — filter_dataframe
# =============================================================================


class TestWatermarkFilterDataframe:
    """Test WatermarkManager.filter_dataframe()."""

    def _make_manager(self, adjusted: str | None = None) -> WatermarkManager:
        cfg = WatermarkConfig(
            enabled=True,
            watermark_column="id",
            initial_watermark=None,
            lookback=0,
            strict=False,
        )
        wm = WatermarkManager("dag_1", cfg)
        wm._adjusted_watermark = adjusted
        return wm

    def test_numeric_filter(self):
        wm = self._make_manager("3")
        df = pl.DataFrame({"id": [1, 2, 3, 4, 5], "val": list("abcde")})

        result = wm.filter_dataframe(df)

        assert list(result["id"]) == [4, 5]

    def test_no_filter_when_disabled(self):
        cfg = WatermarkConfig(
            enabled=False,
            watermark_column="id",
            initial_watermark=None,
            lookback=0,
            strict=False,
        )
        wm = WatermarkManager("dag_1", cfg)
        df = pl.DataFrame({"id": [1, 2, 3]})

        result = wm.filter_dataframe(df)

        assert len(result) == 3

    def test_no_filter_when_watermark_is_none(self):
        wm = self._make_manager(None)
        df = pl.DataFrame({"id": [1, 2, 3]})

        result = wm.filter_dataframe(df)

        assert len(result) == 3

    def test_no_filter_when_column_missing(self):
        cfg = WatermarkConfig(
            enabled=True,
            watermark_column="missing_col",
            initial_watermark=None,
            lookback=0,
            strict=False,
        )
        wm = WatermarkManager("dag_1", cfg)
        wm._adjusted_watermark = "3"

        df = pl.DataFrame({"id": [1, 2, 3]})
        result = wm.filter_dataframe(df)

        assert len(result) == 3

    def test_empty_dataframe_returns_empty(self):
        wm = self._make_manager("3")
        df = pl.DataFrame(schema=["id"])

        result = wm.filter_dataframe(df)

        assert result.is_empty()

    def test_datetime_filter(self):
        cfg = WatermarkConfig(
            enabled=True,
            watermark_column="ts",
            initial_watermark=None,
            lookback=0,
            strict=False,
        )
        wm = WatermarkManager("dag_1", cfg)
        wm._adjusted_watermark = "2025-07-15"

        df = pl.DataFrame(
            {
                "ts": pl.Series(
                    ["2025-07-14", "2025-07-15", "2025-07-16", "2025-07-17"]
                ).str.to_datetime(),
                "val": [1, 2, 3, 4],
            }
        )

        result = wm.filter_dataframe(df)

        assert len(result) == 2
        assert list(result["val"]) == [3, 4]


# =============================================================================
# WatermarkManager — update
# =============================================================================


class TestWatermarkUpdate:
    """Test WatermarkManager.update()."""

    @patch("airflow.sdk.Variable")
    def test_update_persists_max_value(self, mock_variable_cls):
        cfg = WatermarkConfig(
            enabled=True,
            watermark_column="id",
            initial_watermark=None,
            lookback=0,
            strict=False,
        )
        wm = WatermarkManager("dag_1", cfg)

        df = pl.DataFrame({"id": [10, 20, 30]})
        wm.update(df)

        mock_variable_cls.set.assert_any_call("dag_1.high_watermark", "30")
        mock_variable_cls.set.assert_any_call("dag_1.first_run_completed", "true")

    @patch("airflow.sdk.Variable")
    def test_update_noop_when_disabled(self, mock_variable_cls):
        cfg = WatermarkConfig(
            enabled=False,
            watermark_column="id",
            initial_watermark=None,
            lookback=0,
            strict=False,
        )
        wm = WatermarkManager("dag_1", cfg)

        df = pl.DataFrame({"id": [10, 20, 30]})
        wm.update(df)

        mock_variable_cls.set.assert_not_called()

    @patch("airflow.sdk.Variable")
    def test_update_noop_on_empty_df(self, mock_variable_cls):
        cfg = WatermarkConfig(
            enabled=True,
            watermark_column="id",
            initial_watermark=None,
            lookback=0,
            strict=False,
        )
        wm = WatermarkManager("dag_1", cfg)

        df = pl.DataFrame(schema=["id"])
        wm.update(df)

        mock_variable_cls.set.assert_not_called()

    @patch("airflow.sdk.Variable")
    def test_update_noop_when_column_missing(self, mock_variable_cls):
        cfg = WatermarkConfig(
            enabled=True,
            watermark_column="missing",
            initial_watermark=None,
            lookback=0,
            strict=False,
        )
        wm = WatermarkManager("dag_1", cfg)

        df = pl.DataFrame({"id": [1, 2, 3]})
        wm.update(df)

        mock_variable_cls.set.assert_not_called()
