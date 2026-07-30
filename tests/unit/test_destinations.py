"""OCP: a new sink is a new subclass implementing _write + one register() call."""
import polars as pl
import pytest

from rlam_airflow_framework.destinations.base import DestinationLoader, LOADER_REGISTRY
from rlam_airflow_framework.destinations.registry import DESTINATION_REGISTRY
from rlam_airflow_framework.destinations.base import LoadContext
from rlam_airflow_framework.destinations.print_logs import PrintLogsLoader


def _ctx(**kwargs):
    defaults = dict(topic="t", correlation_id="cid", dest_label="primary")
    defaults.update(kwargs)
    return LoadContext(**defaults)


def test_registry_resolves_registered_type_polymorphically():
    loader = DESTINATION_REGISTRY.get("print_logs")
    assert isinstance(loader, PrintLogsLoader)
    assert isinstance(loader, DestinationLoader)


def test_registry_unknown_type_raises_with_supported_list():
    with pytest.raises(ValueError, match="Unsupported destination type: mystery"):
        DESTINATION_REGISTRY.get("mystery")


def test_print_logs_loader_returns_summary(tmp_path):
    df = pl.DataFrame({"a": [1, 2, 3]})
    df_path = str(tmp_path / "test.parquet")
    df.write_parquet(df_path)
    result = PrintLogsLoader().load(df_path, {"type": "print_logs"}, _ctx())
    assert result == "Printed 3 rows to logs"


def test_custom_loader_added_without_touching_existing_code(tmp_path):
    

    @DestinationLoader.register("echo")
    class EchoLoader(DestinationLoader):
        def _write(self, df_path, dest_config, ctx):
            import duckdb

            res = duckdb.query(f"SELECT count(*) FROM '{df_path}'").fetchone()
            count = res[0] if res else 0
            return f"echo:{dest_config.get('message', '')}:{count}"

    df = pl.DataFrame({"x": [1, 2]})
    df_path = str(tmp_path / "test2.parquet")
    df.write_parquet(df_path)
    loader = DESTINATION_REGISTRY.get("echo")
    assert (
        loader.load(df_path, {"type": "echo", "message": "hi"}, _ctx()) == "echo:hi:2"
    )
    assert "echo" in LOADER_REGISTRY


def test_abstract_base_cannot_be_instantiated():
    with pytest.raises(TypeError):
        DestinationLoader()  # type: ignore[abstract]
