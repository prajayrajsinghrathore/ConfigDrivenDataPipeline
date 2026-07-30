# File: rlam_airflow_framework/serializers.py
"""
Custom serializers for Airflow objects.

Registers polars DataFrame serialization for XCom exchange between tasks.
"""

from __future__ import annotations

from typing import cast
import json

try:
    from airflow.utils.module_loading import qualname  # type: ignore[import-not-found]
except ImportError:
    # Airflow 3
    from airflow.sdk._shared.module_loading import qualname
    from airflow.sdk.serde import U


serializers = ["polars.dataframe.frame.DataFrame"]
deserializers = serializers

__version__ = 1


def serialize(o: object) -> tuple[U, str, int, bool]:
    """
    Serialize polars DataFrame to JSON for XCom storage.

    Args:
        o: Object to serialize

    Returns:
        Tuple of (serialized_data, classname, version, is_serialized)
    """
    import polars as pl

    if not isinstance(o, pl.DataFrame):
        return "", "", 0, False

    name = qualname(o)

    # Convert DataFrame to JSON (orient='records')
    # Using write_json produces a JSON string, we load it back to a dict for Airflow JSON serialization
    data = {
        "data": json.loads(o.write_json()),
    }

    return data, name, __version__, True


def deserialize(cls: type, version: int, data: object) -> object:
    """
    Deserialize JSON data back to polars DataFrame.

    Args:
        cls: Class type (should be pl.DataFrame)
        version: Serialization version
        data: Serialized data

    Returns:
        Deserialized polars DataFrame

    Raises:
        TypeError: If version incompatible or wrong class
    """
    import polars as pl

    # Check version compatibility
    if version > __version__:
        raise TypeError(f"serialized {version} of {qualname(cls)} > {__version__}")

    if cls is not pl.DataFrame:
        raise TypeError(f"do not know how to deserialize {qualname(cls)}")

    payload = cast(dict, data)
    df_data = payload.get("data", [])

    # Reconstruct DataFrame
    if df_data:
        df = pl.read_json(json.dumps(df_data).encode("utf-8"))
    else:
        df = pl.DataFrame()

    return df
