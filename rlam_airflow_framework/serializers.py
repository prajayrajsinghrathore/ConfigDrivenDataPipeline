# File: rlam_airflow_framework/serializers.py
"""
Custom serializers for Airflow objects.

Registers pandas DataFrame serialization for XCom exchange between tasks.
"""

from __future__ import annotations


try:
    from airflow.utils.module_loading import qualname
except ImportError:
    # Airflow 3
    from airflow.sdk._shared.module_loading import qualname
    import pandas as pd
    from airflow.sdk.serde import U


serializers = ["pandas.DataFrame"]
deserializers = serializers

__version__ = 1


def serialize(o: object) -> tuple[U, str, int, bool]:
    """
    Serialize pandas DataFrame to JSON for XCom storage.
    
    Args:
        o: Object to serialize
        
    Returns:
        Tuple of (serialized_data, classname, version, is_serialized)
    """
    import pandas as pd
    
    if not isinstance(o, pd.DataFrame):
        return "", "", 0, False
    
    name = qualname(o)
    
    # Convert DataFrame to JSON (orient='split' preserves index and column names)
    # This is more efficient than 'records' for large DataFrames
    data = {
        "data": o.to_dict(orient='split'),
        "dtypes": {col: str(dtype) for col, dtype in o.dtypes.items()}
    }
    
    return data, name, __version__, True


def deserialize(cls: type, version: int, data: object) -> pd.DataFrame:
    """
    Deserialize JSON data back to pandas DataFrame.
    
    Args:
        cls: Class type (should be pd.DataFrame)
        version: Serialization version
        data: Serialized data
        
    Returns:
        Deserialized pandas DataFrame
        
    Raises:
        TypeError: If version incompatible or wrong class
    """
    import pandas as pd
    
    # Check version compatibility
    if version > __version__:
        raise TypeError(f"serialized {version} of {qualname(cls)} > {__version__}")
    
    if cls is not pd.DataFrame:
        raise TypeError(f"do not know how to deserialize {qualname(cls)}")
    
    # Reconstruct DataFrame from split-oriented dict
    df_data = data["data"]
    df = pd.DataFrame(**df_data)
    
    # Restore original dtypes
    dtypes = data.get("dtypes", {})
    for col, dtype_str in dtypes.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype_str)
            except Exception:
                # If dtype conversion fails, keep original
                pass
    
    return df
