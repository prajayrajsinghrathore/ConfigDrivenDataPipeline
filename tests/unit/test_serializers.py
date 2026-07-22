# File: tests/unit/test_serializers.py
"""
Unit tests for custom Airflow serializers.

Tests cover:
- DataFrame serialization and deserialization
- Round-trip data integrity
- Edge cases (empty, nulls, various dtypes)
- Error handling and version compatibility
"""

import pytest
import pandas as pd
import numpy as np

from rlam_airflow_framework import serializers


class TestDataFrameSerializer:
    """Test pandas DataFrame serialization."""
    
    def test_serialize_simple_dataframe(self):
        """Serialize a simple DataFrame with basic types."""
        df = pd.DataFrame({
            "int_col": [1, 2, 3],
            "str_col": ["a", "b", "c"],
            "float_col": [1.1, 2.2, 3.3]
        })
        
        result, classname, version, is_serialized = serializers.serialize(df)
        
        assert is_serialized is True
        assert version == 1
        assert "pandas.DataFrame" in classname
        assert "data" in result
        assert "dtypes" in result
        assert result["dtypes"]["int_col"] == "int64"
        # String columns can be 'object' or 'str' depending on pandas version
        assert result["dtypes"]["str_col"] in ["object", "str", "string"]
        assert result["dtypes"]["float_col"] == "float64"
    
    def test_serialize_empty_dataframe(self):
        """Serialize an empty DataFrame."""
        df = pd.DataFrame()
        
        result, classname, version, is_serialized = serializers.serialize(df)
        
        assert is_serialized is True
        assert "data" in result
        assert result["dtypes"] == {}
    
    def test_serialize_dataframe_with_nulls(self):
        """Serialize DataFrame containing null values."""
        df = pd.DataFrame({
            "col1": [1, None, 3],
            "col2": ["a", "b", None]
        })
        
        result, classname, version, is_serialized = serializers.serialize(df)
        
        assert is_serialized is True
        assert "data" in result
    
    def test_serialize_non_dataframe_returns_false(self):
        """Serializing non-DataFrame objects should return is_serialized=False."""
        obj = {"not": "a dataframe"}
        
        result, classname, version, is_serialized = serializers.serialize(obj)
        
        assert is_serialized is False
        assert result == ""
        assert classname == ""
        assert version == 0
    
    def test_serialize_dataframe_with_datetime(self):
        """Serialize DataFrame with datetime columns."""
        df = pd.DataFrame({
            "date_col": pd.date_range("2024-01-01", periods=3),
            "value": [10, 20, 30]
        })
        
        result, classname, version, is_serialized = serializers.serialize(df)
        
        assert is_serialized is True
        assert "datetime64" in result["dtypes"]["date_col"]
    
    def test_serialize_dataframe_with_index(self):
        """Serialize DataFrame preserving custom index."""
        df = pd.DataFrame(
            {"col1": [1, 2, 3]},
            index=["a", "b", "c"]
        )
        
        result, classname, version, is_serialized = serializers.serialize(df)
        
        assert is_serialized is True
        # split orientation preserves index
        assert "index" in result["data"]


class TestDataFrameDeserializer:
    """Test pandas DataFrame deserialization."""
    
    def test_deserialize_simple_dataframe(self):
        """Deserialize a simple DataFrame."""
        data = {
            "data": {
                "columns": ["col1", "col2"],
                "index": [0, 1, 2],
                "data": [[1, "a"], [2, "b"], [3, "c"]]
            },
            "dtypes": {"col1": "int64", "col2": "object"}
        }
        
        df = serializers.deserialize(pd.DataFrame, 1, data)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ["col1", "col2"]
        assert df["col1"].dtype == np.int64
    
    def test_deserialize_empty_dataframe(self):
        """Deserialize an empty DataFrame."""
        data = {
            "data": {
                "columns": [],
                "index": [],
                "data": []
            },
            "dtypes": {}
        }
        
        df = serializers.deserialize(pd.DataFrame, 1, data)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
    
    def test_deserialize_version_check(self):
        """Should raise error if version is too high."""
        data = {
            "data": {"columns": [], "index": [], "data": []},
            "dtypes": {}
        }
        
        with pytest.raises(TypeError, match="serialized .* > "):
            serializers.deserialize(pd.DataFrame, 999, data)
    
    def test_deserialize_wrong_class(self):
        """Should raise error if wrong class provided."""
        data = {
            "data": {"columns": [], "index": [], "data": []},
            "dtypes": {}
        }
        
        with pytest.raises(TypeError, match="do not know how to deserialize"):
            serializers.deserialize(dict, 1, data)
    
    def test_deserialize_with_nulls(self):
        """Deserialize DataFrame with null values."""
        data = {
            "data": {
                "columns": ["col1", "col2"],
                "index": [0, 1, 2],
                "data": [[1, "a"], [None, "b"], [3, None]]
            },
            "dtypes": {"col1": "float64", "col2": "object"}
        }
        
        df = serializers.deserialize(pd.DataFrame, 1, data)
        
        assert pd.isna(df.loc[1, "col1"])
        assert pd.isna(df.loc[2, "col2"])
    
    def test_deserialize_preserves_index(self):
        """Deserialize preserves custom index."""
        data = {
            "data": {
                "columns": ["value"],
                "index": ["a", "b", "c"],
                "data": [[1], [2], [3]]
            },
            "dtypes": {"value": "int64"}
        }
        
        df = serializers.deserialize(pd.DataFrame, 1, data)
        
        assert list(df.index) == ["a", "b", "c"]


class TestRoundTripSerialization:
    """Test round-trip serialization (serialize then deserialize)."""
    
    def test_round_trip_simple_dataframe(self):
        """Round-trip should preserve data exactly."""
        original_df = pd.DataFrame({
            "int_col": [1, 2, 3],
            "str_col": ["a", "b", "c"],
            "float_col": [1.1, 2.2, 3.3]
        })
        
        # Serialize
        serialized, classname, version, _ = serializers.serialize(original_df)
        
        # Deserialize
        restored_df = serializers.deserialize(pd.DataFrame, version, serialized)
        
        # Compare
        pd.testing.assert_frame_equal(original_df, restored_df)
    
    def test_round_trip_with_nulls(self):
        """Round-trip with nulls should preserve null locations."""
        original_df = pd.DataFrame({
            "col1": [1.0, None, 3.0],
            "col2": ["a", "b", None]
        })
        
        serialized, classname, version, _ = serializers.serialize(original_df)
        restored_df = serializers.deserialize(pd.DataFrame, version, serialized)
        
        # Check nulls are in same positions
        assert pd.isna(restored_df.loc[1, "col1"])
        assert pd.isna(restored_df.loc[2, "col2"])
        assert restored_df.loc[0, "col1"] == 1.0
    
    def test_round_trip_empty_dataframe(self):
        """Round-trip empty DataFrame."""
        original_df = pd.DataFrame()
        
        serialized, classname, version, _ = serializers.serialize(original_df)
        restored_df = serializers.deserialize(pd.DataFrame, version, serialized)
        
        assert len(restored_df) == 0
        assert len(restored_df.columns) == 0
    
    def test_round_trip_with_custom_index(self):
        """Round-trip preserves custom index."""
        original_df = pd.DataFrame(
            {"value": [10, 20, 30]},
            index=["row1", "row2", "row3"]
        )
        
        serialized, classname, version, _ = serializers.serialize(original_df)
        restored_df = serializers.deserialize(pd.DataFrame, version, serialized)
        
        assert list(restored_df.index) == ["row1", "row2", "row3"]
        pd.testing.assert_frame_equal(original_df, restored_df)
    
    def test_round_trip_large_dataframe(self):
        """Round-trip with larger DataFrame."""
        original_df = pd.DataFrame({
            f"col_{i}": range(1000) for i in range(10)
        })
        
        serialized, classname, version, _ = serializers.serialize(original_df)
        restored_df = serializers.deserialize(pd.DataFrame, version, serialized)
        
        pd.testing.assert_frame_equal(original_df, restored_df)
    
    def test_round_trip_mixed_types(self):
        """Round-trip with mixed data types."""
        original_df = pd.DataFrame({
            "int": [1, 2, 3],
            "float": [1.1, 2.2, 3.3],
            "str": ["a", "b", "c"],
            "bool": [True, False, True],
            "datetime": pd.date_range("2024-01-01", periods=3)
        })
        
        serialized, classname, version, _ = serializers.serialize(original_df)
        restored_df = serializers.deserialize(pd.DataFrame, version, serialized)
        
        # Check column types are preserved
        assert restored_df["int"].dtype == original_df["int"].dtype
        assert restored_df["float"].dtype == original_df["float"].dtype
        assert restored_df["bool"].dtype == original_df["bool"].dtype


class TestSerializerRegistration:
    """Test serializer registration metadata."""
    
    def test_serializers_registered(self):
        """Check that DataFrame is in serializers list."""
        assert "pandas.DataFrame" in serializers.serializers
    
    def test_deserializers_registered(self):
        """Check that DataFrame is in deserializers list."""
        assert "pandas.DataFrame" in serializers.deserializers
    
    def test_version_defined(self):
        """Check that version is defined."""
        assert hasattr(serializers, "__version__")
        assert serializers.__version__ == 1


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_dataframe_with_unicode(self):
        """Serialize DataFrame with Unicode strings."""
        df = pd.DataFrame({
            "unicode": ["Hello 世界", "Привет мир", "مرحبا بالعالم"]
        })
        
        serialized, _, version, _ = serializers.serialize(df)
        restored = serializers.deserialize(pd.DataFrame, version, serialized)
        
        pd.testing.assert_frame_equal(df, restored)
    
    def test_dataframe_with_special_characters(self):
        """Serialize DataFrame with special characters in column names."""
        df = pd.DataFrame({
            "col with spaces": [1, 2, 3],
            "col-with-dashes": [4, 5, 6],
            "col_with_underscores": [7, 8, 9]
        })
        
        serialized, _, version, _ = serializers.serialize(df)
        restored = serializers.deserialize(pd.DataFrame, version, serialized)
        
        pd.testing.assert_frame_equal(df, restored)
    
    def test_dataframe_single_row(self):
        """Serialize DataFrame with single row."""
        df = pd.DataFrame({"col1": [1], "col2": ["a"]})
        
        serialized, _, version, _ = serializers.serialize(df)
        restored = serializers.deserialize(pd.DataFrame, version, serialized)
        
        pd.testing.assert_frame_equal(df, restored)
    
    def test_dataframe_single_column(self):
        """Serialize DataFrame with single column."""
        df = pd.DataFrame({"only_col": [1, 2, 3, 4, 5]})
        
        serialized, _, version, _ = serializers.serialize(df)
        restored = serializers.deserialize(pd.DataFrame, version, serialized)
        
        pd.testing.assert_frame_equal(df, restored)
