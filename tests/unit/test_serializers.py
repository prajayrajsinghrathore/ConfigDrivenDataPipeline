import pytest
import polars as pl
from rlam_airflow_framework import serializers

from typing import cast


def _serialize(o):
    res = serializers.serialize(o)
    return cast(dict, res[0]), res[1], res[2], res[3]


class TestDataFrameSerializer:
    def test_serialize_simple_dataframe(self):
        df = pl.DataFrame({"int_col": [1, 2, 3], "str_col": ["a", "b", "c"]})
        result, classname, version, is_serialized = _serialize(df)
        assert is_serialized is True
        assert version == 1
        assert "polars" in classname
        assert "DataFrame" in classname
        assert "data" in result

    def test_serialize_empty_dataframe(self):
        df = pl.DataFrame()
        result, classname, version, is_serialized = _serialize(df)
        assert is_serialized is True
        assert "data" in result

    def test_serialize_dataframe_with_nulls(self):
        df = pl.DataFrame({"col1": [1, None, 3], "col2": ["a", "b", None]})
        result, classname, version, is_serialized = _serialize(df)
        assert is_serialized is True
        assert "data" in result

    def test_serialize_non_dataframe_returns_false(self):
        result, classname, version, is_serialized = _serialize({"not": "a dataframe"})
        assert is_serialized is False


class TestDataFrameDeserializer:
    def test_deserialize_simple_dataframe(self):
        df = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        serialized, _, version, _ = _serialize(df)
        result = serializers.deserialize(pl.DataFrame, version, serialized)
        assert isinstance(result, pl.DataFrame)
        assert result.shape == df.shape

    def test_deserialize_empty_dataframe(self):
        df = pl.DataFrame()
        serialized, _, version, _ = _serialize(df)
        result = serializers.deserialize(pl.DataFrame, version, serialized)
        assert isinstance(result, pl.DataFrame)
        assert result.is_empty()

    def test_deserialize_incompatible_version(self):
        with pytest.raises(TypeError, match=">"):
            serializers.deserialize(pl.DataFrame, 999, {"data": []})

    def test_deserialize_wrong_class(self):
        with pytest.raises(TypeError, match="do not know how to deserialize"):
            serializers.deserialize(dict, 1, {"data": []})


class TestSerializerRegistration:
    def test_serializers_registered(self):
        assert any("DataFrame" in s for s in serializers.serializers)
        assert any("DataFrame" in s for s in serializers.deserializers)
