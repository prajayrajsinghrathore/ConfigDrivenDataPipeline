# File: rlam_airflow_framework/engine/config.py
"""
Pydantic validation models for transformation steps.
These models ensure structured configuration and act as the type payload for
TransformationStep objects in the pipeline.
"""

from typing import Annotated, Any, Dict, List, Literal, Union
from pydantic import BaseModel, Field


class FormulaStepConfig(BaseModel):
    type: Literal["formula"]
    execution: Literal["sql"] = "sql"
    columns: Dict[str, str]


class FilterStepConfig(BaseModel):
    type: Literal["filter"]
    execution: Literal["sql"] = "sql"
    condition: str


class AggregationStepConfig(BaseModel):
    type: Literal["aggregation"]
    execution: Literal["sql"] = "sql"
    aggregations: Dict[str, List[str]]


class TypeCastStepConfig(BaseModel):
    type: Literal["type_cast"]
    execution: Literal["sql"] = "sql"
    columns: Dict[str, str]


class SnowflakeLookupConfig(BaseModel):
    type: Literal["snowflake_lookup"]
    execution: Literal["sql"] = "sql"
    connection_id: str = "snowflake-default"
    table: str
    join_on: Dict[str, str]
    select_columns: List[str]


class PolarsTransformConfig(BaseModel):
    type: Literal["polars_transform"]
    execution: Literal["dataframe"] = "dataframe"
    handler: str
    requires_eager: bool = False
    parameters: Dict[str, Any] = Field(default_factory=dict)


TransformationConfig = Annotated[
    Union[
        FormulaStepConfig,
        FilterStepConfig,
        AggregationStepConfig,
        TypeCastStepConfig,
        SnowflakeLookupConfig,
        PolarsTransformConfig,
    ],
    Field(discriminator="type"),
]


class PipelineConfig(BaseModel):
    """Overall configuration for the transformation phase."""

    transformations: List[TransformationConfig] = Field(default_factory=list)
