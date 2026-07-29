# File: rlam_airflow_framework/transformers/__init__.py
"""
Transformer strategies - Open/Closed dispatch for the unified pipeline
``transformations`` list.

Each step type is a self-contained :class:`Transformer` subclass in its own
module. Manipulations (type_cast, formula, filter, aggregation) and
enrichments (e.g. snowflake_lookup, under ``transformers.enrichments``)
implement the same interface, so they can be freely interleaved in a single
ordered config list. Dispatch is polymorphic via :data:`TRANSFORMER_REGISTRY`.

This package re-exports the full public API, so
``from rlam_airflow_framework.transformers import apply_pipeline_transformations``
(and any transformer class) keeps working regardless of the internal module
layout.

Adding a new step type = add a ``Transformer`` subclass module and register
it in ``factory.py``; no existing code changes (Open/Closed Principle).
"""

from rlam_airflow_framework.transformers.base import Transformer, TransformationError
from rlam_airflow_framework.transformers.factory import (
    TRANSFORMER_REGISTRY,
    apply_pipeline_transformations,
    get_transformer,
)
from rlam_airflow_framework.transformers.aggregation import AggregationTransformer
from rlam_airflow_framework.transformers.filter import FilterTransformer
from rlam_airflow_framework.transformers.formula import FormulaTransformer
from rlam_airflow_framework.transformers.type_cast import TypeCastTransformer
from rlam_airflow_framework.transformers.enrichments.snowflake import (
    SnowflakeLookupTransformer,
)

__all__ = [
    "Transformer",
    "TransformationError",
    "TRANSFORMER_REGISTRY",
    "apply_pipeline_transformations",
    "get_transformer",
    "AggregationTransformer",
    "FilterTransformer",
    "FormulaTransformer",
    "TypeCastTransformer",
    "SnowflakeLookupTransformer",
]
