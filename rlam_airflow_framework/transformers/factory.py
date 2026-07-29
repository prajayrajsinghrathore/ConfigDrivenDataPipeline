# File: rlam_airflow_framework/transformers/factory.py
"""
Factory + pipeline runner for the unified ``transformations`` list.

Adding a new step type = add a ``Transformer`` subclass + register it in
``TRANSFORMER_REGISTRY``; call sites (``taskflow_tasks.transform_data``)
never branch on step type themselves.
"""

import time
from typing import Any, Dict, List, Optional, Type

import pandas as pd
import structlog

from rlam_airflow_framework.transformers.aggregation import AggregationTransformer
from rlam_airflow_framework.transformers.base import Transformer
from rlam_airflow_framework.transformers.enrichments.snowflake import (
    SnowflakeLookupTransformer,
)
from rlam_airflow_framework.transformers.filter import FilterTransformer
from rlam_airflow_framework.transformers.formula import FormulaTransformer
from rlam_airflow_framework.transformers.type_cast import TypeCastTransformer
from rlam_airflow_framework.utils.validation import validate_dataframe

log = structlog.get_logger(__name__)

TRANSFORMER_REGISTRY: Dict[str, Type[Transformer]] = {
    "type_cast": TypeCastTransformer,
    "formula": FormulaTransformer,
    "filter": FilterTransformer,
    "aggregation": AggregationTransformer,
    "snowflake_lookup": SnowflakeLookupTransformer,
}


def get_transformer(step_type: str) -> Transformer:
    """
    Resolve a ``transformations[].type`` to its Transformer instance.

    Raises:
        ValueError: If no transformer is registered for ``step_type``.
    """
    transformer_cls = TRANSFORMER_REGISTRY.get(step_type)
    if transformer_cls is None:
        raise ValueError(
            f"Unsupported transformation type: {step_type}. "
            f"Supported: {', '.join(sorted(TRANSFORMER_REGISTRY))}"
        )
    return transformer_cls()


def apply_pipeline_transformations(
    df: pd.DataFrame,
    transformations_list: List[Dict[str, Any]],
    correlation_id: Optional[str] = None,
) -> pd.DataFrame:
    """
    Run ``transformations_list`` sequentially against ``df``.

    Each step is a dict with a ``type`` key selecting the Transformer and any
    other keys the transformer itself reads (e.g. ``columns``, ``condition``).
    """
    trace_id = correlation_id or f"transform-{int(time.time() * 1000)}"

    df = validate_dataframe(df, "pipeline transformation")

    if df.empty:
        log.warning("Empty DataFrame provided for transformation", trace_id=trace_id)
        return df.copy()

    if not transformations_list:
        log.info(
            "No transformations specified, returning copy of input", trace_id=trace_id
        )
        return df.copy()

    log.info(
        "Starting pipeline transformations",
        trace_id=trace_id,
        row_count=len(df),
        step_count=len(transformations_list),
    )
    start_time = time.time()

    result_df = df.copy()
    for step in transformations_list:
        step_type = step.get("type")
        if not step_type:
            raise ValueError(f"Transformation step missing required 'type' key: {step}")
        transformer = get_transformer(step_type)
        result_df = transformer.transform(result_df, step, trace_id)

    elapsed = time.time() - start_time
    log.info(
        "Pipeline transformations complete",
        trace_id=trace_id,
        input_rows=len(df),
        output_rows=len(result_df),
        elapsed_seconds=round(elapsed, 2),
    )

    return result_df
