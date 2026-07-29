# File: rlam_airflow_framework/transformers/aggregation.py
"""AggregationTransformer - collapse a DataFrame to a summary row."""

from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

from rlam_airflow_framework.transformers.base import Transformer, TransformationError

log = structlog.get_logger(__name__)


class AggregationTransformer(Transformer):
    """Collapses the DataFrame into a single-row summary.

    Step config: ``{type: aggregation, aggregations: {func: [columns]}}``,
    e.g. ``{sum: [quantity], mean: [price]}`` produces columns
    ``quantity_sum`` and ``price_mean``.
    """

    def transform(
        self,
        df: pd.DataFrame,
        config: Dict[str, Any],
        correlation_id: Optional[str] = None,
    ) -> pd.DataFrame:
        aggregations: Dict[str, List[str]] = config.get("aggregations", {})

        if df.empty:
            log.warning(
                "Cannot apply aggregations to empty DataFrame", trace_id=correlation_id
            )
            return df

        agg_dict: Dict[str, Any] = {}
        for agg_func, columns in aggregations.items():
            for col in columns:
                if col not in df.columns:
                    log.warning(
                        "Column not found for aggregation, skipping",
                        trace_id=correlation_id,
                        column=col,
                    )
                    continue

                try:
                    agg_dict[f"{col}_{agg_func}"] = df[col].agg(agg_func)
                except Exception as e:
                    log.error(
                        "Failed to aggregate column",
                        trace_id=correlation_id,
                        column=col,
                        agg_func=agg_func,
                        error=str(e),
                    )
                    raise TransformationError(
                        f"Failed to aggregate '{col}' with {agg_func}: {e}",
                        "aggregation",
                        e,
                    ) from e

        if agg_dict:
            log.debug(
                "Applied aggregations",
                trace_id=correlation_id,
                aggregations=list(agg_dict.keys()),
            )
            return pd.DataFrame([agg_dict])

        return df
