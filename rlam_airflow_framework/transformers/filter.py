# File: rlam_airflow_framework/transformers/filter.py
"""FilterTransformer - row filtering via the safe formula engine."""

from typing import Any, Dict, Optional, cast

import pandas as pd
import structlog

from rlam_airflow_framework.formula_engine import FormulaError, get_formula_engine
from rlam_airflow_framework.transformers.base import Transformer, TransformationError

log = structlog.get_logger(__name__)

formula_engine = get_formula_engine()


class FilterTransformer(Transformer):
    """Keeps rows matching a boolean formula-engine expression.

    Step config: ``{type: filter, condition: "quantity > 0"}``. Chain multiple
    ``filter`` steps in the ``transformations`` list to apply several conditions.
    """

    def transform(
        self,
        df: pd.DataFrame,
        config: Dict[str, Any],
        correlation_id: Optional[str] = None,
    ) -> pd.DataFrame:
        condition = config.get("condition")
        if not condition:
            return df

        original_count = len(df)

        try:
            mask = formula_engine.evaluate_column(df, condition)
            df = cast(pd.DataFrame, df[mask])
        except FormulaError as e:
            log.error(
                "Filter evaluation failed",
                trace_id=correlation_id,
                condition=condition,
                error=str(e),
            )
            raise TransformationError(
                f"Failed to apply filter '{condition}': {e}", "filter", e
            ) from e

        log.debug(
            "Filter applied",
            trace_id=correlation_id,
            condition=condition,
            original_count=original_count,
            filtered_count=len(df),
        )

        return df
