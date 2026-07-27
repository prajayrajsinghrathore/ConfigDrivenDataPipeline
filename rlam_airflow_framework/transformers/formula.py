# File: rlam_airflow_framework/transformers/formula.py
"""FormulaTransformer - new columns computed via the safe formula engine."""

from typing import Any, Dict, Optional

import pandas as pd
import structlog

from rlam_airflow_framework.formula_engine import FormulaError, get_formula_engine
from rlam_airflow_framework.transformers.base import Transformer

log = structlog.get_logger(__name__)

formula_engine = get_formula_engine()


class FormulaTransformer(Transformer):
    """Adds/overwrites columns using formula-engine expressions.

    Step config: ``{type: formula, columns: {col_name: formula}}``.
    """

    def transform(
        self,
        df: pd.DataFrame,
        config: Dict[str, Any],
        correlation_id: Optional[str] = None,
    ) -> pd.DataFrame:
        columns: Dict[str, str] = config.get("columns", {})

        for col_name, formula in columns.items():
            try:
                df[col_name] = formula_engine.evaluate_column(df, formula)
                log.debug(
                    "Created column using formula",
                    trace_id=correlation_id,
                    column=col_name,
                    formula=formula,
                )
            except FormulaError as e:
                log.error(
                    "Failed to create column",
                    trace_id=correlation_id,
                    column=col_name,
                    error=str(e),
                )
                # Store error indicator instead of error string to avoid downstream issues
                df[col_name] = None
                raise

        return df
