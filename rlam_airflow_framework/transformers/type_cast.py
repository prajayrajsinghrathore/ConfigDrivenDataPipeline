# File: rlam_airflow_framework/transformers/type_cast.py
"""TypeCastTransformer - column dtype conversions."""

from typing import Any, Dict, Optional, cast

import pandas as pd
import structlog

from rlam_airflow_framework.transformers.base import Transformer, TransformationError

log = structlog.get_logger(__name__)


class TypeCastTransformer(Transformer):
    """Converts columns to the requested dtype.

    Step config: ``{type: type_cast, columns: {col: dtype}}`` where ``dtype``
    is one of ``datetime``, ``float``, ``int``, ``string``, or any dtype name
    accepted by ``Series.astype``.
    """

    def transform(
        self,
        df: pd.DataFrame,
        config: Dict[str, Any],
        correlation_id: Optional[str] = None,
    ) -> pd.DataFrame:
        columns: Dict[str, str] = config.get("columns", {})

        for col, dtype in columns.items():
            if col not in df.columns:
                log.warning(
                    "Column not found for type conversion, skipping",
                    trace_id=correlation_id,
                    column=col,
                )
                continue

            try:
                if dtype == "datetime":
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                elif dtype == "float":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif dtype == "int":
                    # pd.to_numeric's overloads type a Series input as returning a
                    # scalar-or-Series union; a column is always a Series here.
                    numeric_col = cast(
                        pd.Series, pd.to_numeric(df[col], errors="coerce")
                    )
                    df[col] = numeric_col.astype("Int64")  # Nullable int
                elif dtype == "string":
                    df[col] = df[col].astype(str)
                else:
                    df[col] = df[col].astype(dtype)  # type: ignore

                log.debug(
                    "Converted column",
                    trace_id=correlation_id,
                    column=col,
                    dtype=dtype,
                )

            except Exception as e:
                log.error(
                    "Failed to convert column",
                    trace_id=correlation_id,
                    column=col,
                    dtype=dtype,
                    error=str(e),
                )
                raise TransformationError(
                    f"Failed to convert column '{col}' to {dtype}: {e}",
                    "type_cast",
                    e,
                ) from e

        return df
