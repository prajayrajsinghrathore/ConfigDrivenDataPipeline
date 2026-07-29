# File: rlam_airflow_framework/transformers/base.py
"""
Base abstractions for pipeline transformers: TransformationError and Transformer.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import pandas as pd


class TransformationError(Exception):
    """Custom exception for transformation errors."""

    def __init__(
        self,
        message: str,
        transformation_type: str,
        original_error: Optional[Exception] = None,
    ):
        self.transformation_type = transformation_type
        self.original_error = original_error
        super().__init__(f"[{transformation_type}] {message}")


class Transformer(ABC):
    """
    Strategy interface for one step in the unified ``transformations`` pipeline.

    Each step in the config list is handed, unmodified, to the matching
    ``Transformer.transform`` as ``config`` - implementations read whatever
    keys they need from it (e.g. ``columns``, ``condition``).
    """

    @abstractmethod
    def transform(
        self,
        df: pd.DataFrame,
        config: Dict[str, Any],
        correlation_id: Optional[str] = None,
    ) -> pd.DataFrame:
        raise NotImplementedError
