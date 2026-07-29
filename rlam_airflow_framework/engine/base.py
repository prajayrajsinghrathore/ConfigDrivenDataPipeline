# File: rlam_airflow_framework/engine/base.py
"""
Base interfaces for the Hybrid Pipeline architecture.
Contains core abstractions for Transformations, Sources, and Sinks.
"""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Optional, Any, Mapping

from pydantic import BaseModel

from rlam_airflow_framework.engine.data import DataBackend, ExecutionData
from rlam_airflow_framework.engine.context import ExecutionContext

TConfig = TypeVar("TConfig", bound=BaseModel)


class TransformationStep(Generic[TConfig], ABC):
    """
    An isolated, strictly-typed operation within the pipeline.
    Declares accepted backends and outputs a well-defined backend.
    """
    config_type: type[TConfig]

    @property
    @abstractmethod
    def accepted_backends(self) -> frozenset[DataBackend]:
        """Backends this step can natively process."""
        pass

    @abstractmethod
    def output_backend(self, input_backend: DataBackend, config: TConfig) -> DataBackend:
        """The backend produced when given a specific input backend."""
        pass

    @abstractmethod
    def transform(
        self,
        data: ExecutionData,
        config: TConfig,
        context: ExecutionContext,
    ) -> ExecutionData:
        """Applies the transformation logic to the data wrapper."""
        pass


class SourceSpec(BaseModel):
    path: str
    format: str = "parquet"


class DestinationSpec(BaseModel):
    path: str
    format: str = "parquet"


class DataSource(ABC):
    @abstractmethod
    def load(self, source: SourceSpec, context: ExecutionContext) -> ExecutionData:
        pass


class WriteResult(BaseModel):
    """Operational metadata returned after a successful sink save."""
    destination: str
    row_count: Optional[int]
    byte_count: Optional[int]
    schema_fingerprint: Optional[str]
    metadata: Mapping[str, Any]


class DataSink(ABC):
    @abstractmethod
    def save(
        self,
        data: ExecutionData,
        destination: DestinationSpec,
        context: ExecutionContext,
    ) -> WriteResult:
        pass
