# File: rlam_airflow_framework/transformers/enrichments/__init__.py
"""
Enrichment transformer strategies - reference-data lookups exposed as
regular ``Transformer`` implementations, indistinguishable from any other
pipeline step to the orchestrator.

Future sinks (SQL Server, Dataverse, ...) land here as sibling modules and
get registered in ``transformers.factory``.
"""

from rlam_airflow_framework.transformers.enrichments.snowflake import (
    SnowflakeLookupTransformer,
)

__all__ = ["SnowflakeLookupTransformer"]
