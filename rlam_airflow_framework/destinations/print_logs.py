# File: rlam_airflow_framework/destinations/print_logs.py
"""Print-logs loader (dev/debug sink that dumps a preview to the task logs)."""

import structlog

from rlam_airflow_framework.destinations.base import DestinationLoader

_log = structlog.get_logger(__name__)


class PrintLogsLoader(DestinationLoader):
    """Dump a preview of the DataFrame to the task logs (dev/debug sink)."""

    dest_type = "print_logs"

    def _write(self, df, dest_config, ctx):
        max_rows = dest_config.get("max_rows", 10)
        _log.info(f"=== DATA OUTPUT ({len(df)} total rows) ===")
        _log.info(f"Columns: {list(df.columns)}")
        _log.info(f"Data types:\n{df.dtypes}")
        _log.info(f"First {max_rows} rows:\n{df.head(max_rows).to_string()}")
        if len(df) > max_rows:
            _log.info(f"... and {len(df) - max_rows} more rows")
        _log.info("=== END DATA OUTPUT ===")
        return f"Printed {len(df)} rows to logs"
