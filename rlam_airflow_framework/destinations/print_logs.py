# File: rlam_airflow_framework/destinations/print_logs.py
"""Print-logs loader (dev/debug sink that dumps a preview to the task logs)."""

import structlog

from rlam_airflow_framework.destinations.base import DestinationLoader

_log = structlog.get_logger(__name__)


class PrintLogsLoader(DestinationLoader):
    """Dump a preview of the DataFrame to the task logs (dev/debug sink)."""

    dest_type = "print_logs"

    def _write(self, df_path: str, dest_config, ctx):
        max_rows = dest_config.get("max_rows", 10)
        import duckdb
        try:
            res = duckdb.query(f"SELECT count(*) FROM '{df_path}'").fetchone()
            row_count = res[0] if res else 0
        except Exception:
            row_count = 0
            
        _log.info(f"=== DATA OUTPUT ({row_count} total rows) ===")
        try:
            head_df = duckdb.query(f"SELECT * FROM '{df_path}' LIMIT {max_rows}").df()
            _log.info(f"Columns: {list(head_df.columns)}")
            _log.info(f"Data types:\n{head_df.dtypes}")
            _log.info(f"First {max_rows} rows:\n{head_df.to_string()}")
            if row_count > max_rows:
                _log.info(f"... and {row_count - max_rows} more rows")
        except Exception as e:
            _log.error(f"Failed to read parquet for preview: {e}")
            
        _log.info("=== END DATA OUTPUT ===")
        return f"Printed {row_count} rows to logs"
