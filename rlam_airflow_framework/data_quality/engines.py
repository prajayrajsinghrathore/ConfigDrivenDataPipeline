# File: rlam_airflow_framework/data_quality/engines.py
"""
Validation engine for DataQualityChecker.

Soda 4 is the only supported validation path: ``SodaEngine`` verifies a
SodaCL contract directly against a Parquet file via DuckDB (out-of-core,
no materialization ever happens). ``DataQualityChecker`` (in
``checker.py``) skips validation entirely when Soda 4 isn't configured or
isn't installed, rather than falling back to a hand-rolled engine.
"""

from typing import Any, Dict, Optional, Tuple
import structlog

try:
    from soda_core.contracts import verify_contract_locally
    from soda_duckdb import DuckDBDataSource

    SODA_AVAILABLE = True
except ImportError:
    SODA_AVAILABLE = False

log = structlog.get_logger(__name__)


class ValidationEngine:
    """Base interface for a DQ validation engine."""

    def __init__(self, source_name: str, quality_gates: Dict[str, Any]):
        self.source_name = source_name
        self.quality_gates = quality_gates

    def _determine_status(self, results: Dict[str, Any]) -> str:
        fail_threshold = self.quality_gates.get("fail_threshold", 0.0)
        warn_threshold = self.quality_gates.get("warn_threshold", 0.95)

        pass_rate = results.get("pass_rate", 1.0)

        if pass_rate < fail_threshold:
            return "failed"
        elif pass_rate < warn_threshold:
            return "warning"
        else:
            return "passed"


class SodaEngine(ValidationEngine):
    def __init__(
        self,
        source_name: str,
        quality_gates: Dict[str, Any],
        soda_checks: Dict[str, Any],
    ):
        super().__init__(source_name, quality_gates)
        self.soda_checks = soda_checks

    def run_on_path(
        self, df_path: str, results: Dict[str, Any]
    ) -> Tuple[str, str, Dict[str, Any]]:
        """Verify a Soda contract directly against a Parquet file via DuckDB."""
        if not SODA_AVAILABLE:
            log.warning("soda-duckdb or soda-core contracts not available")
            results["status"] = "error"
            results["error"] = "soda-core 4.0 not available"
            return df_path, "", results

        import duckdb
        import tempfile
        import os

        # We construct a temporary contract.yaml file to pass to Soda 4
        sodacl_yaml = self._build_sodacl_yaml(df_path=df_path)
        if not sodacl_yaml:
            return df_path, "", results

        contract_file = tempfile.NamedTemporaryFile(
            delete=False, suffix=".yml", mode="w", encoding="utf-8"
        )
        contract_file.write(sodacl_yaml)
        contract_file.close()

        try:
            # Create transient in-memory connection
            conn = duckdb.connect(":memory:")
            view_name = "dq_candidate"

            # Map Parquet to DuckDB View
            conn.execute(
                f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{df_path}')"
            )
            count_row = conn.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()
            row_count = count_row[0] if count_row else 0
            results["total_rows"] = row_count

            # Create Soda DataSource
            data_source = DuckDBDataSource.from_existing_cursor(
                conn.cursor(),
                name="pipeline_duckdb",
            )

            # Verify Contract
            contract_result = verify_contract_locally(
                data_sources=[data_source],
                contract_file_path=contract_file.name,
            )

            if contract_result.is_passed:
                results["status"] = "passed"
                results["pass_rate"] = 1.0
                results["passed"] = len(self.soda_checks.get("checks", []))
            else:
                results["status"] = "failed"
                results["pass_rate"] = 0.0
                results["failed"] = len(self.soda_checks.get("checks", []))

            if results["status"] == "failed":
                failed_conditions = []
                for check in self.soda_checks.get("checks", []):
                    check_type = check.get("type")
                    col = check.get("column")
                    if not col:
                        continue
                    if check_type == "missing_count":
                        failed_conditions.append(f'"{col}" IS NULL')
                    elif check_type == "invalid_percent":
                        regex = check.get("valid_regex")
                        if regex:
                            escaped_regex = regex.replace("'", "''")
                            failed_conditions.append(
                                f"NOT regexp_matches(CAST(\"{col}\" AS VARCHAR), '{escaped_regex}')"
                            )

                if failed_conditions:
                    where_invalid = " OR ".join(failed_conditions)
                    dir_name = os.path.dirname(df_path)
                    base_name = os.path.basename(df_path)
                    valid_path = os.path.join(dir_name, "valid_" + base_name)
                    invalid_path = os.path.join(dir_name, "invalid_" + base_name)

                    conn.execute(
                        f"COPY (SELECT * FROM read_parquet('{df_path}') WHERE NOT ({where_invalid})) TO '{valid_path}' (FORMAT PARQUET)"
                    )
                    conn.execute(
                        f"COPY (SELECT * FROM read_parquet('{df_path}') WHERE ({where_invalid})) TO '{invalid_path}' (FORMAT PARQUET)"
                    )

                    valid_count_res = conn.execute(
                        f"SELECT COUNT(*) FROM read_parquet('{valid_path}')"
                    ).fetchone()
                    valid_count = valid_count_res[0] if valid_count_res else 0

                    invalid_count_res = conn.execute(
                        f"SELECT COUNT(*) FROM read_parquet('{invalid_path}')"
                    ).fetchone()
                    invalid_count = invalid_count_res[0] if invalid_count_res else 0

                    results["valid_rows"] = valid_count
                    results["invalid_rows"] = invalid_count

                    return (
                        valid_path if valid_count > 0 else "",
                        invalid_path if invalid_count > 0 else "",
                        results,
                    )

                results["valid_rows"] = 0
                results["invalid_rows"] = row_count
                return "", df_path, results

            results["valid_rows"] = row_count
            results["invalid_rows"] = 0
            return df_path, "", results

        except Exception as e:
            log.error("Soda 4 scan failed", error=str(e))
            results["status"] = "error"
            results["error"] = str(e)
            return df_path, "", results
        finally:
            if os.path.exists(contract_file.name):
                os.remove(contract_file.name)

    def _build_sodacl_yaml(self, df_path: Optional[str] = None) -> Optional[str]:
        checks = self.soda_checks.get("checks", [])
        if not checks:
            return None

        # Gather dataset checks and column checks
        dataset_checks = []
        column_checks = {}  # col_name -> list of checks

        for check in checks:
            check_type = check.get("type")
            col = check.get("column")
            if check_type == "row_count":
                min_val = check.get("min", 0)
                dataset_checks.append("  - row_count:")
                dataset_checks.append(
                    f"      must_be_greater_than_or_equal_to: {min_val}"
                )
            elif check_type == "missing_count" and col:
                max_missing = check.get("max", 0)
                if col not in column_checks:
                    column_checks[col] = []
                column_checks[col].append("      - missing:")
                column_checks[col].append(
                    f"          must_be_less_than_or_equal_to: {max_missing}"
                )
            elif check_type == "duplicate_count" and col:
                max_dup = check.get("max", 0)
                if col not in column_checks:
                    column_checks[col] = []
                column_checks[col].append("      - duplicate:")
                column_checks[col].append(
                    f"          must_be_less_than_or_equal_to: {max_dup}"
                )
            elif check_type == "invalid_percent" and col:
                regex = check.get("valid_regex")
                if regex:
                    max_percent = check.get("max", 0)
                    if col not in column_checks:
                        column_checks[col] = []
                    column_checks[col].append("      - invalid:")
                    column_checks[col].append(
                        f"          must_be_less_than_or_equal_to: {max_percent}"
                    )
                    column_checks[col].append(f"          valid regex: {regex}")

        columns_yaml = ["columns:"]
        if df_path:
            import duckdb

            try:
                with duckdb.connect(":memory:") as conn:
                    schema = conn.execute(
                        f"DESCRIBE SELECT * FROM read_parquet('{df_path}')"
                    ).fetchall()
                    for row in schema:
                        col_name = row[0]
                        col_type = row[1].lower()
                        columns_yaml.append(f"  - name: {col_name}")
                        columns_yaml.append(f"    data_type: {col_type}")
                        if col_name in column_checks:
                            columns_yaml.append("    checks:")
                            columns_yaml.extend(column_checks[col_name])
                            del column_checks[col_name]
            except Exception as e:
                import structlog

                log = structlog.get_logger(__name__)
                log.warning(
                    "Failed to extract schema for Soda 4 contract", error=str(e)
                )
                columns_yaml = []

        # Any column checks for columns that weren't in schema (or schema failed)
        for col, col_chk_lines in column_checks.items():
            if not columns_yaml:
                columns_yaml = ["columns:"]
            columns_yaml.append(f"  - name: {col}")
            columns_yaml.append("    checks:")
            columns_yaml.extend(col_chk_lines)

        sodacl_lines = ["dataset: pipeline_duckdb/main/dq_candidate"]
        if dataset_checks:
            sodacl_lines.append("checks:")
            sodacl_lines.extend(dataset_checks)

        if len(columns_yaml) > 1:
            sodacl_lines.extend(columns_yaml)

        return "\n".join(sodacl_lines)
