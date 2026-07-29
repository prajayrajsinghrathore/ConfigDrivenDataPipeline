# File: rlam_airflow_framework/data_quality/engines.py
"""
Validation engines (Strategy pattern) for DataQualityChecker.

Each engine implements one way of evaluating a DataFrame against configured
rules: Soda Core, the built-in check registry, or legacy ``validation_rules``.
``DataQualityChecker`` (in ``checker.py``) selects an engine and delegates
``run()`` to it; it does not know how any given engine works internally.
"""

import pandas as pd
from typing import Dict, Any, List, Optional, Tuple, cast
import structlog

try:
    from soda_core.contracts import verify_contract_locally
    from soda_duckdb import DuckDBDataSource
    SODA_AVAILABLE = True
except ImportError:
    SODA_AVAILABLE = False

from rlam_airflow_framework.data_quality.registry import QUALITY_CHECK_REGISTRY

log = structlog.get_logger(__name__)


class ValidationEngine:
    """Base interface for a DQ validation engine."""

    def __init__(self, source_name: str, quality_gates: Dict[str, Any]):
        self.source_name = source_name
        self.quality_gates = quality_gates

    def run(
        self, df: pd.DataFrame, results: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
        raise NotImplementedError

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
    def __init__(self, source_name: str, quality_gates: Dict[str, Any], soda_checks: Dict[str, Any]):
        super().__init__(source_name, quality_gates)
        self.soda_checks = soda_checks

    def run_on_path(self, df_path: str, results: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
        """
        Verify a Soda contract directly against a Parquet file via DuckDB.

        Not named ``run`` because its signature (a file path in, path-pair out)
        is incompatible with ``ValidationEngine.run``'s DataFrame contract;
        ``checker.py`` isinstance-checks this engine and calls it explicitly
        rather than dispatching polymorphically through the base method.
        """
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

        contract_file = tempfile.NamedTemporaryFile(delete=False, suffix=".yml", mode="w", encoding="utf-8")
        contract_file.write(sodacl_yaml)
        contract_file.close()

        try:
            # Create transient in-memory connection
            conn = duckdb.connect(':memory:')
            view_name = "dq_candidate"
            
            # Map Parquet to DuckDB View
            conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{df_path}')")
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
        column_checks = {} # col_name -> list of checks

        for check in checks:
            check_type = check.get("type")
            col = check.get("column")
            if check_type == "row_count":
                min_val = check.get("min", 0)
                dataset_checks.append("  - row_count:")
                dataset_checks.append(f"      must_be_greater_than_or_equal_to: {min_val}")
            elif check_type == "missing_count" and col:
                max_missing = check.get("max", 0)
                if col not in column_checks:
                    column_checks[col] = []
                column_checks[col].append("      - missing:")
                column_checks[col].append(f"          must_be_less_than_or_equal_to: {max_missing}")
            elif check_type == "duplicate_count" and col:
                max_dup = check.get("max", 0)
                if col not in column_checks:
                    column_checks[col] = []
                column_checks[col].append("      - duplicate:")
                column_checks[col].append(f"          must_be_less_than_or_equal_to: {max_dup}")
            elif check_type == "invalid_percent" and col:
                regex = check.get("valid_regex")
                if regex:
                    max_percent = check.get("max", 0)
                    if col not in column_checks:
                        column_checks[col] = []
                    column_checks[col].append("      - invalid:")
                    column_checks[col].append(f"          must_be_less_than_or_equal_to: {max_percent}")
                    column_checks[col].append(f"          valid regex: {regex}")

        columns_yaml = ["columns:"]
        if df_path:
            import duckdb
            try:
                with duckdb.connect(':memory:') as conn:
                    schema = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{df_path}')").fetchall()
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
                log.warning("Failed to extract schema for Soda 4 contract", error=str(e))
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


class BasicEngine(ValidationEngine):
    """
    Evaluates checks via the check registry when Soda Core is not available.
    Implements common SodaCL check patterns manually.
    """

    def __init__(
        self, source_name: str, quality_gates: Dict[str, Any], soda_checks: Dict[str, Any]
    ):
        super().__init__(source_name, quality_gates)
        self.soda_checks = soda_checks

    def run(
        self, df: pd.DataFrame, results: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
        invalid_mask = pd.Series([False] * len(df), index=df.index)

        checks_config = self.soda_checks.get("checks", [])

        log.info("::group::Data Quality Validation")
        for check in checks_config:
            check_result = self._execute_basic_check(df, check, invalid_mask)
            results["checks"].append(check_result)

            if check_result["outcome"] == "pass":
                results["passed"] += 1
            elif check_result["outcome"] == "fail":
                results["failed"] += 1
        log.info("::endgroup::")

        # Calculate pass rate
        total_checks = results["passed"] + results["failed"]
        if total_checks > 0:
            results["pass_rate"] = results["passed"] / total_checks

        results["status"] = self._determine_status(results)

        # Split valid and invalid
        valid_df = cast(pd.DataFrame, df[~invalid_mask].copy())
        invalid_df = cast(pd.DataFrame, df[invalid_mask].copy())

        results["valid_rows"] = len(valid_df)
        results["invalid_rows"] = len(invalid_df)

        return valid_df, invalid_df, results

    def _execute_basic_check(
        self, df: pd.DataFrame, check: Dict[str, Any], invalid_mask: pd.Series
    ) -> Dict[str, Any]:
        """
        Execute a single basic data quality check via the check registry.

        Dispatch is polymorphic: the registry resolves the config ``type`` to a
        QualityCheck strategy. Unknown types (and column-requiring checks with no
        column configured) are skipped and reported as a pass, matching the
        legacy dispatch that had no ``else`` branch. Any evaluation error is
        captured as an "error" outcome. Rows flagged by the check are OR-combined
        into the shared ``invalid_mask``.
        """
        check_name = check.get("name", "unnamed_check")
        check_type = check.get("type", "custom")
        column = check.get("column")

        result: Dict[str, Any] = {
            "name": check_name,
            "type": check_type,
            "column": column,
            "outcome": "pass",
            "diagnostics": {},
        }

        strategy = QUALITY_CHECK_REGISTRY.get(check_type)
        if strategy is None or (strategy.requires_column and not column):
            return result

        try:
            outcome = strategy.evaluate(df, check)
            result["outcome"] = outcome.outcome
            result["diagnostics"] = outcome.diagnostics
            if outcome.invalid_rows is not None:
                invalid_mask |= outcome.invalid_rows
        except Exception as e:
            log.warning("Check failed with error", check_name=check_name, error=str(e))
            result["outcome"] = "error"
            result["diagnostics"] = {**result.get("diagnostics", {}), "error": str(e)}

        return result


class LegacyEngine(ValidationEngine):
    """Runs the old ``validation_rules`` config section."""

    def __init__(
        self,
        source_name: str,
        quality_gates: Dict[str, Any],
        validation_rules: List[Dict[str, Any]],
    ):
        super().__init__(source_name, quality_gates)
        self.validation_rules = validation_rules

    def run(
        self, df: pd.DataFrame, results: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
        if not self.validation_rules:
            results["status"] = "skipped"
            return df, pd.DataFrame(), results

        invalid_mask = pd.Series([False] * len(df), index=df.index)

        for rule in self.validation_rules:
            field = rule.get("field")
            rule_type = rule.get("type")

            if field not in df.columns:
                if rule.get("required", False):
                    results["checks"].append(
                        {
                            "name": f"required_{field}",
                            "outcome": "fail",
                            "diagnostics": {
                                "error": f"Required field {field} not found"
                            },
                        }
                    )
                    results["failed"] += 1
                continue

            check_result = {
                "name": f"{rule_type}_{field}",
                "column": field,
                "outcome": "pass",
                "diagnostics": {},
            }

            if rule_type == "numeric" and "range" in rule:
                min_val, max_val = rule["range"]
                invalid_rows = (df[field] < min_val) | (df[field] > max_val)
                if invalid_rows.any():
                    check_result["outcome"] = "fail"
                    check_result["diagnostics"] = {
                        "invalid_count": int(invalid_rows.sum())
                    }
                    invalid_mask |= invalid_rows

            elif rule_type == "string" and "max_length" in rule:
                max_len = rule["max_length"]
                invalid_rows = df[field].astype(str).str.len() > max_len
                if invalid_rows.any():
                    check_result["outcome"] = "fail"
                    check_result["diagnostics"] = {
                        "invalid_count": int(invalid_rows.sum())
                    }
                    invalid_mask |= invalid_rows

            results["checks"].append(check_result)
            if check_result["outcome"] == "pass":
                results["passed"] += 1
            else:
                results["failed"] += 1

        total_checks = results["passed"] + results["failed"]
        if total_checks > 0:
            results["pass_rate"] = results["passed"] / total_checks

        results["status"] = self._determine_status(results)

        valid_df = cast(pd.DataFrame, df[~invalid_mask].copy())
        invalid_df = cast(pd.DataFrame, df[invalid_mask].copy())

        results["valid_rows"] = len(valid_df)
        results["invalid_rows"] = len(invalid_df)

        return valid_df, invalid_df, results
