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
    from soda.scan import Scan  # pyright: ignore[reportMissingImports]

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
    """Runs checks via Soda Core, falling back to an error result on failure."""

    def __init__(
        self, source_name: str, quality_gates: Dict[str, Any], soda_checks: Dict[str, Any]
    ):
        super().__init__(source_name, quality_gates)
        self.soda_checks = soda_checks

    def run(
        self, df: pd.DataFrame, results: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
        try:
            scan = Scan()
            scan.set_scan_definition_name(f"{self.source_name}_scan")
            scan.set_data_source_name("pandas_df")

            # Add pandas DataFrame as data source
            scan.add_pandas_dataframe(dataset_name=self.source_name, pandas_df=df)

            # Build SodaCL checks from config
            sodacl_yaml = self._build_sodacl_yaml()
            if sodacl_yaml:
                scan.add_sodacl_yaml_str(sodacl_yaml)

            # Execute scan
            scan.execute()

            # Process results
            scan_results = scan.get_scan_results()

            # Extract check outcomes
            for check in scan_results.get("checks", []):
                check_result = {
                    "name": check.get("name", "unknown"),
                    "outcome": check.get("outcome", "unknown"),
                    "column": check.get("column"),
                    "diagnostics": check.get("diagnostics", {}),
                }
                results["checks"].append(check_result)

                if check_result["outcome"] == "pass":
                    results["passed"] += 1
                elif check_result["outcome"] == "fail":
                    results["failed"] += 1
                elif check_result["outcome"] == "warn":
                    results["warnings"] += 1

            # Calculate pass rate
            total_checks = results["passed"] + results["failed"] + results["warnings"]
            if total_checks > 0:
                results["pass_rate"] = results["passed"] / total_checks

            # Determine overall status
            results["status"] = self._determine_status(results)

            # Get invalid row indices from failed row-level checks
            invalid_indices = self._extract_invalid_indices(scan_results)

            # Split valid and invalid records
            if invalid_indices:
                invalid_df = df.loc[df.index.isin(invalid_indices)].copy()
                valid_df = df.loc[~df.index.isin(invalid_indices)].copy()
            else:
                valid_df = df.copy()
                invalid_df = pd.DataFrame()

            results["valid_rows"] = len(valid_df)
            results["invalid_rows"] = len(invalid_df)

            return valid_df, invalid_df, results

        except Exception as e:
            log.error("Soda scan failed", error=str(e))
            results["status"] = "error"
            results["error"] = str(e)
            return df, pd.DataFrame(), results

    def _build_sodacl_yaml(self) -> Optional[str]:
        """
        Build SodaCL YAML from config.

        Each check renders itself via its strategy's ``to_sodacl`` (checks with
        no SodaCL form, or missing a required column, contribute no line).
        """
        checks = self.soda_checks.get("checks", [])
        if not checks:
            return None

        sodacl_lines = [f"checks for {self.source_name}:"]

        for check in checks:
            strategy = QUALITY_CHECK_REGISTRY.get(check.get("type"))
            if strategy is None:
                continue
            line = strategy.to_sodacl(check)
            if line:
                sodacl_lines.append(line)

        return "\n".join(sodacl_lines)

    def _extract_invalid_indices(self, scan_results: Dict[str, Any]) -> List[int]:
        """
        Extract indices of invalid rows from Soda scan results.
        """
        # This would need to parse Soda's diagnostic output
        # For now, return empty list (Soda doesn't always return row-level details)
        return []


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
