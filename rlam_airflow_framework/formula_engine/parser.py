# File: rlam_airflow_framework/formula_engine/parser.py
"""Syntax translation and parsing utilities for the formula engine."""

import re

def convert_legacy_formula(formula: str) -> str:
    """
    Convert legacy pandas-style formulas to new function-based syntax.

    Conversions:
        - "field.str.upper()" -> "upper(field)"
        - "field.str.lower()" -> "lower(field)"
        - "pd.Timestamp.now()" -> "now()"
        - "field.dt.year" -> "year(field)"
    """
    if "pd.Timestamp.now()" in formula or "pd.Timestamp.today()" in formula:
        formula = formula.replace("pd.Timestamp.now()", "now()")
        formula = formula.replace("pd.Timestamp.today()", "today()")

    if "datetime.now()" in formula:
        formula = formula.replace("datetime.now()", "now()")

    upper_match = re.match(r"^(\w+)\.str\.upper\(\)$", formula)
    if upper_match:
        return f"upper({upper_match.group(1)})"

    lower_match = re.match(r"^(\w+)\.str\.lower\(\)$", formula)
    if lower_match:
        return f"lower({lower_match.group(1)})"

    strip_match = re.match(r"^(\w+)\.str\.strip\(\)$", formula)
    if strip_match:
        return f"strip({strip_match.group(1)})"

    dt_year_match = re.match(r"^(\w+)\.dt\.year$", formula)
    if dt_year_match:
        return f"year({dt_year_match.group(1)})"

    dt_month_match = re.match(r"^(\w+)\.dt\.month$", formula)
    if dt_month_match:
        return f"month({dt_month_match.group(1)})"

    dt_day_match = re.match(r"^(\w+)\.dt\.day$", formula)
    if dt_day_match:
        return f"day({dt_day_match.group(1)})"

    np_where_match = re.match(r"^np\.where\((.+)\)$", formula)
    if np_where_match:
        return f"if_else({np_where_match.group(1)})"

    return formula
