# File: rlam_airflow_framework/formula_engine/security.py
"""Security and DoS protection for the formula engine."""

import os
import signal
from contextlib import contextmanager

from rlam_airflow_framework.formula_engine.errors import FormulaError, FormulaTimeoutError

# Environment variables take precedence, then fall back to defaults.
MAX_FORMULA_LENGTH = int(os.getenv("FORMULA_MAX_LENGTH", "10240"))  # 10KB default
MAX_NESTING_DEPTH = int(os.getenv("FORMULA_MAX_NESTING", "10"))
FORMULA_TIMEOUT_SECONDS = float(
    os.getenv("TIMEOUT_FORMULA_EVALUATION", os.getenv("FORMULA_TIMEOUT", "5.0"))
)

@contextmanager
def timeout_context(seconds: float, formula: str):
    """
    Context manager for timeout protection (Unix only).
    On Windows, this is a no-op due to signal limitations.
    """
    if os.name == "nt":  # Windows doesn't support SIGALRM
        yield
        return

    def timeout_handler(signum, frame):
        raise FormulaTimeoutError(
            f"Formula evaluation timed out after {seconds} seconds: {formula[:100]}..."
        )

    # Set the signal handler
    old_handler = signal.signal(getattr(signal, "SIGALRM", 14), timeout_handler)  # type: ignore
    if hasattr(signal, "setitimer"):
        signal.setitimer(getattr(signal, "ITIMER_REAL", 0), seconds)  # type: ignore

    try:
        yield
    finally:
        if hasattr(signal, "setitimer"):
            signal.setitimer(getattr(signal, "ITIMER_REAL", 0), 0)  # type: ignore
        signal.signal(getattr(signal, "SIGALRM", 14), old_handler)  # type: ignore

def validate_formula(
    formula: str,
    max_length: int = MAX_FORMULA_LENGTH,
    max_nesting: int = MAX_NESTING_DEPTH,
) -> None:
    """
    Validate formula for security constraints (DoS protection).

    Raises:
        FormulaError: If the formula violates security constraints
    """
    if len(formula) > max_length:
        raise FormulaError(
            f"Formula exceeds maximum length of {max_length} characters "
            f"(got {len(formula)})"
        )

    depth = 0
    max_depth = 0
    for char in formula:
        if char == "(":
            depth += 1
            max_depth = max(max_depth, depth)
        elif char == ")":
            depth -= 1

    if max_depth > max_nesting:
        raise FormulaError(
            f"Formula exceeds maximum nesting depth of {max_nesting} "
            f"(got {max_depth})"
        )

    if depth != 0:
        raise FormulaError("Unbalanced parentheses in formula")
