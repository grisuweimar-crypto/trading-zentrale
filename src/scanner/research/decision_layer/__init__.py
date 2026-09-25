"""Research-only Interpretation / Decision Layer building blocks.

Phase 7A defines only the typed evidence-admission boundary.  It deliberately
contains no universal stance, portfolio action, hysteresis or order logic.
"""

from .input_contract import (
    ADMISSION_STATES,
    ALLOWED_FAMILIES,
    DecisionInputError,
    build_input_packet,
    summarize_coverage,
    validate_input_packet,
)

__all__ = [
    "ADMISSION_STATES",
    "ALLOWED_FAMILIES",
    "DecisionInputError",
    "build_input_packet",
    "summarize_coverage",
    "validate_input_packet",
]
