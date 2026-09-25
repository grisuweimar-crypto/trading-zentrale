"""External-evidence research helpers for Phase 8.

This package is intentionally separate from Selection, Timing, Probability,
Risk, Confidence, Elliott and the Phase-7 Decision Layer.
"""

from .sec_edgar import (
    build_acceptance_index,
    classify_publication_stage,
    companyfacts_rows,
    compute_valid_from,
    filing_candidate_events,
    normalize_cik,
    submission_rows,
)

__all__ = [
    "build_acceptance_index",
    "classify_publication_stage",
    "companyfacts_rows",
    "compute_valid_from",
    "filing_candidate_events",
    "normalize_cik",
    "submission_rows",
]
