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
from .sec_history import (
    assemble_full_submission_history,
    companyfacts_accession_coverage,
    historical_submission_file_specs,
    historical_submission_file_url,
)

__all__ = [
    "assemble_full_submission_history",
    "build_acceptance_index",
    "classify_publication_stage",
    "companyfacts_accession_coverage",
    "companyfacts_rows",
    "compute_valid_from",
    "filing_candidate_events",
    "historical_submission_file_specs",
    "historical_submission_file_url",
    "normalize_cik",
    "submission_rows",
]
