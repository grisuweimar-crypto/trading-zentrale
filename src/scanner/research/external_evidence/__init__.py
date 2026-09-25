"""External-evidence research helpers for Phase 8.

This package is intentionally separate from Selection, Timing, Probability,
Risk, Confidence, Elliott and the Phase-7 Decision Layer.
"""

from .fundamental_change import (
    asof_latest_rows,
    build_free_cash_flow,
    build_operating_margin,
    build_yoy_changes,
    first_release_rows,
    is_pit_usable,
    metric_for_row,
)
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
    "asof_latest_rows",
    "assemble_full_submission_history",
    "build_acceptance_index",
    "build_free_cash_flow",
    "build_operating_margin",
    "build_yoy_changes",
    "classify_publication_stage",
    "companyfacts_accession_coverage",
    "companyfacts_rows",
    "compute_valid_from",
    "filing_candidate_events",
    "first_release_rows",
    "historical_submission_file_specs",
    "historical_submission_file_url",
    "is_pit_usable",
    "metric_for_row",
    "normalize_cik",
    "submission_rows",
]
