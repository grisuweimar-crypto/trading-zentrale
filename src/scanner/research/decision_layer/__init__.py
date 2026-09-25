"""Research-only Interpretation / Decision Layer building blocks.

Phase 7A defines typed evidence admission. 7B adds the PIT research dataset and
prospective evidence archive. 7C studies confirmation/conflict topology. 7D maps
that topology into a portfolio-independent Universal Stance. 7E adds research-
only hysteresis while preserving raw stance. 7F is the first position-aware
layer and maps preserved stance/transition context into review-only Portfolio
Action and Swing Management. 7G adds faithful Reliability & Explainability over
those preserved outputs without changing the stance, transition or action. 7H
integrates those preserved states into a private-runtime Depot-Watch without
ranking holdings or generating execution instructions. 7I freezes the
validation/promotion boundary and distinguishes technical shadow readiness from
prospective empirical promotion readiness.
"""

from .input_contract import (
    ADMISSION_STATES,
    ALLOWED_FAMILIES,
    DecisionInputError,
    build_input_packet,
    summarize_coverage,
    validate_input_packet,
)
from .dataset import (
    DATASET_SCHEMA_VERSION,
    DecisionDatasetConfig,
    DecisionDatasetError,
    build_decision_research_dataset,
    run_dataset_build,
    validate_decision_dataset,
)
from .evidence_archive import (
    ARCHIVE_SCHEMA_VERSION,
    EvidenceArchiveError,
    append_prospective_packet,
    load_evidence_archive,
    validate_archive_packets,
    write_normalized_archive,
)
from .conflict_research import (
    SCHEMA_VERSION as CONFLICT_RESEARCH_SCHEMA_VERSION,
    ConflictResearchConfig,
    DecisionConflictResearchError,
    analyze_conflicts,
    attach_timing_topology,
    run_conflict_research,
    timing_topology,
    validate_conflict_report,
)
from .relation_graph import (
    RELATION_GRAPH_SCHEMA_VERSION,
    packet_relation_graph,
)
from .universal_stance import (
    SCHEMA_VERSION as UNIVERSAL_STANCE_SCHEMA_VERSION,
    STANCE_STATES,
    UniversalStanceError,
    compute_universal_stance,
    validate_universal_stance,
)
from .state_transition import (
    SCHEMA_VERSION as STATE_TRANSITION_SCHEMA_VERSION,
    CANDIDATE_DEPTHS,
    DEFAULT_MIN_CONSECUTIVE,
    TRANSITION_STATUSES,
    StateTransitionError,
    build_state_transition_history,
    compare_confirmation_depths,
    validate_state_transition,
)
from .portfolio_action import (
    SCHEMA_VERSION as PORTFOLIO_ACTION_SCHEMA_VERSION,
    POSITION_SCHEMA_VERSION,
    ACTION_STATES,
    POSITION_STATES,
    SWING_CONTEXTS,
    PortfolioActionError,
    compute_portfolio_action,
    validate_portfolio_action,
    validate_position_snapshot,
)
from .reliability_explainability import (
    SCHEMA_VERSION as RELIABILITY_EXPLAINABILITY_SCHEMA_VERSION,
    RELIABILITY_STATES,
    ReliabilityExplainabilityError,
    build_reliability_explanation,
    validate_reliability_explanation,
)
from .promotion_validation import (
    SCHEMA_VERSION as VALIDATION_PROMOTION_SCHEMA_VERSION,
    TRACE_SUMMARY_SCHEMA_VERSION,
    READINESS_STATES,
    PromotionValidationError,
    build_promotion_report,
    run_promotion_review,
    validate_promotion_report,
    validate_shadow_trace_summary,
)
from .depot_watch import (
    SCHEMA_VERSION as DEPOT_WATCH_SCHEMA_VERSION,
    POSITION_BOOK_SCHEMA_VERSION,
    BUNDLE_SCHEMA_VERSION,
    AVAILABILITY_STATES,
    WATCH_STATUSES,
    PRESENTATION_GROUPS,
    DepotWatchError,
    build_depot_watch,
    validate_depot_watch,
    validate_daily_research_snapshot,
    validate_position_book,
    validate_decision_bundle,
    validate_bundle_set,
)

__all__ = [
    "ADMISSION_STATES",
    "ALLOWED_FAMILIES",
    "DecisionInputError",
    "build_input_packet",
    "summarize_coverage",
    "validate_input_packet",
    "DATASET_SCHEMA_VERSION",
    "DecisionDatasetConfig",
    "DecisionDatasetError",
    "build_decision_research_dataset",
    "run_dataset_build",
    "validate_decision_dataset",
    "ARCHIVE_SCHEMA_VERSION",
    "EvidenceArchiveError",
    "append_prospective_packet",
    "load_evidence_archive",
    "validate_archive_packets",
    "write_normalized_archive",
    "CONFLICT_RESEARCH_SCHEMA_VERSION",
    "ConflictResearchConfig",
    "DecisionConflictResearchError",
    "analyze_conflicts",
    "attach_timing_topology",
    "run_conflict_research",
    "timing_topology",
    "validate_conflict_report",
    "RELATION_GRAPH_SCHEMA_VERSION",
    "packet_relation_graph",
    "UNIVERSAL_STANCE_SCHEMA_VERSION",
    "STANCE_STATES",
    "UniversalStanceError",
    "compute_universal_stance",
    "validate_universal_stance",
    "STATE_TRANSITION_SCHEMA_VERSION",
    "CANDIDATE_DEPTHS",
    "DEFAULT_MIN_CONSECUTIVE",
    "TRANSITION_STATUSES",
    "StateTransitionError",
    "build_state_transition_history",
    "compare_confirmation_depths",
    "validate_state_transition",
    "PORTFOLIO_ACTION_SCHEMA_VERSION",
    "POSITION_SCHEMA_VERSION",
    "ACTION_STATES",
    "POSITION_STATES",
    "SWING_CONTEXTS",
    "PortfolioActionError",
    "compute_portfolio_action",
    "validate_portfolio_action",
    "validate_position_snapshot",
    "RELIABILITY_EXPLAINABILITY_SCHEMA_VERSION",
    "RELIABILITY_STATES",
    "ReliabilityExplainabilityError",
    "build_reliability_explanation",
    "validate_reliability_explanation",
    "VALIDATION_PROMOTION_SCHEMA_VERSION",
    "TRACE_SUMMARY_SCHEMA_VERSION",
    "READINESS_STATES",
    "PromotionValidationError",
    "build_promotion_report",
    "run_promotion_review",
    "validate_promotion_report",
    "validate_shadow_trace_summary",
    "DEPOT_WATCH_SCHEMA_VERSION",
    "POSITION_BOOK_SCHEMA_VERSION",
    "BUNDLE_SCHEMA_VERSION",
    "AVAILABILITY_STATES",
    "WATCH_STATUSES",
    "PRESENTATION_GROUPS",
    "DepotWatchError",
    "build_depot_watch",
    "validate_depot_watch",
    "validate_daily_research_snapshot",
    "validate_position_book",
    "validate_decision_bundle",
    "validate_bundle_set",
]
