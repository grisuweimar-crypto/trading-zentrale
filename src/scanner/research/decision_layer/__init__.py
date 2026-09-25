"""Research-only Interpretation / Decision Layer building blocks.

Phase 7A defines the typed evidence-admission boundary. Phase 7B adds the
point-in-time Decision Research Dataset plus a separate prospective typed-
evidence archive. Phase 7C studies confirmation/conflict topology without
resolving it into a universal stance, portfolio action, hysteresis state or
order instruction.
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
]
