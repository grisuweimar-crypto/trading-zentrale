"""Pattern Discovery Lab v2 research-only bounded context.

L0 provides the hard research boundary. L1 adds deterministic pre-registration
and immutable Discovery Run manifests. L2 adds the versioned PIT-safe Feature
Library. L3 adds the bounded discovery-only Search Engine. L4 adds the
Statistical Discovery Guard. Later phases add hard freeze, prospective
confirmation, rating and promotion.
"""

from .boundary import (
    BoundaryViolation,
    PatternDiscoveryBoundary,
    boundary_contract_hash,
    load_boundary_contract,
)
from .run_contract import (
    DiscoveryRunContractError,
    build_run_manifest,
    fingerprint_inputs,
    load_run_contract,
    manifest_repo_path,
    normalize_preregistration,
    run_contract_hash,
    verify_run_manifest,
    write_run_manifest,
)
from .feature_library import (
    FeatureLibrary,
    FeatureLibraryError,
    feature_library_hash,
    feature_version_hash,
    load_feature_library,
)
from .search_engine import (
    DiscoverySearchError,
    load_search_contract,
    run_discovery_search,
    search_contract_hash,
    search_result_repo_path,
    verify_search_result,
    write_search_result,
)
from .statistical_guard import (
    DiscoveryGuardError,
    apply_statistical_guard,
    evidence_repo_path,
    guard_contract_hash,
    load_guard_contract,
    verify_statistical_evidence,
    write_statistical_evidence,
)

__all__ = [
    "BoundaryViolation",
    "PatternDiscoveryBoundary",
    "boundary_contract_hash",
    "load_boundary_contract",
    "DiscoveryRunContractError",
    "build_run_manifest",
    "fingerprint_inputs",
    "load_run_contract",
    "manifest_repo_path",
    "normalize_preregistration",
    "run_contract_hash",
    "verify_run_manifest",
    "write_run_manifest",
    "FeatureLibrary",
    "FeatureLibraryError",
    "feature_library_hash",
    "feature_version_hash",
    "load_feature_library",
    "DiscoverySearchError",
    "load_search_contract",
    "run_discovery_search",
    "search_contract_hash",
    "search_result_repo_path",
    "verify_search_result",
    "write_search_result",
    "DiscoveryGuardError",
    "apply_statistical_guard",
    "evidence_repo_path",
    "guard_contract_hash",
    "load_guard_contract",
    "verify_statistical_evidence",
    "write_statistical_evidence",
]
