"""Pattern Discovery Lab v2 research-only bounded context.

L0 provides the hard research boundary. L1 adds deterministic pre-registration
and immutable Discovery Run manifests. L2 adds the versioned PIT-safe Feature
Library. Later phases implement actual search, candidate freeze, prospective
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
]
