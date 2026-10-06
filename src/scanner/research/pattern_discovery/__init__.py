"""Pattern Discovery Lab v2 research-only bounded context.

Phase L0 exposes only boundary validation. Discovery search, candidate freeze,
prospective confirmation, rating and promotion are implemented in later phases.
"""

from .boundary import (
    BoundaryViolation,
    PatternDiscoveryBoundary,
    boundary_contract_hash,
    load_boundary_contract,
)

__all__ = [
    "BoundaryViolation",
    "PatternDiscoveryBoundary",
    "boundary_contract_hash",
    "load_boundary_contract",
]
