# Pattern Discovery Lab v2 — L2 Feature Library

## Purpose

L2 defines the controlled, versioned and point-in-time safe search vocabulary
available to later Pattern Discovery phases.

The key rule is simple: a future pattern may refer only to an exact feature
version and an exact transformation version registered in this library.

L2 does not search patterns and does not inspect outcomes.

## Initial registered feature families

Feature Library v1 starts only with fields present in the current research view
and with already established Scanner_vNext semantics:

- Score, Opportunity, Risk and Confidence
- RS3M, Trend200 and Cycle
- Volatility and Drawdown
- R-code and score status
- TrendOK and LiquidityOK
- stock and crypto market regime
- internal pillar, official cluster and sector

The current latest_scanner.csv schema is exercised by an integration test. A
registered source field therefore cannot silently drift away from the actual
research view.

## Explicitly not registered

Elliott vNext is not part of Feature Library v1.

The current research view does not expose an Elliott field with demonstrably
point-in-time historical lineage suitable for this library. The masterplan
allows Elliott only when it is PIT-safe and methodically approved, so L2 fails
conservative rather than retrofitting it.

External evidence families are also out until separately approved.

## Transformations

Feature Library v1 registers only:

- raw value
- delta over 1, 5 or 10 prior scanner observations
- change direction over 1, 5 or 10 prior scanner observations
- predeclared threshold crossing
- categorical or boolean state transition
- contemporaneous market-regime context

Thresholds are not free parameters.

The initial threshold registry contains Trend200 at 0 and Cycle at 25, 50 and
75. A request such as Cycle crossing 63 is invalid and fails closed.

## PIT availability

Every feature uses OBSERVED_ROW_FIELD availability.

Availability requires that the observation exists on or before the declared
cutoff, the field and value were actually recorded, and any lagged
transformation has enough genuine prior observations for the same entity.

Missing historical values remain unavailable. No modern value is used to fill
an older gap.

The API exposes explicit states including AVAILABLE,
OBSERVATION_AFTER_CUTOFF, SOURCE_FIELD_NOT_RECORDED, SOURCE_VALUE_MISSING,
INSUFFICIENT_PRIOR_OBSERVATIONS, PRIOR_SOURCE_FIELD_NOT_RECORDED and
PRIOR_SOURCE_VALUE_MISSING.

## Versioning and L1 binding

The library has a feature_library_version, deterministic whole-library SHA-256,
exact feature/version identities, per-feature semantic hashes and exact
transformation/version identities.

L2 also requires an L1 Discovery Run to include the exact feature-library JSON
file among its input fingerprints. The gate checks the declared library version,
source path, file SHA-256 and repository bytes.

Changing the library therefore changes the Discovery Run input identity.

## Definition of Done

L2 is complete when:

- every feature has a unique ID, semantic definition and version
- registered current-source fields exist in the real scanner schema
- deltas and lag lengths are predeclared
- threshold crossings use only registered thresholds
- state transitions are type-safe and registered
- market-regime context is explicit
- unknown features and transformations fail closed
- invalid feature/transformation pairs fail closed
- historical missingness remains unavailable
- future observations cannot leak through the cutoff
- lagged transformations require genuine prior observations
- L1 binds to the exact feature-library version and bytes
- Elliott and external evidence remain unregistered until separately approved
- L2 does not reconstruct feature values
- L2 does not run pattern search or alter productive semantics

## Next phase

L3 — Search Engine / Discovery Core.
