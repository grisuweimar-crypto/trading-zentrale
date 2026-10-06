# Pattern Discovery Lab v2 — L4 Statistical Discovery Guard

## Purpose

L4 turns raw L3 Discovery candidates into guarded Discovery Evidence.

The phase exists because a high raw hit rate or a large raw sample is not enough
when many patterns were searched and observations overlap in time.

L4 remains research-only. It does not freeze PAT objects, register QM-C
hypotheses, use prospective confirmation, assign lifecycle ratings or change
productive scanner/Decision semantics.

## Required bindings

L4 requires:

- the exact frozen L1 manifest
- the exact L2 Feature Library
- a valid hash-protected L3 result
- the original Discovery observations

Before statistics are accepted, L4 reruns L3 from the frozen inputs and requires
the replayed result hash to equal the supplied L3 result hash.

This prevents a statistical report from being attached to a modified or
selectively edited candidate set.

## Multiple Testing

Every tested L3 candidate enters its complete L3 family, including candidates
that already failed the simple L3 minimum-N/effect gate.

L4 v1 implements the pre-registered L1 methods:

- Bonferroni FWER
- Holm FWER
- Benjamini-Hochberg FDR

CUSTOM_PREDECLARED fails closed because no arbitrary custom statistical method
is implemented.

PREDECLARED_SINGLE_PRIMARY also fails closed in L4 v1. L1 v1 stores no family
alpha for that method, so L4 refuses to invent an alpha after the search.

The raw family p-value is a one-sided dependency-aware probability test using
the Effective-N proxy rather than raw N.

## Raw N and dependence

Raw N and dependence diagnostics are explicitly separate.

The v1 Effective-N proxy is the number of unique:

symbol × temporal support-region

clusters.

It is intentionally labelled a proxy rather than a claim of exact independent
sample size.

Temporal support regions are generated on the eligible baseline-date axis.
A new region begins only after at least 2 × horizon sessions from the prior
region start.

This reuses the conservative dependence principle already used by the existing
Probability Calibration.

## Robust uncertainty

L4 v1 uses a deterministic, hash-seeded circular moving-block bootstrap.

- block length: 2 × horizon
- repetitions: 512
- occurrence and baseline dates sampled synchronously
- seed derived from immutable run ID + candidate ID
- no external or wall-clock randomness

The evidence stores robust intervals for:

- aligned mean effect
- probability advantage versus baseline

Both intervals must remain on the favourable side of zero for L5 eligibility.

## Baseline probability and lift

For every target family, the baseline cohort is every Discovery observation that
has a mature target value for that exact target.

Target values are aligned to the pre-registered direction:

- positive target: value unchanged
- negative target: sign inverted

L4 records:

- candidate Direction Probability
- baseline probability
- probability advantage / lift
- mean and median aligned outcome
- mean and median raw outcome
- mean / median peer excess for Relative-Alpha families

The L1 minimum-baseline-lift threshold is enforced here.

## Concentration

L4 records:

- symbol count
- top-symbol share and symbol HHI
- observation-date count
- top-date share and date HHI
- support-region count
- top-region share and region HHI

L4 v1 applies conservative versioned concentration gates:

- at least 2 symbols
- no symbol above 50% of occurrences
- no observation date above 50%
- no support region above 75%

These are fixed methodology constants in the versioned L4 contract. They are
not tuned from candidate outcomes.

When available, sector, pillar, official-cluster and market-regime splits are
stored diagnostically. Missing split metadata stays missing and is not invented.

## Coverage

L4 distinguishes a condition match from a mature outcome.

mature_target_coverage =
mature candidate outcomes / all condition matches

L4 v1 requires at least 80% mature target coverage.

## Eligibility

A candidate can become ELIGIBLE_FOR_L5 only when all applicable gates pass:

- L3 eligibility
- minimum raw N
- minimum effect
- minimum temporal support regions
- minimum baseline lift
- mature target coverage
- symbol/date/region concentration
- robust effect interval
- robust probability-lift interval
- pre-registered Multiple Testing

Other candidates remain permanently visible as REJECTED_L4 or
REJECTED_BEFORE_L4 with machine-readable reason codes.

ELIGIBLE_FOR_L5 does not mean confirmed and does not mean frozen.

## Discovery Evidence

Each evidence record contains:

- CAND and family identity
- exact conditions and L2 feature versions
- L3 state and rejection reasons
- raw N
- Effective-N proxy
- symbols / observation dates / support regions
- Direction Probability
- Baseline Probability
- Probability Advantage / Lift
- mean / median outcome
- Relative-Alpha mean / median peer excess where applicable
- robust moving-block intervals
- concentration diagnostics
- optional regime / sector / segment splits
- raw dependency-aware p-value
- adjusted p-value and Multiple-Testing status
- L4 gate status
- open blockers
- explicit prospective-confirmation=false

## Persistence

The write-once artifact path is:

artifacts/research/pattern_discovery/discovery_runs/{run_id}/l4_discovery_evidence.json

The complete artifact is SHA-256 protected.

## Definition of Done

L4 is complete when:

- high raw hit rate alone can never pass
- all tested family candidates enter multiplicity control
- Multiple-Testing status is machine-readable
- raw N and dependency diagnostics remain distinct
- temporal support regions are explicit
- Effective-N is represented conservatively as a labelled proxy
- moving-block robust uncertainty is present
- concentration is visible and gated
- baseline probability and lift are explicit
- L1 minimum support and lift criteria are enforced
- negative/rejected results remain in the artifact
- insufficient evidence fails closed
- Discovery remains separate from prospective Confirmation
- no hard freeze or productive integration occurs

## Next phase

L5 — Candidate Registry & Hard Freeze.
