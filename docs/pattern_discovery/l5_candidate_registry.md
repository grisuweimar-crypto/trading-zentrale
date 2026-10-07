# Pattern Discovery Lab v2 — L5 Candidate Registry & Hard Freeze

## Purpose

L5 turns statistically guarded Discovery candidates into immutable research
objects.

Only candidates that satisfy both:

- L4 gate status ELIGIBLE_FOR_L5
- L3 shortlist status DISCOVERY_SHORTLIST

may be frozen.

This preserves the candidate budget frozen in L1. L4 eligibility alone cannot
bypass the pre-registered freeze budget.

## PAT identity

Every frozen object receives:

- deterministic PAT ID
- pattern version
- Pattern Spec hash
- natural-language description
- Discovery run identity
- CAND identity
- freeze timestamp
- discovery data cutoff
- code version
- baseline
- target
- horizon
- universe version
- exact L4 Discovery Evidence

The initial version is v1.

The PAT root identity is based on the conceptual pattern definition. Exact
feature versions, universe version and other versioned semantics are retained in
the Pattern Spec and its hash.

Frozen versions cannot be rewritten. The append-only registry supports explicit
successor versions, which must reference the latest prior version. Existing
versions remain retained.

## Pattern Spec

The immutable Pattern Spec contains the masterplan sections:

### Identity
- PAT ID
- version
- Discovery run
- candidate ID
- candidate spec hash

### Semantics
- pattern type
- deterministic natural-language description
- conditions
- exact feature versions and hashes
- exact transformation versions, parameters and states

### Forecast
- target
- expected direction
- horizon
- baseline / reference definition

### Data
- universe version
- Feature Library version
- Discovery period from the L3 observation range
- Discovery cutoff
- PIT rules
- coverage rule

### Statistics
- pre-registered primary method
- Multiple Testing plan
- minimum criteria
- robustness checks
- exact L4 Discovery Evidence hash

### Freeze
- explicit freeze timestamp
- code version
- L1 manifest hash
- L3 result hash
- L4 evidence hash

## Freeze timestamp

L5 never silently calls the wall clock to define the scientific boundary.

The freeze timestamp is an explicit input and must be timezone-aware. It may
not precede the L1 data cutoff or declared Discovery start.

This timestamp becomes the temporal boundary for the later confirmation plan.

## Candidate Registry

The registry is:

artifacts/research/pattern_discovery/pattern_registry.jsonl

It is append-only and hash-chained.

Each PATTERN_FROZEN event stores the immutable frozen PAT record and the
previous event hash. Re-registering the exact same record is idempotent;
changing an already frozen version fails closed.

A semantic successor must use a higher version and explicitly supersede the
latest frozen version.

## QM-C handoff and the L0 boundary

L0 confines Pattern Discovery runtime writes to:

artifacts/research/pattern_discovery/

Therefore L5 does not write the central QM registry files under
artifacts/research/qm/.

Instead every PAT contains an exact QM-C handoff package generated against the
existing governance contracts and hash functions.

The package contains:

- exact QM-C1 hypothesis record
- QM-C1 hypothesis-version hash
- requested FROZEN_FOR_CONFIRMATION target state
- exact QM-C2 analysis-plan record
- QM-C2 analysis-plan hash
- exact freeze context
- freeze-context hash
- requested FROZEN_FOR_CONFIRMATION target state
- exact QM-A identity that QM-C2 will expose after plan freeze
- deterministic handoff hash
- explicit READY_NOT_APPLIED_BY_L5 status

The intended external governance application order is:

1. register QM-C1 confirmation hypothesis
2. register QM-C2 analysis plan
3. freeze QM-C2 plan
4. transition QM-C1 hypothesis to FROZEN_FOR_CONFIRMATION
5. bind the exact QM-A identity in the later governance step

L5 provides a read-only validator that checks an externally applied handoff
against the real HypothesisRegistry and AnalysisPlanRegistry objects.

Thus the laboratory reuses QM-C1/QM-C2 instead of inventing a competing
hypothesis or analysis-plan lifecycle, while still respecting the L0 write
boundary.

## Missing QM identity context

Some QM-C2 freeze identity fields are not derivable from L1-L4 without making
unsupported assumptions.

L5 therefore requires explicit values for:

- universe_ledger_version
- instrument_master_version
- environment_or_dependency_fingerprint

Missing or additional fields fail closed.

Other freeze-context hashes are derived from already frozen Pattern Discovery
identities:

- code/config identity
- input snapshot identity
- target/label definition
- baseline definition
- future evaluation cohort
- temporal boundary

## Discovery versus Confirmation

A frozen PAT is still Discovery evidence.

L5 explicitly stores:

- confirmation_data_used = false
- prospective_capture_started = false
- rating = null
- promotion_status = NOT_EVALUATED

The generated QM handoff is a plan for later prospective confirmation. It is
not confirmation evidence.

## Freeze snapshot

Each Discovery run can persist one write-once L5 snapshot:

artifacts/research/pattern_discovery/discovery_runs/{run_id}/l5_freeze_snapshot.json

The snapshot is SHA-256 protected and binds:

- L1 manifest
- L3 result
- L4 evidence
- L5 contract
- all frozen PAT objects
- current PAT registry head

## Definition of Done

L5 is complete when:

- only L4-eligible shortlisted candidates can freeze
- the L1 candidate budget cannot be exceeded
- every PAT has stable identity, version and Pattern Spec hash
- every PAT has deterministic natural-language description
- exact target, horizon, baseline, universe and feature versions are frozen
- exact Discovery Evidence is embedded and hash-bound
- freeze timestamp and data cutoff are distinct and explicit
- frozen versions cannot be mutated
- successor versions are explicit and old versions remain available
- exact QM-C1 and QM-C2 handoff identities are generated
- handoff compatibility is tested against the real QM-C registries
- no central QM registry is written by L5 itself
- Discovery and Confirmation remain technically separate
- no prospective capture, rating, promotion or productive integration occurs

## Next phase

L6 — Dependency Graph.
