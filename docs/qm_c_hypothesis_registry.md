# QM-C1 – Versioned Hypothesis Registry

QM-C1 establishes the canonical hypothesis identity layer for Scanner_vNext research governance.

## Scope

QM-C1 is research-only. It does not change scanner semantics, Selection, Timing, Probability, Risk, Confidence, Elliott, Decision Layer outputs, portfolio actions or execution.

QM-C1 provides:

- stable `hypothesis_id`
- explicit `hypothesis_version`
- immutable semantic `hypothesis_version_hash`
- explicit `hypothesis_family_id`
- explicit `DISCOVERY` vs `CONFIRMATION`
- append-only, hash-chained registry events
- explicit successor-version references
- retention of rejected and retired versions
- direct QM-A binding through `hypothesis_version_hash`
- fail-closed QM-B binding for strict historical investability claims

Analysis-plan freezing, multiplicity and sequential monitoring are intentionally deferred to later QM-C work packages.

## Versioning rule

A registered hypothesis version is immutable. The same `(hypothesis_id, hypothesis_version)` cannot be registered twice, even if only the text changes.

Any change that creates a new semantic hypothesis version must use a new `hypothesis_version` and explicitly reference the latest predecessor through `supersedes_hypothesis_version`.

This prevents silent in-place rewriting after evidence has been inspected.

## Discovery vs Confirmation

Discovery and confirmation are not interchangeable states of one mutable version.

A `DISCOVERY` hypothesis may move from `DRAFT` to `EXPLORATORY`, but it cannot be frozen for confirmation in place. A confirmatory successor must be registered as a new version with `research_mode=CONFIRMATION`.

A `CONFIRMATION` hypothesis may move from `DRAFT` to `FROZEN_FOR_CONFIRMATION`, but it cannot move to `EXPLORATORY` in place.

Rejected and retired versions remain in the registry.

## QM-A integration

QM-A remains the authority for evidence lifecycle and immutable confirmatory analysis identity.

QM-C1 binds a hypothesis to QM-A with `qm_a_analysis_id`. Once the QM-A analysis identity is frozen, its `hypothesis_version_hash` must exactly match the immutable QM-C1 hash. A mismatch fails closed.

If the QM-C1 hypothesis itself is marked `FROZEN_FOR_CONFIRMATION`, validation also requires the linked QM-A analysis to be in `FROZEN_FOR_CONFIRMATION`.

QM-C1 therefore does not create a second evidence-state machine; it provides the hypothesis identity that QM-A consumes.

## QM-B integration

QM-B remains the authority for point-in-time universe and historical investability integrity.

Each hypothesis declares one universe requirement:

- `NONE`
- `OBSERVED_SCANNER_UNIVERSE`
- `STRICT_ASOF_INVESTABLE`

A confirmatory hypothesis that requests `STRICT_ASOF_INVESTABLE` fails closed while the QM-B closure manifest reports that productive strict as-of investable-universe promotion is blocked.

The current QM-B status is therefore respected rather than bypassed or reinterpreted.

## Registry integrity

The canonical runtime registry is JSONL and append-only. Every event contains:

- monotonically increasing sequence
- event UUID
- timestamp
- hypothesis ID and version
- actor identity and role
- payload
- previous-event hash
- event hash

Replay verifies the full chain before accepting new writes. Tampering, duplicate versions, invalid successor chains and illegal state changes fail closed.

## Definition of Done

QM-C1 is complete only when:

1. the contract exists and is research-only;
2. hypothesis identity/versioning is immutable;
3. Discovery and Confirmation cannot silently collapse into one another;
4. negative/retired versions remain auditable;
5. QM-A integration is tested using the real GovernanceLedger;
6. QM-B external blockers are respected by integration tests;
7. tampering and invalid version chains fail closed;
8. no productive scanner or decision semantics are changed;
9. the closure manifest validates;
10. QM-C2 is the next mandatory work package.

Closure status:

`QM-C1 COMPLETE — VERSIONED HYPOTHESIS REGISTRY ACTIVE`
