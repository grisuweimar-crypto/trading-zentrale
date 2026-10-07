# Pattern Discovery Lab v2 — L7 Prospective Capture

## Purpose

L7 begins the prospective part of the Pattern Discovery lifecycle.

It takes exact immutable PAT versions that were frozen in L5 and whose
QM-C1 hypothesis plus QM-C2 analysis plan have actually reached
FROZEN_FOR_CONFIRMATION. It then evaluates those PAT definitions against a
genuinely later point-in-time scanner snapshot and stores each real match as an
immutable prospective claim.

L7 does not evaluate whether a claim was successful.

Outcome maturation belongs to L8.

## Required ordering

A PAT can create an L7 claim only after all of the following are true:

1. the PAT version exists in a hash-valid L5 freeze snapshot;
2. its complete Pattern Spec hash still matches the frozen record;
3. the current scanner snapshot was generated strictly after the PAT freeze;
4. the exact L5 QM-C handoff has been applied;
5. QM-C1 reports the hypothesis as FROZEN_FOR_CONFIRMATION;
6. QM-C2 reports the analysis plan as FROZEN_FOR_CONFIRMATION;
7. the scanner snapshot is still inside the predeclared capture-freshness
   window;
8. all required frozen conditions can be evaluated from PIT-safe current and
   prior observations;
9. an explicit future start market session is available for the matched symbol.

No condition is silently relaxed.

## No retrospective backfill

Prospective status cannot be manufactured later.

The capture timestamp is an explicit input. The L7 v1 contract allows a
snapshot to enter prospective capture only when:

- capture_at is not earlier than snapshot generation, and
- capture_at is no more than 360 minutes after snapshot generation.

The limit is versioned in:

configs/pattern_discovery/l7_prospective_capture_v1.json

A later methodological change therefore requires a new contract version rather
than result-dependent tuning.

A scanner snapshot generated before or at a PAT freeze cannot create a claim
for that PAT, even if the capture code is run later.

## Snapshot binding

Every capture binds the scientific input identity.

The snapshot binding contains:

- scanner snapshot_id,
- snapshot as-of,
- generated_at,
- research-view schema version,
- upstream source file and source SHA-256,
- SHA-256 of the exact current scanner file supplied to the runner,
- row and symbol counts,
- deterministic hash of the L7 matching projection of every current row.

The history binding separately contains:

- SHA-256 of the exact supplied history file,
- history row and symbol counts,
- deterministic projected-history hash,
- latest historical generated_at.

This prevents the same claim from being silently reproduced from a different
state history.

## PIT matching

L7 reuses the registered L2 Feature Library.

It does not invent new transformations.

L7 v1 supports the frozen transformation semantics currently produced by the
Discovery engine:

- raw,
- regime_context,
- change_direction,
- threshold_crossing,
- state_transition.

For every frozen condition L7 verifies:

- exact feature ID and feature version,
- exact feature-version hash,
- exact transformation ID and version,
- exact parameters,
- point-in-time availability,
- exact expected state.

All frozen conditions must match.

If a required value or prior observation is unavailable, the PAT/symbol match
is UNAVAILABLE. Missing information does not become zero, false, neutral or a
synthetic match.

## Outcome isolation

Claim creation is deliberately blind to outcomes.

The matching projection does not use:

- future return,
- realized return,
- benchmark outcome,
- price outcome,
- peer excess,
- adverse excursion,
- path drawdown.

Prospective claims do not store prices or outcome values.

The immutable forecast definition still contains the frozen target,
direction, horizon and baseline because these are part of the ex-ante Pattern
Spec, not realized outcomes.

The following remain false in every L7 claim:

- outcome_available_at_capture,
- outcome_maturation_performed,
- confirmation_evaluation_performed,
- rating_assigned,
- promotion_performed.

## Start market session

L7 does not guess a session from ticker suffixes, current time, weekends or an
exchange heuristic.

The caller supplies one explicit session object per symbol when available:

- symbol,
- session_id,
- calendar_id,
- start_at,
- source.

start_at must be strictly later than capture_at.

If a matching symbol lacks an explicit next session, L7 records
START_MARKET_SESSION_UNAVAILABLE and does not create a claim.

This preserves an exact handoff for L8, where the full sequence of market
sessions and price outcomes will be matured.

## Event and claim identity

Every matched PAT/symbol/snapshot combination receives a deterministic event
identity.

The claim identity additionally binds:

- target,
- horizon,
- baseline,
- start market session,
- snapshot binding.

Therefore the same scientific event cannot silently acquire a second content
definition.

## Append-only claim registry

Prospective claims are stored under the L0 research namespace:

artifacts/research/pattern_discovery/prospective_claims.jsonl

The registry is:

- append-only,
- hash-chained,
- batch-preflighted before append,
- idempotent for an exact replay,
- fail-closed when an existing claim ID is presented with different content.

No claim is updated when its horizon later matures. L8 must create separate
outcome/maturation evidence.

## Capture reports

Each capture also receives a hash-protected immutable report under:

artifacts/research/pattern_discovery/prospective_captures/{snapshot_id}/{capture_id}.json

It records:

- input hashes,
- frozen L5 snapshot hashes,
- evaluated PAT versions,
- match / no-match / unavailable counts,
- explicit exclusions,
- exact prospective claims,
- all L7 boundary flags.

The same inputs and explicit capture timestamp produce the same capture ID,
claim IDs and hashes.

## Runner

The operational runner is:

scripts/pattern_discovery/run_l7_prospective_capture.py

Required inputs include:

- one or more L5 freeze snapshots,
- current history_metadata.json,
- current latest_scanner.csv,
- prior PIT history CSV,
- explicit market-session JSON,
- explicit capture timestamp,
- QM-C1 hypothesis registry,
- QM-C2 analysis-plan registry.

The runner computes the current-snapshot and history file hashes from the exact
bytes it reads.

## Definition of Done

L7 is complete when:

- frozen PAT versions are matched against genuinely later PIT scanner data;
- exact QM-C1/QM-C2 confirmation freeze is required before capture;
- claim/event identity is deterministic;
- each claim is bound to the exact scanner snapshot and history input;
- exact PAT version is persisted;
- start market session is explicit and future relative to capture;
- horizon is frozen from the Pattern Spec;
- claims are append-only and immutable;
- stale snapshots cannot be backfilled as prospective claims;
- missing feature/history/session information cannot create a fake claim;
- outcome and price information is absent from claim creation;
- PIT/future-history leakage fails closed;
- no L4/L5 object is mutated;
- no outcome maturation, confirmation, rating, promotion, Decision Layer,
  portfolio or execution authority is created;
- the complete L0-L7 regression suite and QM-C1/QM-C2 compatibility suite are
  green.

## Next phase

L8 — Outcome Maturation.
