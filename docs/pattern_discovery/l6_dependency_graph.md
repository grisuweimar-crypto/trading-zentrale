# Pattern Discovery Lab v2 — L6 Dependency Graph

## Purpose

L6 makes redundancy and dependence between immutable frozen Pattern Discovery
objects machine-readable.

The graph is research-only. It does not create a signal, vote, rating,
confirmation result, portfolio action or execution instruction. L4 Discovery
Evidence and L5 PAT versions remain unchanged.

## Node identity

Every graph node is bound to the exact frozen identity:

- pattern_id
- pattern_version
- pattern_spec_hash

The derived node_id is deterministic from those three fields. Two versions of
the same PAT are therefore separate graph nodes and can never be silently
combined.

Before a node enters the graph, L6 verifies:

1. the complete L5 freeze snapshot hash,
2. each frozen-record hash via L5,
3. the complete Pattern Spec hash again,
4. the Pattern Spec identity against the frozen record.

Tampering fails closed.

## Event / context input

L5 intentionally freezes Pattern definitions and Discovery Evidence, but it does
not persist every matched event identity needed for pairwise overlap analysis.
L6 therefore consumes an explicit, hash-protected event/context input.

For every graph node the input must state one of:

- COMPLETE — the supplied event list is the complete L6 event set for that
  node and source,
- UNAVAILABLE — event overlap is not available and the event list must be
  empty.

Each event contains only PIT-safe dependency context:

- symbol,
- as-of timestamp,
- optional sector,
- optional regime.

The event ID is deterministic from (symbol, as_of) and is independent of the
PAT that matched it. That allows exact event overlap to be measured across
patterns.

Sector and regime are never inferred or reconstructed. Null remains null. If a
pair cannot be compared, the corresponding component is UNAVAILABLE, never
synthetic zero.

## Dependency components

L6 deliberately does not collapse everything into one opaque similarity score.
Every edge retains separate provenance.

### Structural

- Feature overlap — Jaccard overlap of feature IDs.
- Condition overlap — Jaccard overlap of complete versioned atomic condition
  signatures including feature version/hash, transformation version,
  parameters and state.
- Forecast scope equality — target, expected direction, horizon, baseline and
  Pattern type are compared only to determine structural equivalence or
  nesting.

### Empirical

- Event overlap — exact (symbol, as_of) event Jaccard.
- Temporal overlap — Jaccard of exact PIT as-of timestamps, independent of
  symbol overlap.
- Symbol overlap — Jaccard of symbol sets, independent of date overlap.
- Sector overlap — category Jaccard plus empirical distribution overlap when
  sector is actually available.
- Regime overlap — category Jaccard plus empirical distribution overlap when
  regime is actually available.

This separation makes statements such as “same dates, different symbols” or
“same symbols, different dates” observable instead of hiding them inside one
number.

## Relationship classes

Classification priority is deterministic:

1. DUPLICATE
2. NESTED
3. RELATED
4. NONE

### Duplicate

A pair is duplicate when it has the same forecast scope and the exact same
versioned condition set. Condition order does not matter.

### Nested

A pair is nested when it has the same forecast scope and one condition set is a
strict subset of the other.

The edge stores which graph side contains the subset. This describes structural
nesting only; it has no market-direction authority.

### Related

A non-duplicate, non-nested pair is related when at least one versioned L6
threshold is met for feature, condition, event, temporal, symbol, sector or
regime overlap.

The exact threshold triggers are persisted on the edge.

### None

No pre-registered related threshold is met.

All node pairs are stored, including NONE. This distinguishes an explicitly
evaluated low/no-dependency pair from a pair that was never evaluated.

## Dependency severity

Severity is descriptive:

- CRITICAL — duplicate,
- HIGH — nested, or related with a very strong component,
- MEDIUM — related,
- LOW — measurable overlap below the related thresholds,
- NONE — no measurable overlap or only unavailable components.

Severity does not mean BUY, SELL, stronger forecast, higher expected return or
higher confidence.

## Versioned thresholds

All relation and severity thresholds live in:

configs/pattern_discovery/l6_dependency_graph_v1.json

They are part of the L6 contract hash and therefore cannot be changed after
seeing concrete PAT results without creating a new L6 methodology version.

## Graph identity and determinism

The graph identity is derived from:

- L6 contract hash,
- sorted L5 snapshot hashes,
- exact event-context hash,
- sorted PAT-ID + version + spec-hash node identities.

The graph hash covers the complete graph payload.

Reordering snapshots, Pattern event sets or events does not change the graph.
Identical scientific inputs therefore produce the same graph ID and graph hash.

## Storage

Runtime graphs are written only inside the L0 namespace:

artifacts/research/pattern_discovery/dependency_graphs/{graph_id}.json

An identical replay is idempotent. A different payload trying to occupy the
same graph identity fails closed.

L6 never writes:

- L4 evidence files,
- L5 freeze snapshots or the PAT registry,
- central QM registries,
- Timing / Decision / Portfolio / Execution artifacts.

## Runner

The runner is:

scripts/pattern_discovery/run_l6_dependency_graph.py

It accepts one or more --l5-snapshot arguments plus --event-context and
--repo-root. Repeating --l5-snapshot allows one graph across PAT versions from
multiple Discovery runs.

## Definition of Done

L6 is complete when:

- exact frozen PAT versions are stable graph nodes,
- duplicate, nested and related patterns are machine-readable,
- feature and exact condition overlap are separate,
- event, temporal and symbol overlap are separate,
- sector/regime overlap uses only actually available PIT context,
- missing context remains UNAVAILABLE,
- every edge exposes component provenance and threshold triggers,
- no opaque single similarity score is used,
- high redundancy is directly recognizable,
- identical inputs produce identical graph identity and hash,
- tampered PAT/spec/event identities fail closed,
- L4 and L5 inputs remain immutable,
- runtime writes stay inside the L0 namespace,
- no directional, confirmation, rating, promotion, portfolio or execution
  authority is created,
- the complete L0–L6 regression and QM-C1/QM-C2 compatibility suite are green.

## Next phase

L7 — Prospective Capture.
