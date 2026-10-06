# Pattern Discovery Lab v2 — L3 Search Engine / Discovery Core

## Purpose

L3 generates new research candidates inside the search space frozen by L1 and
restricted by the PIT-safe Feature Library from L2.

L3 is discovery-only. It does not confirm a pattern and it does not create
productive evidence.

## Initial scope

The first Search Engine generation implements the conservative scope from the
masterplan:

- one to three atomic conditions
- separate pattern families
- 5 / 20 / 40 / 60 session horizons
- Directional and Relative Alpha only
- minimum raw N
- minimum aligned effect
- discovery-only candidate ranking
- complete output for every tested candidate, including rejections and reason codes

Risk, Path and Modifier discovery remain deferred.

## Target IDs

L3 v1 uses explicit direction-specific target IDs:

- return_5t_gt_0 / return_5t_lt_0
- return_20t_gt_0 / return_20t_lt_0
- return_40t_gt_0 / return_40t_lt_0
- return_60t_gt_0 / return_60t_lt_0
- peer_excess_5t_gt_0 / peer_excess_5t_lt_0
- corresponding 20T / 40T / 60T peer-excess targets

Each supplied observation stores the numeric outcome together with its end_at
timestamp. An outcome whose end_at lies after the frozen L1 data cutoff is
rejected fail-closed. This prevents a Discovery run from silently seeing
confirmation-period labels.

## Atomic conditions

Atomic conditions are generated only from exact L2 feature/transform versions.

Initial atomization is deliberately narrow:

- change_direction -> UP / DOWN
- threshold_crossing -> CROSS_UP / CROSS_DOWN
- state_transition -> exact recorded from->to transition
- regime_context -> exact contemporaneous regime
- raw categorical / boolean -> exact recorded state

Continuous raw values are not converted into arbitrary thresholds. Continuous
delta values are not freely binned. Their initial usable form is the
pre-registered direction transformation.

Thus L3 cannot invent a threshold after observing a promising outcome.

## Search space and budgets

The engine enumerates conditions in canonical deterministic order.

For every target family the family identity contains:

- pattern type
- target
- horizon
- expected direction
- baseline

The total and per-family test budgets come only from the frozen L1 manifest.
When the total budget is smaller than the sum of family budgets, L3 allocates it
deterministically across families before search.

Mutually exclusive conditions from the same feature/transform basis are skipped
before testing.

Every actually tested candidate remains in the result, including candidates
that fail minimum N or minimum effect.

## L3 gates

L3 applies only:

- minimum raw N
- minimum aligned effect

For positive targets, aligned effect is the mean outcome. For negative targets,
it is the sign-inverted mean outcome.

L3 intentionally does not apply:

- Multiple Testing
- robust uncertainty
- Effective-N
- support-region gates
- symbol/date concentration gates
- baseline-lift gate
- dependency/redundancy adjustment

Those belong to L4 and later phases.

## Ranking and shortlist

Candidates passing the two L3 gates receive a rank only within their own
Discovery family. Ranking is deterministic:

1. aligned effect descending
2. hit rate descending
3. raw N descending
4. candidate ID ascending

The L1 candidate budget is used only as an upper bound for an
DISCOVERY_SHORTLIST. A shortlisted object is not frozen, not a QM-C hypothesis
and not productive evidence. Hard freeze belongs to L5.

## Provenance

Every candidate contains:

- deterministic CAND identity
- spec hash
- family identity
- target / horizon / expected direction / baseline
- exact L2 feature versions and feature-version hashes
- exact transformation versions and parameters
- exact atom states
- raw N, symbol count and observation-date count
- mean / median outcome
- raw hit rate
- aligned effect
- L3 gate result
- rejection reason codes
- family rank when eligible
- shortlist status

The whole L3 result is also hash-protected and write-once.

## Persistence

The contract-defined research-only path is:

artifacts/research/pattern_discovery/discovery_runs/{run_id}/l3_search_result.json

The L0 write guard remains authoritative.

## Definition of Done

L3 is complete when:

- bounded one-to-three-condition search exists
- only L2-registered feature/transform versions can create atoms
- Directional and Relative Alpha families are separate
- 5T / 20T / 40T / 60T target semantics are explicit
- post-cutoff observations and outcomes fail closed
- L1 search budgets hard-limit tested candidates
- minimum N and minimum effect are applied
- ranking uses Discovery data only
- every tested rejected candidate is retained with reasons
- candidate provenance is fully machine-readable
- identical run + observations produce identical result
- shortlist is explicitly not a freeze
- no L4/L5/confirmation/productive semantics are activated

## Next phase

L4 — Statistical Discovery Guard.
