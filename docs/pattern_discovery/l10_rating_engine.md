# Pattern Discovery Lab v2 — L10 Rating Engine

L10 is the research-only lifecycle layer above immutable L5 Pattern versions and unchanged L9 confirmation/falsification reports.

It implements the Masterplan rating states:

- **D — Discovery**: frozen Pattern, Discovery evidence only.
- **C — Challenger**: first prospective evidence points in the frozen direction, but L9 confirmatory gates remain unmet.
- **B — Confirmed**: L9 returned `SUPPORTED` on prospective evidence.
- **A — Robust**: repeated final `SUPPORTED` confirmation across distinct predeclared monitoring epochs.
- **U — Unresolved / Insufficient**: maturity or evidence is insufficient/inconclusive; this is explicitly not falsification.
- **F — Falsified / Retired**: L9 returned `FALSIFIED` or final `NEGATIVE_NOT_CONFIRMED`, or the exact Pattern version was formally retired.

## Separation of concepts

Direction Probability, Evidence Strength and Rating remain separate. L10 does not create a numeric rating score. In particular, a high hit rate or Direction Probability alone cannot produce C/B/A.

L10 never recalculates L9 statistics and never rewrites an L9 result class. Discovery evidence is retained in L5 provenance but never counted as confirmation.

## v1 gates

### D → U

An applicable L9 look is `UNRESOLVED_NOT_DUE`. Missing maturity is not negative evidence.

### D/U → C

The L9 result is `INCONCLUSIVE` and both:

- `mean_aligned_outcome > 0`
- `probability_advantage_lift > 0`

This deliberately requires more than Direction Probability alone.

### → B

The applicable L9 result is `SUPPORTED` and the A replication gate is not yet met.

### → A

The exact frozen Pattern version has at least two `SUPPORTED` **final** L9 looks from distinct `monitoring_plan_id + monitoring_plan_version` epochs. Sequential looks inside one cumulative monitoring plan count as one confirmation epoch and cannot manufacture independent replication.

### Degradation

Ratings may degrade. v1 explicitly supports:

- `A → B` when new prospective evidence remains positive but becomes `INCONCLUSIVE`;
- `B → C` under the same condition;
- established ratings may move to U when evidence is genuinely inconclusive/insufficient;
- any non-F state may move to F on terminal negative L9 evidence or explicit retirement.

A merely `UNRESOLVED_NOT_DUE` look does not erase an already established C/B/A rating.

### F is terminal for the exact Pattern version

A falsified or retired frozen Pattern version cannot be automatically resurrected by later favorable evidence. A materially changed hypothesis requires a new Pattern version through the existing freeze/governance process.

## Negative result mapping

L9 classes remain unchanged and are interpreted as follows:

- `SUPPORTED` → B/A gate
- `FALSIFIED` → F
- `NEGATIVE_NOT_CONFIRMED` → F with a distinct reason code
- `INCONCLUSIVE` → C only if the positive Challenger gate passes, otherwise U
- `UNRESOLVED_NOT_DUE` → U before an established rating, otherwise preserve the existing rating

This preserves the distinction between unresolved and falsified evidence.

## Rating History

Every actual status change persists:

- timestamp,
- previous rating,
- new rating,
- reason codes,
- exact Pattern identity/version/spec hash,
- hash-bound evidence snapshot,
- rating contract hash,
- research-only boundary flags.

The global registry is append-only and hash-chained:

`artifacts/research/pattern_discovery/pattern_rating_history.jsonl`

Immutable per-Pattern history snapshots are written below:

`artifacts/research/pattern_discovery/rating_histories/{pattern_id}/{pattern_version}/{history_hash}.json`

Identical replay is idempotent. Divergence, stale history, hash tampering, state-chain breaks, or illegal transitions fail closed.

## Formal retirement

Retirement is an explicit L10 input and requires an evidence hash. v1 permits:

- `FORMAL_RETIREMENT`
- `STRUCTURAL_INVALIDATION`
- `DATA_SEMANTICS_INVALIDATED`

Retirement maps the exact Pattern version to F and is auditable like any other status change.

## Productive boundary

L10 does **not**:

- mutate L5 Pattern specs,
- mutate or recompute L9 confirmation,
- write QM-C confirmation results,
- promote a Pattern,
- alter Scanner Score or Timing,
- alter Universal Stance,
- create Portfolio Action,
- create execution authority.

B or A is therefore not production admission. Promotion remains a separate later phase.

## CLI

`run_l10_rating.py` reads the canonical L9 confirmation registry, verifies its integrity, builds L10 histories for frozen L5 patterns, and persists only research artifacts.

Optional `--pending-l9-report` accepts a hash-verifiable `UNRESOLVED_NOT_DUE` L9 report because such a look is intentionally not persisted as a consumed L9 registry event.

Optional `--retirements` accepts explicit retirement records.
