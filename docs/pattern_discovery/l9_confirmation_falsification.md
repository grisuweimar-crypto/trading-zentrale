# Pattern Discovery Lab v2 — L9 Confirmation & Falsification Engine

## Purpose

L9 is the first phase allowed to evaluate the empirical performance of a frozen
Pattern version.

It uses only evidence that became observable after the Pattern freeze and stops
before rating, promotion or productive integration.

L9 answers:

- did the frozen prospective hypothesis survive later data?
- how large and broad is the observed prospective effect?
- how uncertain is it after temporal dependence and concentration?
- is the result supported, falsified, negative/not confirmed, inconclusive, or
  simply not mature enough for the next predeclared look?

It does **not** answer whether the Pattern is an A/B/C/D/F lifecycle rating.
That belongs to L10.

## Authoritative governance

L9 reuses the existing governance chain instead of creating a second one:

1. **QM-C1** — exact hypothesis/version identity
2. **QM-C2** — frozen confirmatory analysis plan
3. **QM-C3** — exact family membership and multiplicity strategy
4. **QM-C4** — predeclared sequential look schedule
5. **QM-C5** — positive, negative and inconclusive result retention
6. **QM-A** — immutable analysis identity and consumed-evidence state

Before evaluating any Pattern, L9 verifies that the applied QM-C1/C2 objects
match the exact L5 handoff.

The supplied frozen Pattern set must then exactly cover the frozen QM-C3 family.
Adding, omitting or substituting a hypothesis fails closed.

The next L9 look is never chosen by L9. It is the next unconsumed look in the
frozen QM-C4 monitoring plan.

## L0 write boundary

The Pattern Discovery L0 boundary still permits runtime writes only under:

`artifacts/research/pattern_discovery/`

Therefore L9 does not secretly write the central QM-C or QM-A registries.

Instead, an evaluated L9 report contains an exact handoff package describing:

- the QM-C4 monitoring look to record;
- the QM-A transition required after the first consumed look;
- terminal QM-C5 result records when the family reaches its final/terminal look.

The central governance layer remains the authority that applies and validates
those changes.

## Evidence separation

Discovery evidence embedded in the L5 Pattern is provenance only.

It is never added to:

- prospective N,
- Direction Probability,
- Effect Size,
- robust intervals,
- p-values,
- support regions,
- confirmation decisions.

Only hash-valid L8 matured outcomes from the exact frozen Pattern version can
enter the Pattern sample.

Every L8 outcome must match:

- Pattern ID
- Pattern version
- Pattern Spec hash
- target
- expected direction
- horizon
- baseline
- reference definition

and its start session must be strictly later than the Pattern freeze timestamp.

A pre-freeze or mismatched outcome fails closed.

## Baseline / control population

L9 does not derive the baseline from the Pattern matches themselves.

A separate hash-protected baseline bundle is required.

For the initial supported baseline:

`same_horizon_unconditional_return_baseline`

the rule is fixed as:

`ALL_ELIGIBLE_POST_FREEZE_PIT_OBSERVATIONS_IN_FROZEN_UNIVERSE_SAME_TARGET_HORIZON_V1`

The baseline record must bind:

- exact Pattern ID/version/spec hash;
- frozen universe version;
- frozen baseline definition;
- exact target and horizon;
- population source ID and hash;
- deterministic selection-rule ID and rule hash;
- declared eligible population count;
- every included baseline observation and its source hash.

The number of supplied baseline observations must equal the declared eligible
population count.

Unknown baseline semantics fail closed in L9 v1. They require a new explicit
methodology rather than an improvised comparator after seeing outcomes.

Every baseline observation must also begin strictly after Pattern freeze.

## PIT-safe context diagnostics

Sector, segment, pillar, cluster and market-regime diagnostics are optional
context fields in a separate hash-protected context bundle.

They may not be populated from today's taxonomy.

Each context row is bound to:

- L8 claim ID and symbol;
- the exact capture snapshot ID;
- the exact capture snapshot binding hash;
- an observation timestamp strictly before the Pattern's start session;
- a context-source hash.

The capture identity must equal the immutable identity already carried by the
L8 outcome.

This prevents current sector/regime metadata from being retrofitted into old
prospective events.

Context splits are diagnostic only. They cannot replace the full prospective
sample and cannot rescue a failed primary result by post-hoc subgroup choice.

## Look readiness

Each frozen Pattern already contains its predeclared Discovery/Confirmation
minimum criteria.

For a QM-C4 look with information fraction `f`, L9 v1 requires:

`required_n = ceil(frozen_minimum_raw_n × f)`

for both:

- matured Pattern observations;
- eligible baseline observations.

Every member of the frozen QM-C3 family must be ready before the family look is
evaluated.

If any member is not ready:

- status = `UNRESOLVED_NOT_DUE`;
- no statistical confirmation result is emitted;
- no QM-C4 look is consumed;
- no QM-C5 result is created;
- the hypothesis is not falsified.

This is the explicit implementation of “missing maturity is unresolved, not
falsified.”

## Sequential looks

L9 follows the exact QM-C4 order.

For L9 v1:

- one final look is supported;
- multiple predeclared looks are supported;
- intermediate looks default to `CONTINUE`;
- the final look must map to `FINAL_COMPLETE`.

Automatic early stopping is intentionally fail-closed in v1.

QM-C4 v1 stores whether early stop is allowed and a stopping-rule identity/text,
but it does not provide a machine-readable efficacy/futility statistical
boundary. L9 therefore refuses to invent one.

A future methodology version can enable automatic
`STOP_EFFICACY`/`STOP_FUTILITY` only after such boundaries are explicitly
predeclared and machine-verifiable.

## Multiplicity

The exact frozen QM-C3 strategy is used:

- `PREDECLARED_SINGLE_PRIMARY`
- `BONFERRONI_FWER`
- `HOLM_FWER`
- `BENJAMINI_HOCHBERG_FDR`

Custom methods fail closed in L9 v1.

Family multiplicity is combined conservatively with the predeclared number of
QM-C4 looks.

The L9 v1 sequential rule is:

`sequential_threshold = family_threshold / planned_look_count`

This is fixed in the versioned L9 contract before L9 outcome interpretation.

## Prospective statistics

For each Pattern version L9 reports at least:

- raw N
- baseline raw N
- Effective-N
- symbol count
- observation-date count
- temporal support-region count
- Direction Probability
- Baseline Probability
- Probability Advantage / Lift
- mean/median aligned outcome
- mean/median aligned baseline outcome
- Effect Size versus baseline
- mean/median raw outcome
- mean Peer Excess when present
- median Adverse Excursion
- median Path Max Drawdown
- robust uncertainty intervals
- concentration metrics
- temporal stability
- regime diagnostics
- sector/segment/pillar/cluster diagnostics
- confirmation period
- raw dependency-aware p-value
- family/sequential multiplicity status
- open failure/blocker reasons

### Direction alignment

Positive frozen direction:

`aligned = target_value`

Negative frozen direction:

`aligned = -target_value`

This lets one confirmatory engine evaluate both directional signs without
changing the frozen target.

### Direction Probability

`P(aligned target > 0 | frozen Pattern, post-freeze data)`

### Baseline Probability

`P(aligned baseline target > 0 | frozen baseline population)`

### Probability Advantage

`Direction Probability - Baseline Probability`

### Effect Size

`mean(aligned Pattern outcome) - mean(aligned baseline outcome)`

## Dependence and Effective-N

Raw N is not treated as independent N.

L9 v1 reuses the established Pattern Discovery dependence philosophy:

- block length = `2 × horizon`;
- support regions are non-overlapping temporal regions on the combined
  prospective candidate/baseline date axis;
- Effective-N proxy = unique `symbol × support-region` clusters;
- robust intervals use deterministic hash-seeded circular moving-block
  bootstrap.

Raw N, Effective-N and temporal support are stored separately.

## Concentration

L9 reports:

- top-symbol share
- symbol HHI
- top observation-date share
- observation-date HHI
- top support-region share
- support-region HHI

Support cannot be granted merely because many highly dependent observations
repeat the same symbol/date/region.

## Stability

### Temporal

When enough observations exist, the prospective sample is split
chronologically into two halves.

A non-positive aligned effect in either sufficiently populated half is a
temporal stability warning and blocks `SUPPORTED`.

### Regime

Capture-time market-regime contexts are evaluated separately.

L9 distinguishes generic context coverage from actual regime coverage. A row
that contains only sector/segment metadata does **not** count as regime
evidence. `SUPPORTED` requires the versioned minimum regime-context coverage;
otherwise the result remains `INCONCLUSIVE`.

A sufficiently populated regime with a sign reversal blocks `SUPPORTED`.

The subgroup itself never becomes the primary hypothesis.

## Confirmation

A Pattern can be `SUPPORTED` only when the full prospective sample passes all
applicable predeclared gates, including:

- frozen minimum Effect Size;
- frozen minimum Probability Lift;
- frozen minimum temporal support;
- instrument breadth/concentration;
- robust intervals;
- family + sequential multiplicity;
- required PIT context coverage;
- temporal/regime stability.

This is deliberately stricter than a high raw hit rate.

## Falsification

L9 does not classify every failed confirmation gate as falsification.

Strong falsification in v1 requires both:

- upper bound of the robust Effect Size interval <= 0;
- upper bound of the robust Probability Lift interval <= 0;

at a final permitted look.

Thus:

- a genuinely robust sign reversal can become `FALSIFIED`;
- a final result that merely fails support becomes
  `NEGATIVE_NOT_CONFIRMED`;
- insufficient/blocked evidence becomes `INCONCLUSIVE`;
- an immature scheduled look remains `UNRESOLVED_NOT_DUE`.

This prevents “absence of sufficient evidence” from being silently rewritten as
evidence of the opposite.

## Result classes

L9 emits:

- `SUPPORTED`
- `FALSIFIED`
- `NEGATIVE_NOT_CONFIRMED`
- `INCONCLUSIVE`
- `UNRESOLVED_NOT_DUE`

Terminal results are mapped for QM-C5:

- `SUPPORTED` → `POSITIVE`
- `FALSIFIED` → `NEGATIVE`
- `NEGATIVE_NOT_CONFIRMED` → `NEGATIVE`
- `INCONCLUSIVE` → `INCONCLUSIVE`
- `UNRESOLVED_NOT_DUE` → no result yet

Negative outcomes therefore remain first-class immutable evidence.

## No post-hoc rescue

After a look has been evaluated, L9 does not:

- change the Pattern conditions;
- change target/direction/horizon/baseline;
- select a favorable sector/regime;
- drop an unfavorable support region;
- substitute Discovery evidence;
- change the multiplicity family;
- alter the scheduled look;
- retune the frozen minimum criteria.

Any genuine semantic redesign requires a successor Pattern/Hypothesis/Plan
version and cannot reuse consumed evidence as fresh confirmation.

## Integrity hashes

L9 separates two hashes deliberately.

### Confirmation evidence hash

Binds the statistical/governance evidence core while excluding the QM handoff.
QM-C4 and QM-C5 reference this hash.

This avoids a circular dependency where the handoff itself would contain the
hash of the document containing the handoff.

### Look hash

Binds the complete local L9 report, including the handoff.

Both are verified independently.

## Local append-only persistence

Consumed/evaluated L9 looks are stored under:

`artifacts/research/pattern_discovery/confirmation_looks.jsonl`

The registry is append-only and hash-chained.

The identity is the exact:

`monitoring_plan_id + monitoring_plan_version + look_id`

An identical replay is idempotent.

Different content for an already persisted look fails closed.

Detailed reports are stored under:

`artifacts/research/pattern_discovery/confirmation_reports/{monitoring_plan_id}/{look_id}/{look_hash}.json`

The exact novel confirmation inputs are archived immutably as well:

- baseline bundles:
  `artifacts/research/pattern_discovery/confirmation_inputs/baselines/{baseline_bundle_hash}.json`
- PIT context bundles:
  `artifacts/research/pattern_discovery/confirmation_inputs/contexts/{context_bundle_hash}.json`

This means a terminal negative result does not survive merely as a summary:
the exact baseline population and diagnostic context used by the look remain
hash-addressable for later replay.

`UNRESOLVED_NOT_DUE` is deliberately not written as a consumed look.

## Operational runner

`scripts/pattern_discovery/run_l9_confirmation.py`

Inputs:

- frozen L5 Pattern list/snapshot
- L8 matured outcomes / maturation registry
- L9 baseline bundle
- optional PIT-safe context bundle
- exact QM-C3 control-plan ID/version
- exact QM-C4 monitoring-plan ID/version
- evaluation timestamp
- paths to the existing C1/C2/C3/C4/QM-A registries
- actor identity

The runner produces the next permitted L9 result and persists it only if the
look is actually due/evaluated.

It outputs the exact QM handoff required to apply the consumed look centrally.

## Definition of Done

L9 is complete only when:

- only hash-valid L5/L8 evidence enters confirmation;
- all evidence is strictly post-freeze;
- Discovery evidence cannot affect prospective statistics;
- the baseline is frozen-universe, deterministic and hash-auditable;
- current taxonomy cannot backfill regime/segment context;
- C3 family membership is exact;
- C4 look ordering is exact;
- insufficient maturity consumes no look and cannot falsify;
- 5T/20T/40T/60T remain distinct through Pattern identity;
- Probability, Baseline Lift, Effect Size, Effective-N, support, concentration
  and robust intervals are explicit and separate;
- multiple testing and sequential-look control are machine-readable;
- stability/regime diagnostics cannot become post-hoc rescue mechanisms;
- strong falsification is distinguishable from simple non-confirmation;
- negative/inconclusive terminal results receive exact QM-C5 handoffs;
- the L0 write boundary remains intact;
- no rating, promotion, Decision Layer, portfolio or execution authority is
  created;
- the complete L0-L9 and relevant QM/price-session regression suites are green.

## Next phase

L10 — Rating Engine.
