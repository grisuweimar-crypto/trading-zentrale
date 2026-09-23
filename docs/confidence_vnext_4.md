# Phase 4 — Confidence vNext

Status: research-only audit and research contract.

Phase 4 does **not** change production Confidence, Scanner Score, Opportunity weights,
Risk weights, R0–R5, Depot-Watch or portfolio decisions.

## 1. Target semantics

Confidence must answer:

> How much should we trust the model claim that is currently being made?

It must **not** answer:

> How attractive is this asset?

or:

> How low is this asset's risk?

The target architecture therefore separates three pillars:

1. **Data Quality** — is the claim based on complete, fresh, PIT-valid and technically
   trustworthy data?
2. **Statistical Confidence** — is the empirical evidence behind this claim and
   horizon mature and robust?
3. **Model Agreement** — do independently conceived, statistically mature model
   claims tell a compatible story?

Missing evidence is `unknown / insufficient evidence`, never an automatic neutral
value and never a hidden reward.

---

## 2. Audit of the current production implementation

### 2.1 Call path and configuration

The production path is:

`score_step.apply_scoring()`
→ `engine.calculate_scores_v6_from_row()`
→ `compute_confidence()`

The Confidence configuration is currently hard-coded in the scoring engine:

- Data Coverage: 25%
- Signal Confluence: 25%
- Risk Cleanliness: 20%
- Regime Alignment: 20%
- Liquidity Sanity: 10%
- HIGH >= 75
- MED >= 50
- otherwise LOW

There is no independent empirical evidence configuration in this calculation.

### 2.2 Component audit

#### Data Coverage — 25%

Claimed meaning: data completeness.

Actual implementation: count how many normalized factor keys are non-null.

This does not work as real data coverage in the production path. The scanner first
normalizes factors with `scale_from_universe()`. Missing raw values are converted to
`0.5`. The engine then materializes every core factor key. Consequently the
Confidence layer normally sees numeric values even when the original raw value was
missing.

Result: this component can report full coverage even though source data were absent.
It also contains no source timestamp, freshness, staleness, fetch quality, PIT status
or price-history depth.

Verdict: **invalid for the claimed Data Quality meaning**.

#### Signal Confluence — 25%

Counts these Opportunity factors when their normalized value is above 0.7:

- growth
- ROE
- margin
- relative strength
- Trend200

Verdict: **Selection/Opportunity strength, not Confidence**.

#### Risk Cleanliness — 20%

Counts these Risk factors as "clean" when their normalized value is below 0.6:

- volatility
- max drawdown
- debt-to-equity

Because missing raw values become normalized `0.5`, missing risk inputs can satisfy
`< 0.6` and be counted as clean.

Verdict: **Risk level, not confidence in the Risk model; missingness may be rewarded**.

#### Regime Alignment — 20%

- bull: relative strength + Trend200
- bear: inverse volatility + ROE + margin
- neutral: hard-coded 0.5

Verdict: **directional/model desirability heuristic, not statistical confidence**.

Historical version caveat: before the alias compatibility fix on 2026-09-22, the
bull branch looked for legacy `rs3m` / `trend200` keys while the engine supplied
canonical `relative_strength` / `trend_200dma`. Therefore aggregate Confidence is
not one invariant metric across that code change.

The deployment day 2026-09-22 is treated as a transition day unless the exact
`config_version` of a row proves which code version produced it.

#### Liquidity Sanity — 10%

Computes `1 - liquidity_risk`; missing liquidity risk receives 0.5.

Verdict: **favorable Risk level, not reliability of the estimate**.

### 2.3 Mechanical double counting

The current Confidence directly reuses inputs already used elsewhere:

**Selection / Opportunity**

- growth
- ROE
- margin
- relative strength / RS3M
- Trend200

**Risk**

- volatility
- max drawdown
- debt-to-equity
- liquidity risk

**Regime Alignment additionally reuses**

- relative strength / RS3M
- Trend200
- volatility
- ROE
- margin

Therefore legacy Confidence is structurally correlated with the levels of the models
it is supposed to qualify. This is not independent evidence that those models are
reliable.

### 2.4 Missingness failure

The current normalizer returns 0.5 for missing numeric values. With every legacy
Confidence input neutralized to 0.5 in a bull regime, the legacy formula produces:

- Data Coverage = 1.0
- Signal Confluence = 0.0
- Risk Cleanliness = 1.0
- Regime Alignment = 0.5
- Liquidity Sanity = 0.5
- aggregate Confidence = 60.0 / MED

This is the opposite of the Phase-4 fail-closed principle. A model with unknown raw
inputs must not receive a medium-confidence interpretation merely because missingness
was converted to neutral numbers upstream.

---

## 3. Historical storage audit

### 3.1 Directly stored

The long-term scanner history stores:

- aggregate `confidence`
- `confidence_label`
- several raw scanner fields used by the old heuristic
- run / universe / config metadata where available

The first archived date is 2026-02-10, but Confidence is blank on that date.
Populated aggregate Confidence is observed in the archive from 2026-02-11.

Same-day reruns exist. Historical research must keep the established keep-last
source-order semantics per `(date, symbol)` and must not treat reruns as independent
observations.

### 3.2 Not directly stored

The long-term history does **not** store:

- Data Coverage component value
- Signal Confluence component value
- Risk Cleanliness component value
- Regime Alignment component value
- Liquidity Sanity component value
- the full `ConfidenceBreakdown`
- normalized factor payloads by default

`ConfidenceBreakdown` is available in the current scored watchlist output but is not
persisted into the append-only `score_history` archive.

The scheduled scanner also does not enable `SCANNER_STORE_SCORE_FACTORS`, so the
normalized Opportunity/Risk factor payload is not a reliable historical archive.

### 3.3 Consequence

The five old component time series must **not** be reconstructed by simply plugging
archived raw values into today's formulas. The old Confidence consumed normalized
cross-sectional values, and exact normalized historical inputs were not archived.

The aggregate legacy Confidence itself may be audited because it was stored, but:

- its formula changed around 2026-09-22;
- the transition day is ambiguous without exact producing code metadata;
- the current post-fix formula has almost no matured forward history yet;
- it is only a baseline audit target, not the definition of Confidence vNext.

---

## 4. PIT testability matrix

### Legacy aggregate Confidence

**Status:** directly testable with version segmentation.

Use only as a baseline. Do not pool pre-fix, transition and post-fix observations as
one invariant model.

### Legacy component breakdown

**Status:** not directly testable.

Reasons:

- component values were not archived;
- normalized input payloads were not archived by the normal scheduled run;
- raw values are not equivalent to historical normalized factors.

### Data Quality

**Status:** partially testable now.

Available retrospectively:

- raw-field presence
- run/config/universe identifiers where stored
- price-history coverage/session depth from the research data layer
- some fetch/coverage diagnostics in the current research metadata

Not safely available historically:

- per-field source timestamps
- per-field staleness
- source/fetch quality for every historical observation
- complete per-factor PIT verification
- historical fundamental freshness

Those missing fields must be archived prospectively.

### Statistical Confidence

**Status:** testable only with as-of evidence.

Current Phase-2/Phase-3 results are knowledge available now. They may not be assigned
retroactively to February, April or August observations.

A historical Statistical Confidence backtest therefore needs a walk-forward / expanding
as-of reconstruction:

1. At observation date `t`, use only historical model observations available by `t`.
2. A training outcome with horizon `H` is usable only after its full `H`-session forward
   window has matured by `t`.
3. Frozen model/pattern definitions stay frozen.
4. Discovery and holdout remain separate.
5. Uncertainty uses circular moving observation-date blocks with effective block length
   `2 × H`.
6. Complete date clusters stay together.
7. Occurrence and baseline are sampled on the same date sequence.
8. Robust intervals fail closed with fewer than two time-separated occurrence support
   regions.
9. iid Wilson/Beta/binomial diagnostics never establish strong evidence.

### Model Agreement

**Status:** partially testable now, fully testable only after as-of Statistical Confidence
is available.

Potential historical model states:

- Selection: available from Scanner Score / score percentile
- Timing: only frozen Phase-1B pattern definitions
- Probability: only evidence known as-of the observation date
- Risk: 5T volatility and stored drawdown are the currently validated PIT-capable
  protection factors
- Regime: context only unless independently validated for the claim being evaluated

An immature model contributes `unknown`, not agreement.

---

## 5. Confidence vNext research architecture

## A. Data Quality

Data Quality is model-claim-specific. A field may be required for one claim and
irrelevant to another.

Candidate evidence:

- required-field coverage
- missing field count
- invalid/range errors
- source/fetch success
- source timestamp / observation timestamp
- staleness / age
- PIT verification status
- price-history session count
- run ID
- universe version
- config version
- evidence schema version

Forbidden shortcuts:

- high growth means high Data Quality
- low volatility means high Data Quality
- a good Score means high Data Quality
- missing value => 0.5

A first retrospective raw-presence proxy may be studied, but it remains explicitly a
coverage-only diagnostic and cannot be renamed full Data Quality.

## B. Statistical Confidence

Statistical Confidence attaches to a **model claim and horizon**, not merely to an
asset.

Candidate evidence:

- validation N
- unique observation dates
- time-separated support regions
- robust moving-block CI width and whether it excludes the relevant null
- Discovery-vs-Validation direction consistency
- probability calibration error
- temporal stability
- ticker concentration / top-symbol share
- effective symbol count
- evidence age/version

Suggested evidence states:

- `robust`
- `directional_but_immature`
- `insufficient_evidence`
- `unavailable`

Do not convert a large point estimate with weak temporal support into high Statistical
Confidence.

## C. Model Agreement

Agreement uses pre-specified model states, not raw score magnitudes.

Examples:

- strong Selection + robust positive Timing + no high downside-risk warning
  → compatible
- strong Selection + robust negative Timing
  → conflict
- robust positive Timing + high short-term downside risk
  → tension/conflict
- attractive Selection + immature Timing
  → Timing is unknown, not agreement

Possible output fields:

- available models
- mature models
- compatible pairs
- conflict pairs
- agreement state: `aligned / mixed / conflicted / insufficient`
- machine-readable reasons

---

## 6. Validation targets

Confidence is not optimized for maximum return.

### Selection reliability

Test whether higher Confidence corresponds to:

- stronger out-of-sample Score → future peer-alpha ordering
- lower future-rank prediction error
- lower dispersion of prediction error

### Timing reliability

For frozen Timing claims only, test whether higher Confidence corresponds to a higher
rate of the **pre-declared expected direction** being confirmed.

Do not discover new patterns in holdout while testing Confidence.

### Probability reliability

Use calibration quality, not positive-return frequency alone:

- Brier score
- calibration-in-the-large
- calibration slope / reliability
- absolute calibration error

Higher Confidence should mean smaller error.

### Risk reliability

Test whether higher Confidence makes the established protection relationship more
reliable:

- future path max drawdown
- adverse excursion
- tail-drawdown classification/separation

Do not define success as low-risk assets producing more alpha.

### Agreement reliability

Compare external failure/error rates for pre-specified `aligned`, `conflicted` and
`insufficient` states. Statistical maturity gates participation.

---

## 7. Prospective archival contract

Two different evidence tables are preferable to hiding everything in one scalar.

### Symbol/claim evidence history

Keyed at least by:

- as_of
- run_id
- symbol
- confidence_evidence_version
- claim_id

Data Quality fields:

- data_quality_state
- required count
- present count
- fresh count
- PIT-verified count
- invalid count
- price-history sessions
- source-failure count

Agreement fields:

- agreement state
- available models
- mature models
- conflicts / reasons

### Model/horizon evidence history

Keyed at least by:

- as_of
- model_id
- claim_id
- horizon_sessions
- evidence_version

Statistical fields:

- validation N
- unique observation dates
- support regions
- robust interval low/high
- calibration error where applicable
- top-symbol share
- effective symbol count
- evidence state

This archive starts prospectively. Missing historical provenance is not fabricated.

---

## 8. Research sequence

1. Audit legacy Confidence and formula versions. **Implemented in this first Phase-4
   research step.**
2. Freeze the PIT availability matrix and semantic contract. **Implemented.**
3. Start prospective archival for unavailable evidence fields.
4. Build Data Quality candidates that contain no desirability inputs.
5. Build walk-forward Statistical Confidence using only matured outcomes as-of each date.
6. Freeze a Model Agreement compatibility matrix before final holdout validation.
7. Validate reliability with the established circular `2 × horizon` moving-date bootstrap.
8. Keep 20T/40T/60T fail-closed until temporal support is mature.
9. Decide only after validation whether a scalar 0–100 presentation is empirically
   justified. A structured Confidence output is acceptable if a scalar would hide
   important uncertainty.
10. Production migration, if justified, occurs in a separate PR after Phase-4 research
    is complete.

---

## 9. Implementation guardrails

The Phase-4 research code must fail CI if it accidentally claims production changes.
The research report records explicit booleans confirming that production Confidence,
Scanner Score, Opportunity/Risk weights, R-codes and portfolio logic remain unchanged.

The report also records formula epochs around the 2026-09-22 alias fix rather than
silently treating the aggregate legacy Confidence as homogeneous.
