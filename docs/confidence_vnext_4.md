# Phase 4 — Confidence vNext

Status: research-only legacy audit, PIT testability contract, and preparation for empirical Confidence-vNext research.

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

The Confidence configuration is hard-coded in the scoring engine:

- Data Coverage: 25%
- Signal Confluence: 25%
- Risk Cleanliness: 20%
- Regime Alignment: 20%
- Liquidity Sanity: 10%
- HIGH >= 75
- MED >= 50
- otherwise LOW

There is no independent empirical evidence input in this calculation.

### 2.2 Component audit

#### Data Coverage — 25%

Claimed meaning: data completeness.

Actual implementation: count how many normalized factor keys are non-null.

This does not measure real data coverage in the production path. The scanner first
normalizes factors with `scale_from_universe()`. Missing raw values become numeric
`0.5`. The engine then materializes the normalized keys before Confidence sees them.
A missing raw input can therefore look like a present factor.

The component also contains no source timestamp, freshness, staleness, fetch quality,
PIT status or price-history depth.

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

Because missing raw values become normalized `0.5`, missing Risk inputs can satisfy
`< 0.6` and be counted as clean.

Verdict: **Risk level, not confidence in the Risk model; missingness can be rewarded**.

#### Regime Alignment — 20%

- bull: relative strength + Trend200
- bear: inverse volatility + ROE + margin
- neutral: hard-coded 0.5

Verdict: **directional/model desirability heuristic, not statistical confidence**.

Before the alias compatibility fix merged on 2026-09-22, the bull branch looked for
legacy `rs3m` / `trend200` keys while the engine supplied canonical
`relative_strength` / `trend_200dma`. Aggregate Confidence is therefore not one
invariant metric across that code change.

The deployment date is **not** automatically treated as ambiguous. Provenance is used
when available. The archived run `github-35753072370-1` on 2026-09-22 is known to be
post-fix because its publication ancestry contains the alias-fix commit. Only rows on
the fix date whose producing version cannot be established remain
`transition_unknown`.

#### Liquidity Sanity — 10%

Computes `1 - liquidity_risk`; missing liquidity information is neutralized rather
than treated as missing evidence.

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

Therefore legacy Confidence is structurally tied to the levels of the models it is
supposed to qualify. This is not independent evidence that those models are reliable.

### 2.4 Missingness failure

With all legacy Confidence inputs neutralized to 0.5 in a bull regime, the current
formula can produce:

- Data Coverage = 1.0
- Signal Confluence = 0.0
- Risk Cleanliness = 1.0
- Regime Alignment = 0.5
- Liquidity Sanity = 0.5
- aggregate Confidence = 60.0 / MED

This is the opposite of the Phase-4 fail-closed principle. Unknown evidence must not
be converted into medium confidence merely because missingness became neutral numeric
values upstream.

### 2.5 Call-site audit

The Phase-4 report records the principal call sites and their roles. Important
findings include:

- `engine.py` feeds Confidence already-normalized factors, so original missingness is
  no longer distinguishable there.
- `score_step.py` writes `ConfidenceScore`, `ConfidenceLabel` and
  `ConfidenceBreakdown` into the runtime watchlist.
- the canonical schema maps only aggregate `ConfidenceScore` to `confidence`.
- `daily_research.py` archives aggregate Confidence and label, not the five component
  values.
- the legacy snapshot helper also stores only aggregate Confidence and label.
- presets already use Confidence as a secondary sort key.
- the UI labels it as trust in scoring even though that interpretation has not been
  empirically validated.

There is also an archival mismatch in the optional score-factor payload path:
`compute_scores()` exposes factor breakdown keys `opportunity_factors_0_1` and
`risk_factors_0_1`, while `score_step.py` looks for `opportunity` and `risk` when
`SCANNER_STORE_SCORE_FACTORS` is enabled. Therefore this optional path cannot be
used as evidence that historical normalized factor payloads were archived.

---

## 3. Historical storage audit

### 3.1 Directly stored

The long-term scanner history contains aggregate `confidence` and, for part of the
history, `confidence_label`, along with several raw scanner fields. Provenance fields
such as run/config/universe versions were added prospectively and are not complete
for the old archive.

A manual audit of the current score-history snapshot through 2026-09-23 found:

- 35,627 physical rows before scanner filtering/deduplication
- scan-date range 2026-02-10 through 2026-09-23
- aggregate Confidence first populated on 2026-02-11
- Opportunity/Risk archive fields only from the recent September period
- `liquidity_risk` historically empty in this archive snapshot
- run/config/provenance fields present only prospectively

Same-day reruns exist. Phase-4 history handling therefore retains valid unmarked
legacy scanner rows, excludes known non-scanner sources, and keeps the last appended
observation per `(date, symbol)`.

A crucial migration rule is that blank `observation_type` / `data_source` on old rows
must **not** be interpreted as “not a scanner observation”: those columns were added
later. Known non-scanner rows are excluded instead.

### 3.2 Not directly stored

The historical archive does **not** provide trustworthy time series for:

- Data Coverage component value
- Signal Confluence component value
- Risk Cleanliness component value
- Regime Alignment component value
- Liquidity Sanity component value
- the full `ConfidenceBreakdown`
- normalized factor payloads by default
- historical per-factor source timestamps/freshness/PIT provenance

Consequently the five old component series must not be reconstructed by plugging
archived raw values into today's formula. The old formula consumed cross-sectionally
normalized factors, and the exact historical normalized payload was not archived.

### 3.3 Fail-closed raw-factor presence proxy

The audit proxy now includes **all archived raw factor inputs** relevant to the five
legacy components:

- growth
- ROE
- margin
- debt ratio
- volatility
- drawdown
- RS3M
- Trend200
- liquidity risk

Missing columns count as absent. Because `liquidity_risk` is absent in the audited
history snapshot, the complete raw-factor presence rate is 0. This is deliberate:
the proxy describes archived presence only and must not hide the strongest missingness.
It is **not** a Data Quality score.

### 3.4 Formula-version break

The audit splits aggregate Confidence into:

- `pre_alias_fix`
- `transition_unknown`
- `post_alias_fix`

Classification uses date plus known run/config provenance. On the audited snapshot,
the known 2026-09-22 run is post-fix rather than transition-unknown.

The history shows a large structural shift around the fix: mean Confidence moves from
about 49 on 2026-09-20/21 to about 59 on 2026-09-22/23 while the stock market regime
remains bull. Across overlapping symbols from 2026-09-21 to 2026-09-22, the average
increase is about 10 Confidence points. This supports treating the alias fix as a
formula break rather than market evidence.

### 3.5 Legacy Confidence behaves like a second model score

On the audited post-fix observations (2026-09-22/23), same-observation Spearman
relationships are approximately:

- Confidence vs Scanner Score: +0.82
- Confidence vs Opportunity: +0.74
- Confidence vs Risk: -0.62
- Confidence vs RS3M: +0.70
- Confidence vs Trend200: +0.76
- Confidence vs volatility: -0.44
- Confidence vs drawdown: -0.53

These are **descriptive correlations, not predictive validation**. Their purpose is
to quantify the structural audit finding: the old Confidence is strongly entangled
with Selection/Opportunity/Risk inputs and is not an independent reliability layer.

### 3.6 Label/display boundary

The stored score is rounded to one decimal only after the legacy label has already
been assigned from the unrounded value. The audited history contains a concrete
boundary example on 2026-09-18: `NOW` is stored/displayed as Confidence `50.0` with
label `LOW`. A consumer applying the published threshold to the rounded display value
would expect `MED`.

This is an explainability issue to define cleanly in vNext. Phase 4 does not modify
production labeling in this research PR.

---

## 4. PIT testability matrix

### Legacy aggregate Confidence

**Status:** directly testable as a baseline with version segmentation.

Rules:

- do not pool pre-fix and post-fix observations as one invariant model;
- use provenance on the deployment date;
- same-day reruns are not independent observations;
- post-fix forward windows require time to mature.

### Legacy component breakdown

**Status:** not directly testable.

Reasons:

- component values were not archived;
- normalized input payloads were not archived by the scheduled scanner;
- raw values are not equivalent to historical normalized factors.

### Data Quality

**Status:** partially testable retrospectively.

Retrospective evidence includes raw-field presence and selected run/config metadata.
The following are not safely available throughout history and require prospective
archival:

- per-field source timestamps
- staleness / age
- per-field fetch/source quality
- complete per-factor PIT verification
- fundamental freshness
- source-failure reasons
- exact price-history depth as it was known at each old scan

Current price backfill may be used for outcomes, but its current depth must not be
retroactively presented as historical Data Quality.

### Statistical Confidence

**Status:** testable only as-of.

Current Phase-2/Phase-3 validation results are knowledge available now. They may not
be injected into older scanner dates.

Historical Statistical Confidence requires an expanding/walk-forward construction:

1. At observation date `t`, use only evidence available by `t`.
2. A training outcome with horizon `H` becomes usable only after the full `H`-session
   forward window has matured by `t`.
3. Frozen model/pattern definitions remain frozen.
4. Discovery and validation remain separate.
5. Circular moving observation-date blocks use effective length `2 × H`.
6. Complete date clusters stay together.
7. Occurrence and baseline use the same sampled date sequence.
8. Robust intervals fail closed below two time-separated support regions.
9. iid Wilson/Beta/binomial diagnostics never establish strong evidence.

**Holdout rule:** the Phase-2/Phase-3 holdout has already been used to validate those
phases. It may describe known evidence, but it must **not** be reused to select or tune
Phase-4 Confidence weights, thresholds, mappings or agreement rules. Production
promotion requires new unspent or prospective validation evidence.

### Model Agreement

**Status:** partially testable now, fully testable only with as-of Statistical
Confidence.

Potential model states:

- Selection: available historically from Scanner Score / score percentile
- Timing: only frozen Phase-1B pattern definitions
- Probability: only evidence known as-of the observation date
- Risk: currently validated short-horizon protection information from volatility and
  stored drawdown
- Regime: context only unless separately validated for the claim

An immature model contributes `unknown`, not agreement.

---

## 5. Confidence vNext research architecture

## A. Data Quality

Data Quality is model-claim-specific. A field may be required for one claim and
irrelevant to another.

Candidate evidence:

- required-field coverage
- missing-field count
- invalid/range errors
- source/fetch success
- source timestamp / observation timestamp
- staleness / age
- PIT verification status
- price-history session count observed at scan time
- run ID
- universe version
- config version
- evidence schema version

Forbidden shortcuts:

- high growth means high Data Quality
- low volatility means high Data Quality
- a good Score means high Data Quality
- missing value => 0.5
- today's source/provenance metadata attached retroactively to an old scan

A retrospective raw-presence proxy remains only a coverage diagnostic and cannot be
renamed full Data Quality.

## B. Statistical Confidence

Statistical Confidence attaches to a **model claim and horizon**, not merely to an
asset.

Candidate evidence:

- validation N
- unique observation dates
- time-separated support regions
- robust moving-block interval width and sign
- discovery/validation direction consistency
- probability calibration error
- temporal stability
- ticker concentration / top-symbol share
- effective symbol count
- evidence age/version

Candidate evidence states can include:

- `robust`
- `directional_but_immature`
- `insufficient_evidence`
- `unavailable`

A large point estimate with weak temporal support is never high Statistical
Confidence by itself.

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

A favorable risk level alone does not increase Confidence.

Candidate output fields:

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

Do not discover new patterns in validation data while testing Confidence.

### Probability reliability

Use calibration quality, not positive-return frequency alone:

- Brier score
- calibration-in-the-large
- calibration slope / reliability
- absolute calibration error

Higher Confidence should mean smaller calibration error.

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

Two evidence tables are preferable to hiding all semantics in one scalar.

### Symbol/claim evidence history

Key fields:

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

Key fields:

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

1. Audit legacy Confidence, formula versions, call sites and archive availability.
   **Implemented in this audit PR.**
2. Freeze PIT testability and semantic contract. **Implemented.**
3. Start prospective archival for evidence that cannot be reconstructed historically.
4. Build Data Quality candidates containing no desirability inputs.
5. Build walk-forward Statistical Confidence using only outcomes matured as-of each
   date.
6. Define and freeze a Model Agreement compatibility matrix before fresh validation.
7. Validate reliability using circular `2 × horizon` moving observation-date blocks.
8. Keep 20T/40T/60T fail-closed until temporal support is mature.
9. Use new/unspent or prospective validation evidence for Phase-4 mapping/threshold
   decisions.
10. Decide only after validation whether a scalar 0–100 presentation is empirically
    justified. A structured output is acceptable if a scalar would hide uncertainty.
11. Any production migration occurs in a separate PR after Phase-4 research is
    complete.

---

## 9. PR and CI guardrails

The dedicated Phase-4 workflow runs on pull requests without a path filter, but its
research job is limited to the `phase4-confidence-vnext` head branch. On that PR it
computes the actual Git diff against the base commit and rejects every changed path
outside this explicit research-only allowlist:

- `.github/workflows/confidence_vnext_4.yml`
- `docs/confidence_vnext_4.md`
- `scripts/run_confidence_vnext_4.py`
- `src/scanner/reports/confidence_vnext.py`
- `tests/test_confidence_vnext.py`

This is the actual production-change guard. The booleans in the JSON report document
intent/semantics but are not treated as proof that production code is unchanged.

Before merge, the project rule remains:

1. read the full Codex review history, including older/outdated threads and every
   `Reviewed commit` state;
2. classify every P1/P2 against current head as fixed, partially fixed, reintroduced,
   or irrelevant with evidence;
3. require current CI green;
4. require the historical Phase-4 audit run and report validation green;
5. only then merge.
