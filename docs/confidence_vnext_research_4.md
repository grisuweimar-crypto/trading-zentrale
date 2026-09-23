# Phase 4B-D — Confidence vNext empirical research

Research-only. This phase does **not** change production Confidence, scanner scoring, Opportunity/Risk weights, R0-R5, Depot Watch, or portfolio decisions.

## Important terminology

Three different things must not be confused:

1. **Scanner Score / scanner points** — the existing scanner output. Phase 4 does not reinterpret a higher raw Score as higher Confidence. The current Score is used only to map the row into the already validated Phase-1A/2 cross-sectional selection backbone B0/B1/B2/B3/B4/B5.
2. **Statistical point estimate** — e.g. observed mean peer alpha or probability advantage. A large point estimate alone is not robust evidence.
3. **Confidence vNext evidence** — the research question is how reliable a current scanner statement is under comparable historical conditions. No new scalar 0-100 Confidence mapping or HIGH/MED/LOW thresholds are created here.

## Asset scope

Phase 1A, 1B, 2 and 3 were validated on **stocks only**. Phase 4B-D therefore applies their evidence only to stock rows. Current crypto rows are explicitly reported as unsupported/excluded; stock evidence is never transferred to crypto. A later crypto-specific evidence layer would require its own validation.

The Score percentile used for the B0-B5 Selection backbone is still computed in the same scanner cross-section before the stock-only evidence filter, matching the existing Phase-1A event construction. Excluding unsupported crypto from Phase 4 does not silently redefine the historical Selection bands.

## Frozen inputs

Phase 4B-D consumes already defined research layers instead of rediscovering them:

- Phase 1A/2: cross-sectional Selection bands and calibrated peer-alpha/probability evidence.
- Phase 1B/2: frozen Timing patterns and their validation evidence.
- Phase 3: downside-protection evidence.

Probability is not an independent model vote; it calibrates Selection and Timing. Risk is not a positive return vote; it is a downside-protection layer. Regime is not counted as a model vote until it has its own empirical validation.

## Statistical Confidence states

The research registry uses ordinal evidence states rather than invented weights:

- `robust`: required moving-block intervals support the same direction and temporal support is sufficient.
- `directional_only`: point estimates agree in direction, but the robust interval test is not fully satisfied.
- `immature`: the horizon/sample/independent temporal support is not mature enough for robust uncertainty.
- `mixed`: available evidence disagrees internally.
- `unavailable`: the required evidence is absent.

For Phase 2-style return evidence, the robust method remains the circular moving observation-date block bootstrap with an effective block length of 2 × the forward horizon. Missing robust intervals or fewer than two independent support regions fail closed as `immature`; they are never replaced by zero-width intervals or a neutral score.

## Risk metric comparability guard

Phase 4 discovered a metric-version/scale discontinuity that makes the current volatility value incomparable with the historical Phase-3 cutoffs.

The stock cross-section median of stored `volatility` is approximately:

- **0.03065 on 2026-09-16**
- **0.48651 on 2026-09-17**

The recent/post-break median is about **16.6×** the pre-break median. It remains near the new level through 2026-09-23. This is an abrupt scale/definition break, not a plausible one-day market-volatility move.

Stored `drawdown` is the control: its median changes from the older historical distribution to the recent distribution by roughly 1.3× and its daily series moves gradually rather than jumping by an order of magnitude on 2026-09-17.

Consequences for Phase 4:

- historical volatility evidence is retained in the registry as a historical research result;
- **pre-break volatility cutoffs are not applied to post-break current values**;
- no multiplicative conversion factor is guessed or backfilled;
- volatility current application is `scale_incompatible` / fail-closed;
- current Model Agreement ignores volatility until the post-2026-09-17 metric epoch has enough independent forward evidence for its own validation;
- stored drawdown remains usable where its existing Phase-3 evidence and current data are otherwise valid.

This does not mean volatility is empirically useless. It means the *current metric version* is not yet comparable with the historical version used to establish the Phase-3 cutoffs.

The workflow runs `scripts/audit_phase4_risk_scale.py` before the Phase-4 registry and archives the diagnostic with the research artifact.

## Data Quality

Historical per-factor freshness/source/fetch-quality is not fully archived, so Phase 4B-D does not pretend to reconstruct it. The current implementation records only a claim-specific presence/provenance proxy:

- whether the fields actually needed for the current Selection/Timing/Risk claim are present;
- whether run/as-of/snapshot/data-source provenance is present;
- whether robust Timing patterns are unevaluable because prerequisites are missing;
- whether a historically validated metric is currently scale/version compatible.

The output explicitly marks this as a proxy, not a complete Data Quality score. Missing or incompatible data are `unknown/insufficient`, never neutral.

## Model Agreement

Agreement is conflict-oriented and does not add model scores together.

- Selection and Timing may both produce a mature return direction.
- Opposite mature Selection/Timing directions create a return conflict.
- A positive mature return claim combined with **comparable and empirically validated** elevated downside risk creates a risk tension/conflict.
- Low risk never counts as positive return support.
- Probability is not counted again as another vote.
- Regime is currently `unknown_unvalidated` and is not counted as a vote.
- A scale-incompatible risk metric cannot create a current conflict merely because its numerical value is above an old cutoff.

This keeps Selection, Timing, Probability, Risk and Regime conceptually separate.

## Point-in-time safeguards

The current scanner date is compared with the `as_of` date of the Phase 2 and Phase 3 evidence artifacts. Evidence dated after the current scanner state is rejected. Historical scanner metrics are not reconstructed from present-day values. Metric-version incompatibility is also treated as unavailable evidence rather than repaired with an assumed transformation.

## Phase 4E gate

Phase 4B-D only freezes the candidate evidence states and agreement logic. Phase 4E must test whether these pre-specified states actually improve reliability on **new/unspent or prospective outcomes**. The Phase-2/3 holdout is already spent for prior validation and must not be reused to tune new Confidence weights or thresholds.

Only after Phase 4E demonstrates monotonic/reliable improvement may a scalar Confidence mapping be considered. A 0-100 score or HIGH/MED/LOW thresholds are deliberately out of scope until then.

## Run

Phase 3 evidence must exist for the same research snapshot. The workflow regenerates it, audits risk-metric comparability, and then runs Phase 4B-D.

```bash
python scripts/run_risk_vnext_3.py --output artifacts/research/risk_vnext_3.json
python scripts/audit_phase4_risk_scale.py
python scripts/run_confidence_vnext_research_4.py
```

Outputs:

- `artifacts/research/confidence_vnext_research_4.json`
- `artifacts/research/confidence_vnext_risk_scale_4.json`
