# Phase 4B-D — Confidence vNext empirical research

Research-only. This phase does **not** change production Confidence, scanner scoring, Opportunity/Risk weights, R0-R5, Depot Watch, or portfolio decisions.

## Important terminology

Three different things must not be confused:

1. **Scanner Score / scanner points** — the existing scanner output. Phase 4 does not reinterpret a higher raw Score as higher Confidence. The current Score is used only to map the row into the already validated Phase-1A/2 cross-sectional selection backbone B0/B1/B2/B3/B4/B5.
2. **Statistical point estimate** — e.g. observed mean peer alpha or probability advantage. A large point estimate alone is not robust evidence.
3. **Confidence vNext evidence** — the research question is how reliable a current scanner statement is under comparable historical conditions. No new scalar 0-100 Confidence mapping or HIGH/MED/LOW thresholds are created here.

## Frozen inputs

Phase 4B-D consumes already defined research layers instead of rediscovering them:

- Phase 1A/2: cross-sectional Selection bands and calibrated peer-alpha/probability evidence.
- Phase 1B/2: frozen Timing patterns and their validation evidence.
- Phase 3: downside-protection evidence. At the current maturity only volatility and stored drawdown have robust 5-session protection evidence.

Probability is not an independent model vote; it calibrates Selection and Timing. Risk is not a positive return vote; it is a downside-protection layer. Regime is not counted as a model vote until it has its own empirical validation.

## Statistical Confidence states

The research registry uses ordinal evidence states rather than invented weights:

- `robust`: required moving-block intervals support the same direction and temporal support is sufficient.
- `directional_only`: point estimates agree in direction, but the robust interval test is not fully satisfied.
- `immature`: the horizon/sample/independent temporal support is not mature enough for robust uncertainty.
- `mixed`: available evidence disagrees internally.
- `unavailable`: the required evidence is absent.

For Phase 2-style return evidence, the robust method remains the circular moving observation-date block bootstrap with an effective block length of 2 × the forward horizon. Missing robust intervals or fewer than two independent support regions fail closed as `immature`; they are never replaced by zero-width intervals or a neutral score.

## Data Quality

Historical per-factor freshness/source/fetch-quality is not fully archived, so Phase 4B-D does not pretend to reconstruct it. The current implementation records only a claim-specific presence/provenance proxy:

- whether the fields actually needed for the current Selection/Timing/Risk claim are present;
- whether run/as-of/snapshot/data-source provenance is present;
- whether robust Timing patterns are unevaluable because prerequisites are missing.

The output explicitly marks this as a proxy, not a complete Data Quality score. Missing data are `unknown/insufficient`, never neutral.

## Model Agreement

Agreement is conflict-oriented and does not add model scores together.

- Selection and Timing may both produce a mature return direction.
- Opposite mature Selection/Timing directions create a return conflict.
- A positive mature return claim combined with empirically elevated downside risk creates a risk tension/conflict.
- Low risk never counts as positive return support.
- Probability is not counted again as another vote.
- Regime is currently `unknown_unvalidated` and is not counted as a vote.

This keeps Selection, Timing, Probability, Risk and Regime conceptually separate.

## Point-in-time safeguards

The current scanner date is compared with the `as_of` date of the Phase 2 and Phase 3 evidence artifacts. Evidence dated after the current scanner state is rejected. Historical scanner metrics are not reconstructed from present-day values.

## Phase 4E gate

Phase 4B-D only freezes the candidate evidence states and agreement logic. Phase 4E must test whether these pre-specified states actually improve reliability on **new/unspent or prospective outcomes**. The Phase-2/3 holdout is already spent for prior validation and must not be reused to tune new Confidence weights or thresholds.

Only after Phase 4E demonstrates monotonic/reliable improvement may a scalar Confidence mapping be considered. A 0-100 score or HIGH/MED/LOW thresholds are deliberately out of scope until then.

## Run

Phase 3 evidence must exist for the same research snapshot. The workflow regenerates it before Phase 4B-D.

```bash
python scripts/run_risk_vnext_3.py --output artifacts/research/risk_vnext_3.json
python scripts/run_confidence_vnext_research_4.py
```

Output:

`artifacts/research/confidence_vnext_research_4.json`
