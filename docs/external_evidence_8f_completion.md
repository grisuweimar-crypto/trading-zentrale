# Phase 8F Completion Report

Status: `PHASE_8F_SOURCE_AND_EXPOSURE_LAYER_FROZEN_OUTCOME_RESEARCH_PENDING_8G`

Frozen at: `2026-09-28T08:02:30+02:00`

## Scope completed

Phase 8F now has a frozen, outcome-blind macro/exposure source layer suitable as the input boundary for Phase 8G incremental evidence research.

The freeze contains:

1. strict PIT/vintage rules for macro observations;
2. versioned documentary exposure mappings with human review;
3. a research domain pinned to the exact pre-8F active-stock universe;
4. full documentary accounting of all 207 frozen-domain subjects;
5. an append-only real prospective macro ledger;
6. deterministic domain and completion audits;
7. explicit source/factor states for implemented, unobserved and deferred families;
8. a cryptographic freeze manifest binding the passing live run and its evidence artifacts.

## Frozen research-domain accounting

- frozen subjects: **207**;
- active documentary mappings: **195**;
- explicit-unmapped subjects: **12**;
- unaccounted subjects: **0**;
- historical mapping intervals retained: **201**, consisting of 195 active intervals and 6 superseded intervals.

The domain remains pinned to pre-8F `universe_master.csv` blob `2c6efec912ea7478096f77dc8f88d7eed2a0581b` at source ref `feef4e572283613739b2f24cf42b837ff68a1508`.

## Real PIT freeze snapshot

GitHub Actions run `36381751961` on Phase-8F head `10f37e7d28e533d1ec9599ab75edc5beb0e82e6f` produced the real outcome-blind snapshot used for the freeze.

The completion gate returned:

- `PASS_8F_COMPLETION`;
- `freeze_allowed=true`;
- blockers: none;
- 25 knowable ledger rows;
- 4 observed series;
- 195 active reviewed mappings;
- 207/207 domain subjects accounted.

Observed in the freeze snapshot:

- Federal Reserve H.15 effective federal funds rate;
- Federal Reserve H.15 2-year Treasury constant maturity;
- Federal Reserve H.15 10-year Treasury constant maturity;
- ECB USD/EUR reference rate.

All 25 rows are prospective-ingestion PIT observations. No historical current value was retrojected.

## Factor/source states at freeze

### Implemented and observed in the real freeze snapshot

- `rates_policy` — Federal Reserve H.15;
- `yield_curve` — Federal Reserve H.15 2Y and 10Y;
- `fx` — ECB USD/EUR.

### Implemented but not observed in this freeze snapshot

- `inflation` — BLS archived-release adapter exists, but the freeze live snapshot did not contain a CPI release observation;
- `oil` and `gas` — EIA prospective adapters exist, but the freeze run had `EIA_API_KEY` unavailable and therefore recorded `SKIPPED_NO_API_KEY`.

Unobserved means unknown for that snapshot; it is not converted to neutral evidence.

### Explicitly deferred, not promoted in Phase 8F

- `gold`;
- `silver`;
- `copper`.

World Bank Pink Sheet remains the preferred open candidate. Official World Bank material establishes monthly commodity publications and reusable dataset metadata, but Phase 8F did not prove a complete row-level historical first-release/vintage reconstruction. These families are therefore frozen as `DEFERRED_HISTORICAL_VINTAGE_RECONSTRUCTION_NOT_PROMOTED_IN_8F`, not silently treated as validated PIT series.

### Challenger-only families

- `uranium`;
- `lithium`.

Their registered market proxies remain challengers and may not be relabelled as underlying spot prices.

## Hard boundaries retained

- market outcomes not read;
- no bullish/bearish macro direction;
- no signed exposure inference;
- no weights;
- no threshold selection;
- no cross-factor interaction research;
- no Phase-7 integration;
- no production external evidence;
- no missing-to-neutral fallback;
- no historical current-value retrojection;
- no later-revision overwrite of an earlier vintage.

## What Phase 8F completion means

Phase 8F completion means the macro/exposure source layer, research denominator, documentary exposure map and PIT boundaries are deterministic, explicit, auditable and frozen sufficiently to begin outcome research without changing the design after seeing results.

It does **not** mean any macro factor has predictive value. It does not promote deferred or challenger factors, does not assign market direction and does not alter Phase 7.

Incremental/OOS outcome testing begins only in **Phase 8G**. Cross-factor interactions remain Phase 8H work, and Decision Layer integration remains Phase 8I work.
