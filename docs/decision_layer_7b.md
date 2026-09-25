# Phase 7B — Decision Research Dataset

## Purpose

Phase 7B builds the historical/prospective research table that later Phase 7
steps may use to study evidence combinations. It remains strictly upstream of
Evidence Fusion, Universal Stance, hysteresis and Portfolio Action.

The frozen contract is:

`configs/decision_research_dataset_v1.json`

The builder is:

`src/scanner/research/decision_layer/dataset.py`

The default outputs are:

- `artifacts/research/decision_research_7b.csv`
- `artifacts/research/decision_research_7b_metadata.json`

Generated outputs are research artifacts; Phase 7B itself does not wire them
into Scanner production or Depot-Watch decisions.

## Core methodological boundary

A useful Decision Layer needs a table containing both claim-time evidence and
later outcomes. Those two classes of information must never be confused.

Every row therefore has two time domains:

1. **feature time** — information available at the scanner observation;
2. **label time** — forward outcomes that become knowable only when the future
   horizon has completed.

A future return, peer excess, adverse excursion or future drawdown is always a
label. It can never participate as an input feature on the observation date.

## Research partitions

Phase 7B freezes a spent/unspent boundary before Phase-7 fusion research starts:

- through **2026-09-25**: `legacy_replay_spent`
- from **2026-09-26**: `prospective_unspent`

The old partition can be used for diagnostics, replay and rule discovery. It
cannot later be presented as fresh confirmation for a Phase-7 rule that was
selected using knowledge of the same historical period.

This is intentionally conservative. Even rows from the final freeze day remain
spent.

## Row identity and source data

The initial v1 dataset is stock-only and uses the already validated Phase-1/2/3
research sources:

- `artifacts/research/history_analysis.csv`
- `artifacts/research/price_backfill.csv`
- `artifacts/research/timing_patterns_1b_frozen.json`

The row identity is:

`symbol + obs_date + start_market_date`

The same market-session alignment used by Phase 1A is retained. Same-session
weekend/holiday scanner repeats therefore do not become duplicate independent
research events.

Crypto is deliberately deferred in v1 because the completed Selection/Timing /
Probability/Risk validation architecture was stock-only. Crypto must receive a
separate validated extension rather than inheriting stock evidence silently.

## Selection replay

Selection fields are reconstructed from the point-in-time scanner row:

- Score
- Score percentile
- score-percentile quality band (`B0`/`B1`…`B5` backbone)
- exact R-code only where it was genuinely stored
- Score status
- TrendOK
- LiquidityOK

The B4/B5 backbone remains distinct from exact historical R4/R5. Missing R-code
gates are never guessed.

## Timing replay

Phase 7B applies the already frozen Phase-1B pattern definitions to historical
point-in-time feature rows.

For every 5T/20T/40T/60T horizon it records all matching frozen patterns with:

- deterministic pattern ID
- pattern definition
- original frozen discovery direction
- horizon

The Phase-1B catalog became available on 2026-09-23. Matches before that date
are explicitly marked `retrospective_replay_spent`; they are not represented as
claims that the model knew the pattern at that historical date.

Pattern replay is not Evidence Fusion. Phase 7B does not choose a winning
pattern, net the directions, or convert matches into BUY/HOLD/SELL.

## Probability

Current Phase-2 calibrated probability knowledge is **not** copied back onto old
rows.

Each historical horizon therefore records that a typed probability claim was
not historically archived as-of that row. Genuine probability evidence must be
captured prospectively through the Phase-7A typed evidence boundary.

This is necessary because using today's validation report as a historical
feature would be look-ahead leakage.

## Risk

The historical table retains raw point-in-time risk state where actually stored:

- aggregate scanner risk
- volatility
- stored drawdown
- debt ratio
- liquidity risk
- downside deviation
- beta

Missing values remain missing. The current Phase-3 conclusion that volatility
and stored drawdown have validated short-horizon protection value is **not**
retrojected into old rows as if it had been known then.

Thus historical Risk in 7B is raw state for research, not a pre-certified vote.

## Confidence

Historical aggregate legacy Confidence and its stored label are retained only as
`legacy_confidence` diagnostics.

They are not renamed Confidence-vNext and do not receive the semantics of Phase
4/5 retrospectively. The dataset explicitly records that Confidence-vNext is not
retrojected.

Prospective Phase-5/7A reliability evidence must be archived as typed evidence
when it is actually available.

## Elliott vNext

Old Elliott-like scanner columns are not treated as Module-6H evidence.

Module 6 was completed only later and 6H has a strict causal output contract.
Historical rows therefore report 6H as unavailable/not retrojected. Genuine 6H
objects can enter future Phase-7B data only through the Phase-7A boundary.

## Forward outcome labels

For every 5 / 20 / 40 / 60 trading-session horizon the dataset records:

- horizon end market date
- adjusted-close forward return
- leave-one-symbol-out peer excess using the validated same-currency peer method
- entry-relative adverse excursion
- future path maximum drawdown
- peer-excess direction
- `label_available_from_H`
- `label_mature_H`

Adjusted close remains mandatory. Raw-close fallback is forbidden.

`label_available_from_H` is the horizon end date. This makes it mechanically
visible when a label did not yet exist.

## Dependence

The dataset deliberately retains overlapping daily observations. It does **not**
pretend they are iid samples.

Phase 7C and later statistical work must preserve the existing research rule:
complete observation-date clusters and horizon-aware moving-block methods are
required where uncertainty or confirmation is assessed.

The dataset builder does not thin rows simply to manufacture independence.

## Metadata and reproducibility

The metadata artifact records:

- source file SHA-256 fingerprints
- output SHA-256 fingerprint
- row and symbol counts
- date range
- spent/unspent partition counts
- mature-label counts by horizon
- timing-match coverage
- Timing and Risk feature-coverage diagnostics
- semantic guard flags

A rebuilt dataset can therefore be tied to exact source inputs.

## Explicitly forbidden in 7B

The research table rejects any of these columns:

- `universal_stance`
- `portfolio_action`
- `trade_decision`
- `order_instruction`
- `buy_signal`
- `sell_signal`
- `position_size`
- `target_weight`

Phase 7B also performs no weighting, threshold optimization or conflict
resolution.

## Run

```bash
python scripts/run_decision_research_7b.py
```

Custom output paths can be used for validation or CI:

```bash
python scripts/run_decision_research_7b.py \
  --output /tmp/decision_research_7b.csv \
  --metadata /tmp/decision_research_7b_metadata.json
```

## Boundary to Phase 7C

Phase 7C may use the **spent** replay partition to discover and pre-register
candidate Evidence-Fusion / conflict rules. It must not claim that performance
on the same partition is independent confirmation.

Final confirmation must use evidence that was genuinely unspent relative to the
rule freeze. The Phase-7B partition markers are the mechanical guard for that
separation.
