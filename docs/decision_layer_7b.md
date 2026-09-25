# Phase 7B — Decision Research Dataset

## Purpose

Phase 7B builds the historical/prospective research package that later Phase-7
steps may use to study evidence combinations. It remains strictly upstream of
Evidence Fusion, Universal Stance, hysteresis and Portfolio Action.

The frozen contract is:

`configs/decision_research_dataset_v1.json`

The historical builder is:

`src/scanner/research/decision_layer/dataset.py`

Prospective typed evidence is handled separately by:

`src/scanner/research/decision_layer/evidence_archive.py`

The default research outputs are:

- `artifacts/research/decision_research_7b.csv`
- `artifacts/research/decision_research_7b_metadata.json`
- optional prospective sidecar: `artifacts/research/decision_evidence_7a.jsonl`

Generated outputs are research artifacts; Phase 7B does not wire them into
Scanner production or Depot-Watch decisions.

## Core methodological boundary

A useful Decision Layer needs both claim-time evidence and later outcomes. Those
two classes of information must never be confused.

Every historical row therefore has two time domains:

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
cannot later be presented as fresh confirmation for a Phase-7 rule selected
using knowledge of the same period.

Even rows from the final freeze day remain spent.

## Row identity and source data

The initial v1 historical table is stock-only and uses the validated research
sources:

- `artifacts/research/history_analysis.csv`
- `artifacts/research/price_backfill.csv`
- `artifacts/research/timing_patterns_1b_frozen.json`

The row identity is:

`symbol + obs_date + start_market_date`

The same market-session alignment used by Phase 1A is retained. Same-session
weekend/holiday repeats therefore do not become duplicate independent events.

Crypto is deliberately deferred in v1 because the completed Selection/Timing /
Probability/Risk validation architecture was stock-only. Crypto must receive a
separate validated extension rather than inheriting stock evidence silently.

## Selection replay

Selection fields are reconstructed from the point-in-time scanner row:

- Score
- Score percentile
- score-percentile quality band (`B0`/`B1`…`B5` backbone)
- exact R-code only where genuinely stored
- Score status
- TrendOK
- LiquidityOK

The B4/B5 backbone remains distinct from exact historical R4/R5. Missing R-code
gates are never guessed.

## Timing replay

Phase 7B applies the frozen Phase-1B pattern definitions to historical
point-in-time feature rows. For every 5T/20T/40T/60T horizon it records all
matching frozen patterns with deterministic pattern ID, frozen definition,
original discovery direction and horizon.

The corrected Phase-1B catalog was frozen on **2026-09-23**. Because the exact
intraday ordering relative to scanner observations on that date is not proven,
Phase 7B fails closed: **2026-09-23 remains replay-only** and the first
unambiguous `available_as_of_observation` date is **2026-09-24**.

Pattern matches before that availability date are explicitly marked
`retrospective_replay_spent`; they are not represented as claims that the model
already knew the frozen pattern then.

Pattern replay is not Evidence Fusion. Phase 7B does not choose a winning
pattern, net directions, or convert matches into BUY/HOLD/SELL.

## Probability

Current Phase-2 calibrated probability knowledge is **not** copied back onto old
rows. Each historical horizon records that a typed probability claim was not
historically archived as-of that row.

Using today's calibration report as an old feature would be look-ahead leakage.
Genuine future Probability evidence is instead preserved in validated Phase-7A
packets in the prospective sidecar archive.

## Risk

The historical table retains raw point-in-time Risk state where actually stored:

- aggregate scanner risk
- volatility
- stored drawdown
- debt ratio
- liquidity risk
- downside deviation
- beta

Missing values remain missing. Current Phase-3 conclusions about validated
short-horizon protection are **not** retrojected as though they had been known
at old observation dates.

Historical Risk in 7B is therefore raw state for research, not a pre-certified
vote. Future typed Risk evidence can additionally be preserved through the 7A
packet archive.

## Confidence

Historical aggregate legacy Confidence and its stored label are retained only as
`legacy_confidence` diagnostics.

They are not renamed Confidence-vNext and do not receive Phase-4/5 reliability
semantics retrospectively. Prospective Confidence-vNext evidence must enter as a
typed Phase-7A packet from the time it genuinely exists.

## Elliott vNext

Old Elliott-like scanner columns are not treated as Module-6H evidence.

Module 6 was completed later and 6H has a strict causal output contract.
Historical rows therefore report 6H as unavailable/not retrojected. Genuine 6H
objects are preserved prospectively through validated Phase-7A packets.

## Prospective typed-evidence sidecar

The prospective archive is:

`artifacts/research/decision_evidence_7a.jsonl`

It is valid for this file not to exist before the first future packet. That is
reported as explicit zero coverage rather than replaced with synthetic data.

Every packet is revalidated through the complete 7A contract before archival.
Duplicate `(source_snapshot_id, symbol, as_of)` identities fail closed.

The archive is deliberately **not joined to historical rows by calendar date**.
Date-only joining could attach a later intraday packet to an earlier scanner run
on the same day. Alignment requires a sufficiently strong scanner snapshot
identity, principally `source_snapshot_id`.

The full sidecar rules are documented in:

`docs/decision_layer_7b_prospective_archive.md`

## Forward outcome labels

For every 5 / 20 / 40 / 60 trading-session horizon the historical table records:

- horizon end market date
- adjusted-close forward return
- leave-one-symbol-out peer excess using the validated same-currency peer method
- entry-relative adverse excursion
- future path maximum drawdown
- peer-excess direction
- `label_available_from_H`
- `label_mature_H`

Adjusted close remains mandatory. Raw-close fallback is forbidden.

`label_available_from_H` is the horizon end date, making it mechanically visible
when a label did not yet exist.

## Dependence

The dataset deliberately retains overlapping daily observations. It does **not**
pretend they are iid samples.

Phase 7C and later statistical work must preserve the existing research rule:
complete observation-date clusters and horizon-aware moving-block methods are
required where uncertainty or confirmation is assessed.

The builder does not thin rows merely to manufacture independence.

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
- prospective typed-packet archive status and counts
- semantic guard flags

A rebuilt dataset can therefore be tied to exact source inputs while missing
future evidence remains explicit.

## Explicitly forbidden in 7B

The historical research table rejects:

- `universal_stance`
- `portfolio_action`
- `trade_decision`
- `order_instruction`
- `buy_signal`
- `sell_signal`
- `position_size`
- `target_weight`

The prospective archive inherits the same 7A action/order prohibitions.

Phase 7B performs no weighting, threshold optimization or conflict resolution.

## Run historical dataset build

```bash
python scripts/run_decision_research_7b.py
```

Custom output paths can be used for validation or CI:

```bash
python scripts/run_decision_research_7b.py \
  --output /tmp/decision_research_7b.csv \
  --metadata /tmp/decision_research_7b_metadata.json
```

## Archive one future 7A packet

```bash
python scripts/archive_decision_input_7a.py \
  --input packet.json \
  --archive artifacts/research/decision_evidence_7a.jsonl
```

Packets before 2026-09-26 are rejected by default. They can be retained only for
diagnostics with the explicit `--allow-spent` option and remain labelled spent.

## Boundary to Phase 7C

Phase 7C may use the **spent** replay partition to discover and pre-register
candidate Evidence-Fusion / conflict rules. It must not claim that performance
on the same partition is independent confirmation.

Full six-family combinations cannot be claimed historically where Probability,
Confidence-vNext or Elliott 6H did not exist. As genuinely prospective 7A
packets accumulate, later Phase-7 evaluation can align them by snapshot identity
and test those interactions out of sample.

Final confirmation must use evidence genuinely unspent relative to the rule
freeze. The Phase-7B partition markers and prospective sidecar provide the
mechanical boundary for that separation.
