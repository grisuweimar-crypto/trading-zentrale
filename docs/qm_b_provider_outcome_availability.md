# QM-B Provider Coverage and Outcome Availability Audit

## Purpose

This package closes a methodological gap between three distinct facts that must not be conflated:

1. a provider/fetch attempt exists;
2. stored price sessions exist for a symbol;
3. a forward outcome for a historical scanner event is actually calculable and eligible for a research denominator.

The package is research-only and changes no productive scanner, scoring, Decision Layer, portfolio, execution or order behavior.

## Frozen audit inputs

The formal audit is frozen at repository commit:

`d329e497aed7699325f2ad761482699224448a80`

Expected input hashes are pinned in `configs/qm_b_provider_outcome_availability_v1.json` and must also match `history_metadata.json`:

- `history_analysis.csv`: `4861bacd140b332699c02cb5c941970ff3e4bb54d218a0eb546ca2c6e4a04382`
- `price_backfill.csv`: `8256d3559783d41d4896d5c5c518239a54f62878c7f1c3d3dc1b456724fc5fe9`
- `latest_scanner.csv`: `a70db1f5cd6d0191477dd790e9007cce3d8dba244fce4cf414ff2e7240e2c7f8`
- metadata as-of: `2026-09-29`

Any mismatch fails the audit rather than silently moving the sample.

## Provider coverage semantics

Price history is **not** historical provider-coverage proof.

A current provider reachability observation requires:

- a provider symbol,
- a stored fetch-attempt timestamp,
- positive stored price coverage (`price_data_ok` or `price_data_partial`), and
- chronological comparison with the published research snapshot timestamp.

Current reachability is classified as:

- `PIT_OBSERVED_AT_OR_BEFORE_SNAPSHOT`
- `POST_SNAPSHOT_OBSERVED`
- `ATTEMPT_WITHOUT_PRICE_COVERAGE`
- `NO_ATTEMPT_EVIDENCE`

A fetch that happened after the research snapshot is explicitly not allowed to become snapshot-PIT evidence. No current observation is back-projected into historical provider coverage.

The audit promotes **zero** strict `provider_coverage` ledger rows. A future strict ledger requires source/family/instrument/as-of evidence with its own PIT support.

## Outcome availability semantics

Historical events use the same conservative event identity as `HistoricalMatcher`:

- only stored scanner observations;
- first stored observation per symbol/date;
- no scanner metric reconstruction from prices.

For each event and each horizon 5/10/20/40 trading sessions:

- start must be an exact valid stored price close on the scanner event date;
- target must be the h-th later valid stored price session for the same observed symbol;
- `AVAILABLE` only when both start and target are present in the frozen price data by audit as-of;
- missing exact start is `MISSING`;
- missing target is `UNKNOWN`.

`NOT_YET_AVAILABLE` is deliberately **not inferred** merely because a target is absent. Without exchange-calendar or equivalent evidence, absence cannot distinguish immaturity from missing coverage.

Only `AVAILABLE` is denominator-eligible. `MISSING` and `UNKNOWN` are excluded; neither may be imputed as neutral.

## PIT limitation

The frozen repository snapshot proves that the price evidence is possessed by this audit. It does **not** prove that the same outcome was already available to an earlier historical analysis at its original execution time.

Therefore this package does not retroactively validate prior denominators and promotes zero strict instrument-level outcome ledger rows. Prior-analysis-time availability belongs in the later evidence-impact review using the relevant historical repository state.

## Definition of done for this package

PASS requires:

- frozen hashes match both contract and metadata;
- unit tests pass;
- current provider observations are timestamp-classified without retrojection;
- every stored historical scanner event is classified for all four forward horizons;
- only `AVAILABLE` outcomes are denominator-eligible;
- no neutral imputation;
- strict provider/outcome promotion remains disabled.

The package can PASS while overall QM-B remains `RESEARCH_REQUIRED` for strict historical provider coverage, stable-instrument outcome binding and prior-analysis-time evidence impact.
