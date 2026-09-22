# Danelfin comparison harness

Purpose: compare the existing Trading-Zentrale scanner with Danelfin as an
independent external model. This is a research harness only. It does not change
production scoring, watch logic, scanner history, or portfolio decisions.

## Security

The Danelfin API key is read only from `DANELFIN_API_KEY`.

Recommended local setup in the repository root:

```text
DANELFIN_API_KEY=your_key_here
```

The repository already ignores `.env`. Never put the key in source code, CLI
arguments, URLs, generated reports, issues, workflow files, or commits.

## Run

From an environment where the package dependencies are installed:

```bash
python scripts/run_danelfin_comparison.py
```

Defaults:

- source: `artifacts/research/history_analysis.csv`
- optional extra prices: `artifacts/research/price_backfill.csv`
- first pass: US-listed scanner symbols only
- up to 30 symbols
- 5-session cooldown between retained observations per symbol
- scanner state must be no more than 3 calendar days older than the Danelfin date
- horizons: 5, 20, 40, and 60 observed trading sessions
- Danelfin positive threshold: AI Score >= 8
- scanner positive threshold: stored `rank_percentile <= 0.20`
- benchmark: SPY when SPY price observations are available

Useful examples:

```bash
python scripts/run_danelfin_comparison.py --limit 20
python scripts/run_danelfin_comparison.py --symbols AAPL,MSFT,ISRG,ROK
python scripts/run_danelfin_comparison.py --markets us,europe --limit 30
python scripts/run_danelfin_comparison.py --danelfin-positive-min 9 --scanner-top-percentile 0.10
```

## Outputs

- `artifacts/research/danelfin_comparison_events.csv`
- `artifacts/research/danelfin_comparison.json`

The CSV contains the aligned point-in-time observations and forward outcomes.
The JSON summarizes, per horizon:

- number of usable observations
- Spearman rank correlation of scanner score vs outcome
- Spearman rank correlation of Danelfin AI Score vs outcome
- separate correlations for Fundamental, Technical, Sentiment and Low Risk
- agreement groups: both positive, Danelfin only, scanner only, neither
- positive rate, median outcome and mean outcome for each group

If benchmark observations exist, outcome is alpha versus the benchmark.
Otherwise the report explicitly falls back to raw forward return for that
horizon instead of inventing benchmark data.

## Point-in-time rules

1. Danelfin score date is the event date.
2. The scanner state must be from the same date or an earlier date; future
   scanner observations are never used.
3. A maximum scanner staleness of three calendar days is allowed by default.
4. The event-day observed close is the start price.
5. Forward horizons use the N-th later observed price session, not calendar days.
6. Repeated daily signals are thinned with a five-session cooldown to reduce
   autocorrelation.
7. Unsupported markets are excluded rather than guessed. The first pass uses US
   symbols; Europe can be enabled explicitly. Canada, Hong Kong, Japan,
   Australia, Korea and crypto require separate coverage/mapping work.

## Interpretation

This harness is deliberately not a trading signal. Its job is to answer a more
basic question before Scanner-vNext work begins: which Danelfin dimensions add
independent explanatory or predictive information beyond the existing scanner,
and over which horizon?
