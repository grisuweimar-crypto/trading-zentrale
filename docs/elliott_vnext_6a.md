# Module 6A – Elliott vNext data and causal pivots

Status: research-only implementation candidate.

## Scope

6A creates the causal market-data/pivot substrate required by all later Elliott-vNext work. It does **not** count waves, calculate Fibonacci targets, route portfolio actions, alter Scanner scores, or make trading decisions.

The authoritative parent contracts remain:

- `configs/elliott_vnext_contract_v2.json`
- `configs/elliott_vnext_output_schema_v2.json`

## Canonical daily source

6A reuses `scanner.data.price_history.validated_rows` as the daily observed-session source instead of introducing another price-history definition.

Rules:

- no synthetic sessions;
- incomplete O/H/L/C bars fail closed;
- mixed currencies within one symbol history fail closed;
- an optional `as_of` cutoff is applied before research calculations;
- raw OHLC remains available only through an explicit raw basis;
- adjusted analysis uses the already archived `adj_close / close` factor to scale O/H/L consistently;
- `auto` uses adjusted OHLC only when adjusted close is available for every retained session, raw OHLC only when adjusted close is absent for every retained session, and fails closed on partial adjusted coverage.

This avoids silently mixing split/dividend-adjusted and unadjusted geometry.

## Weekly aggregation

Weekly bars are derived deterministically from daily observations using ISO weeks.

A weekly row stores:

- `date`: last actually observed session in the week;
- O/H/L/C/V aggregated from observed sessions only;
- `session_count`;
- `bar_confirmed_time`: first observed session of the next ISO week.

The newest week is therefore not considered confirmed merely because the current file happens to end on a Friday. This is deliberately conservative and keeps the logic usable for different market calendars, including seven-day instruments.

## Pivot causality

Each pivot has two distinct dates:

- `pivot_time`: the historical bar on which the local extreme occurred;
- `confirmed_time`: the first time the fixed right-hand confirmation window is itself causally available.

`available_from == confirmed_time`.

The detector truncates its input to bars whose own `bar_confirmed_time <= as_of` before evaluating a pivot. A point-in-time prefix test verifies that running the detector on the full history with an `as_of` cutoff produces exactly the same confirmed pivots as physically truncating the history at that cutoff.

## Candidate wave-degree parameters

`DEFAULT_PIVOT_SPECS` currently contains several daily and weekly parameter candidates combining fixed left/right windows with causal ATR/excursion filters.

These values are intentionally marked `validated=False`. They are a research grid, not an Elliott truth claim and not a production calibration. Their stability, confirmation latency, gap sensitivity and behavior on illiquid series must be measured before later phases rely on them.

## Outside bars and ambiguity

If one daily/weekly bar is simultaneously the unique local high and local low of its confirmation window, both candidate extrema may be emitted with `sequence_ambiguous=true`. Daily OHLC does not reveal which intrabar extreme occurred first, so 6A must preserve the ambiguity rather than invent an order.

## Current acceptance tests

6A must prove at minimum:

1. adjusted OHLC scaling is internally consistent;
2. partial adjusted coverage fails closed;
3. a weekly bar is unavailable until the following week is observed;
4. a pivot is unavailable before `confirmed_time`;
5. confirmed historical pivots are prefix-invariant;
6. default parameter candidates remain explicitly unvalidated;
7. pivot output is research-only and contains no trade/order action.

## Next step

After 6A is green, 6B may consume only these causally available pivots to generate multiple plausible structural scenarios and enforce Elliott hard rules. Fibonacci remains out of scope until 6C.
