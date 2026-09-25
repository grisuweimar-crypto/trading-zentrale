# Module 6F — External Market / Sector Context

Status: research-only context data layer for Elliott vNext.

6F does not change productive Scanner scoring, R0–R5, Timing, Risk, Confidence,
Depot-Watch, or portfolio decisions.

## Purpose

6F creates a separate, auditable context universe for market, sector, industry,
theme, commodity, and externally defined peer context.  The data is deliberately
kept outside `history_analysis.csv` so Scanner observations are not silently
reinterpreted as external market evidence.

The canonical artifact is:

`artifacts/research/market_context_history.csv`

The proxy registry is:

`data/inputs/market_context_registry.csv`

The PIT symbol→context assignment registry is:

`data/inputs/market_context_assignments.csv`

## Why the registries are initially empty

No market/sector proxy or asset classification is inserted merely because it
looks plausible.  The foundation registry remains header-only until a proxy has
a documented external source, quality grade, validity interval, and price-basis
policy.  The assignment registry likewise remains header-only until a symbol's
historical relationship to that context can be supported point-in-time.

This is intentional.  Filling the table with today's sector labels and then
projecting them backwards would violate the PIT rule 6G is supposed to test.

## Proxy priority

Frozen priority:

1. official / broad index;
2. broad liquid sector or theme ETF;
3. externally defined, PIT-versioned peer basket;
4. Scanner peers only as separate internal context.

A hand-made basket of a few Scanner symbols must never be presented as an
external sector wave.

## Quality gate

Allowed quality values:

- `sufficient`
- `limited`
- `unreliable`
- `unavailable`

Only `sufficient` and `limited` external proxies can become real-market
evidence.  `unreliable` and `unavailable` fail closed.  `scanner_peer_context`
and `scanner_peer_context_only` never become external market evidence even if
the row otherwise has high technical data quality.

## Versioned registry

Every context row has at least:

- `context_id`
- `context_type`
- `name`
- provider/source symbol
- `proxy_kind`
- source
- `context_quality`
- `price_basis`
- `valid_from`
- `valid_to`

Versions for the same `context_id` may be sequential, but validity intervals may
not overlap.  This makes proxy changes explicit rather than silently rewriting
history.

## PIT assignments

A symbol→context assignment has:

- asset symbol
- `context_id`
- relationship (`market`, `sector`, `industry`, `theme`, `commodity`, etc.)
- source
- assignment quality
- `pit_verified`
- `valid_from`
- `valid_to`
- optional classification version / retrieval metadata

A current classification cannot be used at an older observation date unless its
valid historical interval has been explicitly supported.  `pit_verified=false`
is preserved as provenance but is excluded from historical context resolution.

## OHLC(V) history rules

Observed context data is validated before it enters the canonical history:

- no synthetic missing sessions;
- date + context ID uniquely identify a session;
- conflicting same-day duplicates fail closed;
- open/high/low/close must be finite positive prices;
- volume may be missing, but cannot be negative;
- high/low geometry must be internally valid;
- each row must map to exactly one registry version valid on that date;
- provider/source metadata cannot contradict the registry;
- an `as_of` cutoff hides future context rows.

## Price-basis policy

Context structure can be badly distorted by splits, distributions, or futures
rolls.  6F therefore stores an explicit price-basis policy.

### `raw`

Allowed for an index/spot-like series only when the registry explicitly declares
raw geometry appropriate.

### `adjusted`

Requires complete adjusted-close coverage.  OHLC is scaled coherently by
`adj_close / close`; missing adjusted data fails closed.  There is no fallback to
raw prices.

### `provider_adjusted`

The source itself declares the supplied OHLC as already adjusted or continuous.
6F preserves that declaration and uses the supplied geometry without silently
re-adjusting it.

### `not_eligible_for_elliott`

The context can remain useful descriptively but cannot generate a real-market
Elliott wave.

## Reusing the 6A pivot engine

`prepare_context_ohlcv_for_elliott()` converts an eligible context series into
the same canonical Daily OHLC(V) shape used by 6A.  The existing causal pivot,
scenario, Fibonacci and routing layers can therefore be reused instead of
building a separate Elliott engine for benchmarks.

The function rejects:

- unreliable/unavailable context;
- Scanner-peer-only context;
- `not_eligible_for_elliott` price basis;
- mixed price bases;
- partial adjusted-close coverage.

This means an unreliable proxy cannot produce a purported sector/market wave.

## Context snapshot

`build_context_snapshot()` resolves active PIT assignments for an asset and
returns only the last context observation visible on or before `as_of`.

It records the calendar age of the observation.  6F does not hard-code a
predictive staleness threshold; an optional threshold can annotate staleness,
but 6G must pre-register any rule that affects validation.

## What 6F deliberately does not claim

6F does not yet establish that market/sector context improves returns or Elliott
accuracy.  It does not decide:

- whether relative strength is predictive;
- whether a stock is a leader/follower in a profitable sense;
- whether benchmark Elliott state confirms a stock Elliott state;
- whether context lead/lag has edge;
- whether W3/W4/W5 context improves swing routing.

Those are 6G validation questions.

## Current data status

At merge time the external proxy and assignment registries remain intentionally
unpopulated.  Therefore the canonical history starts as a header-only artifact.
This is **zero verified external-context coverage**, not a failure and not a
license to substitute Scanner peers or guessed classifications.

When verified proxies and PIT assignments are supplied, the same 6F builder can
populate and validate the history without changing the research semantics.

## Local builder

```bash
python scripts/run_elliott_market_context_6f.py
```

The runner validates the registry, assignments and context OHLC(V), rewrites the
canonical history, and emits a coverage/provenance summary to:

`artifacts/research/elliott_market_context_6f.json`

It does not fetch or invent external data by itself.  External acquisition can
be implemented as a separate provider adapter without changing the canonical
6F validation contract.

## Acceptance boundary

6F is technically accepted when:

- context history is separate from Scanner history;
- every context row is registry-backed and quality graded;
- every historical asset→context relationship is PIT-versioned;
- unreliable/unavailable context cannot become real-market Elliott evidence;
- Scanner peers cannot masquerade as external market/sector evidence;
- adjusted series cannot silently fall back to raw prices;
- future context rows remain invisible before their date;
- no trade decision or order instruction is emitted.

Empirical usefulness remains a 6G question.
