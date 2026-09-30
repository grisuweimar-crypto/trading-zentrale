# QM-B Market Tradability Evidence

## Scope

This package defines a research-only, PIT-safe evidence contract for the `market_tradability` dimension used by QM-B project investability.

Tradability is **venue-specific**. It is separate from:

- stable instrument identity,
- project-universe membership,
- listing state / venue assignment,
- execution-channel or broker availability,
- project restrictions.

## Core rule

`TRADABLE` requires explicit instrument-level evidence from at least one registered venue/source that was actually available no later than the as-of time.

The following are **not** accepted as positive tradability evidence:

- a price or OHLCV row exists,
- non-zero volume exists,
- the scanner observed the instrument,
- a provider quote request succeeded,
- the symbol has an exchange-like suffix,
- the instrument is present in the project universe,
- the instrument is listed.

Likewise, absence of a halt/suspension event is not enough to infer `TRADABLE` unless the source semantics explicitly provide a complete current-state feed and that source has been registered for promotion.

## Statuses

- `TRADABLE`
- `SUSPENDED`
- `NOT_TRADABLE`
- `UNKNOWN`

Aggregation is conservative:

1. any PIT-verified venue `TRADABLE` -> instrument `TRADABLE`;
2. otherwise any venue `SUSPENDED` -> instrument `SUSPENDED`;
3. otherwise, if at least one venue is observed and every observed venue is `NOT_TRADABLE` -> `NOT_TRADABLE`;
4. otherwise -> `UNKNOWN`.

Conflicting latest evidence on the same venue fails closed to `UNKNOWN` for that venue.

## Candidate official sources

The current source assessment records candidate official exchange sources, but none is yet registered for strict promotion:

- Nasdaq Trader Trade Halt RSS / halt history: instrument-level halt/resume information for U.S. securities; candidate for prospective halt-state evidence, subject to adapter and license/persistence review.
- Japan Exchange Group / TSE Trading Halts: current and archived issue-level trading halt/resume information; candidate for prospective Japanese tradability evidence, subject to adapter and license/persistence review.
- HKEX suspension reporting: useful reference evidence but not currently treated as a complete current instrument-level tradability ledger.

No candidate source is automatically promoted merely because it is public or official.

## Current repository audit

The repository currently has:

- 207 stable positive project-membership claims,
- zero registered market-tradability sources,
- zero durable instrument-level market-tradability evidence.

Therefore the current audit must return:

- `UNKNOWN`: 207
- positive tradability count: 0
- gap: `BLOCKED_NO_MARKET_TRADABILITY_EVIDENCE`

This is a source gap, not evidence that the instruments are suspended or non-tradable.

## Promotion boundary

This package does not change scanner production behavior and does not write strict As-of Universe rows. Market tradability can participate in strict investability only after a source is explicitly registered, PIT semantics are validated, stable identity/venue mapping is aligned, and storage/license policy is cleared.
