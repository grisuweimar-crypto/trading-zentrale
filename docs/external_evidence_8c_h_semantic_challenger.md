# Phase 8C-H – High-Precision Semantic Challenger

## Objective

8C-H is the first layer allowed to turn an 8C-G text anchor into a concrete **issuer-side semantic candidate**. It remains a quarantined challenger and is not production evidence.

The first version intentionally supports only two narrow families:

1. explicit regular quarterly dividend declarations with an amount per share;
2. explicit new share/stock repurchase authorizations with a stated maximum amount.

Guidance, capital raises and earnings beat/miss remain disabled.

## Why the scope is narrow

A broad keyword parser would create attractive but unreliable labels. 8C-H therefore requires explicit action verbs and numeric structure rather than treating historical descriptions or generic mentions as events.

Examples:

- `The board declared a quarterly dividend of ... per share` can become a dividend declaration observation.
- `The board authorized a new ... share repurchase program` can become a buyback-action candidate.
- `Under the existing share repurchase program ...` is only historical/context language and is not promoted.

## Dividend safety rules

A dividend observation does **not** mean increase/cut. It records a declared amount only.

For later comparison, both currency and security class must be explicit:
- `USD 0.30 per common share` may be comparison-eligible;
- `$0.30 per common share` keeps currency `UNKNOWN`;
- `USD 0.30 per share` keeps security class `UNKNOWN`.

Special dividends are excluded from the regular-dividend challenger.

## Buyback safety rules

Only an explicit authorization/approval plus repurchase language and a stated `up to` amount is accepted by the first challenger grammar.

The semantic action is `NEW_AUTHORIZATION`, but `market_direction` always remains `UNKNOWN`.

A bare `$` currency marker is not automatically converted to USD.

## Evidence integrity

Before semantic extraction, the 8C-H parser recomputes the SHA-256 of every 8C-G excerpt. A modified or corrupted anchor is rejected as `ANCHOR_HASH_MISMATCH`.

Every accepted candidate retains:
- symbol/CIK;
- accession number;
- source `valid_from`;
- source document;
- evidence excerpt hash;
- deterministic extraction method;
- parser version;
- semantic status;
- reason codes.

All accepted outputs remain:

`semantic_status = EXTRACTED_CANDIDATE`

and

`market_direction = UNKNOWN`

## Disabled areas

### Guidance

Still disabled. Reliable extraction needs metric, period/horizon, unit, accounting basis and numeric range/value under the 8C-E comparability contract.

### Capital raises

Still disabled. The parser must reliably distinguish announced/priced/completed/cancelled state and equity/debt/convertible/mixed instruments.

### Earnings beat/miss

Still blocked by Phase 8B. A historical beat/miss calculation requires historical point-in-time analyst consensus, not just an issuer filing.

## Research boundary

8C-H does not inspect returns, assign positive/negative market impact, modify Phase 7 or create portfolio actions. Promotion requires separate precision/coverage validation on real SEC snapshot data before any outcome study is allowed.
