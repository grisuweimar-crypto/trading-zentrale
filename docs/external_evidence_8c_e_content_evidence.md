# Phase 8C-E – Issuer Content Evidence Contract

## Objective

8C-E defines how filing content may support richer issuer-side event semantics without assigning market direction or using future returns.

This slice is still a **contract/validator layer**. It does not enable a production filing-text parser yet.

## Supported issuer-side semantics

### Guidance changes

Guidance can be compared only when the prior and current disclosures refer to the same:
- metric;
- period/horizon;
- unit;
- accounting basis.

For range guidance:
- both bounds higher/non-lower with at least one strict increase → `RAISE`;
- both bounds lower/non-higher with at least one strict decrease → `CUT`;
- identical range → `UNCHANGED`;
- one bound up while the other moves down → `MIXED`.

These are issuer semantic changes. `market_direction` remains `UNKNOWN`.

### Dividend changes

Regular dividends are comparable only for the same security class, currency and frequency. Special dividends are modeled separately and must never be folded into regular dividend-growth/cut logic.

### Buybacks

The contract permits explicit issuer actions such as:
- new authorization;
- expansion;
- extension;
- cancellation;
- completion update.

No action is automatically positive or negative.

### Capital raises

The contract distinguishes action state (`ANNOUNCED`, `PRICED`, `COMPLETED`, `CANCELLED`) and instrument (`EQUITY`, `DEBT`, `CONVERTIBLE`, `MIXED`).

A shelf registration is financing capacity, not proof that capital was raised. It may not be labeled as a completed raise without transaction evidence.

## Earnings beat/miss is intentionally blocked

Beat/miss is not an issuer-only fact. It is defined relative to an analyst-consensus value that must itself be known point-in-time before the earnings release.

Because Phase 8B historical revisions/consensus is paused for cost/PIT reasons, `EARNINGS_BEAT` and `EARNINGS_MISS` remain disabled in 8C-E. They can be reopened only when a historical consensus source is classified `SAFE`, or after the project has collected enough prospective consensus history itself.

This avoids a common leakage error: comparing a historical reported EPS to today's revised/aggregated consensus record.

## Evidence provenance

Any future parser output must retain:
- CIK;
- accession number;
- source `valid_from`;
- source document;
- SHA-256 of the exact evidence excerpt;
- extraction method;
- parser version;
- semantic status;
- explicit reason codes.

A later amended filing cannot retroactively replace the original evidence record.

Free-form semantic labels without a provenance record are not admissible production evidence.

## Research boundary

8C-E does not:
- read market outcomes;
- assign market direction;
- alter Phase 7;
- create portfolio actions;
- enable beat/miss;
- enable a production text parser.

The next implementation slice can add content-document acquisition and deterministic extraction candidates under this contract, preferably against the same frozen SEC snapshot architecture used by 8C-C/8C-D.
