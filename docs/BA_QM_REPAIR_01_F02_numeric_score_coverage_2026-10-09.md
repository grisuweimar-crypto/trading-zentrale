# BA-QM-REPAIR-01 / F02 — Independent numeric score coverage guard

**Source:** Audit `AUD-20261008-A-55991982`, finding `QM-H-AUD-20261008-F02`, [issue #244](https://github.com/grisuweimar-crypto/trading-zentrale/issues/244). Scope: isolated BA-QM12 continuous monitoring and QM-H; not scanner scoring, market data, historical outcomes, Decision, positions or promotion.

## Root cause

The existing BA-QM12 `_snapshot_monitor` consumed `history_metadata.validation.numeric_score_count` and checked the symbol inventory, but did **not** independently enforce `history_metadata.validation.policy.min_score_ratio`. It trusted `validation.status=ok` upstream. This allowed an artificially inconsistent metadata snapshot to pass the monitoring function without verifying the numeric-score threshold. This is a **static independent-control defect / near miss**, *not* proof of a bad live scanner publication.

## Authoritative policy and current production evidence

- The upstream scanner `src/scanner/reports/research_views.py::validate_run` already enforces `numeric_count >= ceil(row_count * policy.min_score_ratio)` and records the policy and numeric count into the published `history_metadata.json`.
- The repaired independent BA-QM12 monitor now uses that **same snapshot's** `validation.policy.min_score_ratio`, never invents or hardcodes a different operating threshold, and fails closed for missing/nonfinite/out-of-range threshold, invalid/missing score counts, or counts below `ceil(symbol_count * threshold)`.
- Baseline before implementation: `snapshot_id=32739832-2481-491f-b131-f7413c07f6b2`, as-of `2026-10-08`, `required_symbol_count=215`, `symbol_count=215`, `numeric_score_count=215`, `min_score_ratio=0.9`. This valid snapshot remains healthy.
- Monitoring receipt adds `required_numeric_score_count`, `numeric_score_ratio`, `min_score_ratio` and `numeric_score_coverage_status=PASS`. No scanner scores or policy thresholds are changed.

## Falsification, review and separate effectiveness gate

- Boundary positive test: 9 of 10 numeric scores at a 0.9 threshold passes.
- Mutated upstream-ok metadata: 8 of 10 at 0.9, 0 of 10, or 9 of 10 at 0.91 all **reject** without changing the upstream status.
- Missing, non-integral, negative and overfull numeric counts fail closed; missing, boolean, text, nonpositive, >1, NaN or infinite thresholds fail closed.
- Independent production snapshot check still passes under original published 0.90 threshold. The test suite uses isolated temporary artifacts and does not edit the frozen production snapshot.
- QM-H CAPA `QM-H-CAPA-AUD-20261008-F02` has append-only transitions #30–33 through `IMPLEMENTED`. Do **not** mark `EFFECTIVENESS_VERIFIED` or `CLOSED` until PR tests and subsequent main checks independently verify the guard.
- All six empirically blocked research residuals, F03/F04 findings, and the unrelated Elliott prospective PIT issue remain unchanged.

**No silent score/decision/portfolio changes; no reconstruction of missing source values or historical promotion.**
