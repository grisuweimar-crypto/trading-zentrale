# Phase 8D — B3 PASS and B4 descriptive feature contract

## B3 real validation result

The frozen 2026Q2 SEC insider audit completed with:

- decision: `PASS_SOURCE_SEMANTICS_VALIDATION`
- reviewed annotations: 299
- market outcomes read: false
- Phase-7 integration enabled: false
- one mechanical `FALSE` for `transaction_price_correct_when_present`
- one mechanical `FALSE` for `transaction_shares_correct`
- every other mechanically checked field was `TRUE` for all 299 reviewed rows

The pass validates source semantics/provenance for the defined P/S challenger. It does not establish market direction or predictive value.

## Consequence for B4

B4 is an outcome-blind descriptive feature layer. It is implemented in:

- `configs/external_evidence_8d_insider_features_v1.json`
- `src/scanner/research/external_evidence/sec_insider_features.py`
- `scripts/run_external_evidence_8d_insider_features.py`
- `tests/test_external_evidence_8d_insider_features.py`

### Frozen windows

- primary: trailing 30 calendar days
- robustness only: trailing 90 calendar days

The 90-day window may not replace the 30-day primary window after outcome inspection without a new preregistration.

### PIT semantics

A transaction may contribute only when:

1. the issuer CIK matches the requested as-of identity,
2. `strict_pit_eligible == true`,
3. `valid_from <= as_of`,
4. the transaction date lies inside the trailing calendar window,
5. the source row is Form 4, not Form 4/A,
6. the row remains in the validated high-precision P/S scope,
7. 10b5-1-flagged, unresolved-10b5-1 and equity-swap rows remain excluded.

The implementation requires continuous source-quarter coverage across the complete feature window. Missing earlier quarters produce `INSUFFICIENT_SOURCE_WINDOW`, never zero/neutral evidence.

### Feature tiers

Validated descriptive core:

- `purchase_count`
- `sale_count`

Validated only with complete single-owner identity across included transactions:

- `distinct_buyer_count`
- `distinct_seller_count`

Numeric challenger only:

- `purchase_shares`
- `sale_shares`
- `purchase_value_when_price_known`
- `sale_value_when_price_known`

The numeric features remain challenger-only because the real B3 mechanical audit contained one shares mismatch and one price mismatch. B4 may compute them for QA/research preparation, but `numeric_features_promoted` remains false.

### Amendments

Form 4/A remains preserved as versioned source evidence but is excluded from B4 v1 aggregation. This avoids silently double-counting an original filing and its amendment before an explicit amendment-reconciliation rule has itself been validated.

## Research boundary

B4 does not read market outcomes, assign bullish/bearish direction, select thresholds, alter Phase 7, or enable production external evidence. Incremental predictive testing belongs to Phase 8G after the relevant Phase-8 source families are completed/frozen.
