# Phase 8D-B3 — Real SEC Insider Validation Finalization

Status: outcome-blind real-data validation in progress.

## Frozen real corpus

- Quarter: `2026Q2`
- Audit rows: `299`
- Blind semantic labels are assigned only from the official SEC quarterly raw tables.
- Market outcomes, future returns, Phase-7 decisions and parser candidate-status/reason-code fields are not visible to the semantic annotator.

## Two-stage validation

### Stage 1 — blind semantic annotation

Input:

- `validation_source_evidence_2026q2.json`
- `validation_annotations_blind_2026q2.csv`

The raw pack exposes only the frozen `validation_id` and original SEC `SUBMISSION`, `REPORTINGOWNER` and `NONDERIV_TRANS` rows. The annotator assigns the preregistered truth labels without seeing parser candidate status or reason codes.

The first completed real annotation produced these raw-source labels:

- `VALID_P_TRANSACTION`: 52
- `VALID_S_TRANSACTION`: 140
- `VALID_EXCLUDED_10B5_1`: 106
- `NOT_TARGET`: 1
- total: 299

These counts are descriptive source-validation results, not market-direction evidence.

### Stage 2 — mechanical parser/PIT verification

`sec_insider_b3_finalize.py` and `scripts/finalize_external_evidence_8d_b3_audit.py` fill the critical-field checks mechanically. Human annotators do not assert parser correctness.

Each frozen audit row is compared against the independent raw evidence for:

- issuer CIK
- accession number
- transaction code
- acquired/disposed code
- transaction date
- transaction shares
- transaction price when present
- reporting-owner linkage

`valid_from` is independently reconstructed from raw SEC submissions metadata in the operator-attested Phase-8C snapshot using the original `filingDate` / `acceptanceDateTime`. The finalizer does not copy the B1 `valid_from` value back as its own proof.

The local SEC snapshot remains unchanged.

## Final command

Place the completed semantic annotation CSV at:

`artifacts/external_evidence/8d_sec_insider/validation_annotations_blind_completed_2026q2.csv`

Then run:

```powershell
.venv-1\Scripts\python.exe scripts\finalize_external_evidence_8d_b3_audit.py
```

Outputs:

- `validation_annotations_final_2026q2.csv`
- `validation_mechanical_checks_2026q2.json`
- `validation_result_2026q2.json`

The command then evaluates the original preregistered B3 gates. It does not alter thresholds, sampling, truth labels or market-outcome guards.

## Hard boundary

A mechanical `PASS_SOURCE_SEMANTICS_VALIDATION` validates only source semantics / extraction / provenance. It does **not** establish that insider purchases or sales predict returns, and it does not enable production external evidence or Phase-7 integration. Outcome research remains a later Phase-8 step.
