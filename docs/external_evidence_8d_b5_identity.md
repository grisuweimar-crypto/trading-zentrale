# Phase 8D-B5 — PIT-safe insider-family identity resolution

Status: `IMPLEMENTED_OUTCOME_BLIND`

B5 implements the identity step already required by the Phase-8A `external_universe_coverage_contract_v1` for the SEC insider family. It does not create a second universe contract.

## Purpose

Historical scanner observability and external-source identity are separate facts:

- the internal scanner/history determines that a symbol was observable by the project at an `as_of` timestamp;
- the SEC insider evidence may resolve that historical scanner symbol to an issuer CIK only after the symbol/CIK relationship itself was publicly known;
- failure to resolve identity remains explicit missingness and may not become neutral evidence.

## Inputs

1. historical scanner observations containing exact `symbol` and timezone-aware `as_of`;
2. one or more PIT-validated `external_evidence_8d_sec_insider_bulk_v1` evidence artifacts.

B5 does **not** use current SEC ticker arrays, the current scanner universe, fuzzy company-name matching, ticker-suffix stripping or ADR substitution to fill historical gaps.

## Resolution semantics

For each issuer CIK, B5 considers only identity evidence with `valid_from <= scanner_as_of` and determines the latest symbol that was known for that CIK at that time.

A scanner observation is:

- `VERIFIED_FAMILY_PIT_IDENTITY` when exactly one issuer CIK has that exact latest-known symbol;
- `UNKNOWN_NO_PIT_IDENTITY` when no prior PIT symbol/CIK evidence exists;
- `AMBIGUOUS_PIT_IDENTITY` when multiple CIKs or conflicting same-time symbol evidence prevent a unique mapping.

A later SEC identity observation may change the latest-known symbol for a CIK. From that later `valid_from` onward, the former symbol is no longer resolved to that CIK by B5.

Date-only scanner observations are rejected. B5 does not invent an intraday time because doing so could make a filing appear known before the scanner actually ran.

## Conservative coverage limitation

The B5 v1 identity evidence comes only from the already PIT-validated SEC insider evidence rows. Therefore a company may be observable in scanner history but remain `UNKNOWN_NO_PIT_IDENTITY` until the first eligible historical SEC insider identity observation becomes available.

This is intentional. B5 prefers lower coverage to retrospective current-ticker leakage.

## B4 bridge

`scripts/run_external_evidence_8d_insider_identity.py` writes:

- `insider_asof_identity.json` with every scanner observation and explicit identity status;
- `insider_feature_grid.csv` containing only deduplicated `(issuer_cik, as_of)` pairs from `VERIFIED_FAMILY_PIT_IDENTITY` rows.

The second artifact is suitable as the `--asof-grid` input to the B4 feature runner. `UNKNOWN` and `AMBIGUOUS` identity rows are never passed into the feature grid.

## Hard boundaries

- scanner history remains the primary project observability ledger;
- current ticker retrojection is disabled;
- future identity evidence cannot resolve an earlier scanner observation;
- exact symbols only;
- unknown or ambiguous identity is explicit;
- no market outcomes are read;
- no market direction is assigned;
- no Phase-7 integration is enabled;
- no production external evidence is enabled.
