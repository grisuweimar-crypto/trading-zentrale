# BA-QM8 – End-to-End Scanner Audit

Status: **Contract foundation active / engineering in progress**

BA-QM8 audits the complete Scanner-vNext information path. It does not add new
investment logic, change productive scanner semantics, promote research evidence
or release any existing governance block.

## Audited stage chain

```text
Data
  -> Scanner
  -> Selection
  -> Timing
  -> Probability
  -> Risk
  -> Confidence
  -> Learning
  -> Elliott
  -> Decision Layer
  -> External Evidence
```

Every adjacent transition is required to cover the same seven error classes:

1. Leakage
2. Retrojection
3. Double Counting
4. Semantic Drift
5. Missing-as-neutral
6. Uncontrolled Multiplicity
7. Hidden Evidence-Reuse

The canonical machine-readable contract is
`configs/ba_qm8_scanner_e2e_audit_v1.json`.

## Foundation guarantees

The first BA-QM8 work package binds the audit to the existing governance stack:

- BA-QM7 / QM-J must remain engineering-complete.
- The triggered Phase-1A Lag-1 negative control remains visible.
- Finding `QM-H-QMJ-PHASE1A-LAG1-001` remains active.
- CAPA `QM-H-CAPA-QMJ-PHASE1A-LAG1-001` remains implemented but not
  effectiveness-verified.
- Evidence impact remains `PROMOTION_BLOCKED`.
- A green BA-QM8 audit, W11 result or Decision Watch run cannot release that
  promotion block.
- QM-I lineage remains authoritative for ancestry and independence review.
- Missing lineage is not evidence of independence.
- External Evidence stays a separate post-decision research family and may not
  retroactively rewrite Selection, Timing, Probability, Risk, Confidence,
  Elliott, historical Decision state or Portfolio Action.
- Missing external evidence is not silently neutral evidence.
- No order or execution path is enabled.

## Existing contracts reused, not rebuilt

BA-QM8 deliberately reuses existing controls:

- **W10**: same-snapshot orchestration, availability timestamps, upstream freeze,
  no-backdating.
- **W11**: real-snapshot 7A–7H acceptance, PIT, missing-evidence, duplicate-claim,
  super-score and Phase-8 boundary guards.
- **W12 / Decision Watch**: Ferrari F1–F4 regression behavior and review-only
  decision presentation constraints.
- **QM-I**: typed evidence lineage and double-counting review.
- **QM-J / QM-H**: negative-control finding, CAPA and promotion-stop lifecycle.
- **External Evidence Phase 8 contract / 8C freeze**: PIT provenance,
  non-retrojection, non-productive research boundaries.

BA-QM8 is therefore an umbrella audit across the existing modules, not a second
implementation of them.

## Current package

Implemented in the foundation package:

- machine-readable BA-QM8 stage and transition contract;
- exact seven-class audit coverage for every adjacent stage transition;
- fail-closed stage missingness requirements;
- QM-I lineage binding;
- External Evidence non-retrojection and non-rewrite boundaries;
- explicit preservation of the Lag-1 `PROMOTION_BLOCKED` state;
- manipulation tests that reject weakened contracts;
- CI regression workflow against BA-QM7/QM-J, freshness gate, W11 and W12.

## Not yet complete

This package does **not** close BA-QM8.

The next required package is the executable runtime audit layer: create
transition-level fail-closed guards and manipulation fixtures for each of the
seven error classes, then run regressions across BA-QM7/QM-J, QM-H, QM-I, W11,
W12/Decision Watch and External Evidence. Only after those controls pass may a
BA-QM8 closure package be considered.
