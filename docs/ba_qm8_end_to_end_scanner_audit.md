# BA-QM8 – End-to-End Scanner Audit

Status: **Engineering in progress / real-stage audit active / closure blocked**

BA-QM8 audits the complete Scanner-vNext information path. It adds no new
investment logic, does not change productive scanner semantics, does not promote
research evidence and cannot release existing governance blocks.

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

## Governance invariants

BA-QM8 preserves the existing QM state:

- BA-QM7 / QM-J remains engineering-complete.
- Finding `QM-H-QMJ-PHASE1A-LAG1-001` remains active.
- CAPA `QM-H-CAPA-QMJ-PHASE1A-LAG1-001` remains implemented but not
  effectiveness-verified.
- Evidence impact remains `PROMOTION_BLOCKED`.
- A green BA-QM8, W11 or Decision Watch result cannot release that block.
- Prospective unspent evidence is still required for CAPA effectiveness.
- Missing lineage is not evidence of independence.
- Missing evidence is not silently neutral evidence.
- External Evidence remains a separate post-decision research family.
- No order or execution path is enabled.

## Implemented BA-QM8 packages

### Package 1 – Contract foundation

Implemented:

- explicit eleven-stage BA-QM8 contract;
- all ten adjacent transitions;
- exact seven-class error coverage;
- fail-closed missingness policy;
- QM-I lineage binding;
- External Evidence non-retrojection/non-rewrite boundaries;
- explicit preservation of the open Lag-1 promotion block;
- contract manipulation tests;
- BA-QM7, freshness-gate, W11 and W12 regression CI.

### Package 2 – Transition guard engine

Implemented:

- executable guard for every adjacent BA-QM8 transition;
- falsification tests for all seven mandatory error classes;
- future-evidence / availability leakage block;
- retrojected-value and backdating block;
- duplicate-material-evidence and unresolved ancestry-review block;
- semantic-role and declared-input drift block;
- Missing-as-neutral block;
- uncontrolled multiplicity block;
- hidden / undeclared evidence-reuse block;
- same-snapshot binding check;
- non-adjacent stage shortcuts rejected.

The guard engine is generic and fail-closed. A transition cannot receive a pass
unless all seven error classes are explicitly satisfied.

### Package 3 – Real sealed-snapshot binding

The audit is now also run against the current checked-in real artifacts:

- `artifacts/research/daily_research.json`
- `artifacts/research/decision_snapshot_w10.json`
- `artifacts/research/current_decision_packets_7a.json`
- External Evidence contract and frozen 8C completion boundary

Current audited snapshot:

- snapshot date: `2026-10-03`
- snapshot id: `e0181fa8-04f2-432a-adf2-9fc33668bf05`
- W10 status: `sealed`
- final 7A SHA-256:
  `dd6e48407788895cade648dc69c65c96b183ab2570945a86e49eea1d1a85e711`
- 213 packets
- 2,954 material claims

Current claim inventory:

| Family | Claims |
| --- | ---: |
| Selection | 213 |
| Timing | 211 |
| Probability | 1,063 |
| Risk | 426 |
| Confidence | 1,041 |

### Package 4 – Prospective 10/10 verification gate

Implemented:

- BA-QM8 runs automatically after a successful main-branch Decision Watch seal;
- overlapping prospective audits are consolidated so only the latest sealed state is evaluated;
- the workflow now distinguishes two valid runtime states instead of hard-coding the historical one:
  - pre-provenance snapshot: DATA remains `PARTIAL`, 9/10 transitions pass, closure remains pending;
  - provenance-bound prospective snapshot: DATA is `PASS`, all 10/10 transitions pass, engineering closure becomes eligible;
- neither branch may release the independent Lag-1 `PROMOTION_BLOCKED` state;
- prospective 10/10 eligibility still does not itself perform BA-QM8 engineering closure or claim empirical validation/promotion.

This package is the handoff from engineering preparation to the first real
prospective end-to-end verification.

## Current real-stage result

| Stage | Status | Current audit result |
| --- | --- | --- |
| Data | **PARTIAL** | historical source paths are named, but raw source hashes are not snapshot-bound |
| Scanner | PASS | snapshot identity and publication time verified |
| Selection | PASS | exactly one Selection claim per current packet |
| Timing | PASS | 211 PIT timing claims from frozen Phase-1B discovery direction |
| Probability | PASS | preserved Phase-2 source; not reconstructed |
| Risk | PASS | preserved Phase-3 source; missing values not backfilled |
| Confidence | PASS | same snapshot verified; non-directional |
| Learning | PASS | Phase-5 governance is shadow-only and non-productive |
| Elliott | PASS_BOUNDARY | currently not supplied; explicitly not neutral; decision effect = none |
| Decision Layer | PASS_PRIVACY_BOUNDARY | 7A hash verified; private 7D–7H runtime remains non-persisted by design |
| External Evidence | PASS_DISABLED_BOUNDARY | no Phase-7 integration, no portfolio-action replacement, no order generation |

## Open BA-QM8 finding — refined after pipeline trace

The original real audit correctly found that DATA -> SCANNER provenance was
missing, but the first diagnostic wording was too broad. A full pipeline trace
shows that `history_recent.csv` and `price_backfill.csv` are downstream
research/outcome inputs. They are **not** the direct inputs that create the
scanner score snapshot.

The direct scanner path is:

```text
persistent watchlist state
  -> optional universe_master sync
  -> Yahoo enrichment (previous values retained on individual fetch failures)
  -> exact post-enrichment scoring rows
  -> scoring engine
  -> watchlist_full.csv
  -> latest_scanner.csv
```

Therefore the current 2026-10-03 snapshot remains open for the more precise
reason:

`DATA -> SCANNER: scanner_input_provenance_missing`

No attempt is made to retroactively reconstruct that missing provenance.

### Prospective fix implemented

The next scanner run will prospectively create
`artifacts/research/scanner_input_provenance.json` and bind:

- the persistent watchlist state before the run;
- the optional active universe-master state;
- cached taxonomy/pillar mappings when present;
- the exact post-enrichment table passed into scoring;
- the exact universe file used for percentile scaling;
- Yahoo enrichment outcome metadata;
- code revision plus scanner version/build;
- the resulting `watchlist_full.csv` hash;
- the resulting `latest_scanner.csv` hash;
- the new scanner `snapshot_id`.

The reference is carried into `daily_research` and then into W10. Production
Autopilot uses a fail-closed `SCANNER_REQUIRE_PROVENANCE=1` gate.

Historical snapshots are not backfilled. The first DATA/SCANNER PASS can only be
established by a new prospective scanner run after this implementation is on
`main`.

## Existing contracts reused, not rebuilt

BA-QM8 deliberately reuses existing controls:

- **W10** – same-snapshot orchestration, availability timestamps, upstream freeze,
  no-backdating, and prospective scanner-provenance carry.
- **W11** – real-snapshot 7A–7H acceptance, PIT, missing-evidence, duplicate-claim,
  super-score and Phase-8 boundary guards.
- **W12 / Decision Watch** – Ferrari F1–F4 regression behavior and review-only
  decision constraints.
- **QM-I** – typed evidence lineage and ancestry review.
- **QM-C3** – frozen multiplicity families / predeclared multiplicity treatment.
- **QM-J / QM-H** – negative-control finding, CAPA and promotion-stop lifecycle.
- **External Evidence / 8C** – PIT provenance, non-retrojection and
  non-productive research boundaries.

## Next mandatory work

BA-QM8 is **not complete** and no closure is claimed.

Next:

1. merge and regression-test the prospective Data -> Scanner provenance package;
2. execute a fresh scanner run;
3. verify the new provenance manifest against the fresh scanner snapshot;
4. rebuild/seal W10 and verify that the exact provenance reference survives into
   the Decision chain;
5. feed real stage identities into the seven-class transition guard engine;
6. rerun BA-QM7/QM-J, QM-H, QM-I, W11, W12/Decision Watch and External Evidence
   regressions;
7. only after all real transition gaps are resolved may BA-QM8 closure be
   considered.

The open Lag-1 CAPA remains independent of BA-QM8 closure and remains
`PROMOTION_BLOCKED` until its own prospective effectiveness criteria are met.
