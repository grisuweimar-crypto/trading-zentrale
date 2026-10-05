# QM-I / BA-QM3 — Evidence Lineage & Double Counting

## Scope

QM-I makes material evidence ancestry machine-readable without changing scanner logic, Decision-Layer semantics, portfolio actions or execution. It consumes the stable identities already governed by QM-A, QM-B, QM-C and the continuous QM-H CAPA control.

Canonical lineage remains:

`RAW_SOURCE -> FEATURE -> INDICATOR -> SCORE -> CLAIM -> CALIBRATION -> DECISION -> WATCH`

For governed research artifacts QM-I also supports `HYPOTHESIS`, `ANALYSIS_PLAN`, `CONTROL_PLAN`, `MONITORING_PLAN` and `RESULT`.

## Core rules

- Every node has a stable ID, explicit version, typed node class and immutable SHA-256 content hash.
- Material lineage edges are immutable and acyclic.
- Missing lineage is never evidence of independence.
- Direct dependency is not independent confirmation.
- Common ancestry triggers review; it is not automatically a defect.
- `INDEPENDENT_SUPPORTED` requires complete registered lineage plus an explicit review reference.
- Later provenance may invalidate an earlier independence claim without deleting history.
- Double-counting review never changes weights, signals, stances or actions automatically.

## Current upstream integration

QM-I is bound to the current BA-QM2 handoff (`ba_qm2_handoff_v2`). Stable identities are reused without re-keying for:

- hypothesis ID/version/hash,
- analysis-plan ID/version/hash,
- multiplicity-control ID/version/hash,
- sequential-monitoring ID/version/hash,
- result ID/version/hash.

`register_qm_c_result_chain(...)` validates these objects through the current QM-C1/C2/C3/C4/C5 registries. A confirmatory result that contains a monitoring-plan identity requires the corresponding `SequentialMonitoringRegistry`; QM-I does not fabricate a missing monitoring node.

QM-H is a prerequisite, not a subordinate registry. The BA-QM3 closure validates that QM-H engineering is complete and its continuous control remains active. QM-I does not close CAPA findings or mutate QM-H state.

QM-A remains authoritative for evidence-consumption state. QM-B remains authoritative for PIT universe/investability. QM-C remains authoritative for its research identities. QM-H remains authoritative for defects, near misses and CAPA.

## Phase-7 integration

QM-I reuses existing Phase-7 IDs and relations:

- `source_snapshot_id` is reused as the packet source,
- existing `claim_id` and `source_version` are reused,
- `claim_ref` becomes a material `REFERENCES` edge,
- Phase-7D/7F IDs must be explicitly supplied if the source artifact has no native stable ID,
- Phase-7H `watch_id` is reused unchanged.

Probability, Confidence and same-family Timing relationships are not reinterpreted as independent votes.

## Closure

Engineering closure target:

`QM-I COMPLETE — TYPED EVIDENCE LINEAGE AND DOUBLE-COUNTING REVIEW ACTIVE`

BA-QM3 target:

`BA-QM3 COMPLETE — EVIDENCE LINEAGE READY FOR DEPENDENCE AND CALIBRATION QM`

QM-B strict historical promotion remains blocked by the external listing, market-tradability and execution-channel evidence gaps; QM-I does not override or reopen those blockers.

The next mandatory work package is:

**BA-QM4 / QM-D + QM-E — Dependence, Effective N & Calibration**.


## BA-QM11 addendum — material post-7D action ancestry

BA-QM11 found that the canonical Portfolio Action path had gained material
post-7D parents after the original QM-I integration:

- W6 Elliott review context can change a positive confirmed long position from
  `HOLD` to `REDUCE_REVIEW` or `ADD_REVIEW` without changing Universal
  Stance.
- W7 scanner path/history context can be consumed by W8 and can change the final
  7F review state.

QM-I therefore treats both as material immediate Action ancestry.

### W6

The W6 source output is registered as a typed `DECISION_CONTEXT` node and is
connected to the final `PORTFOLIO_ACTION` with a material `INFORMS` edge.

Its upstream Elliott-to-raw-data lineage is deliberately marked incomplete
until that provenance is explicitly bound into QM-I. Missing upstream lineage
must never be interpreted as evidence independence.

### W7/W8

The source scanner-path claim referenced by
`decision_state_history_context_v1.source_claim_id` is connected directly to
the final W8-resolved `PORTFOLIO_ACTION`.

If W8 reports `action_changed=true` but no registered state-history ancestry
is available, registration fails closed.

These changes affect lineage and auditability only. They do not change
Universal Stance, W8 action rules, broker execution or empirical promotion
status.
