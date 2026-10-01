# QM-H — Defect / Near-Miss / CAPA Management

## Status

`QM-H COMPLETE — DEFECT / NEAR-MISS / CAPA CONTINUOUS CONTROL ACTIVE`

QM-H is the continuous quality-management layer for defects, near misses, methodology findings and external evidence gaps. It is research/governance-only and does not alter productive scanner, scoring, decision or execution logic.

## Scope and boundary

QM-H records and audits findings and CAPA actions. It does **not** replace QM-A. QM-A remains authoritative for evidence consumption, confirmatory state and frozen research identity. QM-H may record an evidence impact and a disposition reference, but it cannot mutate a QM-A state.

QM-H also does not reopen completed QM-B engineering. The existing `LISTING_EVIDENCE`, `MARKET_TRADABILITY_EVIDENCE` and `EXECUTION_CHANNEL_EVIDENCE` gaps remain external `UNKNOWN_FAIL_CLOSED` blockers until external evidence actually resolves them.

No historical finding is backfilled merely because QM-H did not yet exist. Existing historical records such as `QM-B-POA-001` stay where they are documented; QM-H does not invent missing historical lifecycle events.

## Finding types

- `DEFECT` — an observed failure or incorrect implementation/behavior.
- `NEAR_MISS` — a condition that could have caused an invalid outcome but was caught before it did.
- `METHODOLOGY_FINDING` — a research-method or governance weakness that requires controlled treatment.
- `EXTERNAL_EVIDENCE_GAP` — missing external evidence that must remain fail-closed rather than being replaced by assumptions.

## Lifecycle

The append-only lifecycle is:

`OPEN → TRIAGED → ROOT_CAUSE_IDENTIFIED → ACTION_PLANNED → IMPLEMENTED → EFFECTIVENESS_VERIFIED → CLOSED`

A finding may become `INVALIDATED` with an explicit reference and rationale. If implementation or effectiveness review shows that more work is needed, the lifecycle can return to `ACTION_PLANNED`; the previous events remain immutable.

`ACTION_PLANNED` introduces the stable `capa_id`. The same CAPA identity must be retained through implementation, effectiveness verification and closure.

## Evidence impact

A finding records one of:

- `NO_KNOWN_EVIDENCE_IMPACT`
- `EVIDENCE_REVIEW_REQUIRED`
- `EVIDENCE_USE_RESTRICTED`
- `PROMOTION_BLOCKED`
- `EVIDENCE_INVALIDATION_REVIEW_REQUIRED`

These are QM-H audit classifications, not QM-A state transitions. Any finding with a non-zero evidence impact requires an explicit upstream evidence-disposition reference before closure. An `EXTERNAL_EVIDENCE_GAP` additionally requires an external evidence resolution reference before closure.

## Stable upstream identities

QM-H reuses the stable identities already established by BA-QM2 without rekeying them:

- QM-A analysis: `analysis_id / version_id / identity_hash`
- QM-B universe: `universe_ledger_version / instrument_master_version`
- QM-C hypothesis: `hypothesis_id / hypothesis_version / hypothesis_version_hash`
- QM-C analysis plan: `analysis_plan_id / analysis_plan_version / analysis_plan_hash`
- QM-C multiplicity control: `control_plan_id / control_plan_version / control_plan_hash`
- QM-C sequential monitoring: `monitoring_plan_id / monitoring_plan_version / monitoring_plan_hash`
- QM-C result: `result_id / result_version / result_hash`

Known QM-B external blockers can be referenced by their existing blocker ID and current state. They are not silently converted into resolved CAPA findings.

## Auditability

QM-H uses JSONL event sourcing with sequence numbers, immutable event IDs, previous-event hashes and a SHA-256 entry hash. Candidate events are replayed before they are appended. In-place edits break integrity verification.

## Handoff

QM-H closes only the missing continuous CAPA governance layer. It does not implement BA-QM3. The next authorized work package remains:

`BA-QM3 / QM-I — Evidence Lineage, Double Counting & Independence Claims`
