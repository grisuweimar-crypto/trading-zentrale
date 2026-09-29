# QM-A – Research Governance & Evidence Consumption

Status: implemented on the dedicated QM-A branch. Research-only; no productive scanner, Decision-Layer, portfolio or broker behavior is changed.

## Purpose

QM-A is the governance control plane for future Scanner-vNext research. It answers five questions in a machine-checkable way:

1. Which exact analysis version existed?
2. Which state was that version in when evidence was inspected?
3. Which immutable code/config/data/universe/label/benchmark identity was frozen for confirmation?
4. Which outcome-revealing artifacts were inspected, by whom, and for what decision?
5. Was the inspected evidence still usable for independent confirmation, or had it been consumed/spent for design?

QM-A does not decide whether a signal is good. It prevents research history from being silently rewritten after outcomes become visible.

## Existing protections retained

QM-A does not replace the existing Point-in-Time, prospective-unspent, holdout, snapshot-identity or research-data protections. Those remain authoritative in their own modules. QM-A adds the cross-module governance layer above them.

Examples already present in the project before QM-A:

- Phase 7 separates legacy replay from `prospective_unspent` evidence.
- prospective typed packets cannot be appended as unspent when they belong to the spent replay partition.
- the research architecture requires validated complete scanner runs, explicit provenance and publication metadata.
- Phase 8 already introduced the rule that outcome-driven design changes consume the inspected evidence.

Before QM-A, those rules were distributed across phase-specific contracts and documentation. QM-A turns the common governance subset into one executable ledger and state machine.

## Evidence State Machine

Canonical states:

`DRAFT -> EXPLORATORY -> FROZEN_FOR_CONFIRMATION -> CONFIRMATORY_EVALUATED -> CONFIRMATORY_SPENT -> REPLICATION_PENDING -> PROSPECTIVE_SHADOW -> PROMOTION_ELIGIBLE -> PROMOTED`

Terminal/review states are also available:

- `REJECTED`
- `RETIRED`
- `INVALIDATED`

Not every theoretical edge is allowed. The actual transition map lives in `configs/qm_a_research_governance_v1.json` and is enforced by code.

Important consequences:

- evaluated confirmatory evidence cannot move back to exploratory/unspent states;
- forbidden transitions fail closed;
- promotion cannot be reached by skipping the frozen confirmatory/prospective path;
- invalidation and retirement remain explicit historical events, not destructive rewrites.

## Immutable Analysis Identity

At the transition into `FROZEN_FOR_CONFIRMATION`, QM-A requires a complete identity bundle containing:

- hypothesis version hash,
- analysis-plan hash,
- code/commit hash,
- config hash,
- dataset snapshot hash,
- universe-ledger version,
- instrument-master version,
- label-definition hash,
- benchmark-definition hash,
- environment/dependency fingerprint,
- evaluation cohort ID,
- temporal boundary.

The bundle is canonically serialized and SHA-256 hashed. After freeze, supplying a different identity causes a hard failure. Mutable names such as `latest`, `main`, or a human-readable label are not substitutes for the frozen identity.

## Append-only governance ledger

Default runtime path:

`artifacts/research/qm/qm_a_governance_events.jsonl`

Event types:

- `ANALYSIS_REGISTERED`
- `STATE_TRANSITION`
- `EVIDENCE_INSPECTION`

Each event includes a monotonically increasing sequence number, a previous-event hash and its own SHA-256 hash. Validation replays the entire ledger and checks both cryptographic chain integrity and semantic state-machine rules.

The ledger is event-sourced: current state is derived from history. There is no supported API for editing old entries in place. A short-lived lock file prevents two local writers from intentionally appending at the same time.

## Evidence Consumption

QM-A defines three change classes.

### `MECHANICALLY_EQUIVALENT_REPAIR`

Use only when equivalence has actually been demonstrated: the repair does not change estimand, eligibility, labels, evidence population or decision rule. An equivalence/review reference is mandatory.

### `PREVENTIVE_QA_NEW_VERSION`

Use for preventive architecture/provenance/methodology improvement that is not outcome-driven but also cannot be proven mechanically equivalent. This is the conservative default when equivalence is uncertain. A successor version is mandatory.

### `OUTCOME_DRIVEN_RESEARCH_CHANGE`

Use when visible future outcomes, realized performance or performance-revealing summaries influenced the rule/threshold/feature/policy design. The inspected evidence is marked `spent_for_design`, and a successor version is mandatory. That spent evidence may not later serve as independent confirmation for the redesigned successor.

Helper function:

`classify_change(equivalence_demonstrated=..., outcome_driven=...)`

The helper deliberately defaults uncertain equivalence to `PREVENTIVE_QA_NEW_VERSION`.

## Performance-revealing access

Outcome consumption is not limited to raw rows. QM-A records access modes including:

- raw rows,
- aggregate metrics,
- charts,
- dashboards,
- exported files,
- generated reports,
- API queries,
- test/failure reports.

Outcome visibility levels distinguish:

- none,
- blinded,
- aggregate,
- subgroup,
- row-level,
- indirect performance reveal.

Aggregate/dashboard/report access can therefore be recorded as evidence inspection even when no raw future row was opened.

## Inspection / Decision Log

Every inspection record must carry the contract-defined fields, including:

- actor and role,
- access mode,
- dataset/label/universe/analysis-plan versions,
- artifact hash,
- scope/question/reason,
- outcome visibility,
- decision taken,
- change class,
- `spent_for_design`,
- affected hypotheses,
- code/config version,
- evidence effect,
- review/approval reference.

Additional review metadata may state an independent reviewer. The same actor cannot be marked as an independent reviewer of their own action.

## Superseding versions

QM-A does not support in-place rewriting of an analysis version. Successors are separately registered with `supersedes_version_id`. The previous version remains in the ledger with its original state and evidence-consumption history.

This is especially important after outcome inspection: a new rule is a new version, not a cleaned-up reinterpretation of the old version.

## CLI

Verify a ledger:

```bash
PYTHONPATH=src python scripts/qm_a_governance.py verify
```

Register a version:

```bash
PYTHONPATH=src python scripts/qm_a_governance.py register \
  --analysis-id example-analysis \
  --version-id v1 \
  --actor-id researcher-1 \
  --actor-role researcher
```

Freeze for confirmation using an identity JSON file:

```bash
PYTHONPATH=src python scripts/qm_a_governance.py transition \
  --analysis-id example-analysis \
  --version-id v1 \
  --to-state FROZEN_FOR_CONFIRMATION \
  --actor-id researcher-1 \
  --actor-role researcher \
  --reason "freeze before outcomes" \
  --identity-json identity.json
```

Append an inspection record:

```bash
PYTHONPATH=src python scripts/qm_a_governance.py inspect \
  --analysis-id example-analysis \
  --version-id v1 \
  --actor-id researcher-1 \
  --actor-role researcher \
  --record-json inspection.json
```

## Scope boundaries

QM-A explicitly does **not**:

- change scanner scoring;
- redefine Selection, Timing, Probability, Risk, Confidence, Elliott or External Evidence semantics;
- recalculate historical PIT data;
- turn missing evidence into neutral evidence;
- generate portfolio actions or orders;
- promote any research result automatically;
- rewrite the Phase-7 frozen validation contract;
- retroactively make spent evidence unspent.

## Definition of Done

QM-A is complete when all of the following are true:

1. A dedicated machine-readable QM-A contract exists.
2. Evidence states and allowed transitions are explicit and fail closed.
3. Confirmatory freeze requires a complete immutable analysis identity.
4. Frozen identity cannot drift during later evaluation/promotion states.
5. Analysis versions are registered separately; superseding versions preserve ancestry.
6. Evidence inspections are append-only and capture actor/role/access/outcome visibility/evidence effect.
7. Aggregate and indirect performance views can be recorded as evidence consumption.
8. Outcome-driven redesign requires a successor and marks evidence `spent_for_design`.
9. Mechanically equivalent repair requires an explicit equivalence/review reference.
10. Uncertain equivalence defaults to a preventive new version.
11. Same-actor review cannot be falsely labeled independent.
12. Ledger tampering, state rollback and identity drift are covered by regression tests.
13. CI runs the QM-A tests independently of productive scanner workflows.
14. No productive scanner/Decision-Layer behavior changes as a side effect.

## Files

- `configs/qm_a_research_governance_v1.json`
- `src/scanner/research/governance/qm_a.py`
- `src/scanner/research/governance/__init__.py`
- `scripts/qm_a_governance.py`
- `tests/test_qm_a_research_governance.py`
- `.github/workflows/qm_a_research_governance.yml`
- `docs/qm_a_research_governance.md`
