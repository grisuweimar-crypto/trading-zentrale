# BA-QM11 – Gesamtsystem-Audit

## Scope

BA-QM11 evaluates Scanner-vNext as one research and decision system. It does
not reopen BA-QM8, BA-QM9 or BA-QM10 and does not add investment logic.

Baseline: `f097b9817c430d778d7dfe9e9a3d177cc92dcee7`.

## System result

| Dimension | Result | System-level conclusion |
| --- | --- | --- |
| Architecture | PASS | Research modules remain semantically separated. Phase-8 External Evidence is still quarantined from the canonical Decision path. |
| Information flow | PASS after CAPA | BA-QM11-F01 found missing material post-7D Action parents in QM-I. W6 and W7/W8 immediate Action ancestry is now explicit. |
| Decision logic | PASS / empirical value RESEARCH_REQUIRED | Review actions remain deterministic, review-only and non-executing. No claim of empirical utility is made by this audit. |
| Uncertainty | PASS | Conflict, insufficient evidence, pending confirmation and provisional reliability remain visible. |
| Missingness | PASS | Missing Decision evidence fails closed and cannot fall back to legacy scanner heuristics. |
| Falsifiability | PASS | QM-J has already produced a real promotion stop; the Lag-1 block survives BA-QM11. |
| Learning | PASS after CAPA | BA-QM11-F02 gives the post-Ferrari W8 policy its own outcome-driven evidence boundary. |
| Operationalization | PASS | Depot Watch remains a same-snapshot presentation of the Decision chain; BA-QM9/10 safeguards remain authoritative. |
| Purpose fidelity | PASS | The system identifies instruments/positions deserving closer review and explains why; it does not generate automatic orders. |

## BA-QM11-F01 – incomplete material Action lineage

### Reproduction

The existing QM-I `register_phase7_portfolio_action()` represented a
Portfolio Action with only these material parents:

- 7D Decision
- Position Snapshot

The real canonical path can additionally use:

- W6 Elliott review context; the existing W6 regression proves that positive
  Universal Stance plus `profit_protection_review` can change
  `HOLD -> REDUCE_REVIEW`.
- W7 scanner path/history context consumed by W8; current runtime evidence has
  five actions changed by W8:
  `DSV.TO`, `NOW`, `PLTR`, `SAP.DE`, `WPM.TO`.

Therefore the old graph could hide material ancestry even though no false order
or execution was observed.

### CAPA

- `DECISION_CONTEXT` added as a typed QM-I node for W6 immediate context.
- W6 context -> Portfolio Action is a material `INFORMS` edge.
- W7 source claim -> W8-resolved Portfolio Action is a material `INFORMS`
  edge.
- A W8-changed action without registered state-history ancestry fails closed.
- W6 upstream Elliott provenance remains explicitly
  `lineage_complete=false`; it may not be interpreted as independence.

No Action rule or Universal Stance rule was changed.

## BA-QM11-F02 – post-freeze W8 governance

### Reproduction

Phase 7I was frozen on 2026-09-25.

W8/Ferrari F1-F4 was introduced on 2026-10-01 after inspection of the
Ferrari/RACE source case and can change the final 7F review state. It therefore
cannot silently inherit the original 7I empirical identity.

### CAPA

New contract: `configs/decision_depot_action_policy_v1.json`.

W8 is now explicitly classified as:

`OUTCOME_DRIVEN_RESEARCH_CHANGE`

Evidence rules:

- Ferrari/RACE F1-F4 is regression evidence only.
- Evidence through 2026-10-01 is spent for independent W8 confirmation.
- W8 prospective-unspent validation starts on 2026-10-02.
- W8 is a required private downstream shadow-trace layer for 7I.
- A pre-2026-10-02 W8 trace is rejected by 7I.
- W8 remains empirically unvalidated and not promotion-eligible.

No W8 action semantics were changed.

## Independent blocks preserved

The independent Phase-1A Lag-1 finding remains:

- `QM-H-QMJ-PHASE1A-LAG1-001`
- CAPA: `QM-H-CAPA-QMJ-PHASE1A-LAG1-001`
- effectiveness: `PENDING_PROSPECTIVE_UNSPENT_EVIDENCE`
- evidence impact: `PROMOTION_BLOCKED`

BA-QM11 cannot release this block.

W8 also remains empirically blocked from promotion until its post-change
prospective evidence requirement is met.

## Residual risks

1. W6 immediate Action ancestry is now visible, but the upstream
   Elliott-to-raw-data path is still explicitly incomplete in QM-I.
2. W8 empirical utility is unknown until post-2026-10-01 prospective evidence
   matures.
3. The Phase-1A Lag-1 block remains unresolved.
4. External Evidence remains intentionally outside the canonical Decision path.

## Definition of Done

BA-QM11 is complete only when:

- all nine system dimensions are assessed;
- both findings are regression-protected;
- QM-H records their evidence impact and CAPA disposition;
- the affected QM-I / 7I / Decision regressions pass;
- the Lag-1 block remains unchanged;
- no research, decision or investment semantics were altered by the audit.

Next work package:

**BA-QM12 – Konsolidierung & produktiver QM-Betrieb**
