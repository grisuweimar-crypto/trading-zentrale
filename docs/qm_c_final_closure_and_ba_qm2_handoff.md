# QM-C4 — Final Closure and BA-QM2 Handoff

## Work section

**QM-C4 — Negative Results, Audit & Closure**

QM-C4 closes the QM-C axis without changing productive scanner or Decision-Layer semantics. It adds symmetric outcome retention, deterministic duplicate-hypothesis auditing, full QM-C cross-reference auditing and the stable-ID handoff required by BA-QM3 / QM-I.

## Audit object

The audited chain is:

`QM-C1 hypothesis identity -> QM-C2 analysis-plan identity -> QM-A immutable analysis identity -> QM-C3 family/control identity and monitoring history -> QM-C4 result identity`

QM-B remains the authority for historical universe / investability evidence and remains fail-closed where external evidence is unavailable.

## Methods and contracts

### Symmetric result retention

QM-C4 records evaluated outcomes using the same immutable result-version rules for:

- `POSITIVE`
- `NEGATIVE`
- `INCONCLUSIVE`

It separately records hypotheses that were never evaluated as:

- `REJECTED_PRE_EVALUATION`
- `RETIRED_WITHOUT_EVALUATION`

A no-outcome record is not allowed to carry an evidence artifact or confirmatory bindings. This prevents a rejected hypothesis from being given invented historical evidence merely so that the registry looks complete.

A confirmatory result is accepted only when it binds exactly to:

- QM-C1 hypothesis ID/version/hash
- QM-C2 analysis-plan ID/version/hash
- QM-C3 control-plan ID/version/hash
- QM-A analysis ID/version

The control plan must be `COMPLETE` or `STOPPED`, at least one predeclared look must exist, the result must be an exact family member, and QM-A must show that confirmatory evidence has been consumed/evaluated.

### Duplicate-hypothesis audit

The duplicate audit is deliberately deterministic rather than fuzzy. It fingerprints normalized:

- research question
- hypothesis statement
- research mode
- universe requirement

Only matches across distinct `hypothesis_id` values are reported as `POTENTIAL_EXACT_SEMANTIC_DUPLICATE`. Multiple versions of the same stable hypothesis ID are not treated as cross-ID duplicates. Findings are never automatically merged or deleted.

### Full-system audit

`audit_qm_c_system(...)` validates the append-only hash chains of QM-C1 through QM-C4 and verifies static cross-references among hypotheses, plans, control-family members and results. It reports duplicate findings separately from integrity failures rather than silently rewriting research objects.

## End-to-end regression path

The regression suite constructs a real governed negative-result path:

1. register a QM-C1 confirmatory hypothesis;
2. register and freeze the QM-C2 analysis plan;
3. freeze the exact QM-A immutable identity produced by QM-C2;
4. freeze the QM-C1 hypothesis;
5. register and freeze a QM-C3 family/control plan;
6. record the predeclared final monitoring look;
7. transition QM-A to `CONFIRMATORY_EVALUATED`;
8. register a QM-C4 `NEGATIVE` result;
9. run the full QM-C1-to-C4 system audit.

The audit must return `PASS`, with no cross-reference errors, for the test to succeed.

## Stable lineage identities delivered to QM-I

BA-QM2 now supplies stable, versioned, hash-bound identities for:

- hypothesis: `hypothesis_id`, `hypothesis_version`, `hypothesis_version_hash`
- analysis plan: `analysis_plan_id`, `analysis_plan_version`, `analysis_plan_hash`
- multiplicity/monitoring control: `control_plan_id`, `control_plan_version`, `control_plan_hash`
- result: `result_id`, `result_version`, `result_hash`
- QM-A analysis: `analysis_id`, `version_id`, `identity_hash`
- QM-B universe identity: `universe_ledger_version`, `instrument_master_version`

QM-I may reference these IDs directly. It must not re-key them from mutable labels or names.

## Result

Target final QM-C status:

`QM-C COMPLETE — VERSIONED RESEARCH DISCIPLINE CHAIN ACTIVE`

Target BA-QM2 handoff status:

`BA-QM2 GOVERNANCE COMPLETE — READY FOR QM-I LINEAGE; QM-B STRICT PROMOTION REMAINS EXTERNALLY BLOCKED`

The second status is intentional: completion of the engineering/governance work does not convert missing external listing, market-tradability or execution-channel evidence into historical proof.

## Findings / CAPA

No new CAPA is created by QM-C4 itself. Any test or cross-reference failure blocks closure rather than being waived.

Deterministic duplicate findings are retained as review findings and are never automatically merged. If a real registry later contains such a finding, the affected research object must be reviewed rather than silently deduplicated.

## Evidence Impact

QM-C1 through QM-C4 are governance and research-discipline infrastructure. Their implementation does not by itself promote a scanner rule, retrain Selection/Timing/Probability/Risk/Confidence/Elliott, alter Decision-Layer semantics or create portfolio actions.

No historical outcome is synthesized to populate the new result registry. Existing evidence-consumption status remains controlled by QM-A; historical universe constraints remain controlled by QM-B.

## Remaining risks

QM-B strict historical promotion remains blocked by external evidence gaps for:

- listing evidence
- market-tradability evidence
- execution-channel evidence

These are visible constraints for future lineage and interpretation. They do not reopen completed QM-B engineering and they do not prevent building QM-I itself.

The duplicate audit intentionally avoids fuzzy semantic matching. Near-duplicates with materially different wording may therefore require a later human or separately governed review; QM-C4 does not guess equivalence.

## Not worked on

QM-C4 does not implement QM-I lineage, double-counting detection, dependence/effective-N work, calibration, Decision Ablation, Elliott challengers, negative controls for the full scanner, production promotion or order execution.

## Next mandatory work package

**BA-QM3 / QM-I — Evidence Lineage / Double Counting**

QM-I must consume the stable identities produced by QM-B and QM-C rather than inventing a new identity namespace.
