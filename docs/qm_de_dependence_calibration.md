# BA-QM4 — QM-D + QM-E

## Dependence, Effective N & Calibration

BA-QM4 is a research-governance layer. It does not change scanner scoring, Selection, Timing, Probability, Risk, Confidence, Elliott, Universal Stance, Portfolio Action or execution.

The work package is intentionally split by semantics but closed as one integrated block:

- **QM-D** audits dependence, effective information and robustness.
- **QM-E** audits probability calibration from explicit point-in-time prediction/outcome pairs.

Both audits reuse the exact stable identities established by QM-C and QM-I.

## Identity binding

Every audit requires:

- `audit_id`
- `audit_version`
- `audit_as_of`
- `analysis_plan_id`
- `analysis_plan_version`
- `analysis_plan_hash`
- `result_id`
- `result_version`
- `result_hash`
- `dataset_snapshot_hash`
- `lineage_registry_head_hash`

QM-E additionally requires:

- `prediction_definition_hash`
- `label_definition_hash`

The referenced analysis plan, result and lineage registry must exist and match their hashes. QM-E additionally requires the result to bind to the exact analysis plan supplied by the audit identity. When the analysis plan contains a frozen `dataset_snapshot_hash`, QM-E requires exact equality with the audit identity.

## QM-D — Dependence and effective information

`N_raw` is never relabelled as a universally valid effective sample size.

QM-D reports several labelled diagnostics instead:

- raw N
- pooled within-symbol lag-1 AR(1)-style diagnostic
- symbol-cluster concentration
- sector-cluster concentration
- time-block-cluster concentration
- overlap-concurrency proxy for overlapping evaluation windows

Robustness checks include:

- leave-one-symbol-out
- leave-one-sector-out
- leave-one-time-block-out
- symbol-cluster bootstrap
- time-block bootstrap

These methods answer different questions. BA-QM4 deliberately does **not** choose one of them as the single true `N_eff`.

Missing sector or time-block metadata produces an explicit `UNKNOWN_MISSING_CLUSTER_METADATA` state. It is never converted into independence or neutrality.

Every audited observation must reference an existing QM-I lineage node/version.

## Overlapping forward windows

Observation intervals are explicit. The overlap-concurrency diagnostic counts overlapping interval pairs and reports a conservative information-reuse proxy. It is a diagnostic, not a universal correction formula and not permission to treat overlapping windows as independent observations.

## QM-E — Point-in-time calibration

QM-E requires row-level prediction/outcome pairs with:

- immutable prediction ID
- predicted probability in `[0,1]`
- binary outcome
- `prediction_as_of`
- `outcome_available_at`
- QM-I lineage node/version

The PIT rule is fail-closed:

1. `prediction_as_of < outcome_available_at`
2. `outcome_available_at <= audit_as_of`

An outcome that was not available by the declared audit time cannot be used.

QM-E computes:

- Brier score
- log loss
- logistic calibration intercept
- logistic calibration slope
- reliability bins

Calibration regression requires a minimum row count and both outcome classes. If those requirements are not met, the regression remains explicitly unavailable rather than being imputed.

Subgroup and time-block calibration obey the same minimum-sample rule. Small groups remain `INSUFFICIENT_ROWS`.

## Existing Phase-2 probability research

`scanner.reports.probability_calibration` already contains useful dependence-aware machinery, including circular moving observation-date blocks and explicit labelling of IID diagnostics as diagnostics only.

BA-QM4 recognizes and preserves that work. It does **not** pretend that the aggregate Phase-2 report is a row-level prediction/outcome dataset. Without explicit PIT prediction/outcome pairs, QM-E returns:

`INSUFFICIENT_ROW_LEVEL_PREDICTION_OUTCOME_PAIRS`

This prevents post-hoc fabrication of Brier score, log loss, slope or intercept from aggregate summaries.

## No automatic model changes

BA-QM4 performs no:

- hypothesis reselection
- probability retraining
- confidence retraining
- scanner-weight change
- Decision-Layer semantic change
- recalibration of productive outputs
- portfolio-action change
- broker/order generation
- empirical promotion

Any later outcome-driven model change remains subject to QM-A evidence-consumption rules and the appropriate frozen analysis/version identity.

## Closure

The fail-closed closure validator is `scanner.research.governance.qm_de_closure`.

Closure requires the already completed BA-QM3/QM-I lineage layer and preserves the still-open QM-B external evidence blockers:

- Listing Evidence
- Market Tradability Evidence
- Execution Channel Evidence

Those blockers do not reopen QM-B engineering and are not bypassed by BA-QM4.

Final statuses:

- `QM-D COMPLETE — DEPENDENCE AND EFFECTIVE-N AUDIT ACTIVE`
- `QM-E COMPLETE — POINT-IN-TIME CALIBRATION AUDIT ACTIVE`
- `BA-QM4 COMPLETE — DEPENDENCE AND CALIBRATION QM READY FOR DECISION ABLATION`

Next mandatory work package:

`BA-QM5 / QM-F — Decision Ablation`
