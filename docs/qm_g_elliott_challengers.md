# QM-G / BA-QM6 – Elliott Challenger Research

QM-G keeps the existing Elliott-vNext implementation frozen as the research core. New Elliott ideas are not added to the hard-rule engine. They enter only as versioned, research-only sidecar challengers.

## Mandatory challenger bindings

Every challenger version binds to:

- one QM-C hypothesis/version and family;
- one QM-C analysis-plan version;
- one frozen QM-C multiplicity control plan;
- one QM-A analysis/version and its current evidence state;
- an explicit feature definition;
- a PIT contract that forbids future data and retroactive reclassification;
- a frozen Elliott count/scenario identity created before outcome inspection;
- a frozen Elliott core identity;
- QM-I lineage references for core and challenger;
- the existing QM-F stateful B5-vs-B6 incremental ablation contract for Decision-Layer value.

The example challenger ideas listed in the QM Masterplan are candidates, not activated rules.

## First executable challenger: Scenario Stability

`qm_g_scenario_stability_v1` operationalizes **Scenario Stability** without a newly fitted threshold.

The feature compares only `primary_scenario.scenario_id` between consecutive, validated and genuinely available 6H outputs for the same symbol, timeframe and Elliott degree:

- equal non-missing IDs -> `STABLE`;
- unequal non-missing IDs -> `CHANGED`;
- either ID missing -> `INSUFFICIENT_EVIDENCE`.

No direction, pattern class, stage, return or later outcome is substituted for the scenario identity. No smoothing or minimum-run threshold is introduced. The feature itself does not claim that stability is beneficial or that change is harmful.

Every executable feature output can be registered in QM-I as a `FEATURE` node with material `DERIVED_FROM` edges from all declared 6H source nodes. Incomplete or hash-mismatched source lineage fails closed.

## Frozen Core vs Challenger evaluation

`qm_g_challenger_evaluation_v1` provides the research-only shadow comparison:

- `B6_CORE_FROZEN`
- `B6_CHALLENGER_SHADOW`

Both arms are evaluated by the existing QM-F `B6` stateful policy engine. They must have the same starting exposure, observation grid, eligibility, tradeability, action availability, transaction-cost assumptions and realized asset-return inputs. Path divergence is allowed only as the consequence being measured.

QM-I lineage must be identical after removing exactly the challenger node registered in the QM-G record. Extra, missing or core-contaminating challenger lineage blocks interpretation.

The evaluator reports incremental return/turnover/drawdown quantities. It deliberately does **not** declare a winner and cannot promote a challenger.

## Hard boundaries

QM-G does not:

- change Elliott hard rules or make a historical count fit after seeing outcomes;
- replace W6 or infer Elliott direction into Universal Stance;
- replace W8;
- create Portfolio Actions directly;
- create orders or enable execution;
- perform productive promotion.

Shared ancestry between core and challenger must remain visible to QM-I. It must never be presented as independent confirmation.

## Promotion semantics

A challenger registry entry, an executable feature, or a positive shadow comparison is not by itself evidence for production promotion. Confirmatory evaluation remains bound to QM-C/QM-A/QM-I and preserves QM-B fail-closed constraints. Any later promotion requires separately demonstrated incremental benefit under the frozen plan and a separate promotion decision.

## BA-QM6 engineering closure

BA-QM6 is technically complete when `ba_qm6_qm_g_closure_v1` validates. That closure is deliberately narrower than an empirical validation claim:

- the challenger governance and evaluation path is complete;
- **Scenario Stability** is the first executable challenger and proves the end-to-end research path;
- the other eleven Masterplan examples remain documented candidates rather than implemented or validated rules;
- no challenger is promoted;
- empirical usefulness remains `NOT_ESTABLISHED`;
- productive integration and execution remain disabled.

The next mandatory work package is **BA-QM7 / QM-J — Negative Controls & Falsifikation**.
