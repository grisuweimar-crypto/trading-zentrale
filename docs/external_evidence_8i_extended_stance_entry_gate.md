# Phase 8I-F — Extended Stance Preregistration Entry Gate

## Status

`BLOCKED_WAITING_FOR_8I_E_EMPIRICAL_REVIEW`

This is deliberately an **entry gate**, not an extended-stance rule and not a Portfolio Action bridge. The authoritative 8I-E completion contract names the next subblock after a successful empirical review as `8I-F_EXTENDED_STANCE_PREREGISTRATION`.

The current real 8I-E review is started but not empirically complete. Its one-shot terminal family has not been evaluated, real outcomes remain sealed, no eligible 8I component is bound, and no successful empirical-review receipt exists. Therefore 8I-F rule design is not yet permitted.

## What this subblock does now

The gate freezes the conditions that must be satisfied before any 8I-F stance mapping may be designed:

- 8I-E empirical review complete;
- the one-shot prospective family terminal evaluation complete and consumed;
- a separate manual empirical-review receipt exists;
- the receipt authorizes only `APPROVED_FOR_8I_F_RESEARCH_DESIGN_ONLY`;
- effect size and uncertainty were reviewed separately;
- the frozen Holm family was reviewed;
- failed hypotheses were not inverted;
- the receipt authorizes neither production, Phase-7 mutation, Portfolio Action changes nor orders/trades.

A green workflow, a p-value, technical completion or a completed terminal evaluation without the manual receipt is insufficient to open the gate.

## Boundaries preserved

Phase 7 remains the authoritative baseline. The frozen Universal Stance, state transition and Portfolio Action contracts are unchanged and must remain reconstructible. The gate cannot:

- define an extended stance;
- select a stance rule family, threshold, sign or weight;
- create numeric vote counting or a weighted meta-score;
- route raw external components directly into stance;
- route external relation states directly into Portfolio Action;
- let `EXTERNAL_ONLY` create a Portfolio Action;
- change Phase-7 reliability, stance, hysteresis or Portfolio Action;
- generate position sizing, target weights, broker orders or trades.

Technical failure of the 8I-F gate cannot disable the Phase-7 path. Any gate failure resolves to `PHASE7_ONLY + NO_CHANGE_TO_PHASE7_DECISION`.

## Evidence consumption

When the gate eventually opens, any 8I-F stance-rule design informed by the final 8I-E empirical review must mark the 8I-E evidence as **spent for design**. That evidence cannot then be presented as fresh validation of the resulting stance rule. A later stance challenger requires fresh evidence under a separately frozen validation plan.

## Current real repository state

The latest stored 8I-E review status is `STARTED_WAITING_FOR_PROSPECTIVE_START` with `empirical_review_complete=false`, `terminal_family_evaluation_complete=false` and `real_outcomes_opened=false`. Consequently the real 8I-F gate must remain closed.

## Completion semantics

Technical completion of this entry gate means only that the transition boundary is implemented, tested and guarded in CI. It does **not** mean that 8I-F Extended Stance Preregistration is empirically authorized or complete, and it does not authorize 8I-G.
