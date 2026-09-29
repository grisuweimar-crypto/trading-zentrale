# Phase 8I-H — Production Readiness / Final Review

## Status

`BLOCKED_WAITING_FOR_8I_G_EMPIRICAL_COMPLETION`

8I-H is the final manual review layer of Phase 8I. It is deliberately **not** a production switch. The current 8I-G contract still has `8i_g_validation_plan_frozen=false`, `8i_g_prospective_validation_complete=false`, `8i_g_holdout_complete=false`, `8i_g_empirical_completion=false` and `8i_h_may_start_now=false`. Therefore the real 8I-H final review cannot yet open.

## Purpose

When 8I-G eventually completes a frozen prospective validation and one-shot holdout, 8I-H verifies whether that exact, pre-registered challenger is ready for a manual final review. A valid 8I-G handoff must include exact hashes for the 8I-F stance specification, the stance preregistration, the 8I-G validation plan, holdout manifest and terminal result.

The 8I-G receipt must confirm fresh evidence, one-shot holdout consumption, no post-freeze tuning, separate effect-size/uncertainty review, multiplicity review and no failed-hypothesis inversion. It may authorize **only** entry into the 8I-H final review.

## Final-review states

The frozen manual states are:

- `NOT_REVIEWED`
- `CONTINUE_RESEARCH`
- `REJECTED`
- `APPROVED_FOR_SEPARATE_PRODUCTION_INTEGRATION_CHANGE_ONLY`

Even the approved state does not activate production. It authorizes only a later, explicit production-integration change. That later change must bind the exact approved rule and validation hashes and define runtime fallback, auditability and any Portfolio Action bridge separately.

## Production boundary

8I-H itself may not:

- overwrite Phase 7;
- activate extended reliability or extended stance in production;
- alter Portfolio Action;
- generate position sizing or target weights;
- enable broker orders or trades;
- treat CI success or statistical significance as automatic approval;
- silently convert a failed holdout into an inverse strategy.

Phase 7 remains the authoritative fallback. External technical failure must not disable it.

## Readiness review

A future manual final review must verify at least:

- complete PIT and provenance lineage;
- complete evidence-consumption lineage;
- preserved Phase-7 reconstructibility;
- no hidden meta-score or numeric vote shortcut;
- no direct external-evidence → Portfolio Action path;
- no validation-result → order path;
- rollback/disable path before any production change;
- auditable production lineage;
- private position-state information remains outside the public repository.

## Current meaning of technical completion

Technical completion of 8I-H means only that this final-review protocol, validator, tests, documentation and CI guard exist and correctly fail closed on the current repository state. It does **not** mean that Phase 8I is empirically complete or production-ready.
