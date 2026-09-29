# Phase 8I-G — Prospective Validation / Holdout Entry Gate

## Status

`BLOCKED_WAITING_FOR_8I_F_STANCE_PREREGISTRATION`

8I-G is the prospective validation and holdout layer for a future frozen 8I-F extended-stance challenger. The current 8I-F contract has not completed stance preregistration and explicitly states that 8I-G may not start empirically yet. Therefore this subblock is implemented as a fail-closed entry gate only.

## What is frozen now

The entry gate fixes the validation boundary before any 8I-G outcome access:

- the exact 8I-F stance rule, spec and preregistration hashes must be bound;
- 8I-F must issue a dedicated `FROZEN_FOR_8I_G_PROSPECTIVE_VALIDATION_ONLY` receipt;
- any 8I-E evidence used to design 8I-F must be marked `spent_for_design`;
- fresh 8I-G confirmation evidence must begin strictly after the 8I-F rule freeze;
- the validation plan, primary metric, horizon family and multiplicity family must be frozen before any 8I-G outcome value is opened;
- validation and holdout may not tune sign, threshold, horizon, variant or rule selection;
- failed hypotheses may not be inverted;
- the holdout is one-shot and cannot be reused for rule selection;
- overlapping forward windows are not treated as IID;
- effect size and uncertainty remain separate;
- significance cannot automatically promote a challenger.

The inherited statistical floor remains the Phase-7I floor: minimum group N 30, at least two temporal support regions, circular moving observation-date blocks, and block length `2 x evaluated horizon sessions`. The existing reference horizons remain 5, 20, 40 and 60 sessions. Rule-specific family details are deliberately deferred until a valid 8I-F handoff exists; choosing them now would invent a stance model that does not yet exist.

## Required future 8I-F handoff

A valid handoff must contain:

- `stance_rule_id`;
- `stance_spec_version`;
- `stance_spec_sha256`;
- `stance_preregistration_sha256`;
- timezone-aware `frozen_at`;
- `8i_e_evidence_consumption_status = spent_for_design`;
- authorization limited to freezing an 8I-G validation plan.

The handoff may not authorize production, Phase-7 mutation, Portfolio Action changes, orders or trades.

## Current repository state

The current 8I-F contract still has:

- `actual_8i_f_stance_preregistration_complete = false`;
- `8i_g_may_start_now = false`.

There is no real 8I-F stance-preregistration receipt. Therefore the current 8I-G result must remain:

`BLOCKED_WAITING_FOR_8I_F_STANCE_PREREGISTRATION`

No validation plan is frozen, prospective validation has not started, the holdout is closed, and no real 8I-G outcomes are opened.

## Decision-layer boundary

Phase 7 remains reconstructible and authoritative. 8I-G cannot create a direct external-evidence-to-Portfolio-Action path, cannot route a validation result directly into an order, and cannot enable broker execution. A future Portfolio Action change requires a separate later gate.

## Completion semantics

Technical completion of this entry gate means the 8I-G transition boundary is implemented, tested and guarded in CI. It does not mean prospective validation or holdout evaluation has begun or completed. It does not authorize 8I-H.
