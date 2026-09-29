# Phase 8I-A — Decision Extension Contract

Status: `FROZEN_OUTCOME_BLIND_WAITING_FOR_PROMOTED_EXTERNAL_EVIDENCE`

Phase 8I-A freezes the methodological and technical boundary for attaching promoted external evidence to the frozen Phase-7 Decision Layer. This block is contract-only. It does not read real Decision outcomes, bind runtime components, calculate external states in production, alter Reliability, recompute Universal Stance, modify Portfolio Action, or generate orders.

## Frozen baseline

Phase 7 remains the immutable baseline. Phase 8I is additive and must preserve three separately reconstructible layers:

1. `phase7_core_state`
2. `external_evidence`
3. `extended_decision_state`

No hidden meta-score, numeric vote, arbitrary weighting, or silent replacement of Phase-7 output is permitted.

The 8I-A branch was created from the completed Phase-8H HEAD:

- parent branch: `phase8h-cross-factor-interaction`
- parent commit: `e31990b826ab093bbfdc5d7426c58d657bffc51c`

Critical Phase-7 and Phase-8 contracts are bound by exact Git blob SHA in `configs/external_evidence_8i_decision_extension_contract_v1.json`.

## Upstream promotion scope finding

The upstream review scopes do not currently authorize any external component to affect the Decision Layer.

### 8G main effects

The 8G-H promotion contract can grant at most:

`APPROVED_FOR_8H_RESEARCH_ONLY`

That approval explicitly does **not** authorize Phase-8I Decision-Layer integration. Therefore an 8G main effect cannot be treated as 8I-active merely because it was approved for 8H interaction research. A separate explicit 8I research authorization is required before future binding.

This is a governance/research-scope gap, not a technical implementation defect and not evidence that the 8G research itself is invalid. Phase 8I-A does not retroactively modify the 8G contract.

### 8H interactions

The 8H-H review can grant at most:

`APPROVED_FOR_8I_RESEARCH_ONLY`

Even this approval authorizes 8I research design, not automatic 8I integration, production, Phase-7 mutation, or orders. Exact identity, PIT, provenance, promotion receipt, evidence consumption, horizon, source, and spec/hash binding must still occur in a later 8I binding block.

At the current repository state, the 8H-H approved list is empty.

## External state vocabulary

Phase 8I-A inherits the existing external conflict contract rather than inventing new semantics.

Raw external direction states:

- `POSITIVE`
- `NEGATIVE`
- `MIXED`
- `UNKNOWN`
- `INSUFFICIENT_EXTERNAL`

External-evidence relation states relative to the Phase-7 core:

- `CONFIRMING`
- `CONFLICTING`
- `EXTERNAL_ONLY`
- `MIXED_EXTERNAL`
- `UNKNOWN`
- `INSUFFICIENT_EXTERNAL`

`UNKNOWN` and `INSUFFICIENT_EXTERNAL` remain distinct. The relation is descriptive, not a trade or stance policy.

## Current empirical entry state

No external factor or interaction is currently authorized for Decision-Layer influence.

The required current state is therefore:

- state: `NO_PROMOTED_EXTERNAL_EVIDENCE`
- operational status: `WAITING_FOR_PROMOTED_EXTERNAL_EVIDENCE`
- external direction: `INSUFFICIENT_EXTERNAL`
- external evidence state: `INSUFFICIENT_EXTERNAL`
- decision effect: `NO_CHANGE_TO_PHASE7_DECISION`

This is a valid waiting state, not an error.

## Fail-closed rule

External evidence is excluded from Decision-Layer influence if any required promotion, scope, identity, PIT, provenance, freshness, mapping, source, hash, or evidence-consumption condition fails.

If no valid external evidence remains, the system must resolve to:

`INSUFFICIENT_EXTERNAL` + `NO_CHANGE_TO_PHASE7_DECISION`

A technical failure in the external-evidence path must not disable or mutate the Phase-7 core.

## Backward compatibility

When no eligible external evidence is available:

- Phase-7 Universal Stance is unchanged.
- Phase-7 Reliability is unchanged.
- Phase-7 state-transition output is unchanged.
- Phase-7 Portfolio Action is unchanged.
- Phase-7 Swing Management is unchanged.
- the Phase-7 output remains reconstructible.
- only the additional neutral external-evidence block may appear.

The explicit invariant is:

`NO_PROMOTED_EXTERNAL_EVIDENCE -> INSUFFICIENT_EXTERNAL + NO_CHANGE_TO_PHASE7_DECISION`

## Reliability before Stance

Phase 8I-A freezes the research ordering, not a production rule.

The first future Decision-Layer research layer is an **extended Reliability interpretation kept separate from the existing Phase-7 Reliability**. Phase-7 Reliability must remain reconstructible and cannot be changed without a later empirical gate.

Any Universal-Stance extension is a separate, higher-bar research question. It requires its own later preregistration and validation and is disabled in 8I-A.

## Aggregation and double counting

Phase 8I-A forbids:

- numeric vote counting,
- arbitrary weights,
- treating a main effect and an interaction depending on that main effect as independent votes,
- automatic sign inversion after a failed hypothesis,
- automatic activation after future upstream promotion.

Interaction parentage must remain explicit. Concrete aggregation rules are deferred to the appropriate later 8I state-engine block and must be frozen outcome-blind before evaluation.

## Provenance

Future eligible external Decision evidence must retain at least:

- source
- source ID
- factor ID
- interaction ID if applicable
- `as_of`
- `valid_from`
- mapping version
- promotion receipt
- evidence-consumption status
- model/spec version
- horizon
- PIT status
- component artifact hash

No anonymous end score may erase this lineage.

## Outcomes and evidence consumption

Phase 8I-A does not read real Decision outcomes and does not choose a primary 8I Decision metric from outcomes.

The existing Phase-7 label catalog remains the allowed reference family:

- `return`
- `peer_excess`
- `adverse_excursion`
- `path_max_drawdown`

The exact 8I primary metric and exact Discovery/Validation/Holdout/Prospective partitions must be frozen before the first 8I outcome read. They may not be selected after seeing outcomes.

Evidence already consumed by Phase 7, 8G, or 8H does not become fresh independent confirmation for 8I. Outcome-informed rule design marks the evidence `spent_for_design`.

Overlapping forward windows are not independent observations, and effect size and uncertainty must remain separate.

## Production boundary

Phase 8I-A is research-only and shadow-only.

It does not authorize:

- Phase-7 overwrite,
- productive Decision-Layer influence,
- Portfolio Action changes,
- orders or trades,
- automatic promotion,
- automatic activation of future 8G/8H results.

Technical completion is not empirical promotion.

## Parked audit point

The possible earlier prospective collection start around `2026-09-18` remains explicitly parked and unchanged. Phase 8I-A may not move that date. Any change requires a separate PIT/outcome-blind audit.

## Definition of done for 8I-A

8I-A is technically complete only when:

- the contract is frozen,
- the upstream scopes and exact parent versions are guarded,
- state vocabulary matches the existing external conflict contract,
- the no-promotion fail-closed invariant is tested,
- Phase-7 backward compatibility is tested,
- production and direct-trade paths remain closed,
- tests pass in CI.

Passing 8I-A does not start 8I-B and does not enable external Decision-Layer influence.
