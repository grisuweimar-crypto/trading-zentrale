# Phase 8I-B – Promotion & Provenance Binding

## Status

Phase 8I-B is an **outcome-blind, research-only binding gate**. It does not activate external evidence in the Phase-7 Decision Layer.

The maximum state created by this block is:

`BOUND_FOR_8I_RESEARCH_ONLY`

That state does **not** authorize:

- decision-outcome access,
- Phase-7 mutation,
- reliability or stance recomputation,
- portfolio-action changes,
- production integration,
- orders or trades.

Aggregation is explicitly deferred to **8I-C**.

## Promotion is not binding

8I-B separates four concepts:

1. **Upstream promotion** – an upstream research result has passed its own manual gate.
2. **8I authorization** – the upstream promotion scope is sufficient for 8I research binding.
3. **Provenance binding** – component identity, source identity, mapping identity, PIT state, hashes and evidence-consumption state are exact and auditable.
4. **Decision activation** – not part of 8I-B and remains disabled.

### 8G main effects

8G-H can only produce `APPROVED_FOR_8H_RESEARCH_ONLY`. That receipt explicitly does not authorize 8I integration. Therefore 8I-B requires an additional per-component manual receipt:

`AUTHORIZED_FOR_8I_RESEARCH_BINDING_ONLY`

This is a new forward governance receipt. It does not rewrite the original 8G-H decision and it still does not authorize decision influence.

### 8H interactions

8H-H can produce `APPROVED_FOR_8I_RESEARCH_ONLY`. No second manual approval is required, but the exact 8H-H decision receipt must still pass the 8I-B technical provenance binding.

## Required provenance

A bindable component must carry exact, verifiable identity for at least:

- component type and ID,
- factor / parent factors,
- horizon,
- model/spec version,
- component artifact hash,
- upstream promotion receipt hash,
- 8I authorization receipt hash when required,
- canonical source IDs and source-row hashes,
- as-of and valid-from timestamps,
- mapping version and mapping artifact hash,
- PIT status,
- evidence-consumption status.

Missing or inconsistent provenance fails closed. If no valid bound evidence remains, the invariant is:

`EXTERNAL_EVIDENCE = INSUFFICIENT_EXTERNAL`

and

`NO_CHANGE_TO_PHASE7_DECISION`.

## Upstream source-identity blocker discovered during 8I-B

A technical upstream contract inconsistency is currently present:

- the frozen 8G challenger specs use source IDs `FED_H15` and `ECB_EXR`;
- the actual 8F adapters use canonical IDs `federal_reserve_board_h15` and `ecb_data_portal`;
- the 8F source-candidate catalog also uses those canonical IDs;
- `external_source_registry_v1.json`, against which 8G-H source governance is configured to resolve the frozen IDs, does not contain the 8G aliases.

Classification:

`TECHNICAL_SOURCE_IDENTITY_CONTRACT_ERROR_NOT_EMPIRICAL_EVIDENCE_FAILURE`

Impact: the current 8G-H source-governance path cannot validly clear a future promotion using the frozen aliases. Therefore 8I-B treats the current repository as:

`BLOCKED_UPSTREAM_SOURCE_IDENTITY_CONTRACT`

while preserving the Phase-7 fallback:

`INSUFFICIENT_EXTERNAL + NO_CHANGE_TO_PHASE7_DECISION`.

The alias relation is documented, but **is not itself accepted as a repair**. A future binding requires a separate, versioned, outcome-blind upstream source-identity correction receipt. Historical 8G evidence must not be rewritten or silently reinterpreted.

## Evidence consumption

8G/8H promotion evidence is not fresh independent 8I confirmation. The binding records evidence-consumption state explicitly. 8I-B does not select metrics, signs, thresholds or weights from outcomes.

## Completion boundary

8I-B is technically complete when its contract, implementation, tests, documentation and CI pass. Technical completion remains valid even while no real external component is bindable.

Technical completion does not grant empirical release or decision influence.

The next block, only after a separate manual start, is:

`8I-C_OUTCOME_BLIND_EXTERNAL_AGGREGATION_DESIGN`
