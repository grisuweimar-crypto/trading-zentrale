# QM-J / BA-QM7 — Negative Controls & Falsification

QM-J does not try to confirm Scanner-vNext. It deliberately creates synthetic, causally isolated controls whose purpose is to expose leakage, overfitting, multiple-testing artefacts, dependence errors and pipeline bias.

This package starts BA-QM7 on top of the completed BA-QM6 / QM-G handoff and the current W10 same-snapshot orchestration. It is research-only and cannot alter productive data, Universal Stance, Portfolio Action, Depot Watch or execution.

## Control levels

The executable harness covers the Masterplan levels directly:

- **Data:** `SHIFTED_DATA`
  - shifts declared values only within the declared entity;
  - uses past rows only;
  - leading unavailable values remain missing.
- **Feature:** `PERMUTED_FEATURE`, `IRRELEVANT_FEATURE`
  - deterministic permutation preserves the feature marginal distribution while destroying alignment;
  - irrelevant features are deterministic pseudo-features derived only from frozen identity + seed.
- **Research:** `PSEUDO_SIGNAL`, `PSEUDO_EVENT`
  - deterministic pseudo-signals/events are created without outcomes.
- **Decision Layer:** `PLACEBO_EVIDENCE`
  - synthetic sidecar evidence is explicitly barred from becoming a directional vote or entering the live Decision path.
- **End-to-End:** `DESTROYED_PREDICTIVE_INFORMATION`
  - declared predictive fields are deterministically permuted;
  - protected identity/outcome fields remain unchanged;
  - the resulting dataset is for isolated research pipelines only.

Every control artifact contains the source snapshot identity, source-content hash, exact transform definition, transformed-content hash and artifact hash. Source records are deep-copied; transformations never mutate the supplied productive object.

## Frozen falsification plan

A real result and its negative controls are compared only under a predeclared plan containing:

- plan identity/version;
- freeze timestamp;
- `outcome_visibility_at_freeze = NONE`;
- one primary metric;
- metric direction;
- a non-negative similarity margin.

The similarity margin is deliberately not hard-coded by QM-J. It must be frozen before outcomes are inspected. This prevents post-hoc tuning of what counts as a "similar" placebo.

All compared arms must use the same metric, comparison-context hash and observation count. A mismatched grid or sample fails closed.

## Promotion-stop rule

For `HIGHER_IS_BETTER`, a control triggers when:

`real_metric - control_metric <= frozen_similarity_margin`

For `LOWER_IS_BETTER`, the sign is reversed analogously.

A trigger produces:

`PROMOTION_STOP_INVESTIGATION_CAPA_REQUIRED`

and marks:

- promotion blocked by QM-J;
- investigation required;
- CAPA required;
- diagnostic classes to examine: leakage, overfitting, multiple testing, dependence error and pipeline bias.

A non-triggering result does **not** promote anything. It only means the tested negative controls were not similarly strong under the frozen plan.

## QM-H bridge

A triggered evaluation can register an open `METHODOLOGY_FINDING` in the existing QM-H ledger with evidence impact `PROMOTION_BLOCKED`. QM-J does not invent a root cause, does not auto-plan a CAPA and cannot close the finding. QM-H remains the CAPA authority.

## Hard boundaries

QM-J controls:

- never replace productive data;
- never enter the live W10/7A/7D–7H chain;
- never change scanner weights or phase semantics;
- never change Elliott Core, W6 or W8;
- never generate Portfolio Actions or broker orders;
- never enable execution;
- never claim empirical promotion.

This is the **foundation package** for BA-QM7. It establishes executable controls and the promotion-stop/CAPA bridge. It does not yet close BA-QM7; the next BA-QM7 work is to apply these controls to concrete Scanner-vNext research/Decision artifacts and record falsification results.
