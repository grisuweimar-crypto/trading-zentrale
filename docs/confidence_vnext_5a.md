# Phase 5A — Readiness / Purged Walk-forward Contract

Status: 2026-09-24

Phase 5 is research-only. It does not replace productive Confidence, modify
Selection/Timing/Probability/Risk, change R0-R5, touch Depot-Watch, or create
portfolio actions.

## Freeze and current readiness

Phase 4E entered `main` at merge commit:

`fb038c78682e4e9fe87f11a1c3c9472b763bd3c4`

Merge time:

`2026-09-24T01:12:14Z`

This commit/time is the Phase-5 prospective freeze boundary. The Phase-4E
workflow only accepts scanner publications whose first-parent commit already
contains the Phase-4E workflow. Phase 5 additionally enforces the boundary in
its own consumers: every claim used for readiness or training must have a
parseable `generated_at` strictly after the freeze. Pre-freeze or temporally
unprovable claims fail closed.

At the Phase-5A audit start, `main` still pointed to the Phase-4E merge commit
and `phase4e-shadow-data` did not yet exist. Consequently the honest readiness
state was:

| Horizon | Claims | Evaluable | Mature outcomes | Symbols | Mature snapshots | Independent/time-separated support |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 5T | 0 | 0 | 0 | 0 | 0 | 0 |
| 20T | 0 | 0 | 0 | 0 | 0 | 0 |
| 40T | 0 | 0 | 0 | 0 | 0 | 0 |
| 60T | 0 | 0 | 0 | 0 | 0 | 0 |

No adaptive result can be estimated from this state. The correct result is
`insufficient_evidence`.

The first legitimate claims can only appear after a new
`Scanner_vNext Autopilot` publication on `main` after the freeze. 5T outcomes
will mature first; 20T/40T/60T must mature on their own clocks.

## Readiness gap discovered before the first claim

The immutable Phase-4E claim schema correctly freezes:

- Data-Quality proxy states for Selection, Timing and Risk;
- current Selection state/direction;
- current Timing model state/direction and matched robust patterns;
- current Risk model state;
- Model Agreement and conflicts;
- claim-time evidence fingerprints and PIT source timestamps.

However, two Phase-4C ordinal states are not frozen at claim level:

- `timing_state` is a current model state such as `robust_claim`, `unknown` or
  `internal_conflict`; it is not the Phase-4C state
  `robust / directional_only / immature / mixed / unavailable`.
- `risk_state` is a current downside-model state such as `elevated`, `middle`,
  `low` or `unknown`; it is not the Phase-4C statistical-evidence state.

`selection_state` does preserve the claim's Phase-4C ordinal state.

Phase 5 must not silently reinterpret the Timing/Risk model states as
Statistical Confidence. Until a prospective claim-level Statistical Confidence
context is frozen, adaptive weighting of the full
Data Quality / Statistical Confidence / Model Agreement triad remains blocked.
The readiness report exposes this as:

`claim_level_phase4c_timing_and_risk_states_not_archived`

This gate cannot be cleared by a command-line assertion. Readiness derives it
only from archived `timing_statistical_state` and `risk_statistical_state`
fields whose values are valid Phase-4C ordinal states. Because no real Phase-4E
claims existed when this gap was discovered, the archive can still be extended
prospectively without rewriting historical claims.

## Purged walk-forward contract

For each horizon separately:

1. A model version has a unique immutable version ID.
2. Training may use only post-freeze Phase-4E claims with fully matured outcomes.
3. The outcome must already have been known by `training_cutoff`.
4. Its stored `end_market_date` must be strictly before `evaluation_start`.
5. Claim time must itself precede the evaluation period.
6. Parameters, feature definitions and hyperparameters are frozen before the
   evaluation period starts.
7. Evaluation uses only the next untouched period.
8. That period may enter training only for a later model version, after it has
   completely ended and matured.
9. 5T/20T/40T/60T are never pooled into a shared maturity assumption.
10. Overlapping forward windows are not counted as independent observations.
11. Robust uncertainty continues to use circular moving observation-date
    blocks with full date clusters and effective block length `2 x horizon`.

Purging is validated twice: once when training pairs are built and again when
an immutable model manifest is constructed. The manifest builder rejects rows
that are pre-freeze, matured after the training cutoff, overlap the evaluation
period, or belong to a horizon not declared by that model version.

The exact training evidence used by each model version is hashed into an
`evidence_fingerprint`. The fingerprint covers every training column and value,
including later engineered features, rather than a fixed allowlist. A model
manifest records:

- version ID;
- training cutoff and freeze time;
- evidence fingerprint;
- horizons;
- feature definition;
- parameters;
- hyperparameters;
- evaluation start/end;
- result;
- promotion/reject status.

Historical manifests must not be rewritten after evaluation starts.

## Frozen baseline

The Phase-4 architecture is the non-adaptive baseline. The Phase-5 evaluator
can report descriptive prospective reliability by:

- Selection Statistical Confidence state;
- Model Agreement state;
- Data-Quality states;
- directional hit rate where a directional claim exists;
- signed peer excess;
- adverse excursion;
- path max drawdown.

Descriptive point estimates are not robust evidence. No scalar Confidence,
weights, HIGH/MED/LOW thresholds, or promotion decision is created by the
baseline evaluator.

Readiness counts mature snapshot cohorts from claims that actually have mature
outcomes. Merely having additional unevaluated claim snapshots cannot satisfy
the temporal-cohort gate. The reported maturity time span uses `evaluated_at`,
not the original claim date.

## Promotion gate

A candidate can only become eligible for a separate promotion review after all
of the following are true:

- multiple genuine walk-forward evaluation epochs exist;
- their version IDs are distinct and their evaluation windows do not overlap;
- PIT/leakage audit passes;
- model versions are reproducible;
- robust uncertainty is available;
- concentration checks pass;
- temporal stability checks pass;
- a reproducible advantage over the frozen baseline is demonstrated.

Even when all structural gates pass, the Phase-5A code only returns
`eligible_for_separate_promotion_review`. It does not change production.

## Files introduced by Phase 5A

- `src/scanner/reports/confidence_vnext_walkforward.py`
- `scripts/run_confidence_vnext_readiness_5.py`
- `tests/test_confidence_vnext_walkforward.py`
- `.github/workflows/confidence_vnext_5a.yml`
- `docs/confidence_vnext_5a.md`

The workflow is read-only with respect to repository contents. It may fetch the
dedicated Phase-4E shadow branch, run the audit, and upload a report artifact,
but it never pushes to `main` or to `phase4e-shadow-data`.
