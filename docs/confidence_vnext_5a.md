# Phase 5A — Readiness / Purged Walk-forward Contract

Status: 2026-09-24

Phase 5 is research-only. It does not replace productive Confidence, modify Selection/Timing/Probability/Risk, change R0-R5, touch Depot-Watch, or create portfolio actions.

## Freeze and starting readiness

Phase 4E entered `main` at merge commit `fb038c78682e4e9fe87f11a1c3c9472b763bd3c4` at `2026-09-24T01:12:14Z`. This commit/time is the immutable Phase-5 prospective freeze boundary.

Every claim consumed by Phase 5A must have a parseable `generated_at` strictly after that freeze. Pre-freeze or temporally unprovable claims fail closed.

At Phase-5A start, `phase4e-shadow-data` did not yet exist. The honest initial state was therefore 0 claims and 0 mature outcomes on 5T/20T/40T/60T, with status `insufficient_evidence`. No empirical adaptive result was invented from that state.

## Statistical-Context gap

The Phase-4E `phase4e_shadow_v1` claim schema integrity-binds Data-Quality states, Selection state/direction, current Timing model state/direction and matched robust patterns, current Risk model state, Model Agreement, fingerprints and PIT source timestamps.

It does **not** integrity-bind claim-level Phase-4C Timing/Risk ordinal Statistical-Confidence states (`robust / directional_only / immature / mixed / unavailable`). `timing_state` and `risk_state` are model/application states and must not be relabelled as Phase-4C Statistical Confidence.

Accordingly, Phase 5A now treats this gap as structurally unresolvable inside shadow-v1:

`claim_level_phase4c_timing_and_risk_states_not_archived`

Adding similarly named columns to an in-memory DataFrame cannot clear this gate. Phase 5A requires the exact immutable Phase-4E schema and rejects extra columns. A later prospective archive schema must add the Statistical Context to its immutable claim payload and claim hash before this gate can ever become true.

This means current shadow-v1 evidence can still be audited descriptively, but it cannot become eligible for adaptive learning merely through an operator/configuration assertion.

## Maturity and provenance validation

Before a stored outcome may count as mature evidence, Phase 5A verifies against its immutable claim that:

- the claim was outcome-eligible;
- claim and outcome IDs are unique;
- claim/outcome symbol, horizon, `as_of` and start market session agree;
- the claim-time start session is not after claim `as_of`;
- `end_market_date` is after the claim date;
- `evaluated_at` is after the completed target market date;
- outcome provenance is therefore genuinely forward in time.

Invalid chronology/provenance fails closed before mature snapshots, symbols, support regions or baseline estimates are counted.

## Five-session event spacing

The inherited Phase-4/4E event spacing is a real contract, not a label:

`fixed_event_spacing_sessions = 5`

Phase 5A applies the cooldown per symbol before forming the evaluated/readiness cohort. Repeated daily or same-session claims therefore cannot overweight descriptive estimates or artificially increase temporal-support distance.

Spacing uses ordered immutable claim-time start sessions represented for that symbol, never calendar-day distance. Multiple reruns on the same market session share one session position. Missing scanner publications make the spacing conservative rather than inventing unseen sessions.

Peer labels remain derived from the complete matured immutable snapshot cohort first; the five-session cooldown is applied to the evaluated claim cohort afterwards. This preserves the Phase-4E peer-baseline definition while preventing repeated events from being overcounted.

## Temporal-support rule

After the five-session cooldown, Phase 5A mirrors the Phase-4 robust-uncertainty support semantics. A new mature support cohort is counted only when its observation position is at least the effective uncertainty block length after the previous counted cohort:

`block_length = 2 x horizon`

Counted forward outcome windows must also not overlap. Calendar distance alone is not temporal support, and overlapping forward windows are never described as independent.

The fixed Phase-4 constants are immutable in Phase 5A:

- uncertainty block multiplier: `2`;
- minimum time-separated support regions: `2`;
- fixed event spacing: `5` sessions;
- Statistical Context gate: mandatory.

Supplying weaker values raises instead of changing readiness semantics.

## Purged walk-forward contract

For each horizon separately:

1. Training may use only post-freeze Phase-4E evidence.
2. Claims must be generated no later than `training_cutoff`.
3. Only fully matured outcomes known by `training_cutoff` may enter training.
4. Claim and outcome must share horizon, symbol, claim date and start-market provenance.
5. `end_market_date` must be genuinely after the claim and before `evaluation_start`.
6. `evaluated_at` must be after the completed outcome date and no later than `training_cutoff`.
7. Duplicate `claim_id` rows are forbidden.
8. Five-session spacing is applied to training claims as well.
9. 5T/20T/40T/60T remain separate.
10. Evaluation uses only the next untouched period.
11. That period may enter later training only after it has fully ended and matured.
12. Forward-window overlap is not treated as independence.
13. Robust uncertainty continues to use circular moving observation-date blocks with effective block length `2 x horizon`.

Purging is checked when training pairs are created and revalidated again at immutable model-manifest construction.

## Immutable evidence fingerprints and model manifests

The training evidence fingerprint covers every column/value with typed, null-safe encoding. Literal strings cannot collide with missing values, numeric values cannot collide with string representations, and the exact column schema including dtypes is hashed even for zero-row frames.

Every model version records at least:

- unique version ID;
- training cutoff;
- Phase-5 freeze commit/time;
- evidence fingerprint;
- horizons;
- feature definition;
- parameters and hyperparameters;
- evaluation start/end;
- result and promotion/reject status.

Historical manifests are immutable after evaluation starts.

## Frozen baseline

The current Phase-4 architecture remains the non-adaptive reference. The Phase-5A evaluator is descriptive only and can report prospective reliability grouped by Selection state, Model Agreement and Data-Quality states using directional hit rate, signed peer excess, adverse excursion and path max drawdown.

The baseline evaluator applies the five-session event spacing to the evaluated cohort. It does not create adaptive weights, a scalar Confidence mapping, HIGH/MED/LOW thresholds or a production decision.

Robust intervals are not claimed until the temporal-support contract is actually satisfied.

## Promotion gate

A candidate can only become eligible for a separate promotion review after all structural gates pass, including multiple genuine walk-forward epochs, distinct version IDs, non-overlapping market-date evaluation windows, PIT/leakage audit, reproducibility, robust uncertainty, concentration checks, temporal stability and reproducible advantage over the frozen baseline.

Even then Phase 5A only returns `eligible_for_separate_promotion_review`; it never changes production.

## Phase-5A files

- `src/scanner/reports/confidence_vnext_walkforward.py`
- `scripts/run_confidence_vnext_readiness_5.py`
- `tests/test_confidence_vnext_walkforward.py`
- `.github/workflows/confidence_vnext_5a.yml`
- `docs/confidence_vnext_5a.md`

The Phase-5A workflow is read-only with respect to repository contents. It may fetch `phase4e-shadow-data`, run the audit and upload a report artifact, but it cannot push to `main` or the shadow branch.
