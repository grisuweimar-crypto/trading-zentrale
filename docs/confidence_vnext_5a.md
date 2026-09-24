# Phase 5A — Readiness / Purged Walk-forward Contract

Status: 2026-09-24

Phase 5 remains research-only. Phase 5A does not replace productive Confidence, modify Selection/Timing/Probability/Risk, change R0-R5, touch Depot-Watch, or create portfolio/trading actions.

## Immutable Phase-5 freeze

Phase 4E entered `main` at merge commit `fb038c78682e4e9fe87f11a1c3c9472b763bd3c4` at `2026-09-24T01:12:14Z`. This commit/time is the immutable prospective Phase-5 freeze boundary.

At the start of Phase 5A no `phase4e-shadow-data` branch existed. The honest initial state was therefore 0 claims and 0 mature outcomes for 5T/20T/40T/60T and `insufficient_evidence`.

Claims consumed by Phase 5A must be provably post-freeze and contemporaneous. For `phase4e_shadow_v1`, `generated_at` must be strictly after the freeze and on the same normalized date as immutable claim `as_of`; retroactively generated claims fail closed.

## What Phase 5A discovered about `phase4e_shadow_v1`

The v1 archive is useful audit evidence, but it is not sufficient to certify Phase-5 learning evidence.

Two structural gaps are binding:

1. **Statistical Context** — v1 does not integrity-bind claim-level Phase-4C Timing/Risk ordinal Statistical-Confidence states (`robust / directional_only / immature / mixed / unavailable`). `timing_state` and `risk_state` are application/model states and must not be relabelled as Phase-4C Statistical Confidence.
2. **Exact horizon-session provenance** — v1 stores declared horizon plus start/end market dates, but it does not integrity-bind the exact elapsed market-session count needed to prove that a stored label is truly the declared 5T/20T/40T/60T outcome.

The permanent Phase-5A blockers for v1 are therefore:

- `claim_level_phase4c_timing_and_risk_states_not_archived`
- `phase4e_shadow_v1_lacks_integrity_bound_elapsed_session_count`

Adding similarly named columns in memory cannot clear either gate. Phase 5A requires the exact immutable v1 schema and verifies the original claim hash. A later prospective archive schema must integrity-bind the missing Statistical Context and exact horizon-session provenance before adaptive learning or a certified Frozen-Baseline evaluation can begin.

## Authentication and chronology of v1 audit evidence

Before Phase 5A even audits v1 rows, it validates the archive itself.

Claims:

- exact immutable `CLAIM_COLUMNS` schema required;
- `schema_version` must equal `phase4e_shadow_v1`;
- duplicate `claim_id` values rejected;
- the canonical Phase-4E claim payload is reconstructed and SHA-256 is recomputed;
- stored `claim_id` must match that hash;
- claim generation must be strictly post-freeze and contemporaneous with claim `as_of`;
- eligible claims require a valid frozen start market session and positive start price;
- unevaluable claims require their immutable unavailable reason.

Raw outcomes:

- exact immutable `OUTCOME_COLUMNS` schema required;
- `schema_version` must equal `phase4e_shadow_v1`;
- duplicate or unknown claim IDs rejected;
- only outcome-eligible claims may have outcomes;
- claim/outcome symbol, currency, horizon, `as_of`, and start market session must agree;
- start session must not be after claim `as_of`;
- end date must be after the claim date;
- `evaluated_at` must be after the completed end-market date;
- adjusted start/end prices, return, adverse excursion, and path max drawdown must be finite;
- start/end prices must be positive;
- adverse excursion and path max drawdown must lie in `[0, 1]`;
- stored return must agree with `end / start - 1` within tight numerical tolerance.

A weekday-gap feasibility check rejects obviously impossible declared horizons, but it is only an integrity diagnostic. It is **not** used to certify that exactly H market sessions elapsed.

## Readiness semantics

Because v1 cannot prove exact elapsed market sessions, Phase 5A deliberately reports:

- `raw_archive_outcomes`: stored v1 outcomes that pass archive-integrity checks;
- `verified_mature_outcomes = 0`;
- `mature_outcomes = 0` for Phase-5 learning readiness;
- `mature_symbols = 0`;
- `mature_snapshots = 0`;
- temporal support regions = `0`;
- `horizon_session_provenance_verified = false`;
- `walkforward_evaluation_ready = false`;
- overall status `insufficient_evidence`.

This is intentional. Raw v1 outcomes are not silently upgraded into certified Phase-5 labels.

## Fixed temporal contract for a future certifiable stream

The inherited Phase-4 methodology remains immutable for any later certifiable prospective schema:

- fixed event spacing: `5` sessions;
- robust uncertainty: circular moving observation-date bootstrap;
- effective block length: `2 × horizon`;
- complete observation-date clusters stay together;
- at least `2` time-separated support regions;
- overlapping forward windows are not independent;
- 5T/20T/40T/60T remain separate.

These are contract constants, not tuning parameters. Attempts to weaken them fail closed.

## Purged walk-forward contract

For a future integrity-bound evidence stream, training must satisfy all of the following:

1. only post-freeze evidence;
2. claims known no later than the training cutoff;
3. only fully matured labels already knowable by the training cutoff;
4. verified claim/outcome identity and horizon provenance;
5. no forward outcome leaking into the next evaluation period;
6. horizon-specific purge/embargo;
7. fixed 5-session event spacing;
8. duplicate claims forbidden;
9. immutable evidence fingerprint;
10. next evaluation period untouched until it ends.

`phase4e_shadow_v1` cannot satisfy the exact horizon-provenance requirement. Therefore `purged_training_pairs` refuses v1 outcomes rather than inventing session history.

## Evidence fingerprints and model manifests

The generic evidence fingerprint hashes every training column/value using typed, null-safe serialization and includes the exact column dtype schema even for zero-row frames.

Every model version records at least:

- version ID;
- training cutoff;
- freeze commit/time;
- evidence fingerprint;
- horizons;
- feature/state definition;
- parameters and hyperparameters;
- evaluation start/end;
- result and promotion/reject status.

Under the current v1 contract, manifests may only represent zero-row training evidence. Non-empty v1 training evidence is rejected with the horizon-provenance blocker.

## Frozen Baseline

Phase 4 remains the conceptual non-adaptive reference required by Phase 5. However, Phase 5A does **not** report v1 outcomes as a certified Frozen-Baseline reliability sample.

For v1 the evaluator returns `insufficient_evidence`, `N = 0`, no reliability groups, and the two structural blockers above. It still reports raw archived outcome counts for audit visibility.

A future Phase 5B baseline evaluator may calculate calibration/reliability, robust-vs-weak separation, directional-hit reliability, downside-risk calibration, dispersion and temporal stability only after the prospective schema integrity-binds the missing context and horizon provenance.

## Promotion gate

No adaptive candidate may be promoted from Phase 5A. The generic gate requires, at minimum:

- multiple genuine walk-forward evaluations;
- distinct version IDs;
- non-overlapping market-date evaluation epochs;
- PIT/leakage audit;
- reproducible versions;
- robust uncertainty;
- concentration checks;
- temporal stability;
- reproducible advantage over the Frozen Baseline.

Even when those gates eventually pass, the result is only `eligible_for_separate_promotion_review`; Phase 5 code does not automatically change production.

## Phase-5A files and isolation

Phase 5A changes exactly these five files:

- `src/scanner/reports/confidence_vnext_walkforward.py`
- `scripts/run_confidence_vnext_readiness_5.py`
- `tests/test_confidence_vnext_walkforward.py`
- `.github/workflows/confidence_vnext_5a.yml`
- `docs/confidence_vnext_5a.md`

The workflow has `contents: read`. It may fetch the dedicated Phase-4E shadow branch and upload a readiness report artifact, but it cannot push to `main` or `phase4e-shadow-data`.

## Next Phase-5 step

Phase 5B must be prospective. It must not rewrite or backfill v1 claims.

Before the Frozen Baseline can become empirically evaluable, the prospective archive needs a new integrity-bound schema containing at least:

- the claim-time Statistical Context required by Phase 5, including Timing/Risk evidence without falsely collapsing distinct underlying evidence into an invented score; and
- exact market-session horizon provenance sufficient to prove that each 5T/20T/40T/60T label represents exactly the declared forward horizon.

Only newly collected evidence under that schema may later become eligible for certified walk-forward learning and evaluation.
