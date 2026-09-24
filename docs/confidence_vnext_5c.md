# Phase 5C — Progressive horizon-specific learning

## Purpose

Phase 5C turns the prospective Phase-5B v2 stream into a **progressive research-learning loop** without waiting for every horizon to mature at once.

The central rule is:

> Each horizon starts learning only from its own fully matured labels, but it may start as soon as its own evidence is sufficient.

Therefore 5T may enter pilot learning while 20T, 40T and 60T are still collecting. A 5T outcome is never relabelled, stretched or reused as a 20T/40T/60T target.

Phase 5C remains research-only. It does not change production Confidence, Selection, Timing, Probability, Risk, R0-R5, Depot-Watch, portfolio logic or trading actions.

## Why this is not a 40-session waiting period

The four horizons have independent maturity clocks:

- 5T can mature first;
- 20T starts later from genuine 20T outcomes;
- 40T starts later from genuine 40T outcomes;
- 60T starts last from genuine 60T outcomes.

Phase 5 therefore keeps learning as the evidence base grows instead of treating 60T maturity as a global start gate.

A long horizon itself cannot legitimately learn before its own labels exist. What is avoided is an unnecessary global freeze in which shorter horizons remain unused while waiting for the longest horizon.

## Progressive learning stages

Each horizon is classified independently:

1. `collecting` — no claim-spaced mature evidence yet.
2. `mature_outcomes_accumulating` — real labels exist, but the fixed pilot floor is not yet met.
3. `pilot_learning` — enough prospectively matured evidence exists to freeze a versioned descriptive learner.
4. `robust_learning_base` — pilot requirements are met and the inherited temporal support rule has at least two separated support regions.

These stages are evidence maturity states, not quality scores and not trade signals.

## Pre-registered pilot floor

The pilot floor is fixed before observing Phase-5C outcomes and is not tuned to maximize performance:

- at least 2 distinct claim observation dates after fixed event spacing;
- at least 20 mature training rows;
- at least 10 distinct symbols.

The floor deliberately uses **all valid mature evidence**, not only rows with an explicit directional claim. This allows Phase 5C to begin learning prospective Risk / adverse-excursion / drawdown reliability even when directional claims are still sparse. Directional row and symbol counts remain separately reported and directional metrics remain unavailable where no directional target exists.

The purpose is simply to prevent a model version from being created from one time slice or a tiny handful of names. These values are operational safeguards, not claims of statistical robustness.

Robust status remains stricter:

- at least 2 time-separated support regions;
- inherited circular observation-date block concept with effective block length `2 × horizon`;
- overlapping forward windows are not treated as independent.

## Claim-first five-session spacing

Five-session membership is frozen from the immutable v2 **claims**, before looking at outcome availability.

This matters because an outcome can mature late if a price path was temporarily unavailable. If spacing were calculated only from currently available outcomes, a later-arriving older outcome could retroactively replace a previously selected event. That would make the training sample unstable.

Phase 5C therefore:

1. determines eligible spaced claim IDs from claim-time cooldown context;
2. freezes that membership independently of labels;
3. joins only fully matured outcomes to those already selected claims.

A delayed outcome can add evidence, but it cannot rewrite the historical spacing decision.

## What Phase 5C learns first

The first adaptive research product is intentionally transparent: **versioned empirical reliability tables** for the claim-time states already frozen in v2.

Examples include:

- Selection Statistical Context state;
- Timing matched evidence states;
- Risk feature evidence states;
- Model Agreement;
- Timing and Risk model states;
- Selection/Timing/Risk Data Quality states.

For each exact state, Phase 5C records prospective support and observed reliability/risk diagnostics such as:

- N and directional N;
- symbols and observation dates;
- directional hit rate when a directional target exists;
- mean and median signed peer excess when a directional target exists;
- adverse excursion;
- path maximum drawdown;
- temporal support regions.

State names are not assumed to be ordinal.

Phase 5C does **not** yet collapse these state tables into a scalar 0–100 Confidence value. That combination step must be justified by later walk-forward evidence rather than invented up front.

## Dynamic model versions

Once a horizon reaches `pilot_learning`, its current training evidence receives a deterministic evidence fingerprint. A new immutable candidate version is appended only when the eligible training evidence actually changes.

Every candidate version records:

- horizon;
- training cutoff;
- claim-time span and matured outcome end;
- evidence fingerprint;
- row/support counts;
- readiness state;
- learned state-reliability tables;
- explicit research-only semantics.

Re-running Phase 5C with unchanged evidence is idempotent and creates no duplicate version.

The version archive is append-only:

- `artifacts/research/confidence_vnext_model_versions_5c.jsonl`

Every model version hashes its complete payload and binds the hash of the previous version. The current report stores the chain tip and prior report state anchors the already published prefix. A changed historical version, broken link or truncated archive therefore fails closed on the next run instead of silently becoming a new history.

The current status report is:

- `artifacts/research/confidence_vnext_progressive_5c.json`

Both remain on the isolated `phase4e-shadow-data` branch, not `main`.

## Walk-forward boundary

A candidate version may learn only from outcomes knowable by its recorded training cutoff.

Its own training rows may never be reused as evaluation rows. Evaluation claims must have `generated_at` strictly after the candidate's training cutoff. This prevents the learner from being judged on data that created the version, including same-snapshot evidence that happened to mature later in the processing chain.

Later Phase-5C work can evaluate frozen versions against subsequent evidence and compare them with the Frozen Phase-4 baseline. Promotion still requires the Phase-5A gates: multiple genuine non-overlapping walk-forward epochs, PIT/leakage audit, reproducibility, robust uncertainty, concentration checks, temporal stability and reproducible baseline advantage.

Passing those gates only means `eligible_for_separate_promotion_review`; it never changes production automatically.

## Automation

The Phase-5C workflow runs after successful `Phase 5B Frozen Baseline v2` completion. It restores v2 claims/outcomes plus the existing version chain from `phase4e-shadow-data`, runs the progressive learner, tests the inherited contracts and publishes only the Phase-5C research report/version archive back to the isolated shadow branch.

No productive scanner artifact is modified.
