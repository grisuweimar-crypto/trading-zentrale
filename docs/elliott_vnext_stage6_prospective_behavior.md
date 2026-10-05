# Elliott vNext – Stage 6 Prospective Behavior Audit

Status: post-activation, research-only.

Stage 1–3 activated Elliott technically. Stage 4 validated the frozen Elliott core historically. Stage 5 measured incremental Scanner↔Elliott evidence. Stage 6 now audits how the already frozen Elliott outputs actually behave across consecutive **real prospective scanner publications**.

Stage 6 is deliberately not another historical full replay.

## Goal

The prospective shadow archive already retains every genuinely emitted `(symbol, timeframe, degree)` 6H output, including unchanged outputs. Stage 6 reduces that append-only archive to a compact audit answering the first operational observation questions:

- how stable is the Primary Scenario across consecutive available 6H outputs?
- how stable is the Wave Stage?
- how often do the existing 6D review contexts occur?
- how often do different Elliott degrees/timeframes create simultaneous Add- and Reduce-review contexts for one symbol?
- which missing-evidence warnings recur?
- how much genuinely prospective observation history exists?

No outcome is used to construct these behavior features.

## Source

Canonical source remains the isolated shadow archive on branch `elliott-vnext-shadow-data`:

`artifacts/research/elliott_vnext_prospective_history_6h.jsonl`

Only captures with:

- schema `elliott_vnext_prospective_capture_v1`,
- engine `prospective_capture_engine_v2_iso_date_replay`,
- partition `prospective_unspent`,

are eligible.

The pre-v2 empty capture exposed during activation remains audit history but is excluded rather than relabelled as valid prospective evidence.

## Scenario Stability

Scenario Stability uses exactly the already defined QM-G semantics:

- same non-missing `primary_scenario.scenario_id` → `STABLE`;
- different non-missing ID → `CHANGED`;
- either side missing → `INSUFFICIENT_EVIDENCE`.

Only consecutive **genuinely available outputs of the same symbol/timeframe/degree identity** are compared.

No smoothing, persistence threshold, majority vote, degree reducer or outcome-based reclassification is allowed.

The Stage-6 regression test compares these aggregate counts directly against the existing QM-G Scenario Stability implementation.

## Wave-stage stability

Wave-stage stability is reported separately using strict equality of `current_wave_stage`.

It is descriptive. Stage 6 does not infer that a stable stage is good or that a changed stage is bad.

## Review-context audit

Existing 6D review contexts are counted without reinterpretation:

- `entry_or_add_review`
- `reentry_or_add_review`
- `partial_reduce_review`
- `profit_protection_review`
- `larger_reduce_or_exit_review`
- `hold_review`

Stage 6 reports:

- raw route occurrences,
- per-output context presence,
- per-symbol/per-capture context presence,
- symbol/capture observations with at least one action-relevant review context,
- simultaneous Add + Reduce conflicts.

A conflict is preserved as a conflict. Stage 6 does not select a preferred degree or scenario.

## Missing evidence and warnings

6H warnings are counted as emitted. Stage 6 does not replace missing market context, missing historical expectancy or any other absent evidence with a neutral value.

## Output

Compact report:

`artifacts/research/elliott_vnext_stage6_prospective_behavior.json`

The report is published on the shadow-data branch, not into the productive Scanner artifact path on `main`.

It contains only compact counts and capture summaries; it does not duplicate the large 6H archive.

## Hard boundaries

Stage 6:

- performs no Elliott replay;
- uses no future outcome;
- changes no Elliott rule;
- does not select or weight a degree;
- does not use Elliott direction as a vote;
- does not change Universal Stance;
- does not change Portfolio Action;
- does not turn review contexts into actions;
- does not optimize Scanner thresholds;
- does not create a trade decision or order;
- cannot promote Elliott automatically.

`technical_stage_status = COMPLETE` means only that the audit pipeline completed correctly.

It does **not** mean that prospective evidence is already statistically sufficient or that Elliott is empirically validated. No arbitrary minimum sample threshold is invented in Stage 6.

## Technical completion

Stage 6 is technically complete when:

1. the append-only prospective archive can be parsed fail-closed;
2. every eligible 6H output is revalidated at the W6 boundary;
3. legacy/pre-fix empty captures are excluded rather than reclassified;
4. Scenario Stability matches the frozen QM-G identity semantics;
5. Wave Stage remains a separate descriptive dimension;
6. review-context and Add/Reduce conflict counts preserve all available degrees;
7. missing evidence remains missing;
8. the compact Stage-6 artifact is produced from real shadow history;
9. all research-only/no-promotion/no-action guards remain true.

## What Stage 6 does not answer

Stage 6 does not yet answer whether a review context improves forward returns, reduces drawdown, improves realized Portfolio Action, or adds value after costs.

Those questions require later prospective outcome/impact evaluation. Stage 6 exists first to establish what the live Elliott system is actually emitting and how stable or conflicted those emissions are.
