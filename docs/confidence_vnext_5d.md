# Phase 5D — Prospective Walk-forward Evaluation

Phase 5D evaluates frozen Phase-5C research versions on genuinely later prospective evidence. It remains research-only and does not change production Confidence, scanner weights, thresholds, Selection, Timing, Probability, Risk, R-codes, Depot-Watch, portfolio logic, or trading actions.

## Evaluation contract

Each horizon (5T, 20T, 40T, 60T) is evaluated independently. A Phase-5C version may only consume claims whose `generated_at` is strictly later than that version's `training_cutoff`. Shorter-horizon outcomes are never substituted for longer-horizon outcomes.

The finalized evaluation epoch for one version ends at the `training_cutoff` of the next Phase-5C version for the same horizon. That successor boundary is created by evidence accumulation and versioning, not by observed evaluation performance. This prevents choosing a favorable stopping point after seeing results.

The newest version for a horizon has no successor yet. Its currently available future evidence is therefore reported only as provisional diagnostics and is explicitly excluded from promotion evidence.

## Point-in-time safety

Phase 5D inherits the Phase-5B/5C prospective archive validation and claim-first spacing contract. The evaluation frame is reconstructed using only evidence knowable by the epoch end. Peer-label dependencies must also be mature by that same cutoff.

A finalized epoch therefore satisfies all of the following:

- claim `generated_at` is strictly after the evaluated version's training cutoff;
- claim and outcome evidence is knowable no later than the successor version's training cutoff;
- the peer cohort used for cross-sectional labels is bound to the same PIT boundary;
- training rows from the evaluated version are not reused as its evaluation rows;
- 5T/20T/40T/60T remain separate.

## Evaluation outputs

For each finalized epoch Phase 5D stores:

- immutable evaluated and successor version IDs and SHA-256 hashes;
- training and evaluation cutoff timestamps;
- an evidence fingerprint of the exact evaluation rows;
- sample size, symbol count and observation-date count;
- directional hit rate and signed peer excess where direction is evaluable;
- adverse excursion and path max drawdown;
- concentration by symbol and observation date;
- state-by-state persistence versus the frozen Phase-5C training tables;
- inherited circular moving observation-date bootstrap uncertainty using effective block length `2 × horizon`.

State names are not treated as ordinal scores. Phase 5D does not create a scalar Confidence mapping.

## Robustness and promotion

A finalized epoch can be descriptive before sufficient temporal support exists. Robust uncertainty remains fail-closed and requires the inherited time-separated support contract. Overlapping forward windows are never treated as independent observations.

Phase 5D does not promote anything automatically. Promotion remains a separate decision gate requiring multiple genuine walk-forward epochs, distinct version identities, non-overlapping evaluation periods, PIT/leakage audit, reproducibility, robust uncertainty, concentration and temporal-stability checks, and reproducible advantage versus the Frozen Phase-4 baseline.

The latest provisional epoch never counts toward those requirements.

## Append-only publication

Finalized evaluation records are written to:

- `artifacts/research/confidence_vnext_walkforward_evaluations_5d.jsonl`
- `artifacts/research/confidence_vnext_walkforward_5d.json`

The finalized JSONL archive is SHA-256 hash-chained. Existing records are revalidated on every run. The workflow reads and publishes only on the isolated `phase4e-shadow-data` branch and shares the same publication concurrency lock as Phases 4E, 5B and 5C, preventing stale read/overwrite races.

## Current expected state

At introduction of Phase 5D the prospective v2 archive may still contain zero mature outcomes. In that state Phase 5D must return insufficient evidence without inventing evaluation results. As real 5T outcomes mature and Phase 5C creates successive 5T versions, 5D will finalize 5T evaluation epochs automatically; 20T/40T/60T follow only when their own evidence matures.
