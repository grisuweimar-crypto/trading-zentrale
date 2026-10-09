# Elliott vNext – prospective 6H shadow capture

## Purpose

This is an operational sidecar for the already frozen Module-6 research core. It does **not** add an Elliott rule, change a count rule, tune Fibonacci, select a productive action, or promote Elliott into the Decision Layer.

The purpose is to make the existing 6A→6H machinery run prospectively after a genuinely completed Scanner_vNext publication so that Module 6G and QM-G can accumulate real, point-in-time evidence.

## Frozen chain reused

For each successfully published scanner snapshot the capture binds to the exact publication commit and uses the persisted `artifacts/market_data/yahoo_ohlcv.csv` from that commit.

For every symbol in the validated `daily_research.json` universe it calls the existing causal replay for exactly the published `as_of` date:

1. 6A – `prepare_daily_ohlcv` / confirmed multi-degree pivots;
2. 6B – frozen scenario generation;
3. 6C – frozen Fibonacci geometry;
4. 6D – frozen review-only swing routing;
5. 6H – frozen module-output assembly and validation.

The replay remains prefix-only. Future rows are not used. The existing adjusted-price replay basis is retained. Missing price history or a missing Elliott state remains missing evidence.

No replacement implementation of 6A, 6B, 6C, 6D or 6H is introduced by this capture.

## Optional 6E / 6F / 6G evidence

The first capture stage does not manufacture cross-system, external market-context or validation evidence. The 6H builder therefore retains the existing missing-evidence warnings when these optional inputs are unavailable.

This is deliberate. In particular, an empty market-context registry must not be replaced by scanner peers or inferred proxies.

## Multi-degree preservation

Every genuinely emitted `(symbol, timeframe, degree)` 6H output is retained. The capture does not invent a reducer that selects one degree or timeframe for W6.

Duplicate `(symbol, timeframe, degree)` outputs inside one scanner snapshot fail closed.

## Prospective identity and availability

Each capture records:

- scanner `snapshot_id` and `as_of`;
- scanner `run_id`;
- exact scanner publication commit;
- scanner publication commit time;
- actual capture time;
- hashes of the persisted OHLCV source and `daily_research.json`;
- the existing 6G validation partition and rule-freeze boundary;
- every 6H `output_id`.

`capture_id` is deterministic for the same evidence identity. Re-running the same scanner publication is idempotent. A second record with the same scanner run/snapshot but a different `capture_id` is rejected instead of silently replacing evidence.

## Storage boundary

Prospective capture artifacts are published on a dedicated `elliott-vnext-shadow-data` branch, following the repository's existing isolated-shadow pattern. They are not added to the productive scanner publication on `main`.

The runtime artifacts are:

- `artifacts/research/elliott_vnext_prospective_current_6h.json` – latest full capture;
- `artifacts/research/elliott_vnext_prospective_history_6h.jsonl` – append-only full capture history.

All daily outputs are retained, including unchanged scenario IDs. This is required for QM-G Scenario Stability, which compares genuinely available consecutive 6H outputs and needs stable observations as well as changed ones.

## Activation boundary

The workflow starts only with scanner publications whose commit already contains the prospective-capture workflow. Earlier scanner history is not silently backfilled and cannot become prospective evidence retroactively.

A successful Scanner_vNext Autopilot completion triggers the shadow workflow. It validates actual scanner publication commits and processes the oldest not-yet-captured publication first. If a backlog exists, the workflow can continue the queue serially.

## Decision-layer boundary

Stage 1 emits **no** `decision_elliott_6h_source_v1` object.

Therefore:

- W10 remains `phase6_elliott = not_supplied`;
- Elliott does not change Universal Stance;
- Elliott does not change Portfolio Action;
- Elliott does not create broker instructions;
- productive integration remains disabled.

A later read-only Watch presentation is a separate Stage-2 change. Supplying an Elliott source to W6/W10 is a separate Stage-3 integration review and must not be inferred from the existence of prospective research data.

## Research use

The accumulated sequence is intended to support the already defined Module-6G prospective validation and QM-G challenger research, including Scenario Stability. Technical capture does not itself establish empirical usefulness and cannot promote Elliott into production.


## PIT source-coherence remediation (issue #242; 2026-10-09)

The source-binding workflow may select an older eligible scanner publication while
executing current, repaired Elliott code. Both parts must remain separate:
the code stays current, while **every scanner snapshot artifact consumed by the
capture or `validate_daily_research` is restored from exactly that source commit**.

The bound set now includes:
`history_metadata.json`, `latest_scanner.csv`, `daily_research.json`,
`history_recent.csv`, `price_backfill.csv`,
`scanner_input_provenance.json`, and the published Yahoo OHLCV file.
The research manifest's recorded SHA-256 hashes are checked independently
against all five declared research files, before running the existing complete
`validate_daily_research` PIT validator.

A missing file, modified historical source or mixed old/new snapshot stops the
capture. Before switching to the isolated Elliott shadow-data branch, tracked
scanner inputs are restored to current main. This does not rewrite historical
publication commits, does not fill missing prices, and does not loosen PIT or
research-only/no-production restrictions.

Prospective 6H capture is still **not** automatically proof of Elliott
predictive usefulness or eligibility for Decision integration. Independent
GitHub CI and live backlog processing must succeed before #242 may close.

## Lossless shadow-transport capacity repair (issue #254; 2026-10-09)

A genuine [2026-10-09 live 6H capture run](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37908194648)
successfully validated PIT-bound historical scanner inputs and produced a nonempty
frozen-core result, but `git push` rejected an uncompressed 125.43-MB JSONL
history file (GitHub hard per-blob limit 100 MB). Its uncompressed current capture
was also 52.42 MB, above GitHub's recommended 50 MB size.

The capture engine and append-only semantics still operate on the **original,
full JSON/JSONL byte streams** in the runner. Only the isolated shadow branch
persistently stores `elliott_vnext_prospective_history_6h.jsonl.gz` and
`elliott_vnext_prospective_current_6h.json.gz`. The transport uses deterministic
gzip (fixed timestamp, filename-independent header), verifies the compressed
payload by full decompression and SHA-256/byte-count roundtrip before writing to
the branch, and independently rejects compressed payloads at or above 80,000,000
bytes. A future larger compressed history must be losslessly sharded by a
separately reviewed change; **truncation is never permitted**.

The first successful migrated publish deletes the old uncompressed names only
from the *new shadow-branch tree*, not from Git's immutable historical commits.
Legacy uncompressed shadow branches remain readable; all later captures
decompress the previous full archive into the runner before the unchanged
`archive_capture` deduplication/append step. Missing or corrupted compressed
sources stop the capture. The oldest-unclaimed scanner publication is still
selected by its original run identity. Exact source-SHA binding, original
publication time, no future information, frozen Elliott 6A–6H, research-only
isolation and all six empirical promotion blocks are untouched.

Live effectiveness requires a successful **production** shadow push, archive
roundtrip, subsequent incremental capture/backlog run and independent CI. This
documentation alone does not assert those results.
