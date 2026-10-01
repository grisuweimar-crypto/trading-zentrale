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
