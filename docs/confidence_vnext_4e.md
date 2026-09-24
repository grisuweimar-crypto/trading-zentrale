# Phase 4E — prospective Confidence-vNext shadow validation

## Purpose

Phase 4E does **not** create a new production Confidence score. It creates the prospective evidence stream required to answer the only question that matters before any mapping is allowed:

> Do the pre-specified Phase-4 evidence/agreement states actually make later model claims more reliable?

The Phase-2/3 validation period is already spent. It is therefore not reused to select Phase-4 weights, thresholds or a scalar mapping. Phase 4E starts a new, unspent, prospective stream.

## Freeze rule

Every complete scanner publication creates an immutable claim snapshot. A claim is keyed by `snapshot_id + symbol + horizon_sessions`. The claim stores the contemporaneous Phase-4 states plus provenance/fingerprints of the exact Phase-2, Phase-3 and Phase-4 research artifacts used.

A later rerun may be idempotent, but it may not reinterpret the same historical snapshot. If the same natural key produces a different claim payload, the recorder fails closed.

The contemporaneously available outcome start **session** and adjusted close are frozen into the claim itself. They are taken only from the price history that existed at claim time. A later close or a later backfill may not replace that frozen claim evidence.

If no valid claim-time start session/adjusted close exists, the claim is still archived but marked `outcome_eligibility=unevaluable` with a fixed `outcome_unavailable_reason`. Its start session/price remain empty permanently. That missingness is prospective Data-Quality evidence; it must not abort the whole snapshot and must never be repaired retrospectively.

The claims archive intentionally does **not** contain the raw numerical scanner Score as a Confidence-strength field. Selection is represented only by the already defined B0–B5 backbone state/evidence.

## Claim fields

The compact claim archive stores, among other provenance fields:

- `as_of`, `generated_at`, `run_id`, `snapshot_id`
- `symbol`, original `currency`, `horizon_sessions`
- frozen claim-time `start_market_date`, `start_adjusted_close`
- `outcome_eligibility` and `outcome_unavailable_reason`
- `evidence_version`
- exact SHA-256 fingerprints for Phase-4 report, Phase-2 report, Phase-3 report and optional risk-scale audit
- Selection band/state/direction
- Timing state/direction/matched frozen patterns
- downside Risk state
- Model Agreement state/conflicts
- pre-specified return-claim direction when one unambiguous mature return direction exists
- claim-specific Data Quality proxy states
- current volatility applicability state

Crypto remains outside this stream until non-stock evidence is independently validated.

## Outcomes are separate and later

Claims are never overwritten with future information. Matured **raw** outcomes are written to a separate append-only archive keyed by `claim_id`.

Only claims marked `outcome_eligibility=eligible` may mature. Claims that were unevaluable at claim time remain unevaluable even if a suitable historical price becomes available later. This distinction is intentional: later data can mature an already eligible claim, but it cannot retroactively create the missing entry condition of an unevaluable claim.

For each horizon (5/20/40/60 sessions), the immutable raw outcome archive stores after maturity:

- official adjusted-close forward return using the validated daily session arithmetic
- future adverse excursion
- future path maximum drawdown
- frozen start market session plus the evaluation-basis adjusted closes used for start/end

The claim-time adjusted close is audit evidence, not a value that is spliced into a later adjusted-price series. Yahoo may revise the historical adjustment factor after dividends or splits. Mixing an old frozen adjusted close with a later endpoint would put the two values on different bases and can create a false return. Therefore Phase 4E refreshes a temporary adjusted-price history for all current/open eligible symbols in one pass and uses that **single evaluation-time adjustment basis across the entire frozen-start-session-to-target path**. The immutable claim-time start session is still the anchor; only the numerical adjusted-price basis used for the later outcome is rebased consistently. The claim itself is never rewritten.

A corporate action after the target session may rescale both historical endpoints, but because the full path is refreshed together the start-to-target return remains basis-consistent rather than mixing revisions from different fetch dates.

Peer-relative labels are deliberately **not** frozen when one individual symbol first matures. Different exchanges and holidays can make symbols from the same observation cohort mature on different workflow runs. Freezing a peer median too early would make the result depend on workflow timing.

Instead, the analysis view derives from **all currently matured rows in the same observation cohort**:

- leave-one-symbol-out same-currency peer median, with global peer fallback when no same-currency peer exists
- peer excess
- sign-normalized peer excess and directional hit only when the frozen claim had one unambiguous return direction

When another symbol from that historical cohort matures later, the derived peer view may become more complete, but the original raw outcome rows and the frozen claims are never rewritten.

The price outcome is a daily research target, not an execution-price/PnL simulation.

## Pre-specified reliability questions

### Return reliability

For claims with an unambiguous return direction, test whether `compatible` multi-model states show better sign-normalized peer-excess reliability than `single_model` states.

Metrics include directional hit rate, mean/median sign-normalized peer excess and dispersion. No threshold is tuned from the same prospective stream.

### Risk reliability

For positive return claims, test whether pre-specified Risk tension is followed by larger adverse excursion / path max drawdown than comparable positive claims without elevated validated downside risk.

Low risk is never treated as an extra positive-return vote.

### Data Quality

Test whether complete claim-specific provenance/presence states are associated with lower prediction error / fewer unevaluable claims than partial or insufficient states. Missing evidence remains unknown/insufficient, never neutral.

Claim-time price availability is part of this evidence: a missing valid start session is recorded as a permanent unevaluable reason rather than silently repaired later.

## Dependence and uncertainty

The pre-specified event spacing remains the existing **5-session cooldown**. That reduces serial repetition but does not make longer-horizon outcomes independent.

Overlapping outcomes are not independent. Final inference must reuse the corrected Phase-2/3 method:

- circular moving observation-date blocks
- effective block length = `2 × horizon`
- all rows from one observation date stay together
- fixed state/group membership before resampling
- at least two time-separated support regions required
- otherwise robust uncertainty is `None`

IID intervals may be shown only as diagnostics and may not drive a validation decision.

## What Phase 4E cannot do yet

At the moment the stream starts, there are no unspent 5T/20T/40T/60T outcomes. Therefore Phase 4E must initially report `collecting_prospective_evidence`.

It is explicitly forbidden to:

- backfill Phase-4 states into older scanner history using today's evidence
- backfill a missing claim-time start price later
- reuse the spent Phase-2/3 holdout to select a Confidence mapping
- invent a 0–100 score before prospective reliability is demonstrated
- invent HIGH/MED/LOW thresholds
- convert the 2026-09-17 volatility scale break with a guessed factor

## Publication model

Phase 4E listens to every completed `Scanner_vNext Autopilot` workflow on `main`, not only successful ones. A failed/cancelled/no-publication completion cannot itself create a claim, but it is still allowed to execute the backlog-binding step so an already published unclaimed scanner snapshot cannot become stranded merely because GitHub replaced a pending successful trigger. Non-`main` scanner completions use a separate workflow concurrency group and cannot displace the `main` drain queue.

For every non-PR invocation Phase 4E scans the **first-parent history of `main`** and selects the oldest complete, unclaimed, proven `Scanner_vNext Autopilot` publication after the Phase-4E freeze. A commit is not accepted merely because it touched `history_metadata.json`. It must carry the exact Autopilot publication commit subject, change the required publication artifacts, contain a `daily_run.run_id` in the expected `github-<workflow_run_id>-<attempt>` form, have a `latest_scanner.csv` whose SHA-256 matches metadata, and introduce a run ID different from the parent metadata. Maintenance commits that retain an older `daily_run`, and scanner commits that only became reachable through a later feature-branch merge, therefore fail closed as candidates.

A Scanner workflow that did not actually publish a new snapshot is consequently a clean no-op when no proven backlog exists.

This catch-up rule is intentional. GitHub Actions concurrency may discard an intermediate pending workflow when another run of the same concurrency group is queued. Phase 4E therefore does not rely on one workflow event equalling one preserved snapshot. Instead, each executed Phase-4E run drains the oldest unclaimed proven scanner publication; after a successful shadow publish it checks again and self-dispatches another catch-up run while unclaimed publications remain. Intermediate scanner publications are therefore recovered from the first-parent `main` history instead of being permanently lost because of runner timing.

For the selected publication, Phase 4E reads the exact research bundle from that publication commit, generates the guarded Phase-4B–D registry, appends immutable shadow claims, and evaluates prior eligible claims only when the target session is mature. Outcome pricing is refreshed separately into a temporary one-basis adjusted-price file; that temporary provider view is not published back into the immutable claim evidence.

Open eligible claims keep receiving a fresh evaluation history even if their symbol subsequently leaves the current scanner universe. Permanently unevaluable claims are excluded from those evaluation requests. Missing provider mappings or unavailable prices leave outcomes immature; Phase 4E does not invent a mapping or an adjustment factor.

The append-only Phase-4E archives are persisted on the dedicated branch `phase4e-shadow-data`. Candidate discovery and scanner evidence continue to come exclusively from proven first-parent publications on `main`, but claimed run IDs and the latest shadow archives are restored from this dedicated data branch. On the first run the branch may be absent; it is then created from the current `main` state solely as a seed for the shadow archives.

Phase 4E never pushes its shadow artifacts to `main` and never joins the productive scanner's `scanner-daily-publication` concurrency group. Shadow publication is serialized only within its own `phase4e-shadow-publication` group and pushes exclusively to `phase4e-shadow-data`. Therefore a Phase-4E publication cannot occupy the scanner's single pending concurrency slot and cannot make a later productive scanner push non-fast-forward. The shadow workflow does not trigger from its own data-branch commits.
