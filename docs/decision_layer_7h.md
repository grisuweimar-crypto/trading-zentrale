# Phase 7H — Depot-Watch Integration

## Purpose

Phase 7H turns the already-computed Decision Layer into a compact portfolio watch. It does **not** create another decision model.

For every explicitly supplied portfolio position, 7H answers:

1. Is the position present in the authoritative current `daily_research.json` snapshot?
2. Is there a complete 7A→7G Decision-Layer bundle from exactly that snapshot?
3. Was the 7F action computed from exactly the supplied position snapshot?
4. If yes, what are the preserved 7D stance, 7E transition, 7F action and 7G structural reliability/explanation?
5. If not, why is the decision unavailable?

## Authoritative daily source

The only daily snapshot identity used by 7H is the validated productive research artifact:

`artifacts/research/daily_research.json`

Its `snapshot_id` and `as_of` are authoritative. 7H does not revive old freshness gates. In particular, `history_recent.csv` remains an underlying research source and is **not** used as a freshness gate.

A Decision bundle must match both the daily `snapshot_id` and `as_of`. Same date without the same snapshot identity is not sufficient.

## Inputs

### Position book

Real positions are explicit runtime input under:

`decision_depot_position_book_v1`

Each row reuses the already-frozen 7F `decision_position_snapshot_v1` contract.

The repository's model portfolio (`artifacts/portfolio/portfolio.csv`) is not an actual-position source. Legacy holdings are also not silently promoted into current positions. No fuzzy ticker matching is performed.

### Decision bundle set

Each position may have one `decision_chain_bundle_v1` containing the complete preserved chain:

- 7A `decision_layer_input_contract_v1`
- 7D `decision_universal_stance_v1`
- 7E `decision_state_transition_v1`
- 7F `decision_portfolio_action_v1`
- 7G `decision_reliability_explainability_v1`

7H validates every member and verifies that symbol, `as_of`, `source_snapshot_id`, stance, transition and action remain consistent. It does not rebuild them.

## Availability states

A portfolio row has exactly one state:

- `decision_available`
- `symbol_not_in_daily_research`
- `decision_bundle_missing`
- `decision_bundle_snapshot_mismatch`
- `decision_bundle_invalid`
- `position_context_mismatch`

A missing or invalid bundle does not cause scanner Score, R-code, historical matches, legacy Confidence or any other daily scalar to become a substitute action. The row remains unavailable.

One bad row does not hide valid rows for other holdings. The watch can therefore be `complete`, `partial`, or `unavailable`.

## Compact Decision-Layer view

For an available row, 7H exposes the preserved:

- Universal Stance and direction;
- 7E transition status, stable anchor and pending confirmation state;
- 7F Portfolio Action and reason code;
- swing-management and P/L context;
- 7G structural reliability state;
- coverage admission state;
- missing/limited evidence;
- decision-change triggers;
- information-completion triggers;
- 7G `explanation_id`.

The current Score, R-code, close, currency and related daily fields may be copied into `daily_scanner_context`, but are context only.

## Presentation groups are not rankings

Rows are grouped only by their **already-existing** 7F action:

- `review_now`: `ENTER_REVIEW`, `ADD_REVIEW`, `REDUCE_REVIEW`, `EXIT_REVIEW`
- `waiting_confirmation`: `WAIT_CONFIRMATION`
- `hold_or_no_action`: `HOLD`, `NO_ACTION`
- `unavailable`: no valid same-snapshot Decision chain

There is no ranking by action, structural reliability, Score, expected return or any composite measure.

## Privacy boundary

7H deliberately is **not** added to the public `Scanner_vNext Autopilot` artifact commit.

A real Depot-Watch necessarily contains private position information. Committing an automatically generated real-position artifact to this public repository would violate the intended data boundary. The CLI therefore requires an explicit private position-book path and writes nothing unless an output path is explicitly requested.

This is separate from technical freshness: the public `daily_research.json` remains the authoritative scanner snapshot, while actual positions are runtime/private data.

## CLI

```bash
python scripts/run_depot_watch_7h.py \
  --positions /private/decision_positions.json \
  --bundles /private/decision_bundles.json
```

To write a private output explicitly:

```bash
python scripts/run_depot_watch_7h.py \
  --positions /private/decision_positions.json \
  --bundles /private/decision_bundles.json \
  --output /private/depot_watch.json
```

The CLI first validates the repository's current `daily_research.json` using the existing production research validator.

## Hard guards

7H must never:

- infer a position from model or legacy portfolio files;
- fuzzy-match symbols;
- join a Decision bundle merely because it has the same date;
- reconstruct missing Decision evidence from daily scanner values;
- recompute Universal Stance;
- recompute hysteresis;
- modify the 7F action;
- resolve a conflict;
- create a numeric reliability score;
- rank holdings by action or reliability;
- compute position sizing or target weights;
- generate a buy/sell signal or broker order.

Every output receives a SHA-256 `watch_id`; post-build modification is detectable.

## Current status and Phase 7I boundary

7H is a research-only integration layer. `productive_integration_enabled=false`, `execution_allowed=false`, `integration_empirically_validated=false`, and `promotion_eligible=false` remain mandatory.

The Decision Layer is therefore now consumable by a Depot-Watch without pretending that the downstream action rules are prospectively validated. Phase 7I remains responsible for validation/promotion decisions and for deciding whether any part of this integration may later enter a productive automated path.
