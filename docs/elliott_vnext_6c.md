# Module 6C – Fibonacci geometry and prospective wave map

Status: research-only implementation candidate.

6C consumes the structural scenarios from 6B and attaches Fibonacci geometry **after** the count exists. It must never use Fibonacci levels to create, rank, repair or choose an Elliott count.

## Core one-way dependency

`6A causal pivots → 6B structural scenarios → 6C Fibonacci geometry`

The direction must never reverse.

6C therefore preserves the `primary_scenario` and `alternative_scenarios` ordering produced by 6B. `fibonacci_selects_wave_count=false` is emitted explicitly.

## W2 geometry

W2 uses the following research levels:

- 38.2% → `EW_PREWATCH_382`
- 50.0% → `EW_DEEP_SCAN_500`
- 61.8% → `EW_W2_CORE`
- 78.6% → `EW_W2_DEEP`
- 88.7% → `EW_W2_DANGER`

The 88.7% zone is **not** a hard invalidation. The hard invalidation remains a cross of the W1 origin (100% retracement / structural origin violation).

Because the current 6B implementation starts its full motive scenarios at confirmed W2, 6C also exposes `build_wave2_retracement_map(origin, wave_1, ...)` as a lower-level primitive for future prewatch use. The function does not search for or score anchors: upstream structure must supply the origin and W1 endpoint. This preserves the rule that Fibonacci does not select the count.

## W3 projections

When a 6B scenario is at `wave_2_complete`, 6C projects W3 research zones from the confirmed W2 endpoint using W1 length and the foundation candidate extensions:

- 100.0%
- 161.8%
- 200.0%
- 261.8%
- 323.6%

Each zone stores:

- `scenario_id`,
- wave role,
- projection type,
- low/high zone bounds,
- center price,
- complete basis metadata,
- `available_from` equal to the causal W2 confirmation,
- zone-width provenance,
- research-only status.

## W4 projections

When the scenario is at `wave_3_complete`, W4 zones are retracements of confirmed W3:

- 14.6%
- 23.6%
- 38.2%
- 50.0%

`available_from` equals the W3 confirmation time. No future W4 pivot is used to construct the projection.

## W5 research candidates

Foundation v2 deliberately states that numeric W5 levels are not frozen before empirical research. 6C therefore treats the current W5 ratios as explicit **research hypotheses**, never production constants.

The verified ElliottWaver.live Kompendium states that W5 may be equal to W1 or approximately 61.8% of the W1–W3 structure. 6C implements exactly these two foundation-compatible bases:

1. W1 length projected from confirmed W4 at 100%;
2. origin→W3 structure projected from confirmed W4 at 61.8%.

Both carry:

- `level_validated=false`,
- `numeric_level_frozen=false`,
- source marker `elliottwaver_live_kompendium_verified_2026-09-25`.

Module 6G must compare these hypotheses out-of-sample before any later production use.

## Zone width

6C does not emit exact Fibonacci lines as targets. Every numeric level becomes a zone.

The half-width is:

`max(ATR × degree-specific ATR multiplier, price × degree-specific price floor)`

Thus width is explicitly dependent on:

- volatility/ATR,
- price level,
- Elliott wave degree.

The current width parameters for `minor`, `intermediate`, `major` and `primary` are research candidates only and are all `validated=false`.

If the required anchor ATR is unavailable, the projection is suppressed with an explicit warning. 6C does not invent a fallback volatility estimate.

## Point-in-time behavior

6C never backdates a projection before the anchor required to construct it was confirmed:

- W3 map → W2 `confirmed_time`,
- W4 map → W3 `confirmed_time`,
- W5 map → W4 `confirmed_time`.

If a scenario is already at `wave_5_complete`, 6C deliberately does **not** reconstruct historical W3/W4/W5 projections from the completed future structure. The output warning is `cycle_complete_no_historical_projection_backfill`.

Historical projection histories must later be created through walk-forward execution, not by retrospective reconstruction from a completed count.

## Current-price status

When an external current/as-of price is supplied, a zone may be marked:

- `projected`,
- `approaching`,
- `inside`,
- `reached`.

This is only geometric state. It is not a trade signal.

## Corrections

6C currently does not attach target projections to correction scenarios. Zigzag/flat/triangle/WXY/WXYXZ geometry remains unresolved until a dedicated correction-geometry extension is justified. The structural scenarios remain present; 6C simply returns no motive projection for them.

## Trading separation

6C emits no autonomous BUY/HOLD/REDUCE/SELL result and no order instruction.

`routing_triggers` remains empty in this phase. The conversion of geometry into review contexts belongs to 6D.

## Acceptance criteria

6C is accepted only if tests prove that:

1. 88.7% W2 is danger, not hard invalidation;
2. W1 origin remains the hard W2 invalidation;
3. W3/W4/W5 zones are scenario-specific;
4. projection `available_from` is causal;
5. W5 candidate levels remain unvalidated and unfrozen;
6. completed W5 structures cannot backfill earlier projections;
7. missing ATR suppresses projection instead of inventing width;
8. zone width depends on ATR, price and degree;
9. unknown degrees fail closed;
10. Fibonacci cannot reorder or select the 6B counts;
11. output contains no top-level trade/order instruction;
12. zones are ranges rather than exact target points;
13. bullish and bearish W2 geometry are symmetric.

## Next phase

6D may consume these geometry states and convert them into reproducible **review contexts** such as entry/add review, partial-reduce review, re-entry review or profit-protection review. 6D still may not issue an autonomous trade decision.
