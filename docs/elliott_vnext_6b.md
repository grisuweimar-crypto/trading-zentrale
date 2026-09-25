# Module 6B – Elliott vNext scenario generator and rule checker

Status: research-only implementation candidate.

## Purpose

6B turns the causally confirmed 6A pivot stream into **multiple structural Elliott hypotheses**. It does not try to prove a unique count and it does not use future performance to decide which count looks best.

The field `primary_scenario` is required only for deterministic presentation. The selection policy is explicitly:

`deterministic_structural_recency_not_probability_or_truth`

Every scenario keeps a deterministic `scenario_id`, the exact pivots used, its own `available_from`, rule violations and structural status.

## Point-in-time boundary

Only pivots whose `confirmed_time <= as_of` may participate in a scenario. `available_from` must equal the 6A `confirmed_time`; mismatching provenance fails closed.

The scenario itself becomes available only at the latest `confirmed_time` of all pivots used by that scenario.

A historical run with an `as_of` cutoff must therefore reproduce the same scenario IDs as physically truncating the confirmed-pivot stream to the same point in time.

## Alternation and ambiguous bars

6A can flag one bar as both a local high and local low when daily OHLC does not reveal intrabar ordering. 6B treats such a timestamp as a **sequence boundary** and excludes it from automatic wave ordering. It never invents whether the high or low happened first.

Consecutive confirmed pivots of the same kind are collapsed causally to the more extreme pivot. A later replacement can only affect the scenario state after that later pivot's own confirmation date.

## Motive scenarios

6B supports partial and complete motive skeletons:

- origin → W1 → W2,
- through W3,
- through W4,
- through W5.

The classic impulse hard rules from the frozen foundation are checked only once the necessary endpoints exist:

1. W2 must not cross the W1 origin;
2. after W5 exists, W3 must not be the shortest of W1/W3/W5;
3. after W4 exists, W4 must not overlap W1 price territory.

A separate structural requirement checks that W3 actually extends beyond the W1 endpoint. This is reported separately from the frozen three hard-rule IDs.

### Truncated fifth

W5 is **not** required to make a new extreme beyond W3. A structurally valid count can therefore carry `truncated_fifth=true` without invalidation.

### Diagonal exception

When a motive skeleton fails the classic impulse **only** because W4 overlaps W1, 6B may retain `leading_diagonal` and `ending_diagonal` as conservative alternatives. The classic impulse remains invalidated; the overlap exception is never silently applied to it.

6B deliberately does not pretend that the 6A pivot skeleton alone can fully validate the internal subwave structure of a diagonal. Those scenarios therefore carry `support_level=conservative`.

## Corrective scenarios

### Zigzag

A structural A-B-C zigzag candidate requires:

- alternating A/B/C movement,
- B not crossing the correction origin,
- C extending beyond A in the corrective direction.

No Fibonacci retracement threshold is used in 6B.

### Flat

An alternating A-B-C structure may also remain as a flat candidate, but it is marked `geometry_status=unresolved_until_6c`. This intentionally allows zigzag and flat to coexist when price geometry has not yet separated them.

### Triangle / WXY / WXYXZ

`triangle`, `double_three` and `triple_three` are represented only as **conservative uncertainty alternatives** in 6B. A sufficiently complex alternating pivot sequence can keep these structures alive, but 6B does not claim to have resolved their required internal subwaves.

## What 6B does not use

6B uses none of the following:

- Fibonacci retracement or extension levels;
- W3/W4/W5 target zones;
- current or historical Scanner Score;
- Timing, Probability, Risk or Confidence outputs;
- forward returns or historical expectancy;
- portfolio state;
- BUY/HOLD/REDUCE/SELL decisions or order instructions.

`structural_fit`, `confirmation_strength` and `historical_expectancy` remain `null` at this stage rather than being fabricated.

## Invalidated scenarios

Invalid candidates are retained separately under `invalidated_scenarios`. This is intentional research evidence: later 6G analysis must be able to measure how frequently candidate counts fail and which rule ended them without reconstructing those failures retrospectively.

## Acceptance tests

6B must prove at minimum:

1. bullish and bearish rule symmetry;
2. W2 origin-cross invalidation;
3. W3-shortest invalidation only once W5 exists;
4. W4/W1 overlap invalidates classic impulse but can preserve conservative diagonal alternatives;
5. truncated W5 stays allowed;
6. zigzag and flat can coexist before 6C geometry resolves them;
7. future-confirmed pivots are invisible historically;
8. scenario IDs are deterministic;
9. ambiguous intrabar high/low order is never invented;
10. triangle/WXY/WXYXZ remain uncertainty alternatives rather than asserted counts;
11. symbols, timeframes and wave degrees remain isolated;
12. no Fibonacci, performance or Scanner evidence enters 6B.

## Next step

Once 6B is green and reviewed, 6C may add Fibonacci geometry **after** structural scenario selection. Fibonacci may map zones for a scenario but may never choose the underlying Elliott count.
