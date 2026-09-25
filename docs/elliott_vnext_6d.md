# Module 6D – Elliott vNext swing routing and position-management research

Status: research-only implementation candidate.

## Purpose

6D converts the structural/geometric state produced by 6B/6C into **review contexts**. It does not make a trade decision.

Pipeline:

`6A causal pivots → 6B structural scenarios → 6C Fibonacci geometry → 6D review routing`

The final decision remains outside Module 6 and belongs to the later global Decision Layer / Depot Watch.

## Allowed outputs

6D may emit only these review contexts:

- `entry_or_add_review`
- `hold_review`
- `partial_reduce_review`
- `reentry_or_add_review`
- `profit_protection_review`
- `larger_reduce_or_exit_review`

These strings describe **what should be reviewed**, not what should be executed.

Every route carries:

- `trigger`
- `review_context`
- `reason`
- `scenario_id`
- `scenario_role`
- `available_from`
- evidence/provenance
- `requires_external_confirmation=true`
- transaction-cost status where relevant
- `historical_net_benefit_evaluated=false`
- `actionability=review_only_not_trade_instruction`

No BUY/HOLD/REDUCE/SELL decision or order instruction is emitted.

## Frozen research routing policy

### W2

- `EW_PREWATCH_382` → `hold_review`
- `EW_DEEP_SCAN_500` → `entry_or_add_review`
- `EW_W2_CORE` → `entry_or_add_review`
- `EW_W2_DEEP` → `entry_or_add_review`
- `EW_W2_DANGER` → `hold_review`
- `EW_INVALIDATED` → `larger_reduce_or_exit_review`

Important:

- 88.7% remains a danger zone, not a hard invalidation.
- Hard invalidation remains crossing the W1 origin.
- A confirmed W2 route uses the **confirmed W2 endpoint price**, not the later/current market price.
- The separate pre-confirmation W2 monitor may use the as-of/current price, but its routing event is timestamped at the observation date rather than backdated to the date the Fibonacci map was created.

### W3

- active W3 projection frontier → `EW_W3_TARGET_APPROACH` → `partial_reduce_review`
- causally confirmed W3 endpoint → `EW_W3_EXHAUSTION` → `partial_reduce_review`

A W3 Fibonacci zone alone does not prove exhaustion. `EW_W3_EXHAUSTION` is reserved for a causally confirmed structural W3 endpoint.

### W4

- active W4 retracement frontier → `EW_W4_TARGET_ZONE` → `reentry_or_add_review`
- causally confirmed W4 endpoint → `EW_W4_COMPLETION` → `reentry_or_add_review`

### W5

- active W5 research projection frontier → `EW_W5_TARGET_APPROACH` → `profit_protection_review`
- causally confirmed W5 endpoint → `EW_W5_COMPLETION_RISK` → `larger_reduce_or_exit_review`

A truncated fifth remains valid input. The route records the truncated-fifth flag but does not convert it into an autonomous action.

## Projection-frontier policy

6C can contain several candidate zones for the same future wave. A route is active when:

1. at least one relevant zone is `approaching` or `inside`, or
2. all currently mapped candidate zones have been `reached`.

A single early candidate being `reached` while later candidates remain merely `projected` does **not** create a permanently active target trigger. This prevents a 100% W3 extension, for example, from causing a partial-reduce review forever after price has moved on toward higher extension candidates.

For current-price zone states, the route's `available_from` is the scenario set's `as_of` date. The older projection-creation date is retained only as evidence. This avoids backdating a trigger to a date when price had not yet approached the zone.

## Scenario conflicts

6D must never use routing to choose a preferred Elliott count.

If:

- the primary scenario implies one review context and an alternative scenario another, or
- the same scenario simultaneously produces multiple review contexts,

all routes remain visible. The output marks the conflict and leaves it unresolved for the later Decision Layer.

This is deliberate. Example: a freshly confirmed W4 can justify a `reentry_or_add_review`, while the current price may already be near a W5 projection and simultaneously justify `profit_protection_review`. 6D records both rather than pretending one is automatically correct.

## Transaction-cost gate

Swing research can look attractive before execution costs. Therefore cost-sensitive review contexts carry a mandatory cost gate.

Optional `ExecutionCostSpec` inputs:

- full quoted spread in basis points,
- commission per side in basis points,
- slippage per side in basis points.

Research round-trip estimate:

`spread_bps + 2 × (commission_bps_per_side + slippage_bps_per_side)`

If the cost model is missing:

- the review route remains visible for research,
- `cost_model_status=required_not_supplied`,
- a warning is emitted,
- no net-benefit claim is allowed.

If the cost model is supplied, the estimated cost is attached, but this still does **not** establish that the routing has positive expectancy. That is a 6G validation question.

## Explicitly outside 6D

- no historical forward-return calculation;
- no claim that partial reduction beats holding;
- no claim that sell/rebuy beats buy-and-hold;
- no optimization of trigger levels;
- no Scanner/Timing/Probability/Confidence combination;
- no market/sector context;
- no portfolio-specific final action;
- no order instruction.

## Acceptance criteria

6D is accepted only if tests prove that:

1. only the six allowed review contexts are emitted;
2. 88.7% W2 is not treated as hard invalidation;
3. confirmed W2 depth uses the W2 endpoint, not a future price;
4. current-price projection triggers are not backdated;
5. early passed projection levels do not remain permanently active;
6. W3/W4/W5 structural completions route independently from Fibonacci target touch;
7. scenario conflicts are preserved rather than resolved;
8. transaction costs are explicit for cost-sensitive review contexts;
9. no historical net benefit is claimed in 6D;
10. no trade decision or order instruction can survive the 6D output boundary;
11. output is deterministic.

## Next step

6E will compare these Elliott-stage events and review routes with the already frozen Scanner/Timing/Probability/Confidence states. It may measure confirmation, rescue, conflict and lead/lag, but must not retune the existing scanner phases.
