# Phase 7A — Decision Layer Input & Coverage Contract

## Purpose

Phase 7A is the admission boundary for Phase 7 (Interpretation / Decision Layer).
It defines which evidence families may enter later evidence fusion and how their
point-in-time validity, maturity, coverage and integration status must be carried.

Phase 7A is deliberately **not** a decision engine. It does not compute a
Universal Stance, Portfolio Action, hysteresis state, target weight, position
size or broker/order instruction.

The machine-readable contract is:

`configs/decision_layer_input_contract_v1.json`

The executable validator is:

`src/scanner/research/decision_layer/input_contract.py`

## Why a typed boundary is required

The completed upstream phases answer different questions and must not be collapsed
into one undifferentiated score:

- Selection describes cross-sectional quality / the scanner backbone.
- Timing describes point-in-time changes and frozen timing claims.
- Probability calibrates an existing Selection/Timing claim and is not a second vote.
- Risk primarily describes downside/protection behaviour and is not a direction vote.
- Confidence describes how much a model claim should be trusted, not attractiveness.
- Elliott vNext provides structural scenarios and review context and is not an autonomous action engine.

The Decision Layer therefore receives **typed claims**, not six interchangeable
numbers.

## Packet identity

Every packet is keyed by:

- `symbol`
- `as_of`
- `source_snapshot_id`
- `schema_version = decision_layer_input_contract_v1`

Every evidence row carries:

- evidence `family`
- stable `claim_id`
- evidence `as_of`
- `available_from`
- `source_version`
- `coverage_state`
- `maturity_state`
- `pit_state`
- `integration_mode`
- family-specific `payload`

`available_from` and evidence `as_of` must never lie after the packet `as_of`.
Future information fails closed.

## Evidence families

### Selection

Selection is a possible directional backbone for later interpretation, but the
scanner Score and R-code remain **inputs**, not buy/sell instructions.

The contract permits current-state fields such as Score, score percentile,
quality band, exact R-code where genuinely available, score status, TrendOK and
LiquidityOK.

Historical R4/R5 must not be invented. A B4/B5 score-percentile backbone must not
be silently relabelled as an exact historical R4/R5 state.

### Timing

Timing is a directional claim only when it comes from a frozen pattern definition
and the current match is computed from point-in-time features.

Required payload fields include:

- `pattern_id`
- `horizon_sessions`
- `pattern_frozen = true`
- `match_from_pit_features = true`

Discovery-only strength does not establish mature evidence.

### Probability

Probability is structurally an **attachment** to an existing Selection or Timing
claim. It must carry `claim_ref` and the matching horizon.

It has no independent directional authority and cannot contain a `direction`,
`stance` or `vote` field. This prevents later evidence fusion from double-counting
a Timing claim once as Timing and again as Probability.

`not_yet_mature` remains distinct from failed evidence. iid diagnostics alone do
not establish robust validation.

### Risk

Risk is protection/path evidence, not a directional vote. Phase 3 currently gives
empirically useful short-horizon protection evidence for volatility and stored
drawdown; this does not make either factor an alpha signal.

Risk may therefore qualify or constrain a later thesis, but Phase 7A forbids it
from carrying its own stance/vote that could mechanically invert direction.

### Confidence

Confidence attaches to a model claim and describes reliability. It must carry
`claim_ref` and may not encode attractiveness, stance, direction or vote.

Legacy production Confidence remains version-sensitive and cannot be pooled across
formula epochs as if it were one invariant reliability metric. Phase-5 adaptive
Confidence remains Shadow/Research-only until its separate promotion gates are met.

### Elliott vNext

Only the final 6H module identity is admissible:

- `schema_version = elliott_vnext_output_v2`
- `module = 6H_module_output`
- `research_only = true`
- integration contract `elliott_vnext_integration_contract_v1`
- `decision_layer_required = true`
- `productive_integration_enabled = false`
- `direct_ordering_allowed = false`
- `routing_is_trade_decision = false`

The still-uncalibrated `structural_fit` and `confirmation_strength` fields must
remain `null`. Review routing remains context, not an action.

## Coverage and admission states

Phase 7A produces only a coverage/admission summary:

- `admissible` — contract-valid directional evidence exists and every family is represented without an explicit coverage/maturity gap.
- `admissible_with_gaps` — usable directional evidence exists, but one or more families are missing, insufficient, unavailable, invalid or not yet mature.
- `insufficient_directional_evidence` — there is no PIT-usable Selection or Timing evidence.
- `rejected` — conceptual state for a hard contract violation; the executable validator fails closed instead of returning such a packet.

These states are **not** Universal Stance values. In particular,
`admissible_with_gaps` does not mean HOLD and missing evidence never becomes
neutral evidence.

## Explicit Phase-7A prohibitions

The validator recursively rejects decision/action fields such as:

- `universal_stance`
- `portfolio_action`
- `trade_decision`
- `order_instruction`
- `buy_signal` / `sell_signal`
- `position_size`
- `target_weight`

Portfolio state is intentionally absent. Position-aware logic starts only after
the depot-independent Universal Stance has been defined in later Phase 7.

## Relationship to upstream evidence

Phase 7A preserves these upstream semantics:

- Phase 1A/1B: Selection and Timing stay separate and Timing definitions stay frozen.
- Phase 2: probability/calibration attaches to a claim and maturity is horizon-specific.
- Phase 3: protection and alpha effects stay separate.
- Phase 4/5: Confidence is reliability and Shadow evidence is not silently promoted.
- Module 6H: Elliott is a research sensor requiring the global Decision Layer.

No upstream weights, thresholds or production scanner outputs are modified by 7A.

## CLI

Validate a prepared packet with:

```bash
python scripts/run_decision_input_7a.py --input packet.json
```

or persist the normalized packet plus coverage summary:

```bash
python scripts/run_decision_input_7a.py --input packet.json --output validated_packet.json
```

## Boundary to Phase 7B

Phase 7B may build the point-in-time Decision Research Dataset from these typed
packets. It must not weaken the 7A admission guards or flatten the families into
a super-score. Evidence fusion rules, conflicts, Universal Stance, hysteresis and
portfolio actions remain later work.
