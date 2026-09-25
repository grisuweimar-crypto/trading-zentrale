# Module 6H — Final module output and later integration boundary

## Purpose

6H is the final technical assembly layer of Elliott vNext Module 6.  It combines the already-causal outputs of 6A–6G into one stable research object suitable for display, archiving, comparison and a later explicit Decision-Layer integration review.

6H does **not** add another predictive model.  It does not choose a wave count, calibrate a new score, optimize a threshold, produce a broker order or decide buy/hold/reduce/sell.

The frozen foundation output schema remains `configs/elliott_vnext_output_schema_v2.json`.  The additional integration guardrails are frozen in `configs/elliott_vnext_integration_contract_v1.json`.

## Input chain

The intended chain is:

`6A pivots → 6B scenarios → 6C Fibonacci geometry → 6D review routing → 6E cross-system context → 6F external context → 6G validation → 6H module output`

The 6H builder consumes one already-routed 6D snapshot and can additionally consume:

- the matching full 6A pivot list,
- a 6G validation report,
- a 6F causal market-context snapshot,
- an explicitly PIT-stamped relative-strength object if one exists,
- an optional 6E cross-system summary.

Missing optional evidence stays missing.  No values are reconstructed from present-day knowledge merely to fill the output.

## Final output

`build_module_output()` produces a JSON-serializable object containing at least:

- symbol, `as_of`, timeframe and wave degree,
- primary and alternative scenarios,
- causally confirmed pivots,
- primary-scenario Fibonacci anchor geometry,
- scenario-specific projection zones,
- current wave stage and full wave-cycle map,
- hard invalidations and structural rule violations,
- `structural_fit`,
- `confirmation_strength`,
- historical expectancy from 6G where supplied,
- routing triggers and swing-review contexts,
- market-context evidence where supplied,
- relative-strength evidence where supplied,
- validation/integration status,
- warnings and deterministic `output_id`.

### Structural fit and confirmation strength

They remain `null` in 6H v1.

The foundation contract named these dimensions, but 6G did not freeze a calibrated 0–1 mapping for either field.  6H therefore refuses to invent one.  A later calibration can populate them only through a separately frozen empirical contract.

### Historical expectancy

Historical expectancy is not recomputed by 6H.  It is selected from the supplied 6G validation report by the current wave degree and primary structural stage.

For a motive sequence:

- W2 complete selects W3 projection evidence,
- W3 complete selects W4 projection evidence,
- W4 complete selects W5 projection evidence,
- route-review evidence must match the current primary wave stage and degree.

The 6G evidence freeze remains authoritative.  Legacy development observations cannot support promotion.  Formal evidence is restricted to the `prospective_unspent` partition and automatic promotion remains forbidden.

## Point-in-time guards

6H fails closed if it receives:

- a source pivot confirmed after output `as_of`,
- a projection whose `available_from` lies after output `as_of`,
- a swing route whose `available_from` lies after output `as_of`,
- market context from a future snapshot,
- relative-strength evidence from the future,
- a 6G freeze date later than output `as_of`,
- a validation report that permits automatic promotion,
- a context row marked real evidence despite a non-PIT assignment.

When the full 6A pivot stream is not supplied, 6H can fall back to the pivots embedded in the current scenarios, but it records `top_level_pivots_limited_to_scenario_embedded_subset` so that the reduced coverage is explicit.

## Recursive trading-field removal

Older internal 6B scenario records intentionally carried `trade_decision: null` as an internal placeholder.  The final module contract does not permit that field at all.

6H recursively removes:

- `trade_decision`,
- `order_instruction`.

`validate_module_output()` then scans the entire nested object and rejects the output if either key remains anywhere.  This is stronger than checking only the top level.

Review contexts such as `entry_or_add_review` or `profit_protection_review` remain allowed because they are explicitly review-only research context.  Every 6D route must keep `actionability = review_only_not_trade_instruction`.

## External market context

6H does not create sector or market assignments.  It only serializes a causal 6F snapshot supplied by the caller.

The following 6F rules remain unchanged:

- only `sufficient` / `limited` external evidence can be usable real-market context,
- missing context remains missing,
- Scanner peers are not substituted for external context,
- current classifications are not retrojected historically.

## Relative strength

6H does not manufacture a new live relative-strength metric.  If no separately computed PIT object is supplied, `relative_strength` remains `null` and a warning is emitted.

## Integration status

Every output states:

- `decision_layer_required = true`,
- `productive_integration_enabled = false`,
- `direct_ordering_allowed = false`,
- review contexts are not actions,
- Module 6 may be technically complete without being empirically promoted.

No productive Scanner score, R-code, watchlist, Depot-Watch decision or broker path is modified by 6H.

A future integration phase must separately decide how the global Decision Layer consumes this module, and must respect the 6G evidence status at that later time.

## CLI exporter

`scripts/run_elliott_output_6h.py` exports the final research object from explicit JSON inputs.  It is intentionally not scheduled and not connected to production.

Example shape:

```bash
python scripts/run_elliott_output_6h.py \
  --snapshot routed_snapshot.json \
  --pivots pivots.json \
  --validation validation.json \
  --market-context context.json \
  --output elliott_module_output.json
```

## Technical completion versus empirical promotion

When 6H is merged and its full regression suite is green, Module 6 is technically complete.

That does **not** mean Elliott vNext is empirically proven or production-promoted.  The 6G rules were frozen through 2026-09-25, so independent confirmation must accumulate prospectively after that date.  6H merely makes that status explicit and safely consumable.
