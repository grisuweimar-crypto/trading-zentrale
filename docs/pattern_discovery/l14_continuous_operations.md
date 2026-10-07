# Pattern Discovery Lab v2 — L14 Continuous Lab Operations

L14 turns L0–L13 into a permanent, deterministic research operating loop.

It is an **operations control plane**, not a new statistical layer. Existing L1–L13 modules remain authoritative for their own semantics and persistence.

## Operating principle

The daily L14 cycle may inspect, schedule and audit work, but it may not invent:

- a new statistical threshold after looking at results
- an unplanned QM-C4 confirmation look
- a new L10 rating rule
- a second negative-result registry
- an automatic L12 promotion
- productive Decision behavior

Errors are persisted as a fail-closed cycle state.

## Discovery Trigger

Regular Discovery is **not calendar scheduled**.

The initial versioned research parameters are:

- at least **20 new trading days**
- at least **20% additional usable PIT observations**
- at least **2 additional temporal support regions**
- support-region unit: **10 trading sessions**
- previous regular run processed, or unprocessed frozen candidates at or below **25% of its frozen candidate budget**

All conditions are required.

These are L14-v1 operational research parameters. They are not claimed to be empirically optimal. Changing them requires a new versioned operations contract.

An eligible trigger does not silently start search. It opens:

`DUE_PREREGISTRATION`

A new immutable L1 preregistration remains mandatory before search.

## Extraordinary Discovery

An extraordinary round requires an explicit request with:

- allowed reason code
- evidence SHA-256
- request timestamp
- new L1 preregistration

Allowed v1 reasons are limited to the Masterplan classes:

- new PIT-safe feature
- new provider / data family
- material universe change
- material market-regime change
- new target methodology

L14 does not infer an extraordinary trigger merely because a market move looks interesting.

## Run classification

Every Discovery manifest entering continuous operations is classified append-only as:

- `INITIAL`
- `REGULAR`
- `EXTRAORDINARY`

Only INITIAL / REGULAR runs establish the baseline for the next regular Discovery trigger. This prevents an extraordinary run from resetting the regular-information clock.

An unclassified Discovery manifest blocks the operational cycle until classified.

## Candidate budget

L1 remains the authority for the frozen candidate budget.

L14 defines an unprocessed frozen candidate as:

> an L5 frozen Pattern version with no L10 rating history yet.

L14 cannot enlarge a candidate budget.

## Daily scheduler order

Every cycle evaluates the same fixed stage order:

1. Discovery Trigger
2. Prospective Capture
3. Outcome Maturation
4. Sequential Confirmation
5. Rating Update
6. Dependency Refresh
7. Negative Result Audit
8. Pattern Decay Audit
9. QM Regression

This is a work scheduler. Scientific execution remains inside the existing L1–L13 implementations and their own guards.

## Prospective Capture

L7 becomes due when:

- at least one frozen Pattern exists
- the current Scanner snapshot is valid
- the snapshot has not already been captured

Same-snapshot replay remains idempotent in L7.

## Outcome Maturation

L8 is checked whenever captured claim count exceeds matured outcome count.

L8 itself remains responsible for:

- exact market-session horizons
- adjusted prices
- original currency
- missing-price handling
- immutable maturation records

L14 does not estimate maturity.

## Sequential Confirmation

A changed L8 maturation-registry head schedules an L9 **QM-C4 readiness check**.

L14 cannot create a look.

The existing L9 / QM-C4 chain decides whether the next predeclared look is actually due. If it is not due, it remains unresolved and unconsumed.

## Rating Update

A changed L9 confirmation-registry head schedules L10.

L14 does not recompute L9 statistics and cannot change the rating mapping.

## Dependency Refresh

L6 is refreshed only when the exact frozen Pattern set is not represented by a current dependency graph.

This preserves the Masterplan rule that expensive dependency analysis is not a daily unconditional task.

## Negative Result Registry

QM-C5 remains the sole authority.

L14 only calls its integrity audit and records the result. It creates no Pattern-specific competing negative-result database.

## Pattern Decay alerts

L14-v1 does not invent a new change-point statistic.

Decay alerts use already governed evidence:

- audited L10 downgrades
- new L9 FALSIFIED / NEGATIVE_NOT_CONFIRMED results against an established A/B/C Pattern
- new INCONCLUSIVE evidence against an established A/B/C Pattern

Alerts are:

- WATCH
- WARNING
- CRITICAL

An alert never changes a rating. L10 remains authoritative.

More advanced change-point detection remains a later extension as stated in the Masterplan.

## Continuous QM

Every L14 cycle binds to:

- L0 QM-C authority checks
- QM-C5 integrity
- existing BA-QM12 Continuous QM

A BA-QM12 failure makes the L14 cycle:

`BLOCKED_FAIL_CLOSED`

No scientific due stage is released while the cycle is blocked.

## Persistence

Each cycle is immutable and hash-bound:

- `artifacts/research/pattern_discovery/operations/cycles/{cycle_hash}.json`
- append-only hash chain:
  `artifacts/research/pattern_discovery/operations/operations_registry.jsonl`
- convenience pointer:
  `artifacts/research/pattern_discovery/operations/latest.json`

The latest pointer is not scientific evidence. The immutable cycle artifact and registry are the audit sources.

## GitHub scheduler

The L14 workflow performs a daily scheduled operations check.

The scheduler may run every day because the **check** is cheap and idempotent. Discovery itself remains data-triggered. This follows the Masterplan distinction:

- Discovery: event / data triggered
- Prospective matching: may run daily
- Outcome maturation: may run daily
- formal rating / confirmation: only when governed evidence permits
- dependency analysis: only on relevant change

## Productive boundary

L14 never directly changes:

- Scanner Score
- productive Timing
- Decision output
- position handling
- execution
- L12 admission
- L13 regular integration

L14 also cannot automatically promote any Pattern.

## Definition of Done

L14 is complete when:

- Discovery is released only through the versioned trigger
- extraordinary Discovery is explicitly evidenced
- candidate backlog is budget-controlled
- L7/L8 checks can be scheduled daily without duplicate scientific events
- L9 remains controlled by QM-C4
- L10 refresh follows new L9 evidence
- L6 refresh follows relevant Pattern-set change
- QM-C5 remains the only negative-result registry
- Pattern decay creates alerts but no hidden rating changes
- BA-QM12 is bound as permanent QM
- every cycle and run classification is reproducible and hash-chained
- failures block the cycle rather than being interpreted as neutral
