# Pattern Discovery Lab v2 — L11 Pattern Library / Research UI

L11 implements the Masterplan requirement to make frozen Patterns understandable and comparable without requiring users to read raw JSON.

## Scope

The L11 library is a **research-only presentation layer** over already governed artifacts:

- L5 — immutable Pattern identity/specification and Discovery Evidence
- L6 — Dependency / Redundancy Graph
- L9 — prospective Confirmation / Falsification looks
- L10 — lifecycle Rating History

L11 does not recompute any of those layers.

## What is shown per Pattern

The summary table presents:

- Pattern ID / Version
- human-readable description
- Pattern type
- expected direction
- target and horizon
- current D/C/B/A/U/F rating
- prospective Direction Probability
- prospective Baseline Probability
- prospective Probability Advantage / Lift
- prospective Effect Size
- raw N
- Effective-N / Support Regions
- robust 95% intervals
- Dependency severity
- latest rating change

Each Pattern additionally has a human-readable detail section with:

- **Discovery Evidence**
- **Prospective / Confirmed Evidence**
- Pattern/Lifecycle facts
- Dependency relations
- Confirmation History
- open blockers with source
- provenance / immutable hashes

Discovery and prospective evidence are deliberately rendered as separate cards.

## Missing data

Missing prospective evidence stays missing and is displayed as “—” / “Keine Evidenz vorhanden”.

L11 never substitutes Discovery Evidence for missing Confirmation Evidence. A Pattern without an L9 result receives the presentation-only blocker:

`NO_PROSPECTIVE_CONFIRMATION_RESULT / L11_DERIVED`

This does not change its L10 rating.

## Horizons

5T / 20T / 40T / 60T Pattern versions remain separate research objects. L11 does not aggregate different horizons into a combined metric.

## Dependency

L6 dependency information is descriptive context only. It is shown so that RELATED / DUPLICATE / NESTED and dependency severity remain visible.

Dependency never changes the L10 rating in L11 and is not interpreted as vote count.

## Rating

L11 reads the authoritative current rating and complete lifecycle state from L10.

It does not:

- derive its own rating,
- convert hit rate into rating,
- upgrade or downgrade Patterns,
- revive F Patterns,
- perform Promotion.

B/A remains insufficient for productive admission. Promotion belongs to L12.

## Artifacts

The renderer writes only inside the Pattern Discovery research namespace:

- `artifacts/research/pattern_discovery/library/pattern_library.json`
- `artifacts/research/pattern_discovery/library/pattern_library.html`

The JSON artifact is deterministic and hash-bound. The HTML artifact is a static, self-contained human-readable research page.

## Runner

`scripts/pattern_discovery/run_l11_pattern_library.py`

Inputs are explicit:

- one or more `--l5-snapshot`
- one or more `--rating-history`
- zero or more `--l9-report`
- optional `--dependency-graph`
- required `--generated-at`

Supplying complete source sets is intentional. L11 fails closed when an L6/L9/L10 Pattern identity is not present in the supplied L5 source set.

## Definition of Done

Masterplan L11 DoD is enforced as follows:

1. **No raw JSON required** — the generated HTML exposes the relevant evidence, history and provenance in tables/cards.
2. **Discovery vs Confirmed Evidence visibly separate** — independent sections, never merged.
3. **Uncertainty visible** — robust effect and lift intervals are present in both summary/detail views where available.
4. **No semantic leakage** — L11 has no Promotion, Decision Layer, Portfolio Action or Execution authority.
