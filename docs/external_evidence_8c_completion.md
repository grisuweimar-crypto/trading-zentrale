# Phase 8C Completion Freeze

Status: **COMPLETE_FROZEN**

Date: 2026-09-26

## Scope completed

Phase 8C established and real-data-tested the SEC-based external-evidence foundation for fundamentals and structured corporate events while keeping market outcomes, production promotion and Phase-7 integration disabled.

## Fundamentals

The authoritative direct-SEC bulk path produced 137 SEC-verified companies with CompanyFacts. All 137 passed the real PIT/research-readiness gates.

- manifest companies: 212
- direct SEC verified: 137
- research-ready: 137
- PIT feature observations: 42,825
- latest feature rows: 932

This validates the transport/provenance/history/accession/FIRST_RELEASE feature pipeline. It does **not** establish predictive value or a bullish/bearish direction.

## Filing-content acquisition and anchors

The real SEC filing-content run completed without fetch or parser errors.

- selected filings: 3,200
- fetched documents: 3,200
- fetch errors: 0
- parser errors: 0
- anchors: 22,789
- documents with anchors: 2,700

Anchor families:

- GUIDANCE: 10,556
- CAPITAL_RAISE: 7,320
- BUYBACK: 3,334
- DIVIDEND: 1,579

All anchors remained `ANCHOR_ONLY`, market direction remained unknown, and no market outcomes were read.

## Semantic challenger

The frozen high-precision challenger was run on 4,913 eligible BUYBACK/DIVIDEND anchors.

- candidates: 379
- rejections: 4,534
- BUYBACK candidates: 127
- DIVIDEND candidates: 252

GUIDANCE and CAPITAL_RAISE semantics remained disabled. Earnings beat/miss remained blocked by the paused 8B historical-consensus dependency.

## Preregistered real 8C-I validation

Candidate audit:

- 100 BUYBACK candidates
- 100 DIVIDEND candidates
- market outcomes hidden
- independent family-level gates

Independent anchor audit:

- 200 BUYBACK anchors
- 200 DIVIDEND anchors
- recall descriptive only; no promotion effect

### BUYBACK

- true target candidates: 89 / 100
- precision: 0.89
- Wilson 95% lower bound: 0.813687
- critical-field accuracy on true target events: 1.00
- Wilson 95% lower bound for critical fields: 0.985817
- distinct labeled issuers: 17
- uncertain rate: 0
- promotion status: **REMAIN_CHALLENGER**

Failed gate: precision Wilson lower bound >= 0.90.

The independent anchor audit identified 18 target events among 148 labeled anchors, of which 6 were emitted and 12 were false negatives. Descriptive recall was 0.333333. This recall result is descriptive only and has no promotion effect in the first validation.

### DIVIDEND

- true target candidates: 100 / 100
- precision: 1.00
- Wilson 95% lower bound: 0.963007
- critical-field accuracy: 0.8975
- Wilson 95% lower bound for critical fields: 0.863897
- distinct labeled issuers: 16
- uncertain rate: 0
- promotion status: **REMAIN_CHALLENGER**

Failed gate: critical-field-accuracy Wilson lower bound >= 0.90.

The independent anchor audit identified 26 target events among 111 labeled anchors, of which 24 were emitted and 2 were false negatives. Descriptive recall was 0.923077. This recall result is descriptive only and has no promotion effect in the first validation.

## Final 8C family status

- Fundamentals: **REAL_PIT_VALIDATED**
- BUYBACK semantic evidence: **REMAIN_CHALLENGER**
- DIVIDEND semantic evidence: **REMAIN_CHALLENGER**
- GUIDANCE semantic evidence: **DISABLED_NOT_PROMOTED**
- CAPITAL_RAISE semantic evidence: **DISABLED_NOT_PROMOTED**
- Earnings beat/miss: **BLOCKED_BY_8B_CONSENSUS_DEPENDENCY**

No semantic family is promoted into production or Phase 7 by this completion freeze.

## Completion decision

Phase 8C is complete. A phase does not require every challenger family to pass promotion; it requires the real-data evaluation to be completed and every family to receive an explicit terminal status. That condition is now satisfied.

Next phase: **8D – Positioning / Crowding**.
