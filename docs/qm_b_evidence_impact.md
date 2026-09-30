# QM-B Evidence Impact on Prior Research

## Purpose

This package converts the completed QM-B integrity findings into an explicit impact decision for prior research. It does not rerun every historical model and it does not rewrite historical scanner rows. Instead it answers the governance question: do the validated QM-B findings require global invalidation, local revalidation, scope restriction, or only interpretation restrictions?

## Source gates

The audit is pinned to the reviewed QM-B source documents for:

- alias / PIT boundaries and listing-evidence gaps;
- survivorship / historical sample integrity;
- provider coverage and outcome availability;
- historical taxonomy integrity;
- crypto stable-object semantics.

Their Git blob SHAs are pinned in `configs/qm_b_evidence_impact_v1.json`. Any change to one of those source conclusions fails this gate until the impact review is explicitly updated.

## Result

The evidence-impact result is `PASS_WITH_SCOPED_RESTRICTIONS`.

There is **no basis for global invalidation of prior research** from the completed QM-B findings. In particular:

- the survivorship audit found no confirmed current-universe dependency defining historical research samples;
- the taxonomy audit found no confirmed present-day taxonomy retrojection;
- Phase 1 Selection/Timing already uses session-aware market-date handling and is not invalidated by the HistoricalMatcher calendar-date finding.

However, prior research is not unconstrained:

1. Stable-instrument longitudinal claims are strictly supported only inside verified identity/alias PIT boundaries. The frozen alias audit contains 35,059 strictly supported observations, 761 early non-crypto pre-boundary observations, and 1,293 crypto observations outside strict stable-object identity.
2. `daily_research` / `HistoricalMatcher` historical matches require local methodology revalidation because scanner `MarketDate` is a calendar run date while the matcher requires an exact market session. This does not justify global invalidation and does not apply to Phase 1.
3. Stored price history at the current audit proves present possession, not that every outcome was already available to an older analysis at its execution time. Missing/unknown outcomes remain denominator-ineligible.
4. Historical listing, market tradability, execution-channel availability and complete project investability are not proven. Prior research therefore must not be relabelled as a strict investable-universe backtest.
5. Crypto base references and quote-pair references must remain segmented or excluded from cross-namespace longitudinal identity analysis unless a separate authoritative identity source later proves equivalence.

## Consequence for QM-B

This package closes the evidence-impact question but deliberately does **not** declare strict QM-B promotion ready. Remaining promotion blockers are explicit external-evidence gaps and one scoped HistoricalMatcher methodology review, not an unbounded unknown integrity problem.

No historical observations are deleted, neutralized or automatically excluded by this package. No scanner, scoring, Decision Layer, portfolio or execution behavior is changed.
