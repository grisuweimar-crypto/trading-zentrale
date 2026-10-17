# Preregistration — Uran candidate U1 (research-only)

Registered: 2026-10-14. Source snapshot `ff86b0b4-8b91-444d-a80f-2ad2ff40547a` (2026-10-13). No return outcomes after the registration date are available at registration.

## Identity and intent
- ID: `URAN-REBOUND-U1-v1`, **HYPOTHESIS ONLY**; not registered as an L5 frozen pattern, not promoted to L13.
- Population: contemporaneously identified uranium-cluster equities for which registered point-in-time scanner features exist.
- Condition: R1, Trend200 < -0.20 and RS3M < 0.00. All thresholds frozen before prospective collection. Dataset may contain both UEC and UUUU; they are **not** an independent sample.
- Reference A: exact sealed Phase-7H evaluation on matching snapshot with same private portfolio state; when unavailable state `A_NOT_AVAILABLE`.
- Challenger B: **annotation only** of the setup state and its validation status; never alters 7D Stance, 7F action, scanner score, or portfolio execution.
- Target: sector-adjusted total return **net of reasonable, prespecified trade costs** at 20 exchange trading sessions. Secondary: 5, 10, 40 sessions and maximum adverse excursion.
- Comparisons: (a) all eligible uranium observations without setup; (b) general scanner baseline state; (c) buy/hold or no-change baseline where position-specific costs are known.
- Overlap handling: event cooldown >=20 trading sessions per symbol; cluster inference by event-date and symbol; report small independent effective-N. No cross-asset claims from mismatched historical matches.
- Historical data as exploratory only unless the features and market values demonstrably existed as recorded at event time. Do not reconstruct Elliott labels from future pivots.
- Elliott-Fibonacci confirmation is **not** in this v1 setup: the W10 source was `not_supplied`. Future separately registered sub-arm allowed only from prospective PIT-signed Elliott captures.
- Risk: benchmark and FX alignment, provider data availability, survivorship, splits/dividends, multiple testing and sensitivity to threshold all reported; missing remains missing.
- Success rule: no promotion on in-sample fit; require positive net incremental utility and interval estimate on chronology-separated data, then independent prospective confirmation and existing Pattern Discovery/QM gates. Null, negative, insufficient-N remain reportable.
- Ownership: may enter Pattern Discovery L1 ONLY by the repository's own manifest contract and authorization. This document itself is **not** an L1 manifest.
- Current source research cycle: `artifacts/research/pattern_discovery/operations/latest.json` = `READY_WITH_WORK`, 39,359 observations, 224 dates; **zero** classified regular discovery runs and **zero** frozen candidates/claims/matured outcomes; requires new L1 preregistration.
