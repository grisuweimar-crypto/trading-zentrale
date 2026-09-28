# Phase 8F — Mapping Candidate Batch B16

Status: `HUMAN_REVIEWED_ACTIVE_REPAIRED_HISTORY`

Batch ID: `8F_MAPPING_CANDIDATES_2026-09-27_B16`

Human review completed at `2026-09-27T14:34:54+02:00`.

B16 was originally treated as a 14-subject coverage increase. The repair audit established that **all 14 B16 subjects already existed in the materialized base map**. B16 therefore added **zero new frozen-domain subjects**.

Ten B16 reviews preserve the same subject/factor/relationship identity and are retained as audit-only re-reviews. Four B16 reviews (`SYK`, `GOOGL`, `META`, `MSFT`) changed the descriptive relationship class and are represented as versioned supersessions beginning only at the B16 human-review timestamp. The original intervals remain preserved historically.

## B16 reviewed subjects

| Subject | Factor | B16 relationship class | Repaired interpretation |
| --- | --- | --- | --- |
| ABT | fx | CURRENCY_TRANSLATION | Audit-only re-review. |
| ISRG | fx | OTHER_DOCUMENTED | Audit-only re-review. |
| ROK | fx | CURRENCY_TRANSLATION | Audit-only re-review. |
| SYK | fx | OTHER_DOCUMENTED | Versioned reclassification from prior `CURRENCY_TRANSLATION`. |
| TER | fx | OTHER_DOCUMENTED | Audit-only re-review. |
| AMAT | fx | OTHER_DOCUMENTED | Audit-only re-review. |
| AMZN | fx | CURRENCY_TRANSLATION | Audit-only re-review. |
| ASML | fx | CURRENCY_TRANSLATION | Audit-only re-review. |
| GOOGL | fx | OTHER_DOCUMENTED | Versioned reclassification from prior `CURRENCY_TRANSLATION`. |
| META | fx | OTHER_DOCUMENTED | Versioned reclassification from prior `CURRENCY_TRANSLATION`. |
| MSFT | fx | OTHER_DOCUMENTED | Versioned reclassification from prior `CURRENCY_TRANSLATION`. |
| MU | fx | OTHER_DOCUMENTED | Audit-only re-review. |
| ORCL | fx | CURRENCY_TRANSLATION | Audit-only re-review. |
| PLTR | fx | OTHER_DOCUMENTED | Audit-only re-review. |

## Repaired B16 accounting

- Reviews approved: **14**.
- Genuinely new mapped subjects: **0**.
- Audit-only re-reviews: **10**.
- Versioned supersessions: **4**.
- ACTIVE mapped subjects after B16: **176/207**, unchanged from B15.
- No base-map rewrite or backdating occurred in the repaired representation.
- No market outcomes, direction, exposure sign, weights or thresholds were used.

Later B17-B19 additions must not be retroactively attributed to B16.
