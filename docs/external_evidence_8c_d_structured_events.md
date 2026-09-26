# Phase 8C-D – Structured Corporate Events

## Objective

8C-D converts SEC current-report metadata into a reproducible, point-in-time event layer. It does **not** assign market direction, expected return, portfolio action or Phase-7 stance.

The first layer deliberately uses only filing metadata that is already covered by the 8C PIT contract:
- form;
- accession number;
- 8-K item codes;
- SEC publication/acceptance time;
- amendment status.

## What can be classified from metadata

Examples of metadata-level candidate types:
- 2.02 → `RESULTS_RELEASE`;
- 1.01 → `MATERIAL_DEFINITIVE_AGREEMENT`;
- 2.01 → `ACQUISITION_OR_DISPOSITION_COMPLETED`;
- 3.02 → `UNREGISTERED_EQUITY_SALE`;
- 5.02 → `DIRECTOR_OR_OFFICER_CHANGE`;
- 7.01 → `REGULATION_FD_DISCLOSURE`;
- 8.01 → `OTHER_MATERIAL_EVENT`.

These are filing-event labels, not investment conclusions.

## Hard semantic boundaries

The metadata layer must never convert:
- Item 2.02 into earnings beat/miss;
- Item 7.01 into guidance raise/cut;
- Item 8.01 into dividend/buyback;
- Item 3.02 into a complete capital-raise classification;
- any filing item into positive/negative direction.

Those semantics require later content evidence from the filing itself.

## 6-K treatment

6-K has no 8-K-style item taxonomy suitable for equivalent deterministic classification. Without a validated content parser it remains:

`FOREIGN_CURRENT_REPORT_UNCLASSIFIED`

This preserves international filings without inventing semantics.

## Amendments

8-K/A and 6-K/A remain separate versioned events. They do not overwrite the original accession/event. The layer preserves the later publication time and `AMENDMENT` stage.

## Snapshot transport

The runner consumes only the verified offline SEC snapshot introduced in 8C-C3. It makes no network calls. This means the same filing snapshot can be used repeatedly for:
- coverage review;
- mapping review;
- later content-semantic research;
- future outcome studies after the domain is frozen.

## Next gate

8C-D closes only the filing-metadata event layer. A later content-semantic slice may attempt to identify guidance changes, dividend changes, buybacks, capital raises and earnings beat/miss, but only with explicit text/numeric evidence and its own PIT/coverage validation.

No outcome join is enabled by this slice.
