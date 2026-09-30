# QM-B Survivorship / Historical Sample Audit

## Scope

This package audits whether historical research consumers can accidentally define, filter or enrich historical samples with the **current** project universe or current scanner snapshot.

The audit is deliberately two-stage:

1. static repository candidate discovery;
2. manual call-path review of every candidate before any methodology-risk or defect classification.

A co-occurrence of historical and current-universe markers is therefore `REVIEW_REQUIRED`, not proof of survivorship bias.

## Failure modes under review

- historical rows dropped because their instrument is absent from the current universe;
- current `active=1` used as historical sample eligibility;
- current identifier/alias mapping silently used as historical membership truth;
- current taxonomy or metadata silently treated as point-in-time historical data;
- `latest_scanner` used to define historical labels, membership or eligibility;
- absence from the current universe interpreted as historical `OUT_OF_SCOPE`.

## Already reviewed core paths

`src/scanner/reports/research_views.py` is classified SAFE for current-universe survivorship: historical scanner groups are built from observed historical scanner/archive rows and are not filtered by `universe_master.csv`.

`src/scanner/research/query_api.py` is classified SAFE for current-universe survivorship: it filters history only by explicit query symbols/date bounds and does not load the current universe.

## Status semantics

- `SAFE`: reviewed path does not use current universe/current snapshot to define historical sample truth.
- `REVIEW_REQUIRED`: static candidate awaiting concrete call-path review.
- `CURRENT_UNIVERSE_DEPENDENCY`: reserved for confirmed historical sample dependence on current universe.
- `CURRENT_METADATA_ENRICHMENT`: reserved for confirmed current metadata injected as historical truth.
- `CURRENT_SNAPSHOT_DEPENDENCY`: reserved for confirmed latest-scanner influence on historical sample/labels.

No automatic defect promotion occurs from static discovery alone.
