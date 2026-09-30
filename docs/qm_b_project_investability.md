# QM-B Project Investability

Research-only evidence layer separating project membership, listing, market tradability and project investability.

## Core rule
An instrument is `INVESTABLE` only when every required dimension is PIT-verified positive: stable identity, project membership, listing state, market tradability, execution channel, and project restrictions.

Missing evidence is `UNKNOWN`. Explicit restrictions remain `RESTRICTED`. Explicit hard negatives remain `NOT_INVESTABLE`.

## Current repository audit
The durable membership snapshot provides 207 stable ISIN membership claims. It does not provide listing, tradability, execution-channel, or project-restriction evidence. Therefore current investability remains `UNKNOWN` for all 207 instruments and strict universe promotion remains blocked.

The `active` field in `universe_master.csv` is project membership configuration only. Symbol suffixes, country, currency, scanner presence or price availability are not accepted as substitutes for the missing dimensions.

No productive scanner behavior is changed.
