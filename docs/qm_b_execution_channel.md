# QM-B Execution Channel Evidence

This package defines a neutral research-only interface for instrument-level execution-channel availability.

It does not store credentials, account identifiers or authentication material and cannot place orders.

A status of `AVAILABLE` requires explicit PIT evidence for a specific stable instrument from at least one registered execution channel. Scanner presence, price availability, symbol suffixes and project-universe membership are not accepted as substitutes.

The repository currently has no registered execution channel and no instrument-level execution evidence. Therefore all 207 current stable membership instruments remain `UNKNOWN` for this dimension and the gap is `BLOCKED_NO_EXECUTION_CHANNEL_EVIDENCE`.
