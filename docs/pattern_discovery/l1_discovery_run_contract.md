# Pattern Discovery Lab v2 — L1 Discovery Run Contract

## Purpose

L1 makes every future Discovery run a fully pre-registered, deterministic
research object before any pattern search begins.

A run may not start from an informal collection of parameters. The entire search
declaration and exact input bytes are bound into one immutable manifest.

## Pre-registration

Every run must declare all of the following before search:

- declared start time
- data cutoff
- approved data-source paths
- PIT rules
- universe version
- feature-library version
- allowed transformations
- pattern complexity
- pattern types
- targets
- horizons
- baselines
- minimum evidence criteria
- search budget
- candidate budget
- primary multiple-testing method and parameters
- primary statistical method
- robustness checks
- exclusion rules
- dependency rules
- code/commit version
- determinism and seed policy

Missing fields fail closed. Unknown extra fields also fail closed so a Discovery
run cannot quietly acquire a post-hoc tuning knob.

## PIT rule

`data_cutoff` must be earlier than or equal to `declared_start_at`.

Every declared data source is checked by the L0 read guard. L1 therefore cannot
silently reach into an unapproved repository namespace.

## Input fingerprints

Before search, L1 hashes the exact bytes of every declared input file with
SHA-256 and records:

- repository path
- SHA-256
- byte size

A missing input fails closed. If any input byte changes, the input fingerprint
and therefore the Discovery Run identity change.

## Deterministic identity

L1 derives four identities:

1. `config_hash` — canonical hash of the complete normalized pre-registration
2. `input_fingerprint_hash` — canonical hash of all exact input fingerprints
3. `run_identity_hash` — binds config, inputs, L0 boundary and L1 contract
4. `run_id` — human-usable `DISC-<digest>` identity derived from the run hash

Identical pre-registration plus identical input bytes produces the same
`run_id`. Any semantic plan change or input-byte change produces a different
identity.

The manifest itself is also SHA-256 protected by `manifest_hash`.

## Immutability

The frozen manifest state is `FROZEN_PRE_RUN`.

The only L1 persistence API uses exclusive file creation. It has no overwrite
mode. A second write to the same run manifest fails closed.

The contract-defined location is:

`artifacts/research/pattern_discovery/discovery_runs/{run_id}/manifest.json`

This path remains inside the L0 write boundary.

## Multiple testing

L1 only freezes the declared primary multiple-testing method. It does not
calculate adjusted p-values or perform candidate selection.

Supported declarations align with existing governance terminology:

- PREDECLARED_SINGLE_PRIMARY
- BONFERRONI_FWER
- HOLM_FWER
- BENJAMINI_HOCHBERG_FDR
- CUSTOM_PREDECLARED

Actual Discovery statistics belong to L4 and confirmatory family governance
continues to use QM-C3.

## Initial complexity guard

The initial laboratory generation is bounded to at most three atomic conditions
per pattern. This implements the conservative 1–3-condition search space from
the masterplan. Enlarging this bound requires a new reviewed contract version.

## Definition of Done

L1 is complete when:

- [x] a complete machine-readable Discovery Run contract exists
- [x] incomplete pre-registration fails closed
- [x] unknown post-hoc parameters fail closed
- [x] PIT cutoff ordering is validated
- [x] approved inputs are byte-fingerprinted
- [x] missing/unapproved inputs fail closed
- [x] search and candidate budgets are frozen and bounded
- [x] multiple-testing choice is frozen before search
- [x] deterministic config/input/run identities exist
- [x] identical plan + inputs produce identical run identity
- [x] changed plan or input bytes produce a new identity
- [x] frozen manifests are tamper-evident
- [x] manifest persistence is write-once
- [x] runtime output remains inside the L0 lab namespace
- [x] no search, scoring, Decision Layer or promotion behavior is activated

## Out of scope

L1 does not implement the actual Feature Library, Pattern Search Engine,
Discovery statistics, candidate freeze, prospective capture, outcome maturation,
rating, promotion or Decision-Layer integration.

The next phase is L2 — Feature Library.
