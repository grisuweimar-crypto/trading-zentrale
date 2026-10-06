# Pattern Discovery Lab v2 — L0 Foundations / Boundary Freeze

## Purpose

L0 creates a dedicated research-only bounded context for Pattern Discovery Lab v2.

The lab may discover and describe candidate hypotheses in later phases, but L0
makes one rule executable before any search code exists: the lab cannot write to
productive Scanner_vNext, Timing, Decision Layer, portfolio-action or execution
artifacts.

## Namespace

The bounded context uses:

- `src/scanner/research/pattern_discovery/`
- `configs/pattern_discovery/`
- `artifacts/research/pattern_discovery/`
- `scripts/pattern_discovery/`
- `tests/pattern_discovery/`
- `docs/pattern_discovery/`

Runtime writes are confined to `artifacts/research/pattern_discovery/`.

## Existing governance reused

L0 deliberately does not introduce a second research-governance stack.

Pattern Discovery Lab v2 binds to the existing authorities:

| Concern | Existing authority |
| --- | --- |
| hypothesis identity/versioning | QM-C1 `HypothesisRegistry` |
| immutable analysis-plan freeze | QM-C2 `AnalysisPlanRegistry` |
| hypothesis families / multiplicity | QM-C3 `FamilyMultiplicityRegistry` |
| sequential monitoring | QM-C4 `SequentialMonitoringRegistry` |
| negative/rejected result retention | QM-C5 `NegativeResultRegistry` |

Later Pattern Discovery phases must adapt to these authorities instead of
duplicating them.

## Boundary contract

`configs/pattern_discovery/l0_boundary_v1.json` is the machine-readable L0
contract.

The contract is research-only and explicitly disables:

- productive integration
- execution
- direct scanner-score changes
- direct Timing changes
- direct Decision-Layer changes
- direct portfolio actions

The runtime guard rejects any write outside the lab artifact namespace and also
rejects productive decision/execution keys inside research payloads.

Any future widening of read or write scope requires a new reviewed contract
version. It must not be changed silently as a side effect of a Discovery run.

## Deterministic identity

The complete canonical JSON contract has a deterministic SHA-256 identity via
`boundary_contract_hash()`.

This is the L0 identity anchor. Discovery Run identity, input fingerprints,
config hash and cutoff identity belong to L1.

## Point-in-time boundary

L0 does not implement historical feature reconstruction. It only freezes the
architectural requirement that future Discovery code must consume approved
research/config inputs and preserve existing PIT semantics.

No modern feature may be retrofitted into historical scanner states merely
because it is available today.

## Definition of Done

L0 is complete when all of the following hold:

- [x] dedicated `pattern_discovery_lab` bounded context exists
- [x] purpose and non-purpose are explicit
- [x] runtime write scope is lab-only
- [x] productive Scanner/Decision/portfolio/execution outputs are forbidden
- [x] allowed input namespaces are explicit
- [x] existing QM-C1..C5 governance is referenced and reusable
- [x] deterministic boundary-contract identity exists
- [x] boundary violations fail closed
- [x] tests cover productive-path writes, traversal, payload semantics and QM-C bindings
- [x] no productive scanner or Decision-Layer semantics are modified

## Out of scope

L0 does not implement:

- Discovery Run manifests
- Feature Library
- Pattern Search Engine
- Candidate Registry
- hard Pattern Freeze
- Dependency Graph
- Prospective Capture
- Outcome Maturation
- Confirmation/Falsification
- Rating
- Promotion
- Decision-Layer challenger integration

Those remain L1–L14.
