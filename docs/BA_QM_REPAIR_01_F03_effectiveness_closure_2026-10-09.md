# BA-QM-REPAIR-01 — F03 Operating Playbook CAPA effectiveness (2026-10-09)

## Reference and remedy
- Audit `AUD-20261008-A-55991982`, finding `QM-H-AUD-20261008-F03`, [issue #250](https://github.com/grisuweimar-crypto/trading-zentrale/issues/250).
- Original obsolete `docs/OPERATING_PLAYBOOK.md`: Score 0–200, missing executable commands and unsupported automatic rebalancing/benchmarks.
- [PR #251](https://github.com/grisuweimar-crypto/trading-zentrale/pull/251) merged documentation and contract CI as `281b4f70165884ef5a3a024b0193cb2d65bcb046`; current score scale 0–100, authoritative scanner/Decision/W10/runtime/QM workflows and artifacts, safe diagnostics, no automatic orders.

## Independent published effectiveness
- [PR contract test #37903524994](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37903524994): PASS.
- [Post-merge main contract test #37903634260](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37903634260): PASS.
- The test verifies existing referenced scripts/workflows, excludes retired CLI paths and 0–200 Score, and requires current snapshot/PIT/privacy/QM text.

## QM-H lifecycle and boundaries
- Existing ledger entries 1–39 and prior hash `97ce0204c104c3d79f430e5ade404cc5536de47bb1b59fbc2734256544b398e8` unchanged.
- F03 events 40–45: `OPEN → TRIAGED → ROOT_CAUSE_IDENTIFIED → ACTION_PLANNED → IMPLEMENTED → EFFECTIVENESS_VERIFIED → CLOSED`.
- New terminal chain hash `66d5ce1200133ce5576f2a04b4266429b71a63865289f981350a4c0bd95b1859`.
- Separate F04 stays `IMPLEMENTED` pending postmerge effectiveness. Six genuine empirical blockers remain unchanged.

Only operating documentation and its own contract test have been changed; scanner values, history, positions, actions, thresholds and promotion states remain untouched.
