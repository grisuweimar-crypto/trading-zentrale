# BA-QM-REPAIR-01 / F04 — dynamic, source-bound residual assertions (2026-10-09)

**Audit finding:** `QM-H-AUD-20261008-F04` / `AUD-20261008-A-55991982`, tracked by [Issue #248](https://github.com/grisuweimar-crypto/trading-zentrale/issues/248).

## Problem
Permanent [BA-QM12 CI](../.github/workflows/ba_qm12_continuous_qm.yml) and `tests/test_ba_qm12_continuous_qm.py` required exactly `blocking_residual_count == 6`. This is correct for **today's** observed residuals but not a valid invariant after a future separately evidenced partial resolution. The existing `_masterplan_residual_monitor` already derives blockers from individual upstream source contracts rather than this fixed count.

## Isolated remedy
- Introduce `validate_masterplan_residual_consistency` to fail closed if the monitor omits/adds/reorders required residual identities, reports non-boolean block flags, misstates the ordered blocker-ID set, gives a wrong/noninteger blocker count, or claims `masterplan_end_state_complete` / `full_completion_claim_allowed` inconsistently with any remaining blocks.
- Enforce this before returning the `evaluate_continuous_qm` receipt. Change the BA-QM12 workflow and permanent test to use this dynamic invariant rather than exactly six.
- Add positive current-source coverage; a **purely synthetic** BA-QM6 status change used only inside a test to prove the code accepts five blockers *when the authoritative observed status actually changes*; and negative mutation tests for mismatched count, IDs, missing/unknown residual, invalid flag and premature completion.
- Retain fixed registry identities and the independent Lag-1 `PROMOTION_BLOCKED` and W8 `promotion_eligible=False` checks; the research evidentiary authority and prospective rules are **not** loosened.

## Evidence boundary
- The current `main` state has **six authentic blocked empirical residuals**: `PHASE1A_LAG1_CAPA`, `W8_EMPIRICAL_UTILITY`, `BA_QM2_EXTERNAL_HISTORICAL_INTEGRITY`, `BA_QM6_EMPIRICAL_VALIDATION`, `BA_QM7_EMPIRICAL_VALIDATION`, and `DECISION_LAYER_EMPIRICAL_PROMOTION`.
- A self-consistent synthetic test fixture is *not* a promotion, and these tests do not override the independent validity of underlying source evidence; source-controlled `_masterplan_residual_monitor` still derives the genuine receipt.
- No scanner score, history, portfolio, Decision, execution, control threshold, recorded outcome, or research-promotion status has been changed.
- QM-H prior 35 hash-chained entries remain unchanged. Events 36–39 move F04 to `IMPLEMENTED`; independent CI and production effectiveness confirmation are still required before `EFFECTIVENESS_VERIFIED` or `CLOSED`.

## Acceptance
PR CI for BA-QM12, QM-H and all relevant system checks; verify the production receipt and individual six blockers on `main`; then append a separate QM-H effectiveness and closure receipt. F03 remains independent.
