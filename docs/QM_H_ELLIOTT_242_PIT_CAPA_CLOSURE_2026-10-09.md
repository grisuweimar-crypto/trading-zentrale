# QM-H CAPA: Elliott 6H historical publication PIT binding (#242)

**Date:** 2026-10-09  
**Finding:** `QM-H-ELLIOTT-6H-PIT-242`  
**CAPA:** `QM-H-CAPA-ELLIOTT-6H-PIT-242`  
**Type:** DEFECT / HIGH  
**Evidence impact:** `EVIDENCE_REVIEW_REQUIRED` (technical closure does not unspend or validate research)  
**Proposed finding disposition:** `EFFECTIVENESS_VERIFIED -> CLOSED` after independent QM-H CI  
**Root issue:** https://github.com/grisuweimar-crypto/trading-zentrale/issues/242

## Reproduction and root cause

The original 2026-10-08 Elliott prospective shadow run [37823134769](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37823134769) selected an older eligible Scanner publication (`d292e97836768acc2bb0f12ea9df76e2e22f502e`) while reading `history_recent.csv` and other scanner-side inputs from newer `main`. The existing validator correctly rejected `history_recent snapshot_id mismatch`. This is a fail-closed research availability defect, **not** proof of wrong production decisions.

## Repair and regression

[PR #253](https://github.com/grisuweimar-crypto/trading-zentrale/pull/253), merged as `217923bc86033a588e2d6f10ec484a4dd9970ef1`:

- Restore all seven relevant scanner inputs from the *same* selected historical publication commit (research metadata, scanner CSV, daily research, recent history, backfill, provenance, market OHLCV).
- Independently verify original manifest hashes for all five declared research inputs; call unmodified complete PIT validator.
- Fail closed on missing/mixed/mutated inputs; preserve original historical artifacts and prevent old/new snapshot mixing.
- Keep Elliott frozen 6A–6H in the isolated research-only lane, with no order or canonical stance changes.

## Independent effectiveness evidence

The actual [post-merge Main Elliott run #37921435520](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37921435520) succeeded (capture job SUCCESS) **after PR #257 also merged**. Its logs record:

- 131 frozen-core/prospective-capture tests passed;
- historical selected publication commit `67caebb67d46a12544da2800ed07e9f083382609`, run ID `github-37423544294-1`, snapshot `c0b21f74-29aa-4748-a3a0-d4cf722d8e8f`;
- steps for exact historical restoration, complete 6A–6H capture, research-only boundary validation, and isolated shadow publication succeeded;
- resulting shadow commit `1802727e6250569dacb2333d09117b7b977f2bb0`.

Preceding independent run [#37912636624](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37912636624) also passed PIT restoration and actual capture, separately verifying recovery from the original mixed-source problem.

## Evidence disposition

- Failed historical run 37823134769 remains a failed/run-audit observation; it is **not** a valid predictive research claim and is not retroactively repaired.
- Successfully restored old publications keep their original source commit, run ID, original available-from timestamp and historical snapshot identity. Running repaired code now does **not** turn a known past outcome into unspent prospective confirmation.
- Keep `EVIDENCE_REVIEW_REQUIRED` for historical capture-lineage interpretation; no inference that 6A–6H is empirically profitable or directionally predictive.
- No change to `PHASE1A_LAG1_CAPA`, `BA_QM6_EMPIRICAL_VALIDATION`, `W8_EMPIRICAL_UTILITY`, `DECISION_LAYER_EMPIRICAL_PROMOTION` or other independently governed residual gates.
- Close **only this engineering PIT source-binding CAPA** once CI verifies the appended ledger chain. Any independent newly detected source problem requires a new finding.

## Separation of concerns

`ENGINEERING_EFFECTIVE` does **not** imply `EMPIRICALLY_VALIDATED`, `PROMOTION_ELIGIBLE`, `PRODUCTION_DIRECTIONAL`, or `ORDER_ALLOWED`.
