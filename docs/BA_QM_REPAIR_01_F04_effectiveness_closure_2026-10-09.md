# BA-QM-REPAIR-01 — F04 dynamic empirical blocker count: independent effectiveness and closure (2026-10-09)

## Audit basis and isolated engineering correction
- Finding `QM-H-AUD-20261008-F04`, CAPA `QM-H-CAPA-AUD-20261008-F04`; [issue #248](https://github.com/grisuweimar-crypto/trading-zentrale/issues/248), initial audit `AUD-20261008-A-55991982`.
- Before: BA-QM12 tests and scheduled workflow used a hard-coded `blocking_residual_count == 6` assertion that would reject a later independently evidenced partial clearance. The six observed blocks **were genuine** and had to remain intact.
- After: [PR #249](https://github.com/grisuweimar-crypto/trading-zentrale/pull/249), merged `9d1454a9434d103634ee68ae8f5aaf720cca4861`. Explicit `validate_masterplan_residual_consistency` checks registered residual identities, boolean block flags, ordered blocked IDs, derived integer count, and accurate end-state/full-completion flags. The BA-QM12 CI no longer insists on a fixed number.

## Verified outcome on real production source
- [PR BA-QM12 run 37902788039](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37902788039): **44/44 BA-QM12 tests**, **92/92 permanent regressions**, full receipt PASS.
- [Independent main BA-QM12 run 37903811915](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37903811915): **44/44** and **92/92** again, with the actual production receipt:
  - `blocking_residual_count = 6`;
  - `blocking_residual_ids = [PHASE1A_LAG1_CAPA, W8_EMPIRICAL_UTILITY, BA_QM2_EXTERNAL_HISTORICAL_INTEGRITY, BA_QM6_EMPIRICAL_VALIDATION, BA_QM7_EMPIRICAL_VALIDATION, DECISION_LAYER_EMPIRICAL_PROMOTION]`;
  - `masterplan_end_state_complete = false`, `full_completion_claim_allowed = false`, `empirical_promotion_performed = false`, `ba_qm12_may_release_lag1_block = false`.
- [Main QM-H run 37903811849](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37903811849): PASS.
- Unit tests include a synthetic authority-state change indicating one *hypothetically* cleared blocker plus invalid count, duplicate/unknown/missing ID, invalid flag, and premature-closure mutation refusals. Synthetic fixture changes are **not** proof of an empirical promotion and never modify productive artifacts.

## QM-H lifecycle
- Hash-chained implementation events #36–39 on [PR #249](https://github.com/grisuweimar-crypto/trading-zentrale/pull/249), then independently evidenced:
- Event #46: F04 `IMPLEMENTED → EFFECTIVENESS_VERIFIED`, `EFFECTIVE`; hash `6d30aed452d394922a22bb987aadc8810a29dbbe080ac97692106f0b901f4941`.
- Event #47: F04 `EFFECTIVENESS_VERIFIED → CLOSED`; hash `13637f9a60c087ff12207a67e91cdc095e3fd7c4d8c29367ca14c46a6ba5c942`.
- The F03 CAPA, with its own independent playbook evidence and separate events #40–45, remains distinct even though this final ledger verification is batched in one PR.

No change to scanner scores, empirical evidence, real research promotion, portfolio positions, risk, orders, W10 packet semantics or the six current independent promotion blocks.
