# BA-QM-REPAIR-01 — F02 independent numeric-score coverage: live effectiveness closure (2026-10-09)

## Finding and authority
- Original finding: `QM-H-AUD-20261008-F02` from `AUD-20261008-A-55991982`; [issue #244](https://github.com/grisuweimar-crypto/trading-zentrale/issues/244).
- Code implementation already merged via [PR #245](https://github.com/grisuweimar-crypto/trading-zentrale/pull/245), merge `63548e2d0d1a3e1f1dd368149741a2e1601a3737`. BA-QM12 now independently checks the same captured metadata's `validation.policy.min_score_ratio` against `validation.numeric_score_count` using `ceil(symbol_count * threshold)`.
- Fail-closed cases include any `validation.status=ok` with below-threshold numeric counts, missing/malformed/nonfinite threshold and malformed/out-of-range count. The scanner's own upstream validator was not altered.

## Real production proof and independent retests
- Actual scanner source: `artifacts/research/history_metadata.json`, snapshot `32739832-2481-491f-b131-f7413c07f6b2`, 2026-10-08, 215 required/observed instruments, **215 valid numeric scores**, captured `min_score_ratio=0.90`; integer minimum **194**; numeric-score monitor **PASS**.
- [Pull request BA-QM12 run 37898545258](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37898545258): PASS with mutation/boundary tests.
- [After-merge main BA-QM12 run 37899660077](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37899660077): **29 tests PASS** and **92 permanent control regressions PASS**. Log of current Continuous-QM receipt confirms `ACTIVE_CONTINUOUS_QM` and ongoing six empirical blockers.
- [After-merge QM-H run 37899660064](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37899660064): **259 tests PASS**; append-only chain and closure governance remain effective.

## QM-H append-only status
- Preserve existing entries #1–33 verbatim and their prior chain head `78f1e99b7739741c3d7f7d75383148b8ee8a1fbb9359f6c10e03d8e21b26510e`.
- Entry #34: `QM-H-AUD-20261008-F02` `IMPLEMENTED → EFFECTIVENESS_VERIFIED`, `EFFECTIVE`. Hash `352e968d8ac1cf8e29ecab9c504f6553fefabd46dd47329276297b6763032acd`.
- Entry #35: `EFFECTIVENESS_VERIFIED → CLOSED`, with explicit disposition of original `EVIDENCE_REVIEW_REQUIRED`. Hash `4410913811f86b566dadad8a6157d153568956221cf0b885abf80d9b6517377a`.
- F03 (stale operating playbook) and F04 (brittle fixed six-residual assertion) remain `OPEN`. The six *currently blocked* empirical residuals remain blocked. This closure does not promote any research finding, modify scanner scores or change Decision/portfolio/execution semantics.

No missing price or score observations are fabricated, modified or backfilled.
