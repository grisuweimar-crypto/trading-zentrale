# BA-QM-REPAIR-01 – K0/K1 evidence receipt (2026-10-08)

**Scope:** K0 evidence preservation / append-only CAPA, K1 AUD-20261008-F01 Watch runtime transport capacity. F02–F04 fixes, residual empirical gates, and other 15-pass audit areas are out of scope.
**Status as of this receipt:** `F01 IMPLEMENTED / PENDING_EFFECTIVENESS`. No F01 closure or empirical promotion is authorized by this document.

## Frozen baseline (never rewritten)
- Audit: `AUD-20261008-A-55991982`; baseline git revision `559919821ea48e282ff0da3bb022bdcbaad50b7a`.
- Scanner/W10 snapshot `ed5d54cd-efd2-4189-8782-816ac17dae4e` (2026-10-07; 215 symbols / 2,771 runtime packets).
- Original failing scheduled CI: [BA-QM12 run 37774540579](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37774540579). Nine QM12 cases PASS, 82 regressions PASS, one BA-QM10 capacity test FAIL: stale assertion `capacity_review_recommended is False`, while reserve is genuinely below 20%.
- Original **persisted** max: `artifacts/research/watch_runtime/shard_3f.json` = **1,708,027 bytes** of the 2,000,000-byte transport limit, headroom **14.59865%**; git blob SHA `dfea31fa259e2789e81e7341184971cfbba6d259`. Old runtime manifest SHA `7abf3a4990e108bb4ddb508a64c8d3f4b08c5dbc`, projection SHA-256 `c289a9eb07e30933221ad937ad9cacc4db13b53e8c98c88a889b7f14d34839b7`.
- **Limitation:** The exact pre-change in-memory maximum was not captured by the initial audit. Do not mislabel the persisted byte count as that unobserved in-memory value.

## K0 CAPA chain
- [Issue #238](https://github.com/grisuweimar-crypto/trading-zentrale/issues/238) reproduces F01; [PR #239](https://github.com/grisuweimar-crypto/trading-zentrale/pull/239) implements its isolated code fix.
- The pre-existing `artifacts/research/qm/qm_h_capa_ledger.jsonl` 19-event hash chain was verified without changing its first 19 events.
- Appended 8 hash-chained, sequenced events (sequence 20–27), new chain head `9e63f35f35a6911ea86bc049027cd6fe69df8f2dd73ef8358218c971f132dc31`.
- `QM-H-AUD-20261008-F01`: `OPEN → TRIAGED → ROOT_CAUSE_IDENTIFIED → ACTION_PLANNED → IMPLEMENTED`; CAPA `QM-H-CAPA-AUD-20261008-F01`. Independent effectiveness and closure are intentionally absent.
- `QM-H-AUD-20261008-F02/F03/F04`: registered as `OPEN`, no technical fix or evidence release claimed.

## K1 implementation and before/after
- Keep 64 shards and complete, PIT-preserving symbol histories, but version deterministic distribution `size_balanced_v1` rather than uneven SHA-mod-64 routing. Manifest `symbol_shards` and reader remain compatible with historical manifests.
- Keep the 2-MB hard limit, distinguish a safe transport from the separate 20% reserve health warning, add boundary and negative tests. A transport with 14.60% reserve is **not** called healthy.
- Deterministic preview on the real sealed W10 source: [Decision Watch Runtime run 37795982579](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37795982579), job `capacity-preview`, no publication.
- Repeated on full branch with ledger: [run 37796248173](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37796248173), also PASS.
- Baseline persisted maximum: **1,708,027 B / 14.60% reserve**; new rebuilt maximum: `shard_1b.json` **740,792 B / 62.9604% reserve**. Other top rebuilt shards: `shard_1a.json 740636`, `shard_1d.json 740624`, `shard_1c.json 740567`, `shard_1f.json 740416`.
- Rebuilt data: 64 shards, **215/215 symbols, 2,771/2,771 packets**. Writer/reader roundtrip across representative cohorts and hotspot instruments, W10 snapshot/provenance, privacy flags, long/flat public reference rows all PASS.
- New runtime projection SHA-256: `79139290273f44f34228c44b8214e73641023eac5c43709a14fd152839513844`. This is a **preview hash**, not a claim that the corresponding runtime is published on `main`.
- The branch's [BA-QM10 run 37796247957](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37796247957), [BA-QM11 run 37796247989](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37796247989), and [BA-QM12 run 37796248492](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37796248492) were all successful.

## Independently inherited QA conflict and disposition
- The ledger change exposed **5 stale QM-B tests in the unchanged frozen baseline**: four count assertions 207 vs actual 209, one as-of predating the 2026-10-07 membership capture. Tracked as [Issue #240](https://github.com/grisuweimar-crypto/trading-zentrale/issues/240). The implicated test files and authoritative membership latest metadata were byte-identical between audit baseline and F01 PR.
- In a **separate** [PR #241](https://github.com/grisuweimar-crypto/trading-zentrale/pull/241), only current-state QM-B test/CI assumptions were aligned to the actual captured membership count and PIT time. No membership/evidence/scoring data changed. All 7 applicable QM-B/QM-H workflows passed; PR #241 was merged into main as `d6f30a876f538f3cb07fc2767586e017dc8d22ca`.
- The CAPA finding F01 is **not** considered effective merely because this separate inherited CI issue has been corrected.

## Remaining irreversible gates
1. Retest PR #239 against the updated main and confirm QM-H chain integrity, BA-QM10/11/12, negative boundary controls and capacity preview.
2. Only after accepted tests, merge the F01 fix and run the **real** main W10 runtime publish / symbol view check. Confirm newest snapshot, >=20% headroom, source and publication hashes, full symbol/packet inventory and reader semantics.
3. Observe independent effectiveness (including subsequent scanner run), append a new QM-H `EFFECTIVENESS_VERIFIED` event only after proof, and close F01 via a separate documented transition. Until then status remains `IMPLEMENTED / PENDING_EFFECTIVENESS`.
4. F02–F04 remain separate future repairs, and all six empirical promotion blockers remain unchanged.

**No orders, scoring logic, portfolio positions, history evidence, thresholds, or research promotion statuses were changed by K0/K1.**
