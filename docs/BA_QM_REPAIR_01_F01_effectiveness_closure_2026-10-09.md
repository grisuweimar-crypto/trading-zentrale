# BA-QM-REPAIR-01 — F01 production effectiveness and closure (2026-10-09)

## Scope and baseline
This is an append-only QM-H closure of **F01 only**. Preserve the frozen audit baseline `559919821ea48e282ff0da3bb022bdcbaad50b7a`, pre-fix W10 `ed5d54cd-efd2-4189-8782-816ac17dae4e` and original persisted max shard **1,708,027 B / 14.59865% free headroom**. The original pre-fix in-memory maximum was not observed. [Fix PR #239](https://github.com/grisuweimar-crypto/trading-zentrale/pull/239) merged `70e92cea735ee74beaee54040e271d843085241d`.

## Independent live proof
- Scanner/W10/runtime snapshot `32739832-2481-491f-b131-f7413c07f6b2` (as-of 2026-10-08), matching in `artifacts/research/daily_research.json`, `artifacts/research/decision_snapshot_w10.json`, and `artifacts/research/watch_runtime/manifest.json`.
- **64 shards**, `partition_strategy=size_balanced_v1`, **215/215 instruments**, **2,986 packets**. No private position data; no change in Decision semantics.
- **Largest published shard: 821,344 B / 2,000,000 B**, leaving **58.9328%** headroom; `capacity_review_recommended=false`. Required free margin was at least 20%.
- [Live Runtime Publish 2026-10-08](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37847760206): PASS, including writer/reader, W10, long/flat references and privacy.
- [Subsequent live Runtime Publish 2026-10-09](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37866443302): PASS, same observed capacity and packet counts.
- [Symbol Views 2026-10-09](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37867050055): PASS; matching runtime identity and public projection hash.
- [BA-QM8 2026-10-09](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37866443229): PASS. [main BA-QM12 code regression 2026-10-08](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37822210059): PASS; no claim that BA-QM12 was rerun after latest publication.

## Append-only CAPA disposition
- Prior 27 events and hash-chain head `9e63f35f35a6911ea86bc049027cd6fe69df8f2dd73ef8358218c971f132dc31` are retained without modification.
- Seq 28: F01 `IMPLEMENTED → EFFECTIVENESS_VERIFIED`, result `EFFECTIVE`, hash `cd2713f03a144c4e24e45f90e95e9e2c0b0b908b940ad9cb2bb42406484f968f`.
- Seq 29: F01 `EFFECTIVENESS_VERIFIED → CLOSED`, with explicit disposition of review-required transport evidence, hash `5f89ba5e2e31595dd10853bcfb909cce1e699952bf5cbcd0ddf97adb8ef936b2`.
- **F02 score coverage, F03 operational playbook and F04 brittle six-residual assertion remain OPEN.** The six independent empirical promotion blocks remain fully effective.

No orders, trading decisions, holdings, source evidence, research thresholds or promotion states were changed.
