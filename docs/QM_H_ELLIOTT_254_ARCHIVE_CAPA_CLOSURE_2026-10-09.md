# QM-H CAPA: Elliott 6H lossless shadow archive transport (#254)

**Date:** 2026-10-09  
**Finding:** `QM-H-ELLIOTT-6H-ARCHIVE-254`  
**CAPA:** `QM-H-CAPA-ELLIOTT-6H-ARCHIVE-254`  
**Type:** DEFECT / HIGH  
**Evidence impact:** `EVIDENCE_REVIEW_REQUIRED` (technical closure does not establish predictive validity)  
**Proposed disposition:** `EFFECTIVENESS_VERIFIED -> CLOSED` after independent QM-H CI  
**Root issue:** https://github.com/grisuweimar-crypto/trading-zentrale/issues/254

## Reproduction and root cause

[Production run #37908194648](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37908194648) successfully captured historical PIT-aligned Elliott data, but its GitHub push was rejected by GH001 because the uncompressed prospective JSONL exceeded the 100 MB hard blob limit (125.43 MB). The shadow branch's previous committed history was not erased.

Related safeguards detected during review: restore from the exact `artifacts/research/$path.gz` not a misnamed workflow file; prevent an archive-reset publish; bind archived plaintext legacy identity to **raw blob bytes**, not Git text mode which normalizes CRLF/bare-CR. Each problem was repaired before final review.

## Corrections and prevention

- [PR #255](https://github.com/grisuweimar-crypto/trading-zentrale/pull/255): full-byte deterministic gzip, exact SHA-256/byte-count roundtrip, legacy read compatibility, fail-closed corruption and 80 MB compressed transport ceiling.
- [PR #256](https://github.com/grisuweimar-crypto/trading-zentrale/pull/256): preserve the exact original archive prior to capture, bind its SHA, fail closed on truncation or rewritten prefix; regressions for wrong restore and archive mutation.
- [PR #257](https://github.com/grisuweimar-crypto/trading-zentrale/pull/257): hash raw legacy JSONL Git blobs, not normalized text; LF/CRLF/bare-CR regression. Merged commit `43ca38faa44c95fc85fad212833208433da584a2`; PR CI [#37921254472](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37921254472): **131 tests passed**.

## Independent main effectiveness (real shadow publication)

[Run #37921435520](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37921435520) ran on the merged `main` PR257 commit and succeeded:

| Control | Verified actual log value |
|---|---|
| pre-existing full archive | 165,546,029 B |
| pre-existing archive SHA-256 | `7f01a840bddc729f3b10e69072ef002b2303552dbf8e7df234c73a83fbb2711f` |
| newly appended full bytes | 34,076,711 B |
| final uncompressed history | 199,622,740 B |
| final uncompressed SHA-256 | `c417cd1f1d6b541b24d80cf1059c18a8c8a4a7a53cbe2b6f50d19256250a2f40` |
| compressed history | 22,420,510 B |
| compressed history SHA-256 | `b92818f5f8cdea9818df5080001a235eb6b83b0209cf1accf6d4a40489aeac05` |
| archive check | `append_only_verified=true` |
| transport | `roundtrip_verified=true` |
| full frozen capture regression | 131 tests PASS |
| successfully pushed shadow commit | `1802727e6250569dacb2333d09117b7b977f2bb0` |

Previous real post-migration successful run [#37912636624](https://github.com/grisuweimar-crypto/trading-zentrale/actions/runs/37912636624) independently showed a lossless incremental archive update. The archive was not truncated to meet GitHub size limits.

## Evidence disposition

- Keep all earlier failed pushes and incomplete attempts as operational failure evidence, **not** as independent successful captures.
- The currently published continuous archive has a hash-proved existing-prefix preservation through the tested migration/append sequence. This does **not** magically validate all past Elliott features, prices or historical provenance.
- No hidden clipping, reset, invented backfill, reusing spent outcomes, automatic model promotion or changed Decision/Portfolio Action semantics.
- `EVIDENCE_REVIEW_REQUIRED` still applies to the interpretation and confirmation value of older captures; only the bounded archive transport CAPA is resolved.
- The 80 MB compressed review cap remains binding. If reached later, a separately reviewed *lossless* storage design is required; no truncation.

## Separation of concerns

Technical archival continuity is **not** evidence of forecasting advantage. The six independent empirical Masterplan blockers and Elliott's research-only boundary remain unchanged.
