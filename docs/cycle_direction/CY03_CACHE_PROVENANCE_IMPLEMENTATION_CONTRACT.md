# CY-03 cache-to-snapshot provenance: fail-closed implementation contract

Status: **PROPOSED / NOT IMPLEMENTED**. No scientific release.

## Problem
After PR #326, the scanner can consume cached completed daily bars. A fetch receipt from the scanner execution is no longer a truthful claim about the earlier provider fetch. Missing direct receipts must not be silently treated as valid provenance.

## Required evidence chain
1. **At cache construction** persist an immutable acquisition record with UTC fetch start/end, provider identity, request parameters, actual response fingerprint, per-symbol raw bar identifiers (session date, venue, quote currency/unit, adjustment policy), normalized completed-bar SHA256, cache object SHA256, and acquisition code/version.
2. **At scanner consumption** persist the exact cache object SHA256, its acquisition-record SHA256, selected symbol/bar hashes, scanner run and snapshot identifiers, and the observed cutoff. Reject missing, changed, mismatched or future-dated acquisition records. Do not synthesize fetch timestamps.
3. **At CY-03 archive publication** bind those records to snapshot ID and immutable archive-bars SHA256. Preserve all prior v1 archive bytes and original receipt records; append new records only. Duplicate writes must be byte-identical.
4. **In science watch** verify hash joins, symbol/bar coverage, monotonic timestamps, cache-to-archive equality, append-only semantics and original anchors. Missing evidence remains explicitly unverified; contradictions cause integrity failure. Report source age and stale cache independently of archive publication age.
5. **Tests**: direct fetch, cache hit, mixed source, missing acquisition record, tampered cached bar, wrong snapshot, future fetch, duplicate record, stale source, provider revision, and backward compatibility with historical v1 snapshots.

## Research release gates
Client UTC evidence is not provider-signed publisher PIT evidence. Issue #269 requires independent venue/listing, quote-unit and per-bar availability verification. Issue #290 requires an independently reviewed versioned CY-03-v2 research-only release. No automatic eligibility or CY-05/06/07/08 promotion; preserve research_eligible=0 until all gates are proven.

## Acceptance
Implement the above in the actual cache writer, scanner consumer, archive writer and science-watch code paths; add CI regression tests. Merge only after end-to-end replay on a real new cached scanner snapshot and negative tests. This contract alone is not a fix.
