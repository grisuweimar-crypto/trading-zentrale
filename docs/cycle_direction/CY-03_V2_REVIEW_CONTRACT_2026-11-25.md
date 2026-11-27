# CY-03-v2 review verifier / #269 + #290

25.11.2026 — outcome-blind, no release, no production changes.

## Actual inventory

Manifest: 65 snapshots, 14,280 records, 208/220 latest internally valid,
9,669 / 8,789 / 7,699 provisional 1/5/10obs chains, zero research eligible.
Coverage CSV: 220 assets with 5obs chains; 215 with 10obs chains.
The five without 10obs: 8035.T, CRYPTO:DOGE, CRYPTO:SOL, CRYPTO:XRP, NPN.JO.
These are provisional counts, not independent samples or a PIT certificate.
Today's prefetch receipt exists for a40e915d-71d6-4ba3-89f8-f2c8ae53cf21;
it is client evidence, not publisher PIT. No retrospective stamping.

## External listing evidence found (partial only)

- UEC: issuer SEC 10-K, FY ending 31 July 2026, identifies UEC as NYSE American common stock: https://www.sec.gov/Archives/edgar/data/1334933/000143774926031414/uec20260731_10k.htm
- TTD: issuer SEC 8-K dated 14 September 2026, identifies TTD Class A on Nasdaq: https://www.sec.gov/Archives/edgar/data/1671933/000167193326000099/ttd-20260914.htm
- Nasdaq 2026 exchange calendar and session hours: https://www.nasdaq.com/holiday-trading-hours

These source URLs are documentary leads, not yet archived proof bytes or an independent reviewer decision. They do not establish historical Yahoo adjusted-bar availability or provider quote units.

## Review bundle contract

Separate directory: review.json, review.sig, proofs/<sha256>.bin.
Sign the exact UTF-8 JSON bytes with a separately governed reviewer key
(OpenSSL SHA256 signature). Review authority policy is deliberately empty:
no person or automation is implicitly accredited. A separate reviewed change
must register the independent reviewer and pin their public PEM SHA256.
The verifier validates every approved ledger row, source/identity, native
quote unit, session date/time, exact 60-bar price and archive digests,
four external evidence blobs and review chronology. A signed assertion
is a human attestation of source semantics, not an automated assessment of
the contents of the external files. Reviewers must check the exact adjusted
bar vintage and publication availability for all input bars, not just last bar.
No missing chain interior can yield a verified lag. No outcome inputs are read.

Run: python -m scanner.research.pattern_discovery.cycle_cy03_v2_review --bundle <external-review-directory>

This is the separate review-verification building block; the existing CY-05
adapter intentionally remains blocked pending actual independently verified
evidence, authority approval, audit and adapter integration. No #269 or #290
closure claimed. No legacy modification. For future work: independently review
the verifier, provision a reviewer via governance, obtain true external
provider vintage/PIT evidence or a suitable new documented data provider,
then connect the exact verified rows to CY-05 in a separate PR.
