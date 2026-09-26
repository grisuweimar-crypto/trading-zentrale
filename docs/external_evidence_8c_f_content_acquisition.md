# Phase 8C-F – Filing Content Acquisition

## Objective

8C-F extends the offline SEC snapshot with the **full submission text** for relevant 8-K/8-K/A/6-K/6-K/A accessions. It does not parse or interpret the content yet.

## Why full submission text

The SEC full submission text is accession-bound and contains the primary filing plus included exhibits in one immutable filing package. This avoids choosing individual exhibits prematurely and gives later deterministic parsers the complete issuer-published context.

## Acquisition window

The collector derives the observed research interval directly from:

`artifacts/research/history_recent.csv`

Content acquisition is bounded to that interval plus a configurable pre-history buffer. The default is 365 calendar days.

Important: this buffer is **collection scope only**. It is not a feature definition and does not mean that a future event feature will use a 365-day lookback.

The exact window and buffer are stored in the snapshot manifest.

## Collector usage

```bash
python scripts/collect_external_evidence_8c_sec_snapshot.py \
  --scanner artifacts/research/latest_scanner.csv \
  --research-history artifacts/research/history_recent.csv \
  --include-content-documents \
  --content-lookback-days 365 \
  --output-dir artifacts/external_evidence/sec_snapshot \
  --user-agent "trading-zentrale research <contact>"
```

Collection still runs outside GitHub-hosted Actions because SEC blocked the hosted runner network path during 8C-C3.

## Integrity

Each filing text is stored with:
- accession number;
- form;
- original `published_at` / `valid_from`;
- publication stage;
- exact SEC archive URL;
- SHA-256 digest.

The snapshot validator now verifies these bytes as well as the JSON source files. A modified or missing filing text makes the snapshot invalid.

## Hard boundaries

8C-F does not:
- infer guidance changes;
- infer dividends or buybacks;
- classify capital raises;
- assign positive/negative market direction;
- read future returns;
- change Phase 7.

Those semantics remain behind the 8C-E evidence contract and a later validated parser.
