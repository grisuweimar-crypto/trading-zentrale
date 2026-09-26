# Phase 8C-J — Offline SEC Bulk Import

Status: infrastructure active; empirical Phase 8C remains blocked on authoritative SEC transport.

## Why this exists

On 2026-09-26, both the user environment and the assistant execution environment returned HTTP 403 for the SEC transport paths needed by the original collector, including:

- `https://www.sec.gov/files/company_tickers.json`
- `https://data.sec.gov/submissions/CIK##########.json`
- the official SEC bulk archive path for `submissions.zip`

The SEC documents the public data APIs as keyless and documents nightly bulk archives for submissions and company facts. Therefore the project treats the observed failure as a transport/access problem, not as missing credentials.

No source rule is relaxed because of the 403.

## Goal

8C-J allows locally available SEC bulk archives to be processed without any runtime SEC request. It removes the online ticker bootstrap entirely:

1. open local `submissions.zip`;
2. identify each primary `CIK##########.json` payload;
3. build exact ticker -> CIK candidates from the current `tickers` array inside those payloads;
4. reject ambiguous ticker mappings;
5. re-check exact ticker + CIK inside the selected primary payload;
6. persist the primary submissions JSON and all referenced historical submissions members found in the ZIP;
7. match `CIK##########.json` from local `companyfacts.zip` by the verified CIK;
8. hash every source archive and every persisted JSON/file;
9. write `bulk_manifest.json` with explicit transport provenance and fail-closed eligibility flags.

## Source modes

### `DIRECT_SEC_BULK_OPERATOR_ATTESTED`

Use only when the local archives were downloaded directly from official SEC HTTPS URLs and the operator records those URLs plus the acquisition timestamp. The importer records SHA-256 for both archives.

This mode can make the imported Company Facts data eligible for the *real-data validation step*. It does **not** by itself complete Phase 8C, promote any external family, change Phase 7, or authorize market-outcome research.

Official documented bulk URLs:

- submissions: `https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip`
- company facts: `https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip`

### `NON_AUTHORITATIVE_TRANSPORT_FOR_CHALLENGER_ONLY`

Use for any third-party mirror/copy. The data may support parser development or challenger debugging only.

It may **not**:

- satisfy the final SEC provenance gate;
- promote fundamentals or structured events;
- alter Phase 7 output;
- enable market-outcome research;
- be relabeled as SEC-authoritative merely because accessions/CIKs look plausible.

## Filing text remains a separate dependency

`submissions.zip` contains filing metadata, not the complete 8-K/6-K filing text required by the structured-event semantic challenger. Therefore 8C-J optionally accepts `--filing-content-dir` containing accession-named `.txt` files.

Without authoritative filing text:

- fundamentals may become testable from direct SEC bulk Company Facts;
- structured-event semantic validation remains blocked;
- Phase 8C remains incomplete.

## CLI

```bash
python scripts/import_external_evidence_8c_sec_bulk.py \
  --scanner artifacts/research/latest_scanner.csv \
  --submissions-zip /path/to/submissions.zip \
  --companyfacts-zip /path/to/companyfacts.zip \
  --output-dir artifacts/external_evidence/sec_bulk_snapshot \
  --source-mode DIRECT_SEC_BULK_OPERATOR_ATTESTED \
  --submissions-source-url "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip" \
  --companyfacts-source-url "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip" \
  --acquired-at "2026-09-26T07:00:00+00:00"
```

For a mirror, change `--source-mode` to `NON_AUTHORITATIVE_TRANSPORT_FOR_CHALLENGER_ONLY` and provide the actual mirror URLs. The resulting manifest remains non-promotable.

## Completion rule

8C-J is an infrastructure workaround, not an empirical shortcut. Phase 8C remains `BLOCKED_ON_SEC_TRANSPORT` until authoritative raw data sufficient for the required real-data validation is available. The frozen Phase 7 baseline remains unchanged.
