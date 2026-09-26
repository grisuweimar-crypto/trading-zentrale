# Phase 8C — SEC Snapshot Collection Handoff

A real 8C validation requires one verified SEC snapshot collected from an environment that can reach the official SEC data endpoints.

The collector first prefers the official `https://www.sec.gov/files/company_tickers.json` ticker/CIK mapping. If that endpoint returns an access error, it may use a static GitHub mirror only as a **non-authoritative candidate bootstrap**. Every candidate CIK/ticker must then be confirmed against the current official `https://data.sec.gov/submissions/CIK##########.json` payload before any snapshot bundle is created.

If the official `data.sec.gov/submissions` preflight returns HTTP 403 or 429, collection stops fail-closed **before snapshot files are written**. A mirror result can never substitute for that official identity validation.

## Command

From the repository root in a local terminal / VS Code terminal:

```bash
python -m pip install -e .
python scripts/collect_external_evidence_8c_sec_snapshot.py \
  --scanner artifacts/research/latest_scanner.csv \
  --output-dir artifacts/external_evidence/sec_snapshot \
  --user-agent "trading-zentrale research contact-via-github grisuweimar-crypto/trading-zentrale" \
  --minimum-interval-seconds 0.22 \
  --include-content-documents \
  --research-history artifacts/research/history_recent.csv \
  --content-lookback-days 365
```

On Windows/PowerShell with the project venv:

```powershell
.venv-1\Scripts\python.exe scripts\collect_external_evidence_8c_sec_snapshot.py `
  --scanner artifacts\research\latest_scanner.csv `
  --output-dir artifacts\external_evidence\sec_snapshot `
  --user-agent "trading-zentrale research contact-via-github grisuweimar-crypto/trading-zentrale" `
  --minimum-interval-seconds 0.22 `
  --include-content-documents `
  --research-history artifacts\research\history_recent.csv `
  --content-lookback-days 365
```

When the fallback is needed and the official submissions preflight succeeds, the collector prints:

`ticker_bootstrap_mode=NON_AUTHORITATIVE_MIRROR_BOOTSTRAP identity_authority=CURRENT_SEC_SUBMISSIONS_ONLY`

The collector writes an accession-bound bundle with raw SEC JSON, bounded 8-K/6-K filing text, source URLs and SHA-256 digests. It does not read market outcomes or assign directions. Snapshot validation re-reads the hashed SEC submissions payload and independently verifies exact CIK/ticker identity.

## Return artifact

Compress the generated directory:

`artifacts/external_evidence/sec_snapshot/`

and provide the ZIP to ChatGPT. No API key, paid subscription, brokerage credential or SEC account is required. Do not provide passwords or tokens.

Once the snapshot is available, the remaining 8C work is:

1. verify snapshot integrity;
2. run real current-universe fundamental coverage;
3. run 8C-G anchors and 8C-H challenger over the real filings;
4. create the preregistered 8C-I annotation sample;
5. validate precision/critical-field accuracy without market outcomes;
6. document pass/remain-challenger decisions by family;
7. freeze the 8C completion report and only then decide which external families may proceed to later outcome research.
