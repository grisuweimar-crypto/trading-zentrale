# Phase 8C — SEC Snapshot Collection Handoff

A real 8C validation requires one verified SEC snapshot collected from an environment that the SEC accepts. GitHub-hosted Actions and the current assistant execution environment receive HTTP 403 from the SEC raw-data endpoints, so this one acquisition step must be executed from another network environment.

## Command

From the repository root in a local terminal / VS Code terminal:

```bash
python -m pip install -e .
python scripts/collect_external_evidence_8c_sec_snapshot.py \
  --scanner artifacts/research/latest_scanner.csv \
  --output-dir artifacts/external_evidence/sec_snapshot \
  --user-agent "trading-zentrale research contact-via-github grisuweimar-crypto/trading-zentrale" \
  --minimum-interval-seconds 0.22 \
  --include-filing-content \
  --history artifacts/research/history_recent.csv \
  --content-lookback-days 365
```

The collector writes a resumable, accession-bound bundle with raw SEC JSON, bounded 8-K/6-K filing text, source URLs and SHA-256 digests. It does not read market outcomes or assign directions.

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
