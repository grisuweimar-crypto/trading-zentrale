# Phase 8C – SEC Snapshot Transport

## Why this exists

During 8C-C3, GitHub-hosted Actions runners received HTTP 403 from both the SEC ticker bootstrap path and later SEC data requests. That is an infrastructure/transport condition, not evidence that an issuer lacks filings or fundamentals.

Phase 8C therefore separates **official SEC collection** from **coverage analysis**.

## Stage 1 – collect official SEC data outside GitHub-hosted Actions

Run:

```bash
python scripts/collect_external_evidence_8c_sec_snapshot.py \
  --scanner artifacts/research/latest_scanner.csv \
  --output-dir artifacts/external_evidence/sec_snapshot \
  --user-agent "trading-zentrale research <contact>"
```

The collector:
- reads the official SEC `company_tickers.json`;
- uses exact ticker matches only;
- confirms every candidate ticker through the issuer's current SEC submissions payload;
- downloads the primary submissions payload, every referenced historical submissions block and Company Facts;
- stores the raw JSON without semantic rewriting;
- records source URLs and SHA-256 hashes in `manifest.json`;
- reads no market outcomes and assigns no feature direction.

A ticker candidate that is not confirmed by current SEC submissions is rejected rather than guessed.

## Stage 2 – validate and analyze the frozen snapshot

Run:

```bash
python scripts/run_external_evidence_8c_universe_coverage.py \
  --scanner artifacts/research/latest_scanner.csv \
  --snapshot-dir artifacts/external_evidence/sec_snapshot \
  --output artifacts/research/external_evidence_8c_current_universe_coverage.json
```

The analyzer makes **no network calls**. Before computing coverage it verifies every required snapshot file against its manifest SHA-256. A missing or modified file fails closed.

It then performs:
- complete submission-history assembly;
- accession-to-publication-time resolution;
- Company Facts accession coverage;
- FIRST_RELEASE concept coverage;
- feature-feasibility counts for the frozen 8C-C feature candidates.

## GitHub Actions role

GitHub Actions no longer performs live SEC ingestion. CI is restricted to:
- deterministic unit/contract tests;
- snapshot-integrity tests;
- offline runner self-tests;
- collector CLI validation.

This prevents a hosted-runner network block from becoming a false research conclusion.

## Research boundary

The snapshot is still a **current-universe feasibility measurement**, not a historical-universe backtest. It cannot be used to:
- infer that unmatched symbols have no SEC filings;
- substitute ADR/native listings;
- define historical membership;
- inspect future returns;
- assign positive/negative feature direction;
- promote any feature into Phase 7.
