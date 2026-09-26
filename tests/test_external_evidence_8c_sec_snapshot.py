import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.sec_snapshot import (
    SNAPSHOT_SCHEMA,
    SecSnapshotError,
    validate_snapshot_bundle,
    write_json_with_digest,
)


def _spec(path: str, digest: str) -> dict[str, str]:
    return {"path": path, "sha256": digest, "source_url": "https://data.sec.gov/example"}


def _bundle(tmp_path: Path) -> Path:
    bundle = tmp_path / "snapshot"
    ticker_digest = write_json_with_digest(
        bundle / "raw/company_tickers.json",
        {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}},
    )
    submissions_digest = write_json_with_digest(
        bundle / "raw/companies/0000320193/submissions.json",
        {"cik": "320193", "tickers": ["AAPL"], "filings": {"recent": {"accessionNumber": []}, "files": []}},
    )
    facts_digest = write_json_with_digest(
        bundle / "raw/companies/0000320193/companyfacts.json",
        {"cik": "320193", "entityName": "Apple Inc.", "facts": {}},
    )
    manifest = {
        "schema_version": SNAPSHOT_SCHEMA,
        "source_authority": "U.S. SEC EDGAR",
        "created_at": "2026-09-26T00:00:00+00:00",
        "scanner_as_of": "2026-09-25",
        "company_tickers_file": _spec("raw/company_tickers.json", ticker_digest),
        "companies": [
            {
                "symbol": "AAPL",
                "cik": "0000320193",
                "identity_status": "VERIFIED_BY_SEC_SUBMISSIONS",
                "submissions_file": _spec("raw/companies/0000320193/submissions.json", submissions_digest),
                "history_files": [],
                "companyfacts_file": _spec("raw/companies/0000320193/companyfacts.json", facts_digest),
                "reason_codes": [],
            }
        ],
        "market_outcomes_read": False,
        "direction_assigned": False,
    }
    write_json_with_digest(bundle / "manifest.json", manifest)
    return bundle


def test_valid_snapshot_bundle_verifies_all_required_files(tmp_path):
    summary = validate_snapshot_bundle(_bundle(tmp_path))
    assert summary["verified_company_count"] == 1
    assert summary["verified_file_count"] == 3
    assert summary["market_outcomes_read"] is False


def test_snapshot_digest_mismatch_fails_closed(tmp_path):
    bundle = _bundle(tmp_path)
    target = bundle / "raw/companies/0000320193/companyfacts.json"
    target.write_text(json.dumps({"tampered": True}), encoding="utf-8")
    with pytest.raises(SecSnapshotError, match="digest mismatch"):
        validate_snapshot_bundle(bundle)


def test_snapshot_rejects_non_sec_authority(tmp_path):
    bundle = _bundle(tmp_path)
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["source_authority"] = "mirror"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(SecSnapshotError, match="source_authority"):
        validate_snapshot_bundle(bundle)


def test_snapshot_rejects_outcome_contamination(tmp_path):
    bundle = _bundle(tmp_path)
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["market_outcomes_read"] = True
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(SecSnapshotError, match="outcome-blind"):
        validate_snapshot_bundle(bundle)
