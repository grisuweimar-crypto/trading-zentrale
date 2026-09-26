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


def _bundle(tmp_path: Path, *, authoritative_bootstrap: bool = True) -> Path:
    bundle = tmp_path / "snapshot"
    ticker_digest = write_json_with_digest(
        bundle / "raw/company_tickers.json",
        {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}},
    )
    submissions_digest = write_json_with_digest(
        bundle / "raw/companies/0000320193/submissions.json",
        {
            "cik": "320193",
            "tickers": ["AAPL"],
            "filings": {"recent": {"accessionNumber": []}, "files": []},
        },
    )
    facts_digest = write_json_with_digest(
        bundle / "raw/companies/0000320193/companyfacts.json",
        {"cik": "320193", "entityName": "Apple Inc.", "facts": {}},
    )
    bootstrap = {
        "mode": "SEC_OFFICIAL_LIVE" if authoritative_bootstrap else "NON_AUTHORITATIVE_MIRROR_BOOTSTRAP",
        "source_url": (
            "https://www.sec.gov/files/company_tickers.json"
            if authoritative_bootstrap
            else "https://raw.githubusercontent.com/example/mirror/company_tickers.json"
        ),
        "authoritative": authoritative_bootstrap,
        "requires_current_submissions_validation": True,
        "official_source_url": "https://www.sec.gov/files/company_tickers.json",
    }
    if not authoritative_bootstrap:
        bootstrap["identity_authority"] = "CURRENT_SEC_SUBMISSIONS_ONLY"

    manifest = {
        "schema_version": SNAPSHOT_SCHEMA,
        "source_authority": "U.S. SEC EDGAR",
        "created_at": "2026-09-26T00:00:00+00:00",
        "scanner_as_of": "2026-09-25",
        "ticker_bootstrap": bootstrap,
        "company_tickers_file": _spec("raw/company_tickers.json", ticker_digest),
        "companies": [
            {
                "symbol": "AAPL",
                "cik": "0000320193",
                "identity_status": "VERIFIED_BY_SEC_SUBMISSIONS",
                "identity_authority": "CURRENT_SEC_SUBMISSIONS_ONLY",
                "submissions_file": _spec("raw/companies/0000320193/submissions.json", submissions_digest),
                "history_files": [],
                "companyfacts_file": _spec("raw/companies/0000320193/companyfacts.json", facts_digest),
                "content_filings": [],
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
    assert summary["ticker_bootstrap_mode"] == "SEC_OFFICIAL_LIVE"
    assert summary["market_outcomes_read"] is False


def test_non_authoritative_bootstrap_is_allowed_only_with_official_submissions_authority(tmp_path):
    summary = validate_snapshot_bundle(_bundle(tmp_path, authoritative_bootstrap=False))
    assert summary["verified_company_count"] == 1
    assert summary["ticker_bootstrap_mode"] == "NON_AUTHORITATIVE_MIRROR_BOOTSTRAP"

    bundle = _bundle(tmp_path / "bad", authoritative_bootstrap=False)
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["ticker_bootstrap"]["identity_authority"] = "MIRROR"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(SecSnapshotError, match="CURRENT_SEC_SUBMISSIONS_ONLY"):
        validate_snapshot_bundle(bundle)


def test_snapshot_rechecks_ticker_against_hashed_sec_submissions(tmp_path):
    bundle = _bundle(tmp_path, authoritative_bootstrap=False)
    submissions_path = bundle / "raw/companies/0000320193/submissions.json"
    submissions = json.loads(submissions_path.read_text(encoding="utf-8"))
    submissions["tickers"] = ["MSFT"]
    digest = write_json_with_digest(submissions_path, submissions)

    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["companies"][0]["submissions_file"]["sha256"] = digest
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(SecSnapshotError, match="not confirmed"):
        validate_snapshot_bundle(bundle)


def test_snapshot_rechecks_cik_against_hashed_sec_submissions(tmp_path):
    bundle = _bundle(tmp_path, authoritative_bootstrap=False)
    submissions_path = bundle / "raw/companies/0000320193/submissions.json"
    submissions = json.loads(submissions_path.read_text(encoding="utf-8"))
    submissions["cik"] = "789019"
    digest = write_json_with_digest(submissions_path, submissions)

    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["companies"][0]["submissions_file"]["sha256"] = digest
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(SecSnapshotError, match="not confirmed"):
        validate_snapshot_bundle(bundle)


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
