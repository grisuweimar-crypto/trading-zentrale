import csv
import json
import zipfile
from pathlib import Path

import pytest

from scanner.research.external_evidence.sec_bulk_import import (
    DIRECT_BULK_MODE,
    MIRROR_MODE,
    SecBulkImportError,
    import_sec_bulk_bundle,
)


def _scanner(path: Path, symbols: list[str]) -> Path:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["as_of", "symbol", "name"])
        writer.writeheader()
        for symbol in symbols:
            writer.writerow({"as_of": "2026-09-25", "symbol": symbol, "name": symbol})
    return path


def _write_zip(path: Path, members: dict[str, dict]) -> Path:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in members.items():
            archive.writestr(name, json.dumps(payload, sort_keys=True))
    return path


def _submissions_payload(cik: str, ticker: str, *, history_name: str | None = None) -> dict:
    files = [{"name": history_name}] if history_name else []
    return {
        "cik": str(int(cik)),
        "name": f"{ticker} Corp",
        "tickers": [ticker],
        "exchanges": ["Nasdaq"],
        "filings": {
            "recent": {
                "accessionNumber": [f"{cik[:10]}-26-000001"],
                "form": ["8-K"],
            },
            "files": files,
        },
    }


def test_direct_bulk_import_builds_identity_from_primary_submissions(tmp_path):
    scanner = _scanner(tmp_path / "scanner.csv", ["AAPL"])
    cik = "0000320193"
    history_name = "CIK0000320193-submissions-001.json"
    submissions = _write_zip(
        tmp_path / "submissions.zip",
        {
            f"CIK{cik}.json": _submissions_payload(cik, "AAPL", history_name=history_name),
            history_name: {"accessionNumber": [], "filingDate": []},
        },
    )
    facts = _write_zip(
        tmp_path / "companyfacts.zip",
        {f"CIK{cik}.json": {"cik": str(int(cik)), "entityName": "Apple Inc.", "facts": {}}},
    )

    manifest = import_sec_bulk_bundle(
        scanner_path=scanner,
        submissions_zip=submissions,
        companyfacts_zip=facts,
        output_dir=tmp_path / "out",
        source_mode=DIRECT_BULK_MODE,
        submissions_source_url="https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip",
        companyfacts_source_url="https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
        acquired_at="2026-09-26T07:00:00+00:00",
    )

    assert manifest["source_authority"] == "U.S. SEC EDGAR"
    assert manifest["operator_attested_direct_sec_download"] is True
    assert manifest["coverage"]["scanner_symbol_count"] == 1
    assert manifest["coverage"]["sec_bulk_identity_verified_count"] == 1
    assert manifest["coverage"]["companyfacts_verified_count"] == 1
    assert manifest["eligibility"]["fundamentals_real_data_validation"] is True
    assert manifest["eligibility"]["structured_event_semantic_validation"] is False
    assert manifest["eligibility"]["final_phase8c_completion"] is False
    company = manifest["companies"][0]
    assert company["identity_status"] == "VERIFIED_BY_SEC_BULK_SUBMISSIONS"
    assert company["cik"] == cik
    assert company["history_files"]
    assert company["companyfacts_file"]["sha256"]
    assert (tmp_path / "out" / "bulk_manifest.json").is_file()


def test_mirror_transport_never_unlocks_real_data_validation(tmp_path):
    scanner = _scanner(tmp_path / "scanner.csv", ["AAPL"])
    cik = "0000320193"
    submissions = _write_zip(
        tmp_path / "submissions.zip",
        {f"CIK{cik}.json": _submissions_payload(cik, "AAPL")},
    )
    facts = _write_zip(
        tmp_path / "companyfacts.zip",
        {f"CIK{cik}.json": {"cik": str(int(cik)), "facts": {}}},
    )

    manifest = import_sec_bulk_bundle(
        scanner_path=scanner,
        submissions_zip=submissions,
        companyfacts_zip=facts,
        output_dir=tmp_path / "out",
        source_mode=MIRROR_MODE,
        submissions_source_url="https://example.invalid/submissions.zip",
        companyfacts_source_url="https://example.invalid/companyfacts.zip",
    )

    assert manifest["source_authority"] == "NON_AUTHORITATIVE_TRANSPORT"
    assert manifest["operator_attested_direct_sec_download"] is False
    assert manifest["eligibility"]["fundamentals_real_data_validation"] is False
    assert manifest["eligibility"]["structured_event_semantic_validation"] is False
    assert manifest["eligibility"]["phase7_integration"] is False
    assert manifest["eligibility"]["market_outcome_research"] is False
    assert manifest["companies"][0]["identity_status"] == "VERIFIED_BY_NONAUTHORITATIVE_BULK_COPY"


def test_direct_mode_rejects_non_sec_source_urls(tmp_path):
    scanner = _scanner(tmp_path / "scanner.csv", ["AAPL"])
    cik = "0000320193"
    submissions = _write_zip(tmp_path / "submissions.zip", {f"CIK{cik}.json": _submissions_payload(cik, "AAPL")})
    facts = _write_zip(tmp_path / "companyfacts.zip", {f"CIK{cik}.json": {"cik": str(int(cik)), "facts": {}}})

    with pytest.raises(SecBulkImportError, match="official SEC submissions source URL"):
        import_sec_bulk_bundle(
            scanner_path=scanner,
            submissions_zip=submissions,
            companyfacts_zip=facts,
            output_dir=tmp_path / "out",
            source_mode=DIRECT_BULK_MODE,
            submissions_source_url="https://mirror.invalid/submissions.zip",
            companyfacts_source_url="https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
        )


def test_ambiguous_ticker_is_not_silently_resolved(tmp_path):
    scanner = _scanner(tmp_path / "scanner.csv", ["TEST"])
    cik1 = "0000000001"
    cik2 = "0000000002"
    submissions = _write_zip(
        tmp_path / "submissions.zip",
        {
            f"CIK{cik1}.json": _submissions_payload(cik1, "TEST"),
            f"CIK{cik2}.json": _submissions_payload(cik2, "TEST"),
        },
    )
    facts = _write_zip(
        tmp_path / "companyfacts.zip",
        {
            f"CIK{cik1}.json": {"cik": "1", "facts": {}},
            f"CIK{cik2}.json": {"cik": "2", "facts": {}},
        },
    )

    manifest = import_sec_bulk_bundle(
        scanner_path=scanner,
        submissions_zip=submissions,
        companyfacts_zip=facts,
        output_dir=tmp_path / "out",
        source_mode=DIRECT_BULK_MODE,
        submissions_source_url="https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip",
        companyfacts_source_url="https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
    )

    company = manifest["companies"][0]
    assert company["identity_status"] == "AMBIGUOUS"
    assert company["candidate_ciks"] == [cik1, cik2]
    assert manifest["coverage"]["sec_bulk_identity_verified_count"] == 0
