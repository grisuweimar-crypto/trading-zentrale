import csv
import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.filing_documents import (
    FilingDocumentContractError,
    full_submission_text_url,
    research_window_from_history,
    select_content_filings,
)
from scanner.research.external_evidence.sec_snapshot import (
    SNAPSHOT_SCHEMA,
    SecSnapshotError,
    validate_snapshot_bundle,
    write_bytes_with_digest,
    write_json_with_digest,
)


def test_research_window_uses_history_dates_and_buffer(tmp_path):
    path = tmp_path / "history.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "symbol"])
        writer.writeheader()
        writer.writerow({"date": "2026-04-10", "symbol": "AAPL"})
        writer.writerow({"date": "2026-09-25", "symbol": "MSFT"})
    window = research_window_from_history(path, lookback_days=365)
    assert window["observed_start"] == "2026-04-10"
    assert window["observed_end"] == "2026-09-25"
    assert window["acquisition_start"] == "2025-04-10"
    assert window["acquisition_end"] == "2026-09-25"


def test_research_window_rejects_negative_buffer(tmp_path):
    path = tmp_path / "history.csv"
    path.write_text("date,symbol\n2026-04-10,AAPL\n", encoding="utf-8")
    with pytest.raises(FilingDocumentContractError, match="lookback_days"):
        research_window_from_history(path, lookback_days=-1)


def test_full_submission_text_url_is_accession_bound():
    url = full_submission_text_url("0000320193", "0000320193-26-000001")
    assert url == (
        "https://www.sec.gov/Archives/edgar/data/320193/"
        "000032019326000001/0000320193-26-000001.txt"
    )


def test_content_selection_uses_valid_from_and_allowed_forms():
    rows = [
        {
            "cik": "0000320193",
            "accession_number": "A",
            "form": "8-K",
            "valid_from": "2026-05-01T13:00:00+00:00",
            "filed_date": "2026-05-01",
        },
        {
            "cik": "0000320193",
            "accession_number": "B",
            "form": "10-Q",
            "valid_from": "2026-05-02T13:00:00+00:00",
            "filed_date": "2026-05-02",
        },
        {
            "cik": "0000320193",
            "accession_number": "C",
            "form": "6-K",
            "valid_from": "2025-01-01T13:00:00+00:00",
            "filed_date": "2025-01-01",
        },
    ]
    selected = select_content_filings(
        rows,
        acquisition_start="2026-01-01",
        acquisition_end="2026-09-25",
    )
    assert [row["accession_number"] for row in selected] == ["A"]
    assert selected[0]["content_source_url"].endswith("A.txt")


def test_snapshot_validates_content_document_digest(tmp_path):
    bundle = tmp_path / "snapshot"
    ticker_digest = write_json_with_digest(bundle / "raw/company_tickers.json", {})
    submissions_digest = write_json_with_digest(
        bundle / "raw/companies/0000320193/submissions.json",
        {"cik": "320193", "tickers": ["AAPL"], "filings": {"recent": {"accessionNumber": []}, "files": []}},
    )
    facts_digest = write_json_with_digest(
        bundle / "raw/companies/0000320193/companyfacts.json",
        {"cik": "320193", "facts": {}},
    )
    document_path = bundle / "raw/companies/0000320193/filing_content/x/x.txt"
    document_digest = write_bytes_with_digest(document_path, b"<SEC-DOCUMENT>example</SEC-DOCUMENT>")

    manifest = {
        "schema_version": SNAPSHOT_SCHEMA,
        "source_authority": "U.S. SEC EDGAR",
        "created_at": "2026-09-26T00:00:00+00:00",
        "scanner_as_of": "2026-09-25",
        "company_tickers_file": {"path": "raw/company_tickers.json", "sha256": ticker_digest, "source_url": "sec"},
        "companies": [
            {
                "symbol": "AAPL",
                "cik": "0000320193",
                "identity_status": "VERIFIED_BY_SEC_SUBMISSIONS",
                "submissions_file": {"path": "raw/companies/0000320193/submissions.json", "sha256": submissions_digest, "source_url": "sec"},
                "history_files": [],
                "companyfacts_file": {"path": "raw/companies/0000320193/companyfacts.json", "sha256": facts_digest, "source_url": "sec"},
                "content_filings": [
                    {
                        "accession_number": "x",
                        "document_file": {
                            "path": "raw/companies/0000320193/filing_content/x/x.txt",
                            "sha256": document_digest,
                            "source_url": "sec"
                        }
                    }
                ]
            }
        ],
        "market_outcomes_read": False,
        "direction_assigned": False,
    }
    write_json_with_digest(bundle / "manifest.json", manifest)
    summary = validate_snapshot_bundle(bundle)
    assert summary["verified_content_document_count"] == 1

    document_path.write_bytes(b"tampered")
    with pytest.raises(SecSnapshotError, match="digest mismatch"):
        validate_snapshot_bundle(bundle)
