import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.sec_edgar import SecEdgarContractError
from scanner.research.external_evidence.sec_history import (
    assemble_full_submission_history,
    companyfacts_accession_coverage,
    historical_submission_file_specs,
    historical_submission_file_url,
)

ROOT = Path(__file__).resolve().parents[1]


def load_contract():
    return json.loads(
        (ROOT / "configs" / "external_evidence_8c_coverage_contract_v1.json").read_text(encoding="utf-8")
    )


def primary_fixture():
    return {
        "cik": "0000320193",
        "name": "Example Corp",
        "tickers": ["EXM"],
        "exchanges": ["Nasdaq"],
        "filings": {
            "recent": {
                "accessionNumber": ["0000320193-26-000300"],
                "filingDate": ["2026-08-20"],
                "reportDate": ["2026-06-30"],
                "acceptanceDateTime": ["2026-08-20T20:04:00.000Z"],
                "form": ["10-Q"],
                "items": [""],
                "primaryDocument": ["q2.htm"],
                "primaryDocDescription": ["10-Q"],
                "isXBRL": [1],
                "isInlineXBRL": [1],
            },
            "files": [
                {
                    "name": "CIK0000320193-submissions-001.json",
                    "filingCount": 1,
                    "filingFrom": "2025-01-01",
                    "filingTo": "2025-12-31",
                },
                {
                    "name": "CIK0000320193-submissions-002.json",
                    "filingCount": 1,
                    "filingFrom": "2024-01-01",
                    "filingTo": "2024-12-31",
                },
            ],
        },
    }


def historical_fixture(accession, filed, accepted, form="10-K", document="annual.htm"):
    return {
        "accessionNumber": [accession],
        "filingDate": [filed],
        "reportDate": [filed],
        "acceptanceDateTime": [accepted],
        "form": [form],
        "items": [""],
        "primaryDocument": [document],
        "primaryDocDescription": [form],
        "isXBRL": [1],
        "isInlineXBRL": [1],
    }


def full_history_payloads():
    return {
        "CIK0000320193-submissions-001.json": historical_fixture(
            "0000320193-25-000200", "2025-10-30", "2025-10-30T20:00:00.000Z"
        ),
        "CIK0000320193-submissions-002.json": historical_fixture(
            "0000320193-24-000100", "2024-10-31", "2024-10-31T20:00:00.000Z"
        ),
    }


def test_coverage_contract_blocks_recent_only_backtests():
    c = load_contract()
    assert c["submission_history"]["recent_block_alone_is_complete_history"] is False
    assert c["submission_history"]["all_filings_files_must_be_loaded"] is True
    assert c["research_gate"]["history_complete_required"] is True
    assert "use_only_filings_recent_for_historical_backtest" in c["forbidden_shortcuts"]


def test_history_specs_are_explicit_and_safe():
    specs = historical_submission_file_specs(primary_fixture())
    assert [x["name"] for x in specs] == [
        "CIK0000320193-submissions-001.json",
        "CIK0000320193-submissions-002.json",
    ]
    assert specs[0]["url"].endswith("/submissions/CIK0000320193-submissions-001.json")
    with pytest.raises(SecEdgarContractError):
        historical_submission_file_url("../bad.json")


def test_missing_history_file_fails_closed_when_complete_history_required():
    payloads = full_history_payloads()
    payloads.pop("CIK0000320193-submissions-002.json")
    with pytest.raises(SecEdgarContractError, match="Incomplete SEC submission history"):
        assemble_full_submission_history(
            primary_fixture(),
            historical_payloads=payloads,
            require_complete=True,
        )


def test_full_history_combines_recent_and_all_referenced_files():
    result = assemble_full_submission_history(
        primary_fixture(),
        historical_payloads=full_history_payloads(),
        require_complete=True,
    )
    coverage = result["coverage"]
    assert coverage["history_complete"] is True
    assert coverage["history_files_expected"] == 2
    assert coverage["history_files_loaded"] == 2
    assert coverage["filing_count"] == 3
    assert coverage["unique_accession_count"] == 3
    assert coverage["earliest_filing_date"] == "2024-10-31"
    assert coverage["latest_filing_date"] == "2026-08-20"
    assert coverage["exact_publication_timestamp_count"] == 3
    assert coverage["research_ready"] is True


def test_partial_history_is_explicit_when_allowed_for_diagnostics():
    payloads = full_history_payloads()
    payloads.pop("CIK0000320193-submissions-002.json")
    result = assemble_full_submission_history(
        primary_fixture(),
        historical_payloads=payloads,
        require_complete=False,
    )
    assert result["coverage"]["history_complete"] is False
    assert result["coverage"]["research_ready"] is False
    assert "INCOMPLETE_HISTORY" in result["coverage"]["reason_codes"]


def test_conflicting_duplicate_accession_is_never_latest_row_wins():
    payloads = full_history_payloads()
    payloads["CIK0000320193-submissions-001.json"] = historical_fixture(
        "0000320193-26-000300",
        "2025-10-30",
        "2025-10-30T20:00:00.000Z",
        document="conflicting.htm",
    )
    with pytest.raises(SecEdgarContractError, match="Conflicting metadata"):
        assemble_full_submission_history(
            primary_fixture(),
            historical_payloads=payloads,
            require_complete=True,
        )


def test_companyfacts_accession_coverage_never_hides_unresolved_rows():
    rows = [
        {
            "accession_number": "0001",
            "valid_from": "2026-01-01T00:00:00+00:00",
            "reason_codes": [],
        },
        {
            "accession_number": "0002",
            "valid_from": None,
            "reason_codes": ["ACCESSION_NOT_IN_SUBMISSION_INDEX"],
        },
        {
            "accession_number": None,
            "valid_from": None,
            "reason_codes": ["MISSING_ACCESSION"],
        },
    ]
    result = companyfacts_accession_coverage(rows)
    assert result["fact_row_count"] == 3
    assert result["resolved_accession_count"] == 1
    assert result["unresolved_accession_count"] == 2
    assert result["missing_accession_count"] == 1
    assert result["accession_resolution_rate"] == pytest.approx(1 / 3)
    assert result["research_ready"] is False
