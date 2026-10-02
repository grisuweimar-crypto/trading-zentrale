from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest

from scanner.research.external_evidence.sec_insider_bulk import (
    SecInsiderBulkError,
    import_sec_insider_quarter,
)


def _write_json(path: Path, payload: dict) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def _bundle(tmp_path: Path) -> Path:
    bundle = tmp_path / "bundle"
    cik = "0000320193"
    submissions = {
        "cik": 320193,
        "name": "Example Corp",
        "tickers": ["EXM"],
        "exchanges": ["Nasdaq"],
        "filings": {
            "recent": {
                "accessionNumber": [
                    "0000320193-26-000111",
                    "0000320193-26-000222",
                ],
                "form": ["4", "4/A"],
                "filingDate": ["2026-04-01", "2026-04-03"],
                "acceptanceDateTime": [
                    "2026-04-01T16:30:00-04:00",
                    "2026-04-03T17:15:00-04:00",
                ],
                "reportDate": ["2026-04-01", "2026-04-01"],
                "primaryDocument": ["ownership.xml", "ownership.xml"],
                "primaryDocDescription": ["FORM 4", "FORM 4/A"],
            },
            "files": [],
        },
    }
    sub_rel = f"raw/companies/{cik}/submissions.json"
    sub_sha = _write_json(bundle / sub_rel, submissions)
    manifest = {
        "schema_version": "external_evidence_8c_sec_bulk_bundle_v1",
        "source_mode": "DIRECT_SEC_BULK_OPERATOR_ATTESTED",
        "source_authority": "U.S. SEC EDGAR",
        "operator_attested_direct_sec_download": True,
        "market_outcomes_read": False,
        "companies": [
            {
                "symbol": "EXM",
                "scanner_as_of": "2026-09-25T19:14:22+00:00",
                "cik": cik,
                "identity_status": "VERIFIED_BY_SEC_BULK_SUBMISSIONS",
                "submissions_file": {"path": sub_rel, "sha256": sub_sha},
                "history_files": [],
            }
        ],
    }
    _write_json(bundle / "bulk_manifest.json", manifest)
    return bundle


def _tsv(rows: list[dict[str, object]], fields: list[str]) -> bytes:
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer, fieldnames=fields, delimiter="\t", lineterminator="\n"
    )
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def _insider_zip(tmp_path: Path) -> Path:
    path = tmp_path / "2026q2_form345.zip"
    submission_fields = [
        "ACCESSION_NUMBER",
        "FILING_DATE",
        "PERIOD_OF_REPORT",
        "DOCUMENT_TYPE",
        "ISSUERCIK",
        "ISSUERNAME",
        "ISSUERTRADINGSYMBOL",
        "AFF10B5ONE",
    ]
    submissions = [
        {
            "ACCESSION_NUMBER": "0000320193-26-000111",
            "FILING_DATE": "01-APR-2026",
            "PERIOD_OF_REPORT": "01-APR-2026",
            "DOCUMENT_TYPE": "4",
            "ISSUERCIK": "320193",
            "ISSUERNAME": "Example Corp",
            "ISSUERTRADINGSYMBOL": "OLD",
            "AFF10B5ONE": "0",
        },
        {
            "ACCESSION_NUMBER": "0000320193-26-000222",
            "FILING_DATE": "03-APR-2026",
            "PERIOD_OF_REPORT": "01-APR-2026",
            "DOCUMENT_TYPE": "4/A",
            "ISSUERCIK": "320193",
            "ISSUERNAME": "Example Corp",
            "ISSUERTRADINGSYMBOL": "OLD",
            "AFF10B5ONE": "1",
        },
        {
            "ACCESSION_NUMBER": "0000320193-26-999999",
            "FILING_DATE": "04-APR-2026",
            "PERIOD_OF_REPORT": "04-APR-2026",
            "DOCUMENT_TYPE": "4",
            "ISSUERCIK": "320193",
            "ISSUERNAME": "Example Corp",
            "ISSUERTRADINGSYMBOL": "OLD",
            "AFF10B5ONE": "0",
        },
    ]
    owner_fields = [
        "ACCESSION_NUMBER",
        "RPTOWNERCIK",
        "RPTOWNERNAME",
        "RPTOWNER_RELATIONSHIP",
        "RPTOWNER_TITLE",
        "RPTOWNER_TXT",
    ]
    owners = [
        {
            "ACCESSION_NUMBER": "0000320193-26-000111",
            "RPTOWNERCIK": "123456",
            "RPTOWNERNAME": "Jane Example",
            "RPTOWNER_RELATIONSHIP": "OFFICER",
            "RPTOWNER_TITLE": "CFO",
            "RPTOWNER_TXT": "",
        },
        {
            "ACCESSION_NUMBER": "0000320193-26-000222",
            "RPTOWNERCIK": "123456",
            "RPTOWNERNAME": "Jane Example",
            "RPTOWNER_RELATIONSHIP": "OFFICER",
            "RPTOWNER_TITLE": "CFO",
            "RPTOWNER_TXT": "",
        },
    ]
    trans_fields = [
        "ACCESSION_NUMBER",
        "NONDERIV_TRANS_SK",
        "SECURITY_TITLE",
        "TRANS_DATE",
        "TRANS_FORM_TYPE",
        "TRANS_CODE",
        "EQUITY_SWAP_INVOLVED",
        "TRANS_TIMELINESS",
        "TRANS_SHARES",
        "TRANS_PRICEPERSHARE",
        "TRANS_ACQUIRED_DISP_CD",
        "SHRS_OWND_FOLWNG_TRANS",
        "DIRECT_INDIRECT_OWNERSHIP",
        "NATURE_OF_OWNERSHIP",
    ]
    transactions = [
        {
            "ACCESSION_NUMBER": "0000320193-26-000111",
            "NONDERIV_TRANS_SK": "1",
            "SECURITY_TITLE": "Common Stock",
            "TRANS_DATE": "01-APR-2026",
            "TRANS_FORM_TYPE": "4",
            "TRANS_CODE": "P",
            "EQUITY_SWAP_INVOLVED": "0",
            "TRANS_TIMELINESS": "",
            "TRANS_SHARES": "100",
            "TRANS_PRICEPERSHARE": "12.50",
            "TRANS_ACQUIRED_DISP_CD": "A",
            "SHRS_OWND_FOLWNG_TRANS": "1000",
            "DIRECT_INDIRECT_OWNERSHIP": "D",
            "NATURE_OF_OWNERSHIP": "",
        },
        {
            "ACCESSION_NUMBER": "0000320193-26-000111",
            "NONDERIV_TRANS_SK": "2",
            "SECURITY_TITLE": "Common Stock",
            "TRANS_DATE": "01-APR-2026",
            "TRANS_FORM_TYPE": "4",
            "TRANS_CODE": "A",
            "EQUITY_SWAP_INVOLVED": "0",
            "TRANS_TIMELINESS": "",
            "TRANS_SHARES": "500",
            "TRANS_PRICEPERSHARE": "0",
            "TRANS_ACQUIRED_DISP_CD": "A",
            "SHRS_OWND_FOLWNG_TRANS": "1500",
            "DIRECT_INDIRECT_OWNERSHIP": "D",
            "NATURE_OF_OWNERSHIP": "",
        },
        {
            "ACCESSION_NUMBER": "0000320193-26-000222",
            "NONDERIV_TRANS_SK": "3",
            "SECURITY_TITLE": "Common Stock",
            "TRANS_DATE": "01-APR-2026",
            "TRANS_FORM_TYPE": "4",
            "TRANS_CODE": "S",
            "EQUITY_SWAP_INVOLVED": "0",
            "TRANS_TIMELINESS": "",
            "TRANS_SHARES": "25",
            "TRANS_PRICEPERSHARE": "20",
            "TRANS_ACQUIRED_DISP_CD": "D",
            "SHRS_OWND_FOLWNG_TRANS": "975",
            "DIRECT_INDIRECT_OWNERSHIP": "D",
            "NATURE_OF_OWNERSHIP": "",
        },
        {
            "ACCESSION_NUMBER": "0000320193-26-999999",
            "NONDERIV_TRANS_SK": "4",
            "SECURITY_TITLE": "Common Stock",
            "TRANS_DATE": "04-APR-2026",
            "TRANS_FORM_TYPE": "4",
            "TRANS_CODE": "P",
            "EQUITY_SWAP_INVOLVED": "0",
            "TRANS_TIMELINESS": "",
            "TRANS_SHARES": "10",
            "TRANS_PRICEPERSHARE": "10",
            "TRANS_ACQUIRED_DISP_CD": "A",
            "SHRS_OWND_FOLWNG_TRANS": "10",
            "DIRECT_INDIRECT_OWNERSHIP": "D",
            "NATURE_OF_OWNERSHIP": "",
        },
    ]
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("SUBMISSION.tsv", _tsv(submissions, submission_fields))
        zf.writestr("REPORTINGOWNER.tsv", _tsv(owners, owner_fields))
        zf.writestr("NONDERIV_TRANS.tsv", _tsv(transactions, trans_fields))
    return path


def _source_url() -> str:
    return (
        "https://www.sec.gov/files/datastandardsinnovation/data/"
        "insider-transactions-data-sets/2026q2_form345.zip"
    )


def test_sec_insider_bulk_exact_accession_pit_join_and_ps_scope(tmp_path: Path) -> None:
    payload = import_sec_insider_quarter(
        insider_zip_path=_insider_zip(tmp_path),
        sec_bulk_bundle_dir=_bundle(tmp_path),
        source_url=_source_url(),
        quarter_label="2026Q2",
    )
    assert payload["guards"]["market_outcomes_read"] is False
    assert payload["guards"]["market_direction_assigned"] is False
    assert payload["guards"]["current_ticker_used_as_historical_identity"] is False
    assert payload["counts"]["p_s_evidence_row_count"] == 2
    assert payload["counts"]["high_precision_discretionary_candidate_count"] == 1
    assert payload["counts"]["unmatched_verified_accession_count"] == 1
    assert payload["excluded_transaction_code_counts"]["A"] == 1

    purchase = next(row for row in payload["rows"] if row["transaction_code"] == "P")
    assert purchase["scanner_symbol"] == "EXM"
    assert purchase["issuer_trading_symbol_reported"] == "OLD"
    assert purchase["valid_from"] == "2026-04-01T16:30:00-04:00"
    assert purchase["strict_pit_eligible"] is True
    assert purchase["candidate_status"] == "P_S_HIGH_PRECISION_DISCRETIONARY_CANDIDATE"
    assert purchase["discretionary_semantics_eligible"] is True
    assert purchase["transaction_value_when_price_known"] == pytest.approx(1250.0)
    assert len(purchase["reporting_owners"]) == 1

    sale = next(row for row in payload["rows"] if row["transaction_code"] == "S")
    assert sale["amendment"] is True
    assert sale["aff10b5one"] is True
    assert sale["candidate_status"] == "EXCLUDED_10B5_1_PLAN"
    assert sale["discretionary_semantics_eligible"] is False
    assert "AFF10B5ONE_TRUE" in sale["reason_codes"]


def test_sec_insider_bulk_rejects_non_sec_source_url(tmp_path: Path) -> None:
    with pytest.raises(SecInsiderBulkError, match="official sec.gov"):
        import_sec_insider_quarter(
            insider_zip_path=_insider_zip(tmp_path),
            sec_bulk_bundle_dir=_bundle(tmp_path),
            source_url="https://example.com/2026q2_form345.zip",
            quarter_label="2026Q2",
        )


def _rewrite_transactions(zip_path: Path, rows: list[dict[str, object]]) -> None:
    with zipfile.ZipFile(zip_path, "r") as zf:
        submission = zf.read("SUBMISSION.tsv")
        owners = zf.read("REPORTINGOWNER.tsv")
    fields = [
        "ACCESSION_NUMBER",
        "NONDERIV_TRANS_SK",
        "SECURITY_TITLE",
        "TRANS_DATE",
        "TRANS_FORM_TYPE",
        "TRANS_CODE",
        "EQUITY_SWAP_INVOLVED",
        "TRANS_TIMELINESS",
        "TRANS_SHARES",
        "TRANS_PRICEPERSHARE",
        "TRANS_ACQUIRED_DISP_CD",
        "SHRS_OWND_FOLWNG_TRANS",
        "DIRECT_INDIRECT_OWNERSHIP",
        "NATURE_OF_OWNERSHIP",
    ]
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("SUBMISSION.tsv", submission)
        zf.writestr("REPORTINGOWNER.tsv", owners)
        zf.writestr("NONDERIV_TRANS.tsv", _tsv(rows, fields))


def _base_purchase(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ACCESSION_NUMBER": "0000320193-26-000111",
        "NONDERIV_TRANS_SK": "1",
        "SECURITY_TITLE": "Common Stock",
        "TRANS_DATE": "01-APR-2026",
        "TRANS_FORM_TYPE": "4",
        "TRANS_CODE": "P",
        "EQUITY_SWAP_INVOLVED": "0",
        "TRANS_TIMELINESS": "",
        "TRANS_SHARES": "100",
        "TRANS_PRICEPERSHARE": "12.50",
        "TRANS_ACQUIRED_DISP_CD": "A",
        "SHRS_OWND_FOLWNG_TRANS": "900",
        "DIRECT_INDIRECT_OWNERSHIP": "D",
        "NATURE_OF_OWNERSHIP": "",
    }
    row.update(overrides)
    return row


def test_sec_insider_bulk_marks_inconsistent_ps_acquired_disposed(tmp_path: Path) -> None:
    zip_path = _insider_zip(tmp_path)
    _rewrite_transactions(zip_path, [_base_purchase(TRANS_ACQUIRED_DISP_CD="D")])
    payload = import_sec_insider_quarter(
        insider_zip_path=zip_path,
        sec_bulk_bundle_dir=_bundle(tmp_path),
        source_url=_source_url(),
        quarter_label="2026Q2",
    )
    assert payload["rows"][0]["candidate_status"] == "CONFLICTING_ACQUIRED_DISPOSED_CODE"
    assert payload["rows"][0]["discretionary_semantics_eligible"] is False


def test_sec_insider_bulk_requires_form4_transaction_for_discretionary_scope(tmp_path: Path) -> None:
    zip_path = _insider_zip(tmp_path)
    _rewrite_transactions(zip_path, [_base_purchase(TRANS_FORM_TYPE="5")])
    payload = import_sec_insider_quarter(
        insider_zip_path=zip_path,
        sec_bulk_bundle_dir=_bundle(tmp_path),
        source_url=_source_url(),
        quarter_label="2026Q2",
    )
    assert payload["rows"][0]["candidate_status"] == "EXCLUDED_NON_FORM4_TRANSACTION"
    assert payload["rows"][0]["discretionary_semantics_eligible"] is False


def test_sec_insider_bulk_excludes_equity_swap_from_discretionary_scope(tmp_path: Path) -> None:
    zip_path = _insider_zip(tmp_path)
    _rewrite_transactions(zip_path, [_base_purchase(EQUITY_SWAP_INVOLVED="1")])
    payload = import_sec_insider_quarter(
        insider_zip_path=zip_path,
        sec_bulk_bundle_dir=_bundle(tmp_path),
        source_url=_source_url(),
        quarter_label="2026Q2",
    )
    assert payload["rows"][0]["candidate_status"] == "EXCLUDED_EQUITY_SWAP"
    assert payload["rows"][0]["discretionary_semantics_eligible"] is False
