from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from pathlib import Path

from scanner.research.external_evidence.sec_insider_bulk import import_sec_insider_quarter


ACCESSION = "0001183681-12-000043"
MSFT_CIK = "0000789019"
META_CIK = "0001326801"


def _write_json(path: Path, payload: dict) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def _company(bundle: Path, *, symbol: str, cik: str, include_accession: bool) -> dict:
    accessions = [ACCESSION] if include_accession else []
    submissions = {
        "cik": int(cik),
        "name": symbol,
        "tickers": [symbol],
        "exchanges": ["Nasdaq"],
        "filings": {
            "recent": {
                "accessionNumber": accessions,
                "form": ["4"] if include_accession else [],
                "filingDate": ["2012-05-22"] if include_accession else [],
                "acceptanceDateTime": ["2012-05-22T18:01:00-04:00"] if include_accession else [],
                "reportDate": ["2012-05-22"] if include_accession else [],
                "primaryDocument": ["ownership.xml"] if include_accession else [],
                "primaryDocDescription": ["FORM 4"] if include_accession else [],
            },
            "files": [],
        },
    }
    rel = f"raw/companies/{cik}/submissions.json"
    sha = _write_json(bundle / rel, submissions)
    return {
        "symbol": symbol,
        "cik": cik,
        "identity_status": "VERIFIED_BY_SEC_BULK_SUBMISSIONS",
        "submissions_file": {"path": rel, "sha256": sha},
        "history_files": [],
    }


def _bundle(tmp_path: Path, *, msft_has_accession: bool = True) -> Path:
    bundle = tmp_path / "bundle"
    companies = [
        _company(bundle, symbol="META", cik=META_CIK, include_accession=True),
        _company(bundle, symbol="MSFT", cik=MSFT_CIK, include_accession=msft_has_accession),
    ]
    manifest = {
        "schema_version": "external_evidence_8c_sec_bulk_bundle_v1",
        "source_mode": "DIRECT_SEC_BULK_OPERATOR_ATTESTED",
        "source_authority": "U.S. SEC EDGAR",
        "operator_attested_direct_sec_download": True,
        "market_outcomes_read": False,
        "companies": companies,
    }
    _write_json(bundle / "bulk_manifest.json", manifest)
    return bundle


def _tsv(rows: list[dict[str, object]], fields: list[str]) -> bytes:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields, delimiter="\t", lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def _insider_zip(tmp_path: Path) -> Path:
    path = tmp_path / "2026q2_form345.zip"
    submissions = [{
        "ACCESSION_NUMBER": ACCESSION,
        "FILING_DATE": "22-MAY-2012",
        "PERIOD_OF_REPORT": "22-MAY-2012",
        "DOCUMENT_TYPE": "4",
        "ISSUERCIK": "789019",
        "ISSUERNAME": "MICROSOFT CORP",
        "ISSUERTRADINGSYMBOL": "MSFT",
        "AFF10B5ONE": "0",
    }]
    owners = [{
        "ACCESSION_NUMBER": ACCESSION,
        "RPTOWNERCIK": "1183681",
        "RPTOWNERNAME": "Reporting Owner",
        "RPTOWNER_RELATIONSHIP": "OFFICER",
        "RPTOWNER_TITLE": "Officer",
        "RPTOWNER_TXT": "",
    }]
    transactions = [{
        "ACCESSION_NUMBER": ACCESSION,
        "NONDERIV_TRANS_SK": "1",
        "SECURITY_TITLE": "Common Stock",
        "TRANS_DATE": "22-MAY-2012",
        "TRANS_FORM_TYPE": "4",
        "TRANS_CODE": "P",
        "EQUITY_SWAP_INVOLVED": "0",
        "TRANS_TIMELINESS": "",
        "TRANS_SHARES": "10",
        "TRANS_PRICEPERSHARE": "30",
        "TRANS_ACQUIRED_DISP_CD": "A",
        "SHRS_OWND_FOLWNG_TRANS": "100",
        "DIRECT_INDIRECT_OWNERSHIP": "D",
        "NATURE_OF_OWNERSHIP": "",
    }]
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "SUBMISSION.tsv",
            _tsv(submissions, [
                "ACCESSION_NUMBER", "FILING_DATE", "PERIOD_OF_REPORT", "DOCUMENT_TYPE",
                "ISSUERCIK", "ISSUERNAME", "ISSUERTRADINGSYMBOL", "AFF10B5ONE",
            ]),
        )
        zf.writestr(
            "REPORTINGOWNER.tsv",
            _tsv(owners, [
                "ACCESSION_NUMBER", "RPTOWNERCIK", "RPTOWNERNAME",
                "RPTOWNER_RELATIONSHIP", "RPTOWNER_TITLE", "RPTOWNER_TXT",
            ]),
        )
        zf.writestr(
            "NONDERIV_TRANS.tsv",
            _tsv(transactions, [
                "ACCESSION_NUMBER", "NONDERIV_TRANS_SK", "SECURITY_TITLE", "TRANS_DATE",
                "TRANS_FORM_TYPE", "TRANS_CODE", "EQUITY_SWAP_INVOLVED", "TRANS_TIMELINESS",
                "TRANS_SHARES", "TRANS_PRICEPERSHARE", "TRANS_ACQUIRED_DISP_CD",
                "SHRS_OWND_FOLWNG_TRANS", "DIRECT_INDIRECT_OWNERSHIP", "NATURE_OF_OWNERSHIP",
            ]),
        )
    return path


def _source_url() -> str:
    return (
        "https://www.sec.gov/files/datastandardsinnovation/data/"
        "insider-transactions-data-sets/2026q2_form345.zip"
    )


def test_same_accession_in_meta_and_msft_context_resolves_by_official_issuer_cik(tmp_path: Path) -> None:
    payload = import_sec_insider_quarter(
        insider_zip_path=_insider_zip(tmp_path),
        sec_bulk_bundle_dir=_bundle(tmp_path),
        source_url=_source_url(),
        quarter_label="2026Q2",
    )

    assert payload["counts"]["p_s_evidence_row_count"] == 1
    assert payload["counts"]["issuer_context_mismatch_count"] == 0
    assert payload["guards"]["acceptance_index_key_includes_issuer_cik"] is True
    assert payload["guards"]["cross_entity_accession_contexts_allowed"] is True
    row = payload["rows"][0]
    assert row["accession_number"] == ACCESSION
    assert row["issuer_cik"] == MSFT_CIK
    assert row["scanner_symbol"] == "MSFT"


def test_accession_only_in_wrong_sec_context_is_quarantined_not_reassigned(tmp_path: Path) -> None:
    payload = import_sec_insider_quarter(
        insider_zip_path=_insider_zip(tmp_path),
        sec_bulk_bundle_dir=_bundle(tmp_path, msft_has_accession=False),
        source_url=_source_url(),
        quarter_label="2026Q2",
    )

    assert payload["counts"]["p_s_evidence_row_count"] == 0
    assert payload["counts"]["issuer_context_mismatch_count"] == 1
    assert payload["candidate_status_counts"][
        "UNKNOWN_ACCESSION_PRESENT_ONLY_IN_OTHER_SEC_CONTEXTS"
    ] == 1
