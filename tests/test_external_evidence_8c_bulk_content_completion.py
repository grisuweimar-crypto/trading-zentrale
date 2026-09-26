from __future__ import annotations

import hashlib
import json
from pathlib import Path

from scanner.research.external_evidence.bulk_content_completion import (
    acquire_and_extract_bulk_content,
)


def _write_json(path: Path, payload: dict) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def _bundle(tmp_path: Path) -> Path:
    bundle = tmp_path / "bundle"
    cik = "0000320193"
    accession = "0000320193-26-000123"
    submissions = {
        "cik": 320193,
        "name": "Example Corp",
        "tickers": ["EXM"],
        "exchanges": ["Nasdaq"],
        "filings": {
            "recent": {
                "accessionNumber": [accession],
                "form": ["8-K"],
                "filingDate": ["2026-05-10"],
                "acceptanceDateTime": ["2026-05-10T16:30:00-04:00"],
                "reportDate": ["2026-05-10"],
                "primaryDocument": ["form8-k.htm"],
                "primaryDocDescription": ["CURRENT REPORT"],
            },
            "files": [],
        },
    }
    companyfacts = {"cik": 320193, "entityName": "Example Corp", "facts": {}}
    sub_rel = f"raw/companies/{cik}/submissions.json"
    fact_rel = f"raw/companies/{cik}/companyfacts.json"
    sub_sha = _write_json(bundle / sub_rel, submissions)
    fact_sha = _write_json(bundle / fact_rel, companyfacts)
    manifest = {
        "schema_version": "external_evidence_8c_sec_bulk_bundle_v1",
        "source_mode": "DIRECT_SEC_BULK_OPERATOR_ATTESTED",
        "source_authority": "U.S. SEC EDGAR",
        "operator_attested_direct_sec_download": True,
        "market_outcomes_read": False,
        "companies": [
            {
                "symbol": "EXM",
                "scanner_as_of": "2026-09-01T18:00:00+00:00",
                "cik": cik,
                "sec_title": "Example Corp",
                "identity_status": "VERIFIED_BY_SEC_BULK_SUBMISSIONS",
                "reason_codes": [],
                "submissions_file": {"path": sub_rel, "sha256": sub_sha},
                "history_files": [],
                "companyfacts_file": {"path": fact_rel, "sha256": fact_sha},
                "content_filings": [],
            }
        ],
        "coverage": {
            "scanner_symbol_count": 1,
            "sec_bulk_identity_verified_count": 1,
            "companyfacts_verified_count": 1,
            "content_document_count": 0,
        },
    }
    _write_json(bundle / "bulk_manifest.json", manifest)
    return bundle


def _history(path: Path) -> Path:
    path.write_text("date,symbol\n2026-01-02,EXM\n2026-09-01,EXM\n", encoding="utf-8")
    return path


def _sec_document() -> bytes:
    return b"""<SEC-DOCUMENT>\n<DOCUMENT>\n<TYPE>8-K\n<SEQUENCE>1\n<FILENAME>form8-k.htm\n<DESCRIPTION>CURRENT REPORT\n<TEXT><html><body>Current report.</body></html></TEXT>\n</DOCUMENT>\n<DOCUMENT>\n<TYPE>EX-99.1\n<SEQUENCE>2\n<FILENAME>press.htm\n<DESCRIPTION>PRESS RELEASE\n<TEXT><html><body>The board declared a quarterly cash dividend of USD 0.30 per common share. The board also authorized a new USD 5 billion share repurchase program.</body></html></TEXT>\n</DOCUMENT>\n</SEC-DOCUMENT>"""


def test_bulk_content_completion_fetches_official_archive_and_emits_g_anchors(tmp_path: Path):
    bundle = _bundle(tmp_path)
    history = _history(tmp_path / "history.csv")
    output = tmp_path / "content"
    anchors = tmp_path / "anchors.json"
    calls: list[str] = []

    def fake_fetch(url: str, *, user_agent: str, timeout: float) -> bytes:
        assert url.startswith("https://www.sec.gov/Archives/edgar/data/")
        assert user_agent == "test-agent contact@example.invalid"
        calls.append(url)
        return _sec_document()

    result = acquire_and_extract_bulk_content(
        bundle_dir=bundle,
        research_history_path=history,
        parser_contract_path=Path("configs/external_evidence_8c_content_parser_v1.json"),
        output_dir=output,
        anchors_output_path=anchors,
        user_agent="test-agent contact@example.invalid",
        minimum_interval_seconds=0,
        fetcher=fake_fetch,
        sleep_fn=lambda _: None,
    )

    assert len(calls) == 1
    assert result["coverage"]["selected_filing_count"] == 1
    assert result["coverage"]["content_document_count"] == 1
    assert result["coverage"]["fetch_error_count"] == 0
    payload = json.loads(anchors.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "external_evidence_8c_content_anchors_v1"
    assert payload["market_outcomes_read"] is False
    assert payload["market_direction_assigned"] is False
    assert {row["family"] for row in payload["anchors"]} >= {"DIVIDEND", "BUYBACK"}

    # A second run must reuse the SHA-verified document instead of refetching it.
    second_calls: list[str] = []

    def should_not_fetch(url: str, *, user_agent: str, timeout: float) -> bytes:
        second_calls.append(url)
        raise AssertionError("resume path unexpectedly refetched a verified filing")

    result2 = acquire_and_extract_bulk_content(
        bundle_dir=bundle,
        research_history_path=history,
        parser_contract_path=Path("configs/external_evidence_8c_content_parser_v1.json"),
        output_dir=output,
        anchors_output_path=anchors,
        user_agent="test-agent contact@example.invalid",
        minimum_interval_seconds=0,
        fetcher=should_not_fetch,
        sleep_fn=lambda _: None,
    )
    assert second_calls == []
    assert result2["coverage"]["reused_document_count"] == 1
    assert result2["coverage"]["fetched_document_count"] == 0


def test_bulk_content_completion_rejects_non_authoritative_bulk(tmp_path: Path):
    bundle = _bundle(tmp_path)
    manifest_path = bundle / "bulk_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["source_mode"] = "NON_AUTHORITATIVE_TRANSPORT_FOR_CHALLENGER_ONLY"
    _write_json(manifest_path, manifest)
    history = _history(tmp_path / "history.csv")

    try:
        acquire_and_extract_bulk_content(
            bundle_dir=bundle,
            research_history_path=history,
            parser_contract_path=Path("configs/external_evidence_8c_content_parser_v1.json"),
            output_dir=tmp_path / "content",
            anchors_output_path=tmp_path / "anchors.json",
            user_agent="test-agent contact@example.invalid",
            minimum_interval_seconds=0,
            fetcher=lambda *args, **kwargs: _sec_document(),
            sleep_fn=lambda _: None,
        )
    except ValueError as exc:
        assert "direct SEC bulk mode" in str(exc)
    else:
        raise AssertionError("non-authoritative bulk transport was accepted")
