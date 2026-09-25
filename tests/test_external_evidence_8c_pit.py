import json
from pathlib import Path

from scanner.research.external_evidence.sec_edgar import (
    build_acceptance_index,
    classify_publication_stage,
    companyfacts_rows,
    compute_valid_from,
    filing_candidate_events,
    normalize_cik,
    submission_rows,
)

ROOT = Path(__file__).resolve().parents[1]


def load_contract():
    return json.loads((ROOT / "configs" / "external_evidence_8c_contract_v1.json").read_text(encoding="utf-8"))


def load_taxonomy():
    return json.loads((ROOT / "configs" / "external_event_taxonomy_8c_v1.json").read_text(encoding="utf-8"))


def submissions_fixture():
    return {
        "cik": "320193",
        "name": "Example Corp",
        "tickers": ["EXM"],
        "exchanges": ["Nasdaq"],
        "filings": {
            "recent": {
                "accessionNumber": [
                    "0000320193-26-000100",
                    "0000320193-26-000101",
                    "0000320193-26-000102",
                ],
                "filingDate": ["2026-08-19", "2026-08-20", "2026-08-21"],
                "reportDate": ["2026-08-19", "2026-06-30", "2026-08-21"],
                "acceptanceDateTime": [
                    "2026-08-19T11:51:13.000Z",
                    "2026-08-20T20:04:00.000Z",
                    "",
                ],
                "form": ["8-K", "10-Q/A", "6-K"],
                "items": ["2.02, 9.01", "", ""],
                "primaryDocument": ["a.htm", "b.htm", "c.htm"],
                "primaryDocDescription": ["8-K", "10-Q/A", "6-K"],
                "isXBRL": [1, 1, 0],
                "isInlineXBRL": [1, 1, 0],
            }
        },
    }


def companyfacts_fixture():
    return {
        "cik": 320193,
        "entityName": "Example Corp",
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "label": "Revenue",
                    "description": "Revenue",
                    "units": {
                        "USD": [
                            {
                                "start": "2026-01-01",
                                "end": "2026-06-30",
                                "val": 100,
                                "accn": "0000320193-26-000099",
                                "fy": 2026,
                                "fp": "Q2",
                                "form": "10-Q",
                                "filed": "2026-07-30",
                            },
                            {
                                "start": "2026-01-01",
                                "end": "2026-06-30",
                                "val": 101,
                                "accn": "0000320193-26-000101",
                                "fy": 2026,
                                "fp": "Q2",
                                "form": "10-Q/A",
                                "filed": "2026-08-20",
                            },
                        ]
                    },
                }
            }
        },
    }


def test_8c_contract_keeps_outcomes_and_decision_integration_off():
    c = load_contract()
    assert c["status"] == "FOUNDATION_ACTIVE_NOT_PROMOTED"
    assert c["scope"]["outcome_research_enabled"] is False
    assert c["scope"]["decision_integration_enabled"] is False
    assert c["pit_identity"]["xbrl_fact_without_accession_join_is_pit_safe"] is False
    assert c["versioning"]["amendments_overwrite_prior_records"] is False
    assert "join_outcomes_before_8c_hypotheses_are_frozen" in c["forbidden_shortcuts"]


def test_normalize_cik_is_deterministic():
    assert normalize_cik(320193) == "0000320193"
    assert normalize_cik("CIK0000320193") == "0000320193"


def test_exact_acceptance_timestamp_controls_valid_from():
    pit = compute_valid_from(
        filing_date="2026-08-19",
        acceptance_datetime="2026-08-19T11:51:13.000Z",
    )
    assert pit["pit_status"] == "SAFE"
    assert pit["published_at"] == pit["valid_from"]
    assert pit["published_at"].endswith("+00:00")


def test_missing_acceptance_time_never_becomes_valid_on_filing_date():
    pit = compute_valid_from(filing_date="2026-07-01", acceptance_datetime="")
    assert pit["pit_status"] == "DATE_ONLY_DELAYED"
    assert pit["published_at"] is None
    assert pit["valid_from"].startswith("2026-07-02T00:00:00")
    assert "DATE_ONLY_DELAYED_VALID_FROM" in pit["reason_codes"]


def test_submission_parallel_arrays_become_accession_rows():
    rows = submission_rows(submissions_fixture())
    assert len(rows) == 3
    assert rows[0]["accession_number"] == "0000320193-26-000100"
    assert rows[0]["items"] == ["2.02", "9.01"]
    assert rows[0]["publication_stage"] == "CURRENT_REPORT"
    assert rows[1]["publication_stage"] == "AMENDMENT"
    assert rows[2]["pit_status"] == "DATE_ONLY_DELAYED"
    assert classify_publication_stage("10-K") == "PERIODIC_REPORT"


def test_companyfacts_preserve_original_and_amended_accessions():
    submission_index = build_acceptance_index(submission_rows(submissions_fixture()))
    rows = companyfacts_rows(companyfacts_fixture(), acceptance_index=submission_index)
    assert len(rows) == 2
    assert [row["value"] for row in rows] == [100, 101]
    assert [row["accession_number"] for row in rows] == [
        "0000320193-26-000099",
        "0000320193-26-000101",
    ]
    assert "ACCESSION_NOT_IN_SUBMISSION_INDEX" in rows[0]["reason_codes"]
    assert rows[1]["publication_stage"] == "AMENDMENT"
    assert rows[1]["published_at"] is not None


def test_8k_item_candidate_is_not_directional_evidence():
    taxonomy = load_taxonomy()["8k_item_candidates"]
    filing = submission_rows(submissions_fixture())[0]
    events = filing_candidate_events(filing, item_mapping=taxonomy)
    result = next(x for x in events if x["item_code"] == "2.02")
    assert result["candidate_event_type"] == "RESULTS_OF_OPERATIONS_RELEASE"
    assert result["semantic_state"] == "FILING_ITEM_ONLY"
    assert result["direction"] == "UNKNOWN"


def test_6k_without_item_codes_stays_unclassified():
    taxonomy = load_taxonomy()["8k_item_candidates"]
    filing = submission_rows(submissions_fixture())[2]
    events = filing_candidate_events(filing, item_mapping=taxonomy)
    assert events == [
        {
            "source": "sec_edgar_submissions_8k_6k",
            "cik": "0000320193",
            "accession_number": "0000320193-26-000102",
            "form": "6-K",
            "item_code": None,
            "candidate_event_type": "FOREIGN_CURRENT_REPORT_UNCLASSIFIED",
            "semantic_state": "UNKNOWN",
            "direction": "UNKNOWN",
            "published_at": None,
            "valid_from": events[0]["valid_from"],
            "revision_id": "0000320193-26-000102",
            "status": "KNOWN",
            "reason_codes": ["DATE_ONLY_DELAYED_VALID_FROM"],
        }
    ]
