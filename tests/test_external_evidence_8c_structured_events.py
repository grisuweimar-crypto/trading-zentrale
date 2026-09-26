import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.structured_events import (
    StructuredEventContractError,
    build_event_candidates,
    event_coverage,
    validate_event_contract,
)

ROOT = Path(__file__).resolve().parents[1]


def contract():
    return json.loads(
        (ROOT / "configs" / "external_evidence_8c_structured_events_v1.json").read_text(
            encoding="utf-8"
        )
    )


def filing(*, accession, form="8-K", items=None, stage="CURRENT_REPORT", valid_from="2026-09-25T12:00:00+00:00"):
    return {
        "source": "sec_edgar_submissions_8k_6k",
        "cik": "0000320193",
        "accession_number": accession,
        "form": form,
        "items": list(items or []),
        "published_at": valid_from,
        "valid_from": valid_from,
        "revision_id": accession,
        "publication_stage": stage,
        "status": "KNOWN",
        "reason_codes": [],
    }


def candidates(rows):
    c = contract()
    return build_event_candidates(
        rows,
        item_mapping=c["event_item_mapping"],
        allowed_forms=c["allowed_forms"],
    )


def test_item_202_is_results_release_but_never_beat_or_miss():
    rows = candidates([filing(accession="A", items=["2.02"])])
    assert len(rows) == 1
    assert rows[0]["candidate_event_type"] == "RESULTS_RELEASE"
    assert rows[0]["semantic_state"] == "FILING_ITEM_ONLY"
    assert rows[0]["direction"] == "UNKNOWN"
    assert "BEAT" not in rows[0]["candidate_event_type"]
    assert "MISS" not in rows[0]["candidate_event_type"]


def test_reg_fd_does_not_become_guidance():
    rows = candidates([filing(accession="B", items=["7.01"])])
    assert rows[0]["candidate_event_type"] == "REGULATION_FD_DISCLOSURE"
    assert rows[0]["direction"] == "UNKNOWN"
    assert "GUIDANCE" not in rows[0]["candidate_event_type"]


def test_other_material_event_does_not_become_dividend_or_buyback():
    rows = candidates([filing(accession="C", items=["8.01"])])
    assert rows[0]["candidate_event_type"] == "OTHER_MATERIAL_EVENT"
    assert rows[0]["direction"] == "UNKNOWN"


def test_6k_stays_unclassified_without_content_evidence():
    rows = candidates([filing(accession="D", form="6-K", items=[])])
    assert rows[0]["candidate_event_type"] == "FOREIGN_CURRENT_REPORT_UNCLASSIFIED"
    assert rows[0]["semantic_state"] == "UNKNOWN"
    assert "6K_REQUIRES_CONTENT_EVIDENCE" in rows[0]["reason_codes"]


def test_unmapped_8k_item_remains_visible():
    rows = candidates([filing(accession="E", items=["9.99"])])
    assert rows[0]["candidate_event_type"] == "UNMAPPED_8K_ITEM"
    assert rows[0]["item_code"] == "9.99"
    assert rows[0]["semantic_state"] == "UNKNOWN"
    assert "UNMAPPED_8K_ITEM" in rows[0]["reason_codes"]


def test_original_and_amendment_are_both_preserved():
    rows = candidates(
        [
            filing(accession="F1", form="8-K", items=["2.02"]),
            filing(accession="F2", form="8-K/A", items=["2.02"], stage="AMENDMENT"),
        ]
    )
    assert len(rows) == 2
    assert {row["accession_number"] for row in rows} == {"F1", "F2"}
    assert {row["publication_stage"] for row in rows} == {"CURRENT_REPORT", "AMENDMENT"}
    assert event_coverage(rows)["amendment_count"] == 1


def test_missing_valid_from_forces_unknown_status():
    rows = candidates([filing(accession="G", items=["2.02"], valid_from=None)])
    assert rows[0]["status"] == "UNKNOWN"
    assert "MISSING_VALID_FROM" in rows[0]["reason_codes"]


def test_contract_rejects_direction_assignment():
    c = contract()
    rows = candidates([filing(accession="H", items=["2.02"])])
    rows[0]["direction"] = "POSITIVE"
    with pytest.raises(StructuredEventContractError, match="forbidden direction"):
        validate_event_contract(rows, required_fields=c["required_output_fields"])


def test_contract_rejects_content_semantics_from_metadata_layer():
    c = contract()
    rows = candidates([filing(accession="I", items=["7.01"])])
    rows[0]["candidate_event_type"] = "GUIDANCE_RAISE"
    with pytest.raises(StructuredEventContractError, match="Content-dependent"):
        validate_event_contract(rows, required_fields=c["required_output_fields"])


def test_coverage_confirms_all_directions_unknown():
    rows = candidates(
        [
            filing(accession="J", items=["1.01", "2.02", "5.02"]),
            filing(accession="K", form="6-K"),
        ]
    )
    coverage = event_coverage(rows)
    assert coverage["event_count"] == 4
    assert coverage["all_directions_unknown"] is True
    assert coverage["outcome_research"] == "NOT_RUN"
