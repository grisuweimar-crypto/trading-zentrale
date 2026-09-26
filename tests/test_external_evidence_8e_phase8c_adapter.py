from __future__ import annotations

from datetime import datetime

import pytest

from scanner.research.external_evidence.structured_events_8e_phase8c_adapter import (
    Phase8CReuseAdapterError,
    adapt_phase8c_structured_events,
)


def _row(**overrides):
    row = {
        "source": "sec_edgar_submissions_8k_6k",
        "cik": "0000123456",
        "accession_number": "0000123456-26-000001",
        "form": "8-K",
        "item_code": "5.02",
        "candidate_event_type": "DIRECTOR_OR_OFFICER_CHANGE",
        "semantic_state": "FILING_ITEM_ONLY",
        "direction": "UNKNOWN",
        "published_at": "2026-09-26T14:00:00+00:00",
        "valid_from": "2026-09-26T14:00:00+00:00",
        "revision_id": "0000123456-26-000001",
        "publication_stage": "FIRST_RELEASE",
        "status": "KNOWN",
        "reason_codes": [],
    }
    row.update(overrides)
    return row


def test_management_change_is_reused_without_richer_semantics() -> None:
    payload = adapt_phase8c_structured_events(
        [_row()],
        adapter_ingested_at=datetime.fromisoformat("2026-09-26T16:00:00+00:00"),
    )
    assert payload["status"] == "OUTCOME_BLIND_CONSERVATIVE_REUSE"
    assert payload["row_count"] == 1
    row = payload["rows"][0]
    assert row["event_type"] == "MANAGEMENT_CHANGE"
    assert row["event_state"] == "DISCLOSED"
    assert row["source_class"] == "MANDATORY_ISSUER_FILING"
    assert row["historical_publication_time_independently_proven"] is True
    assert row["event_identity_scope"] == "ACCESSION_SCOPED_DETERMINISTIC"
    assert payload["guards"]["rich_semantics_inferred_from_8c_metadata"] is False


def test_material_agreement_is_not_promoted_to_contract_or_acquisition() -> None:
    payload = adapt_phase8c_structured_events(
        [_row(candidate_event_type="MATERIAL_DEFINITIVE_AGREEMENT", item_code="1.01")],
        adapter_ingested_at=datetime.fromisoformat("2026-09-26T16:00:00+00:00"),
    )
    assert payload["row_count"] == 0
    assert payload["unmapped_candidate_event_type_counts"] == {
        "MATERIAL_DEFINITIVE_AGREEMENT": 1
    }


def test_acquisition_or_disposition_is_not_forced_to_acquisition() -> None:
    payload = adapt_phase8c_structured_events(
        [_row(candidate_event_type="ACQUISITION_OR_DISPOSITION_COMPLETED", item_code="2.01")],
        adapter_ingested_at=datetime.fromisoformat("2026-09-26T16:00:00+00:00"),
    )
    assert payload["row_count"] == 0
    assert payload["unmapped_candidate_event_type_counts"] == {
        "ACQUISITION_OR_DISPOSITION_COMPLETED": 1
    }


def test_direction_assignment_is_rejected() -> None:
    with pytest.raises(Phase8CReuseAdapterError, match="forbidden market direction"):
        adapt_phase8c_structured_events(
            [_row(direction="POSITIVE")],
            adapter_ingested_at=datetime.fromisoformat("2026-09-26T16:00:00+00:00"),
        )


def test_non_filing_item_semantics_are_rejected() -> None:
    with pytest.raises(Phase8CReuseAdapterError, match="filing-item semantic layer"):
        adapt_phase8c_structured_events(
            [_row(semantic_state="EXTRACTED_SEMANTIC")],
            adapter_ingested_at=datetime.fromisoformat("2026-09-26T16:00:00+00:00"),
        )
