from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from scanner.research.external_evidence.structured_events_8e import (
    StructuredEvent8EError,
    build_event_ledger,
)


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "external_evidence_8e_structured_events_v1.json"
SHA = "a" * 64


def _config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def _ranks(config: dict) -> dict[str, int]:
    return {
        key: int(value["authority_rank"])
        for key, value in config["source_classes"].items()
    }


def _row(**overrides):
    row = {
        "canonical_event_key": "issuer-1:product-x:regulatory-decision",
        "event_type": "REGULATORY_APPROVAL",
        "subject_id": "issuer-1",
        "source_id": "issuer_ir",
        "source_class": "ISSUER_PRIMARY_RELEASE",
        "source_event_id": "ir-1",
        "event_state": "ANNOUNCED_APPROVAL",
        "authority_scope": "issuer_statement",
        "published_at": "2026-09-26T10:00:00+00:00",
        "valid_from": "2026-09-26T10:00:00+00:00",
        "ingested_at": "2026-09-26T10:05:00+00:00",
        "historical_publication_time_independently_proven": True,
        "public_release_proof_status": "PROVEN",
        "strict_pit_eligible": True,
        "discovery_only": False,
        "content_sha256": SHA,
    }
    row.update(overrides)
    return row


def test_contract_is_outcome_blind_and_reuses_phase8c() -> None:
    config = _config()
    assert config["principles"]["market_outcomes_may_be_read"] is False
    assert config["principles"]["market_direction_may_be_assigned"] is False
    assert config["principles"]["generic_sentiment_enabled"] is False
    assert config["principles"]["phase8c_sec_event_reuse_required_where_available"] is True
    assert config["overlap_with_phase8c"]["phase8c_remains_source_of_truth_for_existing_sec_extraction"] is True
    assert config["research_gate"]["outcome_research_enabled"] is False
    assert config["research_gate"]["phase7_integration_enabled"] is False


def test_first_public_release_is_earliest_proven_not_highest_authority() -> None:
    config = _config()
    issuer = _row()
    authority = _row(
        source_id="fda",
        source_class="PUBLIC_AUTHORITY_PRIMARY",
        source_event_id="fda-1",
        event_state="APPROVED",
        authority_scope="US_FDA_regulatory_state",
        published_at="2026-09-26T12:00:00+00:00",
        valid_from="2026-09-26T12:00:00+00:00",
        ingested_at="2026-09-26T12:02:00+00:00",
    )

    payload = build_event_ledger(
        evidence_rows=[issuer, authority],
        as_of=datetime.fromisoformat("2026-09-26T13:00:00+00:00"),
        allowed_event_types=config["initial_event_taxonomy"],
        source_ranks=_ranks(config),
    )
    event = payload["events"][0]
    assert event["status"] == "KNOWN"
    assert event["first_public_release_at"] == "2026-09-26T10:00:00+00:00"
    assert event["valid_from"] == "2026-09-26T10:00:00+00:00"
    assert event["event_state"] == "APPROVED"
    assert event["authoritative_source_class"] == "PUBLIC_AUTHORITY_PRIMARY"


def test_later_authoritative_correction_is_new_state_not_backdated() -> None:
    config = _config()
    issuer = _row()
    approval = _row(
        source_id="authority",
        source_class="PUBLIC_AUTHORITY_PRIMARY",
        source_event_id="authority-1",
        event_state="APPROVED",
        authority_scope="regulatory_state",
        published_at="2026-09-26T12:00:00+00:00",
        valid_from="2026-09-26T12:00:00+00:00",
        ingested_at="2026-09-26T12:01:00+00:00",
    )
    correction = _row(
        source_id="authority",
        source_class="PUBLIC_AUTHORITY_PRIMARY",
        source_event_id="authority-2",
        event_state="REJECTED",
        authority_scope="regulatory_state",
        published_at="2026-09-26T14:00:00+00:00",
        valid_from="2026-09-26T14:00:00+00:00",
        ingested_at="2026-09-26T14:01:00+00:00",
    )

    before = build_event_ledger(
        evidence_rows=[issuer, approval, correction],
        as_of=datetime.fromisoformat("2026-09-26T13:00:00+00:00"),
        allowed_event_types=config["initial_event_taxonomy"],
        source_ranks=_ranks(config),
    )["events"][0]
    after = build_event_ledger(
        evidence_rows=[issuer, approval, correction],
        as_of=datetime.fromisoformat("2026-09-26T15:00:00+00:00"),
        allowed_event_types=config["initial_event_taxonomy"],
        source_ranks=_ranks(config),
    )["events"][0]

    assert before["event_state"] == "APPROVED"
    assert after["event_state"] == "REJECTED"
    assert before["first_public_release_at"] == after["first_public_release_at"]
    assert after["valid_from"] == "2026-09-26T10:00:00+00:00"


def test_unconfirmed_news_discovery_cannot_create_known_event() -> None:
    config = _config()
    news = _row(
        source_id="wire",
        source_class="REPUTABLE_NEWS_DISCOVERY_ONLY",
        source_event_id="wire-1",
        event_state="REPORTED",
        authority_scope="discovery",
        strict_pit_eligible=False,
        discovery_only=True,
        historical_publication_time_independently_proven=False,
        public_release_proof_status="INGESTION_ONLY",
        published_at=None,
        valid_from="2026-09-26T10:05:00+00:00",
        ingested_at="2026-09-26T10:05:00+00:00",
    )
    event = build_event_ledger(
        evidence_rows=[news],
        as_of=datetime.fromisoformat("2026-09-26T11:00:00+00:00"),
        allowed_event_types=config["initial_event_taxonomy"],
        source_ranks=_ranks(config),
    )["events"][0]
    assert event["status"] == "UNCONFIRMED_DISCOVERY_ONLY"
    assert event["event_state"] is None


def test_backdating_before_ingestion_fails_without_independent_proof() -> None:
    config = _config()
    row = _row(
        historical_publication_time_independently_proven=False,
        public_release_proof_status="INGESTION_ONLY",
        published_at=None,
        valid_from="2026-09-26T10:00:00+00:00",
        ingested_at="2026-09-26T10:05:00+00:00",
    )
    with pytest.raises(StructuredEvent8EError, match="cannot precede actual ingestion"):
        build_event_ledger(
            evidence_rows=[row],
            as_of=datetime.fromisoformat("2026-09-26T11:00:00+00:00"),
            allowed_event_types=config["initial_event_taxonomy"],
            source_ranks=_ranks(config),
        )


def test_same_rank_same_timestamp_conflicting_states_are_explicit() -> None:
    config = _config()
    a = _row(
        source_id="authority-a",
        source_class="PUBLIC_AUTHORITY_PRIMARY",
        source_event_id="a",
        event_state="APPROVED",
        authority_scope="same_scope",
    )
    b = _row(
        source_id="authority-b",
        source_class="PUBLIC_AUTHORITY_PRIMARY",
        source_event_id="b",
        event_state="REJECTED",
        authority_scope="same_scope",
    )
    event = build_event_ledger(
        evidence_rows=[a, b],
        as_of=datetime.fromisoformat("2026-09-26T11:00:00+00:00"),
        allowed_event_types=config["initial_event_taxonomy"],
        source_ranks=_ranks(config),
    )["events"][0]
    assert event["status"] == "CONFLICTING_SOURCES"
    assert event["event_state"] is None
    assert event["source_conflict"] is True


def test_naive_timestamps_are_rejected() -> None:
    config = _config()
    with pytest.raises(StructuredEvent8EError, match="timezone-aware"):
        build_event_ledger(
            evidence_rows=[_row(valid_from="2026-09-26T10:00:00")],
            as_of=datetime.fromisoformat("2026-09-26T11:00:00+00:00"),
            allowed_event_types=config["initial_event_taxonomy"],
            source_ranks=_ranks(config),
        )
