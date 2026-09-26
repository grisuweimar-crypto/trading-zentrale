from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scanner.research.external_evidence.primary_release_8e import (
    PrimaryRelease8EError,
    build_primary_release_evidence,
)
from scanner.research.external_evidence.structured_events_8e import build_event_ledger


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "configs" / "external_evidence_8e_primary_release_v1.json"
EVENT_CONFIG_PATH = ROOT / "configs" / "external_evidence_8e_structured_events_v1.json"


def _config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def _event_config() -> dict:
    return json.loads(EVENT_CONFIG_PATH.read_text(encoding="utf-8"))


def _record(**overrides: object) -> dict:
    row = {
        "provider_id": "FTC_COMPETITION",
        "source_event_id": "release-123",
        "canonical_event_key": "FTC:LITIGATION_FILED:case-123",
        "subject_id": "AUTHORITY_CASE:case-123",
        "event_type": "LITIGATION_FILED",
        "event_state": "FILED",
        "authority_scope": "FTC_COMPETITION_ENFORCEMENT",
        "source_url": "https://www.ftc.gov/news-events/news/press-releases/example",
        "content_sha256": "a" * 64,
        "source_claimed_published_at": "2026-09-25T12:00:00-04:00",
        "historical_publication_time_independently_proven": False,
        "semantic_label_origin": "HUMAN_REVIEWED_STRUCTURED",
    }
    row.update(overrides)
    return row


def test_observed_release_is_pit_safe_from_actual_ingestion_not_page_date() -> None:
    ingested_at = "2026-09-26T16:00:00+00:00"
    payload = build_primary_release_evidence(
        records=[_record()], ingested_at=ingested_at, config=_config()
    )
    row = payload["rows"][0]
    assert row["source_claimed_published_at"] == "2026-09-25T12:00:00-04:00"
    assert row["published_at"] == ingested_at
    assert row["valid_from"] == ingested_at
    assert row["strict_pit_eligible"] is True
    assert row["historical_publication_time_independently_proven"] is False
    assert payload["guards"]["historical_backdating_without_independent_proof"] is False


def test_observed_release_can_produce_conservative_first_public_release() -> None:
    ingested_at = "2026-09-26T16:00:00+00:00"
    payload = build_primary_release_evidence(
        records=[_record()], ingested_at=ingested_at, config=_config()
    )
    event_config = _event_config()
    source_ranks = {
        key: int(value["authority_rank"])
        for key, value in event_config["source_classes"].items()
    }
    ledger = build_event_ledger(
        evidence_rows=payload["rows"],
        as_of=datetime(2026, 9, 26, 17, 0, tzinfo=timezone.utc),
        allowed_event_types=event_config["initial_event_taxonomy"],
        source_ranks=source_ranks,
    )
    event = ledger["events"][0]
    assert event["status"] == "KNOWN"
    assert event["first_public_release_at"] == ingested_at
    assert event["valid_from"] == ingested_at
    assert event["event_state"] == "FILED"


def test_authority_provider_rejects_non_authority_host() -> None:
    with pytest.raises(PrimaryRelease8EError, match="not allowed"):
        build_primary_release_evidence(
            records=[_record(source_url="https://example.com/release")],
            ingested_at="2026-09-26T16:00:00+00:00",
            config=_config(),
        )


def test_issuer_ir_requires_domain_attestation() -> None:
    record = _record(
        provider_id="ISSUER_IR",
        source_event_id="issuer-release-1",
        canonical_event_key="ISSUER:MAJOR_CONTRACT:issuer-1:contract-1",
        subject_id="ISSUER_CIK:0000000001",
        event_type="MAJOR_CONTRACT",
        event_state="ANNOUNCED",
        authority_scope="ISSUER_PRIMARY_DISCLOSURE",
        source_url="https://investor.example-issuer.com/news/release-1",
    )
    with pytest.raises(PrimaryRelease8EError, match="issuer_domain_attested"):
        build_primary_release_evidence(
            records=[record],
            ingested_at="2026-09-26T16:00:00+00:00",
            config=_config(),
        )


def test_independent_historical_publication_proof_requires_hash() -> None:
    record = _record(
        historical_publication_time_independently_proven=True,
    )
    with pytest.raises(PrimaryRelease8EError, match="archival_proof_sha256"):
        build_primary_release_evidence(
            records=[record],
            ingested_at="2026-09-26T16:00:00+00:00",
            config=_config(),
        )


def test_independent_historical_publication_proof_may_backdate_only_with_proof() -> None:
    record = _record(
        historical_publication_time_independently_proven=True,
        archival_proof_sha256="b" * 64,
    )
    payload = build_primary_release_evidence(
        records=[record],
        ingested_at="2026-09-26T16:00:00+00:00",
        config=_config(),
    )
    row = payload["rows"][0]
    assert row["published_at"] == "2026-09-25T12:00:00-04:00"
    assert row["valid_from"] == "2026-09-25T12:00:00-04:00"
    assert row["archival_proof_sha256"] == "b" * 64


def test_provider_event_scope_is_fail_closed() -> None:
    with pytest.raises(PrimaryRelease8EError, match="not allowed"):
        build_primary_release_evidence(
            records=[_record(event_type="PRODUCT_LAUNCH")],
            ingested_at="2026-09-26T16:00:00+00:00",
            config=_config(),
        )
