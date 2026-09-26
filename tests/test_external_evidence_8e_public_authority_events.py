from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.public_authority_events_8e import (
    PublicAuthority8EError,
    build_public_authority_evidence,
)
from scanner.research.external_evidence.structured_events_8e import build_event_ledger

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "configs" / "external_evidence_8e_public_authority_events_v1.json"
EVENT_CONFIG = ROOT / "configs" / "external_evidence_8e_structured_events_v1.json"


def _cfg() -> dict:
    return json.loads(CONFIG.read_text(encoding="utf-8"))


def _event_cfg() -> dict:
    return json.loads(EVENT_CONFIG.read_text(encoding="utf-8"))


def _record(provider: str = "FTC") -> dict:
    return {
        "provider": provider,
        "event_type": "LITIGATION_FILED",
        "source_event_id": "case-123",
        "subject_id": "AUTHORITY_CASE:case-123",
        "event_state": "FILED",
        "authority_scope": "ANTITRUST_ENFORCEMENT",
        "source_url": "https://www.ftc.gov/legal-library/browse/cases-proceedings/example" if provider == "FTC" else "https://www.justice.gov/atr/case/example",
        "ingested_at": "2026-09-26T18:00:00+00:00",
        "public_release_proof_status": "INGESTION_ONLY",
        "historical_publication_time_independently_proven": False,
        "source_record": {"title": "Example filing", "date": "September 26, 2026"},
    }


def test_authority_date_without_exact_publication_proof_uses_ingestion() -> None:
    payload = build_public_authority_evidence(records=[_record()], config=_cfg())
    row = payload["rows"][0]
    assert row["published_at"] is None
    assert row["valid_from"] == row["ingested_at"]
    assert row["security_identity_status"] == "DEFERRED_NOT_INFERRED_FROM_AUTHORITY_NAME"
    assert payload["guards"]["market_outcomes_read"] is False


def test_proven_exact_timestamp_can_define_historical_valid_from() -> None:
    record = _record("DOJ_ANTITRUST")
    record.update({
        "public_release_proof_status": "PROVEN",
        "published_at": "2026-09-25T14:30:00-04:00",
        "historical_publication_time_independently_proven": True,
    })
    payload = build_public_authority_evidence(records=[record], config=_cfg())
    row = payload["rows"][0]
    assert row["published_at"] == "2026-09-25T14:30:00-04:00"
    assert row["valid_from"] == row["published_at"]


def test_non_authority_host_fails_closed() -> None:
    record = _record()
    record["source_url"] = "https://example.com/story"
    with pytest.raises(PublicAuthority8EError, match="non-authoritative"):
        build_public_authority_evidence(records=[record], config=_cfg())


def test_ingestion_only_authority_evidence_does_not_fake_first_public_release() -> None:
    payload = build_public_authority_evidence(records=[_record()], config=_cfg())
    event_cfg = _event_cfg()
    source_ranks = {key: int(value["authority_rank"]) for key, value in event_cfg["source_classes"].items()}
    from datetime import datetime, timezone
    ledger = build_event_ledger(
        evidence_rows=payload["rows"],
        as_of=datetime(2026, 9, 26, 19, 0, tzinfo=timezone.utc),
        allowed_event_types=event_cfg["initial_event_taxonomy"],
        source_ranks=source_ranks,
    )
    assert ledger["events"][0]["status"] == "INSUFFICIENT_FIRST_PUBLIC_RELEASE_PROOF"


def test_provider_event_scope_is_enforced() -> None:
    record = _record()
    record["event_type"] = "PRODUCT_LAUNCH"
    with pytest.raises(PublicAuthority8EError, match="unsupported event type"):
        build_public_authority_evidence(records=[record], config=_cfg())
