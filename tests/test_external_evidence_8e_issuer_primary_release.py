from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.issuer_primary_release_8e import (
    IssuerPrimaryRelease8EError,
    build_issuer_primary_release_evidence,
)

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "configs" / "external_evidence_8e_issuer_primary_release_v1.json"


def _cfg() -> dict:
    return json.loads(CONFIG.read_text(encoding="utf-8"))


def _record() -> dict:
    return {
        "event_type": "PRODUCT_LAUNCH",
        "issuer_stable_id": "ISSUER:TEST",
        "source_event_id": "release-20260926-1",
        "source_url": "https://ir.example-issuer.test/news/release-1",
        "event_state": "ANNOUNCED",
        "authority_scope": "ISSUER_OFFICIAL_RELEASE",
        "ingested_at": "2026-09-26T18:00:00+00:00",
        "public_release_proof_status": "INGESTION_ONLY",
        "historical_publication_time_independently_proven": False,
        "operator_verified_explicit_event_type": True,
        "source_record": {"headline": "Product launch", "body_sha_hint": "abc"},
    }


def test_prospective_issuer_release_uses_ingestion_without_timestamp_proof() -> None:
    payload = build_issuer_primary_release_evidence(records=[_record()], config=_cfg())
    row = payload["rows"][0]
    assert row["published_at"] is None
    assert row["valid_from"] == row["ingested_at"]
    assert row["market_direction"] == "UNASSIGNED"
    assert payload["guards"]["market_outcomes_read"] is False


def test_guidance_and_capital_raise_remain_blocked_by_phase8c_boundary() -> None:
    for event_type in ("GUIDANCE_RAISE", "GUIDANCE_CUT", "CAPITAL_RAISE"):
        record = _record()
        record["event_type"] = event_type
        with pytest.raises(IssuerPrimaryRelease8EError, match="Phase 8C boundary"):
            build_issuer_primary_release_evidence(records=[record], config=_cfg())


def test_explicit_event_verification_is_required() -> None:
    record = _record()
    record["operator_verified_explicit_event_type"] = False
    with pytest.raises(IssuerPrimaryRelease8EError, match="operator-verified"):
        build_issuer_primary_release_evidence(records=[record], config=_cfg())


def test_page_date_cannot_backdate_without_independent_proof() -> None:
    record = _record()
    record["published_at"] = "2026-09-20T08:00:00+00:00"
    record["public_release_proof_status"] = "INGESTION_ONLY"
    payload = build_issuer_primary_release_evidence(records=[record], config=_cfg())
    assert payload["rows"][0]["published_at"] is None
    assert payload["rows"][0]["valid_from"] == "2026-09-26T18:00:00+00:00"
