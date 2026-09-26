import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.content_evidence import (
    ContentEvidenceError,
    build_evidence_record,
    classify_dividend_change,
    classify_guidance_change,
    require_consensus_for_beat_miss,
    validate_evidence_record,
    validate_issuer_action,
)

ROOT = Path(__file__).resolve().parents[1]


def contract():
    return json.loads(
        (ROOT / "configs" / "external_evidence_8c_content_evidence_v1.json").read_text(
            encoding="utf-8"
        )
    )


def test_guidance_raise_requires_same_metric_period_unit_and_basis():
    previous = {
        "metric": "revenue",
        "period": "FY2027",
        "unit": "USD",
        "basis": "GAAP",
        "lower": 100,
        "upper": 110,
    }
    current = {**previous, "lower": 103, "upper": 115}
    result = classify_guidance_change(previous, current)
    assert result["semantic_change"] == "RAISE"
    assert result["market_direction"] == "UNKNOWN"


def test_guidance_cut_and_mixed_are_distinct():
    previous = {
        "metric": "eps",
        "period": "FY2027",
        "unit": "USD/share",
        "basis": "NON_GAAP",
        "lower": 5.0,
        "upper": 5.5,
    }
    cut = classify_guidance_change(previous, {**previous, "lower": 4.8, "upper": 5.3})
    mixed = classify_guidance_change(previous, {**previous, "lower": 4.9, "upper": 5.7})
    assert cut["semantic_change"] == "CUT"
    assert mixed["semantic_change"] == "MIXED"


def test_guidance_noncomparable_period_is_rejected():
    previous = {
        "metric": "revenue",
        "period": "FY2027",
        "unit": "USD",
        "basis": "GAAP",
        "lower": 100,
        "upper": 110,
    }
    current = {**previous, "period": "Q1-2027"}
    with pytest.raises(ContentEvidenceError, match="Non-comparable evidence"):
        classify_guidance_change(previous, current)


def test_regular_dividend_change_is_semantic_not_market_direction():
    previous = {
        "security_class": "common",
        "currency": "USD",
        "frequency": "quarterly",
        "is_special": False,
        "amount_per_share": 0.25,
    }
    current = {**previous, "amount_per_share": 0.30}
    result = classify_dividend_change(previous, current)
    assert result["semantic_change"] == "INCREASE"
    assert result["market_direction"] == "UNKNOWN"


def test_special_dividend_is_not_folded_into_regular_change():
    previous = {
        "security_class": "common",
        "currency": "USD",
        "frequency": "quarterly",
        "is_special": False,
        "amount_per_share": 0.25,
    }
    current = {**previous, "is_special": True, "amount_per_share": 1.00}
    with pytest.raises(ContentEvidenceError, match="Special dividends"):
        classify_dividend_change(previous, current)


def test_buyback_and_capital_raise_actions_keep_market_direction_unknown():
    c = contract()["supported_issuer_semantics"]
    buyback = validate_issuer_action(
        {"action": "NEW_AUTHORIZATION"},
        event_type="BUYBACK_ACTION",
        allowed_actions=c["buyback_action"]["allowed_actions"],
    )
    raise_event = validate_issuer_action(
        {"action": "PRICED", "instrument": "EQUITY"},
        event_type="CAPITAL_RAISE_ACTION",
        allowed_actions=c["capital_raise_action"]["allowed_actions"],
        allowed_instruments=c["capital_raise_action"]["allowed_instruments"],
    )
    assert buyback["market_direction"] == "UNKNOWN"
    assert raise_event["market_direction"] == "UNKNOWN"


def test_shelf_registration_alone_cannot_be_completed_raise():
    c = contract()["supported_issuer_semantics"]["capital_raise_action"]
    with pytest.raises(ContentEvidenceError, match="Shelf registration"):
        validate_issuer_action(
            {"action": "COMPLETED", "instrument": "EQUITY", "is_shelf_registration": True},
            event_type="CAPITAL_RAISE_ACTION",
            allowed_actions=c["allowed_actions"],
            allowed_instruments=c["allowed_instruments"],
        )


def test_earnings_beat_miss_is_blocked_without_safe_consensus():
    with pytest.raises(ContentEvidenceError, match="SAFE historical point-in-time analyst consensus"):
        require_consensus_for_beat_miss(consensus_pit_status="PARTIAL")
    require_consensus_for_beat_miss(consensus_pit_status="SAFE")


def test_evidence_record_requires_provenance_and_keeps_market_direction_unknown():
    c = contract()
    record = build_evidence_record(
        event_type="GUIDANCE_CHANGE",
        cik="0000320193",
        accession_number="0000320193-26-000001",
        source_valid_from="2026-09-25T12:00:00+00:00",
        source_document="ex99-1.htm",
        evidence_excerpt="Revenue guidance increased to $100-$110 million.",
        extraction_method="DETERMINISTIC_RULE",
        parser_version="v1",
        semantic_status="EXTRACTED_CANDIDATE",
    )
    validate_evidence_record(record, required_fields=c["required_evidence_fields"])
    assert record["market_direction"] == "UNKNOWN"
    assert len(record["evidence_excerpt_sha256"]) == 64


def test_evidence_record_rejects_market_direction_assignment():
    c = contract()
    record = build_evidence_record(
        event_type="DIVIDEND_CHANGE",
        cik="0000320193",
        accession_number="0000320193-26-000002",
        source_valid_from="2026-09-25T12:00:00+00:00",
        source_document="ex99-1.htm",
        evidence_excerpt="Quarterly dividend increased.",
        extraction_method="DETERMINISTIC_RULE",
        parser_version="v1",
        semantic_status="EXTRACTED_CANDIDATE",
    )
    record["market_direction"] = "POSITIVE"
    with pytest.raises(ContentEvidenceError, match="may not assign market direction"):
        validate_evidence_record(record, required_fields=c["required_evidence_fields"])
