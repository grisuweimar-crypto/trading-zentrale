import json
from hashlib import sha256
from pathlib import Path

import pytest

from scanner.research.external_evidence.semantic_challenger import (
    SemanticChallengerError,
    extract_semantic_candidates,
    validate_semantic_candidates,
)

ROOT = Path(__file__).resolve().parents[1]


def contract():
    return json.loads(
        (ROOT / "configs" / "external_evidence_8c_semantic_challenger_v1.json").read_text(
            encoding="utf-8"
        )
    )


def anchor(*, family: str, excerpt: str, accession: str = "0001-26-000001"):
    return {
        "family": family,
        "matched_phrase": "quarterly dividend" if family == "DIVIDEND" else "share repurchase",
        "document_type": "EX-99.1",
        "sequence": "2",
        "filename": "press.htm",
        "description": "PRESS RELEASE",
        "excerpt": excerpt,
        "excerpt_sha256": sha256(excerpt.encode("utf-8")).hexdigest(),
        "character_start": 10,
        "character_end": 20,
        "semantic_status": "ANCHOR_ONLY",
        "market_direction": "UNKNOWN",
        "parser_version": "8c_content_parser_v1",
        "symbol": "TEST",
        "cik": "0000000001",
        "accession_number": accession,
        "source_valid_from": "2026-09-25T12:00:00+00:00",
        "source_document_file": "raw/test.txt",
    }


def extract(rows):
    result = extract_semantic_candidates(rows, contract=contract())
    validate_semantic_candidates(
        result["candidates"],
        required_fields=contract()["required_output_fields"],
    )
    return result


def test_explicit_usd_common_share_quarterly_dividend_is_comparison_eligible():
    result = extract(
        [
            anchor(
                family="DIVIDEND",
                excerpt="The board declared a quarterly cash dividend of USD 0.30 per common share payable next month.",
            )
        ]
    )
    assert result["candidate_count"] == 1
    row = result["candidates"][0]
    assert row["event_type"] == "DIVIDEND_DECLARATION_OBSERVATION"
    assert row["amount_per_share"] == 0.30
    assert row["currency"] == "USD"
    assert row["security_class"] == "common"
    assert row["comparison_eligible"] is True
    assert row["market_direction"] == "UNKNOWN"


def test_bare_dollar_dividend_is_not_assumed_usd():
    result = extract(
        [
            anchor(
                family="DIVIDEND",
                excerpt="The board declared a quarterly dividend of $0.30 per common share payable next month.",
            )
        ]
    )
    row = result["candidates"][0]
    assert row["currency"] == "UNKNOWN"
    assert row["comparison_eligible"] is False
    assert "AMBIGUOUS_CURRENCY_SYMBOL" in row["reason_codes"]


def test_unknown_security_class_blocks_dividend_comparison():
    result = extract(
        [
            anchor(
                family="DIVIDEND",
                excerpt="The board declared a quarterly dividend of USD 0.30 per share payable next month.",
            )
        ]
    )
    row = result["candidates"][0]
    assert row["currency"] == "USD"
    assert row["security_class"] == "UNKNOWN"
    assert row["comparison_eligible"] is False
    assert "SECURITY_CLASS_NOT_EXPLICIT" in row["reason_codes"]


def test_special_dividend_is_rejected_from_regular_dividend_challenger():
    result = extract(
        [
            anchor(
                family="DIVIDEND",
                excerpt="The board declared a special dividend and also discussed the quarterly dividend of USD 0.30 per common share.",
            )
        ]
    )
    assert result["candidate_count"] == 0
    assert result["rejections"][0]["reason"] == "SPECIAL_DIVIDEND_OUT_OF_SCOPE"


def test_explicit_new_buyback_authorization_is_extracted_without_market_direction():
    result = extract(
        [
            anchor(
                family="BUYBACK",
                excerpt="The board authorized a new USD 5 billion share repurchase program effective immediately.",
            )
        ]
    )
    row = result["candidates"][0]
    assert row["event_type"] == "BUYBACK_ACTION"
    assert row["action"] == "NEW_AUTHORIZATION"
    assert row["authorization_amount"] == 5_000_000_000.0
    assert row["currency"] == "USD"
    assert row["market_direction"] == "UNKNOWN"


def test_historical_buyback_context_without_action_verb_is_not_promoted():
    result = extract(
        [
            anchor(
                family="BUYBACK",
                excerpt="Under the existing share repurchase program, the company may repurchase up to $5 billion over time.",
            )
        ]
    )
    assert result["candidate_count"] == 0
    assert result["rejections"][0]["reason"] == "NO_HIGH_PRECISION_BUYBACK_PATTERN"


def test_tampered_anchor_hash_is_rejected_before_semantic_extraction():
    row = anchor(
        family="BUYBACK",
        excerpt="The board authorized a new USD 5 billion share repurchase program.",
    )
    row["excerpt"] += " tampered"
    result = extract([row])
    assert result["candidate_count"] == 0
    assert result["rejections"][0]["reason"] == "ANCHOR_HASH_MISMATCH"


def test_guidance_anchor_is_ignored_because_family_is_disabled():
    result = extract(
        [
            anchor(
                family="GUIDANCE",
                excerpt="The company raised revenue guidance to USD 10 billion.",
            )
        ]
    )
    assert result["candidate_count"] == 0
    assert result["rejection_count"] == 0


def test_validator_rejects_market_direction_assignment():
    result = extract(
        [
            anchor(
                family="BUYBACK",
                excerpt="The board authorized a new USD 5 billion share repurchase program.",
            )
        ]
    )
    result["candidates"][0]["market_direction"] = "POSITIVE"
    with pytest.raises(SemanticChallengerError, match="may not assign market direction"):
        validate_semantic_candidates(
            result["candidates"],
            required_fields=contract()["required_output_fields"],
        )


def test_duplicate_anchor_does_not_duplicate_semantic_candidate():
    row = anchor(
        family="BUYBACK",
        excerpt="The board authorized a new USD 5 billion share repurchase program.",
    )
    result = extract([row, dict(row)])
    assert result["candidate_count"] == 1
    assert result["market_outcomes_read"] is False
