from __future__ import annotations

import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.semantic_validation import (
    SemanticValidationError,
    anchor_id,
    deterministic_sample,
    evaluate_all,
    evaluate_anchor_audit,
    wilson_lower,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads(
    (ROOT / "configs" / "external_evidence_8c_semantic_validation_v1.json").read_text()
)


def _candidate(family: str, idx: int) -> dict:
    base = {
        "family": family,
        "symbol": f"SYM{idx}",
        "cik": f"{idx:010d}",
        "accession_number": f"0000000000-26-{idx:06d}",
        "excerpt_sha256": f"{idx:064x}"[-64:],
        "evidence_excerpt_sha256": f"{idx:064x}"[-64:],
        "source_valid_from": "2026-01-01T12:00:00+00:00",
        "source_document": "ex99.htm",
        "parser_version": "8c_semantic_challenger_v1",
        "market_direction": "UNKNOWN",
        "semantic_status": "EXTRACTED_CANDIDATE",
    }
    if family == "DIVIDEND":
        base.update(
            {
                "amount_per_share": 1.0,
                "currency": "USD",
                "security_class": "COMMON",
                "frequency": "quarterly",
            }
        )
    else:
        base.update(
            {
                "action": "NEW_AUTHORIZATION",
                "authorization_amount": 1_000_000_000.0,
                "currency": "USD",
            }
        )
    return base


def _annotation(candidate: dict, *, correct: bool = True) -> dict:
    family = candidate["family"]
    row = {
        "anchor_id": anchor_id(candidate),
        "family": family,
        "truth_label": (
            "REGULAR_QUARTERLY_DIVIDEND_DECLARATION"
            if family == "DIVIDEND" and correct
            else "NEW_AUTHORIZATION"
            if family == "BUYBACK" and correct
            else "NOT_TARGET"
        ),
        "review_status": "LABELED",
        "market_outcomes_seen": False,
        "notes": "",
    }
    if family == "DIVIDEND":
        row.update(
            {
                "truth_amount_per_share": candidate["amount_per_share"],
                "truth_currency": candidate["currency"],
                "truth_security_class": candidate["security_class"],
                "truth_frequency": candidate["frequency"],
            }
        )
    else:
        row.update(
            {
                "truth_action": candidate["action"],
                "truth_authorization_amount": candidate["authorization_amount"],
                "truth_currency": candidate["currency"],
            }
        )
    return row


def _anchor(family: str, idx: int) -> dict:
    row = _candidate(family, idx)
    return {
        "family": family,
        "symbol": row["symbol"],
        "cik": row["cik"],
        "accession_number": row["accession_number"],
        "excerpt_sha256": row["excerpt_sha256"],
        "excerpt": "audit excerpt",
        "semantic_status": "ANCHOR_ONLY",
        "market_direction": "UNKNOWN",
    }


def test_sampling_is_deterministic_and_family_bounded() -> None:
    rows = [_candidate("DIVIDEND", i) for i in range(8)] + [
        _candidate("BUYBACK", i + 100) for i in range(8)
    ]
    first = deterministic_sample(rows, seed="8C-I-v1", per_family_max=3)
    second = deterministic_sample(reversed(rows), seed="8C-I-v1", per_family_max=3)
    assert [row["anchor_id"] for row in first] == [
        row["anchor_id"] for row in second
    ]
    assert len(first) == 6


def test_wilson_lower_behaves_conservatively() -> None:
    assert wilson_lower(40, 40) < 1.0
    assert wilson_lower(40, 40) > 0.90
    assert wilson_lower(0, 0) is None


def test_market_outcome_visibility_fails_closed() -> None:
    candidate = _candidate("DIVIDEND", 1)
    annotation = _annotation(candidate)
    annotation["market_outcomes_seen"] = True
    with pytest.raises(SemanticValidationError):
        evaluate_all(
            candidate_rows=[candidate], annotations=[annotation], contract=CONTRACT
        )


def test_intrinsic_low_coverage_gets_preregistered_status() -> None:
    candidates = [_candidate("DIVIDEND", i) for i in range(10)]
    annotations = [_annotation(row) for row in candidates]
    result = evaluate_all(
        candidate_rows=candidates, annotations=annotations, contract=CONTRACT
    )
    family = result["families"][0]
    assert family["promotion_status"] == "LOW_COVERAGE_NOT_PROMOTABLE"
    assert family["intrinsic_low_coverage"] is True
    assert family["checks"]["minimum_labeled_emitted_candidates"] is False


def test_perfect_preregistered_sample_can_pass() -> None:
    candidates = [_candidate("DIVIDEND", i) for i in range(60)]
    annotations = [_annotation(row) for row in candidates]
    result = evaluate_all(
        candidate_rows=candidates, annotations=annotations, contract=CONTRACT
    )
    family = result["families"][0]
    assert family["promotion_status"] == "PASS"
    assert family["precision_wilson_95_lower"] >= 0.90
    assert family["critical_field_accuracy_wilson_95_lower"] >= 0.90
    assert result["market_outcomes_read"] is False
    assert result["phase7_integration_enabled"] is False


def test_false_positive_does_not_enter_critical_field_accuracy_denominator() -> None:
    candidates = [_candidate("BUYBACK", i) for i in range(60)]
    annotations = [_annotation(row) for row in candidates]
    annotations[-1] = _annotation(candidates[-1], correct=False)
    annotations[-1]["truth_action"] = None
    annotations[-1]["truth_authorization_amount"] = None
    annotations[-1]["truth_currency"] = None

    result = evaluate_all(
        candidate_rows=candidates, annotations=annotations, contract=CONTRACT
    )
    family = result["families"][0]
    assert family["correct_target_count"] == 59
    assert family["labeled_candidate_count"] == 60
    assert family["critical_field_comparisons"] == 59 * 3
    assert family["critical_field_correct"] == 59 * 3
    assert family["critical_field_accuracy"] == 1.0


def test_pooled_success_cannot_hide_family_failure() -> None:
    dividend = [_candidate("DIVIDEND", i) for i in range(60)]
    buyback = [_candidate("BUYBACK", i + 1000) for i in range(10)]
    annotations = [_annotation(row) for row in dividend + buyback]
    result = evaluate_all(
        candidate_rows=dividend + buyback,
        annotations=annotations,
        contract=CONTRACT,
    )
    states = {row["family"]: row["promotion_status"] for row in result["families"]}
    assert states["DIVIDEND"] == "PASS"
    assert states["BUYBACK"] == "LOW_COVERAGE_NOT_PROMOTABLE"
    assert result["all_families_pass"] is False


def test_anchor_audit_reports_false_negatives_descriptively_only() -> None:
    anchors = [_anchor("DIVIDEND", i) for i in range(4)]
    emitted = [_candidate("DIVIDEND", 0), _candidate("DIVIDEND", 2)]
    annotations = []
    for row in anchors:
        annotations.append(
            {
                "anchor_id": anchor_id(row),
                "family": "DIVIDEND",
                "truth_label": "REGULAR_QUARTERLY_DIVIDEND_DECLARATION",
                "review_status": "LABELED",
                "market_outcomes_seen": False,
                "notes": "",
            }
        )
    result = evaluate_anchor_audit(
        anchor_rows=anchors,
        annotations=annotations,
        candidate_rows=emitted,
    )
    family = result["families"][0]
    assert family["target_count"] == 4
    assert family["emitted_target_count"] == 2
    assert family["false_negative_count"] == 2
    assert family["descriptive_recall"] == 0.5
    assert family["promotion_effect"] == "NONE_DESCRIPTIVE_ONLY"
    assert result["promotion_effect"] == "NONE"
