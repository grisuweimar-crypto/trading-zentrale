from __future__ import annotations

import json
from pathlib import Path

from scanner.research.external_evidence.semantic_validation import anchor_id, evaluate_all

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads(
    (ROOT / "configs" / "external_evidence_8c_semantic_validation_v1.json").read_text()
)


def _candidate(idx: int) -> dict:
    return {
        "family": "BUYBACK",
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
        "action": "NEW_AUTHORIZATION",
        "authorization_amount": 1_000_000_000.0,
        "currency": "USD",
    }


def _annotation(candidate: dict, *, target: bool) -> dict:
    return {
        "anchor_id": anchor_id(candidate),
        "family": "BUYBACK",
        "truth_label": "NEW_AUTHORIZATION" if target else "NOT_TARGET",
        "review_status": "LABELED",
        "market_outcomes_seen": False,
        "notes": "",
        "truth_action": candidate["action"] if target else None,
        "truth_authorization_amount": candidate["authorization_amount"] if target else None,
        "truth_currency": candidate["currency"] if target else None,
    }


def test_false_positive_does_not_enter_critical_field_accuracy_denominator() -> None:
    candidates = [_candidate(i) for i in range(60)]
    annotations = [_annotation(row, target=True) for row in candidates]
    annotations[-1] = _annotation(candidates[-1], target=False)

    family = evaluate_all(
        candidate_rows=candidates,
        annotations=annotations,
        contract=CONTRACT,
    )["families"][0]

    assert family["labeled_candidate_count"] == 60
    assert family["correct_target_count"] == 59
    assert family["critical_field_comparisons"] == 59 * 3
    assert family["critical_field_correct"] == 59 * 3
    assert family["critical_field_accuracy"] == 1.0
