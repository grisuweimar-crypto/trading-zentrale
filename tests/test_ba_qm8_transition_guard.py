from __future__ import annotations

import copy

import pytest

from scanner.research.governance.ba_qm8_end_to_end import (
    EXPECTED_ERROR_CLASSES,
    EXPECTED_TRANSITIONS,
    load_contract,
)
from scanner.research.governance.ba_qm8_transition_guard import (
    BAQM8TransitionError,
    validate_transition_observation,
)


def _roles() -> dict[str, str]:
    contract = load_contract()
    return {
        str(row["stage_id"]): str(row["semantic_role"])
        for row in contract["stage_chain"]
    }


def _valid_observation(from_stage: str, to_stage: str) -> dict:
    roles = _roles()
    return {
        "schema_version": "ba_qm8_transition_observation_v1",
        "transition_id": f"test:{from_stage}->{to_stage}",
        "from_stage": from_stage,
        "to_stage": to_stage,
        "source": {
            "semantic_role": roles[from_stage],
            "snapshot_id": "snapshot-001",
            "as_of": "2026-10-04T17:00:00+00:00",
            "available_from": "2026-10-04T17:05:00+00:00",
            "material_evidence_ids": ["evidence:upstream:1"],
        },
        "target": {
            "semantic_role": roles[to_stage],
            "source_snapshot_id": "snapshot-001",
            "as_of": "2026-10-04T17:10:00+00:00",
            "available_from": "2026-10-04T17:11:00+00:00",
            "declared_input_stage": from_stage,
            "material_evidence_ids": [
                "evidence:upstream:1",
                "evidence:target-native:1",
            ],
            "declared_input_evidence_ids": ["evidence:upstream:1"],
            "target_native_evidence_ids": ["evidence:target-native:1"],
        },
        "controls": {
            "backdating_allowed": False,
            "retrojected_value_ids": [],
            "double_counting_resolution": "NO_TRIGGER",
            "semantic_contract_match": True,
            "missing_input_ids": [],
            "neutralized_missing_input_ids": [],
            "missing_is_neutral": False,
            "multiplicity_applicable": False,
            "multiplicity_control_state": "NOT_APPLICABLE",
            "snapshot_binding_required": True,
        },
    }


@pytest.mark.parametrize("from_stage,to_stage", EXPECTED_TRANSITIONS)
def test_every_adjacent_transition_can_pass_the_same_seven_fail_closed_guards(
    from_stage: str, to_stage: str
) -> None:
    result = validate_transition_observation(_valid_observation(from_stage, to_stage))
    assert result["status"] == "PASSED"
    assert result["from_stage"] == from_stage
    assert result["to_stage"] == to_stage
    assert set(result["checks"]) == set(EXPECTED_ERROR_CLASSES)
    assert all(check["passed"] is True for check in result["checks"].values())
    assert result["all_required_error_classes_passed"] is True
    assert result["promotion_effect"] == "NONE"
    assert result["investment_logic_changed"] is False


def test_leakage_attack_is_blocked() -> None:
    changed = _valid_observation("SCANNER", "SELECTION")
    changed["source"]["available_from"] = "2026-10-04T17:20:00+00:00"
    with pytest.raises(
        BAQM8TransitionError,
        match="LEAKAGE:source_must_be_observable_before_target",
    ):
        validate_transition_observation(changed)


def test_retrojection_attack_is_blocked() -> None:
    changed = _valid_observation("DATA", "SCANNER")
    changed["controls"]["retrojected_value_ids"] = ["fundamental:latest-restatement"]
    with pytest.raises(
        BAQM8TransitionError,
        match="RETROJECTION:future_vintage_or_backdating_detected",
    ):
        validate_transition_observation(changed)


def test_double_counting_attack_is_blocked() -> None:
    changed = _valid_observation("PROBABILITY", "RISK")
    changed["target"]["material_evidence_ids"].append("evidence:upstream:1")
    with pytest.raises(
        BAQM8TransitionError,
        match="DOUBLE_COUNTING:duplicate_material_evidence_or_unresolved_ancestry_review",
    ):
        validate_transition_observation(changed)


def test_semantic_drift_attack_is_blocked() -> None:
    changed = _valid_observation("RISK", "CONFIDENCE")
    changed["controls"]["semantic_contract_match"] = False
    with pytest.raises(
        BAQM8TransitionError,
        match="SEMANTIC_DRIFT:stage_role_or_declared_input_semantics_changed",
    ):
        validate_transition_observation(changed)


def test_missing_as_neutral_attack_is_blocked() -> None:
    changed = _valid_observation("LEARNING", "ELLIOTT")
    changed["controls"]["missing_input_ids"] = ["elliott:missing"]
    changed["controls"]["neutralized_missing_input_ids"] = ["elliott:missing"]
    changed["controls"]["missing_is_neutral"] = True
    with pytest.raises(
        BAQM8TransitionError,
        match="MISSING_AS_NEUTRAL:missing_input_was_neutralized",
    ):
        validate_transition_observation(changed)


def test_uncontrolled_multiplicity_attack_is_blocked() -> None:
    changed = _valid_observation("TIMING", "PROBABILITY")
    changed["controls"]["multiplicity_applicable"] = True
    changed["controls"]["multiplicity_control_state"] = "NOT_APPLICABLE"
    with pytest.raises(
        BAQM8TransitionError,
        match="UNCONTROLLED_MULTIPLICITY:multiplicity_required_but_not_predeclared_and_frozen",
    ):
        validate_transition_observation(changed)


def test_hidden_evidence_reuse_attack_is_blocked() -> None:
    changed = _valid_observation("ELLIOTT", "DECISION_LAYER")
    changed["target"]["material_evidence_ids"].append("evidence:hidden:1")
    with pytest.raises(
        BAQM8TransitionError,
        match="HIDDEN_EVIDENCE_REUSE:target_contains_undeclared_or_hidden_material_evidence",
    ):
        validate_transition_observation(changed)


def test_snapshot_identity_mismatch_is_blocked_even_after_seven_checks_pass() -> None:
    changed = _valid_observation("SELECTION", "TIMING")
    changed["target"]["source_snapshot_id"] = "snapshot-other"
    with pytest.raises(BAQM8TransitionError, match="snapshot_binding_mismatch"):
        validate_transition_observation(changed)


def test_non_adjacent_stage_shortcut_is_blocked() -> None:
    changed = _valid_observation("SCANNER", "SELECTION")
    changed["to_stage"] = "PROBABILITY"
    with pytest.raises(
        BAQM8TransitionError,
        match="non_adjacent_or_unknown_transition:SCANNER->PROBABILITY",
    ):
        validate_transition_observation(changed)


def test_unresolved_common_ancestry_review_is_blocked() -> None:
    changed = _valid_observation("CONFIDENCE", "LEARNING")
    changed["controls"]["double_counting_resolution"] = "REVIEW_REQUIRED"
    with pytest.raises(
        BAQM8TransitionError,
        match="DOUBLE_COUNTING:duplicate_material_evidence_or_unresolved_ancestry_review",
    ):
        validate_transition_observation(changed)


def test_declared_reuse_must_be_unique_and_explicit() -> None:
    changed = _valid_observation("DECISION_LAYER", "EXTERNAL_EVIDENCE")
    changed["target"]["declared_input_evidence_ids"].append("evidence:upstream:1")
    with pytest.raises(
        BAQM8TransitionError,
        match="HIDDEN_EVIDENCE_REUSE:declared_input_evidence_ids_not_unique",
    ):
        validate_transition_observation(changed)
