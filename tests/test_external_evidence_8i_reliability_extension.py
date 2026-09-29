from __future__ import annotations

import copy
import json
import subprocess
from pathlib import Path

import pytest

from scanner.research.external_evidence.reliability_extension_8i import (
    ANNOTATION_SCHEMA,
    GATE_SCHEMA,
    MANIFEST_SCHEMA,
    PRIMARY_PHASE7_STATES,
    RESULT_SCHEMA,
    ExternalEvidence8IReliabilityError,
    build_reliability_annotation,
    current_reliability_status,
    digest,
    evaluate_synthetic_terminal_family,
    freeze_prospective_manifest,
    terminal_family_gate,
    validate_reliability_contract,
)
from scanner.research.external_evidence.external_aggregation_8i import AGGREGATION_RESULT_SCHEMA


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs" / "external_evidence_8i_reliability_extension_research_v1.json"
PHASE7_PATH = ROOT / "configs" / "decision_reliability_explainability_v1.json"
DIRECTION_PATH = ROOT / "configs" / "external_evidence_8i_component_direction_state_engine_v1.json"
AGGREGATION_PATH = ROOT / "configs" / "external_evidence_8i_external_aggregation_design_v1.json"
DATASET_PATH = ROOT / "configs" / "decision_research_dataset_v1.json"
MATRIX_PATH = ROOT / "configs" / "external_conflict_matrix_v1.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _git_blob_sha(path: str) -> str:
    return subprocess.check_output(["git", "rev-parse", f"HEAD:{path}"], cwd=ROOT, text=True).strip()


def _contracts() -> tuple[dict, dict, dict, dict, dict, dict]:
    return (
        _load(CONTRACT_PATH),
        _load(PHASE7_PATH),
        _load(DIRECTION_PATH),
        _load(AGGREGATION_PATH),
        _load(DATASET_PATH),
        _load(MATRIX_PATH),
    )


def _phase7(
    *,
    snapshot_id: str = "snapshot-2026-09-29T17:00:00Z",
    symbol: str = "SYNTH",
    as_of: str = "2026-09-29T17:00:00+00:00",
    state: str = "positive",
    reliability: str = "provisional_unopposed_support",
) -> dict:
    direction = state if state in {"positive", "negative"} else None
    row = {
        "schema_version": "decision_reliability_explainability_v1",
        "phase": "7G",
        "symbol": symbol,
        "as_of": as_of,
        "source_snapshot_id": snapshot_id,
        "decision_context": {
            "universal_stance_state": state,
            "universal_stance_direction": direction,
            "portfolio_action_state": "HOLD",
            "preserved": True,
        },
        "explanation": {"directional_evidence": {"selected_direction": direction}},
        "reliability": {
            "assessment": reliability,
            "numeric_reliability_score": None,
            "weighted_score_used": False,
            "assessment_is_empirical_success_probability": False,
        },
        "change_triggers": {"decision_change_triggers": [], "information_completion_triggers": []},
        "semantics": {
            "stance_recomputed": False,
            "conflict_resolved": False,
            "transition_recomputed": False,
            "portfolio_action_changed": False,
            "new_portfolio_action_generated": False,
            "position_sizing_computed": False,
            "target_weight_computed": False,
            "weighted_super_score_used": False,
            "probability_or_confidence_used_as_vote": False,
            "risk_or_elliott_used_as_directional_vote": False,
            "missing_evidence_treated_as_neutral": False,
            "broker_order_generated": False,
        },
        "validation": {
            "research_only": True,
            "source_consistency_verified": True,
            "explanation_is_descriptive_not_predictive": True,
            "reliability_model_empirically_validated": False,
            "productive_integration_enabled": False,
            "execution_allowed": False,
            "promotion_eligible": False,
        },
    }
    row["explanation_id"] = digest(row)
    return row


def _aggregation(
    *,
    snapshot_id: str = "snapshot-2026-09-29T17:00:00Z",
    symbol: str = "SYNTH",
    horizon: int = 20,
    external_state: str = "POSITIVE",
    relation: str = "CONFIRMING",
    generated_at: str = "2026-09-29T17:00:00+00:00",
) -> dict:
    row = {
        "schema_version": AGGREGATION_RESULT_SCHEMA,
        "phase": "8I-C",
        "state": "SYNTHETIC_AGGREGATION_COMPLETE",
        "synthetic": True,
        "snapshot_id": snapshot_id,
        "symbol": symbol,
        "horizon_sessions": horizon,
        "generated_at": generated_at,
        "included_component_ids": ["synthetic_component"],
        "excluded_component_ids_with_reasons": {},
        "dependency_bundles": [],
        "bundle_states": {},
        "known_direction_set": [external_state] if external_state in {"POSITIVE", "NEGATIVE"} else [],
        "unknown_present": external_state == "UNKNOWN",
        "external_direction_state": external_state,
        "external_evidence_state": external_state,
        "relation_state_if_core_state_supplied": relation,
        "binding_sha256s": {"synthetic_component": "a" * 64},
        "direction_adapter_sha256s": {"synthetic_component": "b" * 64},
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
        "real_aggregation_authorized": False,
        "external_decision_influence_enabled": False,
        "phase7_mutation_authorized": False,
        "extended_reliability_enabled": False,
        "extended_stance_enabled": False,
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subblock": "8I-D_COMPONENT_DIRECTION_STATE_ENGINE_PREREGISTRATION",
    }
    row["aggregation_sha256"] = digest(row)
    return row


def _annotate(phase7: dict, aggregation: dict) -> dict:
    contract, p7, direction, agg, dataset, matrix = _contracts()
    return build_reliability_annotation(
        contract=contract,
        phase7_contract=p7,
        direction_contract=direction,
        aggregation_contract=agg,
        dataset_contract=dataset,
        conflict_matrix=matrix,
        phase7_explanation=phase7,
        aggregation_result=aggregation,
        synthetic=True,
    )


def test_8i_e_contract_is_outcome_blind_research_only_and_exactly_bound() -> None:
    contract, p7, direction, agg, dataset, matrix = _contracts()
    validate_reliability_contract(contract, p7, direction, agg, dataset, matrix)
    assert contract["phase"] == "8I-E"
    assert contract["research_only"] is True
    assert contract["shadow_mode"] is True
    assert contract["real_decision_outcome_read_allowed"] is False
    assert contract["extended_reliability_enabled"] is False
    assert contract["extended_stance_enabled"] is False
    assert contract["portfolio_action_change_enabled"] is False
    assert contract["orders_or_trades_enabled"] is False
    for parent in contract["parent_contracts"].values():
        assert _git_blob_sha(parent["path"]) == parent["git_blob_sha"]


def test_primary_metric_family_and_no_numeric_reliability_are_frozen() -> None:
    contract, p7, direction, agg, dataset, matrix = _contracts()
    validate_reliability_contract(contract, p7, direction, agg, dataset, matrix)
    assert contract["outcome_definition"]["primary_metric"] == "direction_aligned_peer_excess"
    assert contract["outcome_definition"]["formula"] == {"POSITIVE": "peer_excess_{H}t", "NEGATIVE": "-1 * peer_excess_{H}t"}
    assert contract["annotation_model"]["numeric_reliability_score_allowed"] is False
    assert contract["annotation_model"]["ordinal_reliability_ranking_allowed"] is False
    family = contract["primary_hypothesis_family"]
    assert family["family_size"] == 32
    assert len(family["family_members"]) == 32
    assert contract["statistics"]["multiple_testing_method"] == "Holm"
    assert contract["statistics"]["family_wise_alpha"] == 0.05


def test_confirming_and_conflicting_annotations_preserve_phase7_reliability() -> None:
    confirming = _annotate(_phase7(), _aggregation(external_state="POSITIVE", relation="CONFIRMING"))
    assert confirming["schema_version"] == ANNOTATION_SCHEMA
    assert confirming["phase7_reliability_state"] == "provisional_unopposed_support"
    assert confirming["external_relation_state"] == "CONFIRMING"
    assert confirming["research_role"] == "PRIMARY"
    assert confirming["primary_research_eligible"] is True
    assert confirming["numeric_reliability_score"] is None
    assert confirming["extended_reliability_state"] is None
    assert confirming["extended_reliability_enabled"] is False
    assert confirming["decision_effect"] == "NO_CHANGE_TO_PHASE7_DECISION"

    conflicting = _annotate(_phase7(), _aggregation(external_state="NEGATIVE", relation="CONFLICTING"))
    assert conflicting["external_relation_state"] == "CONFLICTING"
    assert conflicting["research_role"] == "PRIMARY"
    assert conflicting["phase7_reliability_state"] == confirming["phase7_reliability_state"]


def test_mixed_unknown_and_external_only_are_descriptive_not_primary() -> None:
    mixed = _annotate(_phase7(), _aggregation(external_state="MIXED", relation="MIXED_EXTERNAL"))
    assert mixed["research_role"] == "SECONDARY_DESCRIPTIVE"
    assert mixed["primary_research_eligible"] is False
    unknown = _annotate(_phase7(), _aggregation(external_state="UNKNOWN", relation="UNKNOWN"))
    assert unknown["research_role"] == "SECONDARY_DESCRIPTIVE"
    external_only = _annotate(
        _phase7(state="conflicted", reliability="blocked_conflict"),
        _aggregation(external_state="POSITIVE", relation="EXTERNAL_ONLY"),
    )
    assert external_only["research_role"] == "SECONDARY_DESCRIPTIVE"
    assert external_only["phase7_core_direction"] is None
    assert external_only["extended_reliability_enabled"] is False


def test_relation_snapshot_and_phase7_integrity_mismatches_fail_closed() -> None:
    with pytest.raises(ExternalEvidence8IReliabilityError, match="snapshot_identity_mismatch"):
        _annotate(_phase7(snapshot_id="A"), _aggregation(snapshot_id="B"))
    wrong = _aggregation(external_state="POSITIVE", relation="CONFLICTING")
    with pytest.raises(ExternalEvidence8IReliabilityError, match="relation_mapping_mismatch"):
        _annotate(_phase7(), wrong)
    broken = _phase7()
    broken["reliability"]["assessment"] = "made_up"
    with pytest.raises(Exception, match="invalid_reliability_assessment"):
        _annotate(broken, _aggregation())


def test_real_annotation_manifest_gate_and_outcome_evaluation_remain_closed() -> None:
    contract, p7, direction, agg, dataset, matrix = _contracts()
    with pytest.raises(ExternalEvidence8IReliabilityError, match="real_reliability_annotation_not_authorized"):
        build_reliability_annotation(
            contract=contract,
            phase7_contract=p7,
            direction_contract=direction,
            aggregation_contract=agg,
            dataset_contract=dataset,
            conflict_matrix=matrix,
            phase7_explanation=_phase7(),
            aggregation_result=_aggregation(),
            synthetic=False,
        )
    annotation = _annotate(_phase7(), _aggregation())
    with pytest.raises(ExternalEvidence8IReliabilityError, match="real_manifest_freeze_not_authorized"):
        freeze_prospective_manifest(
            contract=contract,
            annotations=[annotation],
            author_identity="test",
            authored_at="2026-09-30T00:00:00+00:00",
            synthetic=False,
        )


def test_manifest_freeze_is_outcome_blind_and_prospective_only() -> None:
    contract, *_ = _contracts()
    annotation = _annotate(
        _phase7(as_of="2026-09-29T17:00:00+00:00"),
        _aggregation(generated_at="2026-09-29T17:00:00+00:00"),
    )
    manifest = freeze_prospective_manifest(
        contract=contract,
        annotations=[annotation],
        author_identity="synthetic-test",
        authored_at="2026-09-30T00:00:00+00:00",
        synthetic=True,
    )
    assert manifest["schema_version"] == MANIFEST_SCHEMA
    assert manifest["partition"] == "PROSPECTIVE"
    assert manifest["outcomes_read_while_freezing"] is False
    assert manifest["outcomes_opened"] is False
    assert manifest["repeated_interim_significance_looks_allowed"] is False
    tainted = copy.deepcopy(annotation)
    tainted["peer_excess_20t"] = 0.3
    tainted["annotation_sha256"] = digest({k: v for k, v in tainted.items() if k != "annotation_sha256"})
    with pytest.raises(ExternalEvidence8IReliabilityError, match="pre_gate_outcome_field_forbidden"):
        freeze_prospective_manifest(
            contract=contract,
            annotations=[tainted],
            author_identity="synthetic-test",
            authored_at="2026-09-30T00:00:00+00:00",
            synthetic=True,
        )


def _synthetic_family_manifest(days: int = 4) -> tuple[dict, list[dict]]:
    contract, *_ = _contracts()
    rows = []
    outcomes = []
    relations = ("CONFIRMING", "CONFLICTING", "INSUFFICIENT_EXTERNAL")
    for horizon in (5, 20, 40, 60):
        for state in PRIMARY_PHASE7_STATES:
            for relation in relations:
                for day in range(days):
                    date = f"2027-01-{day + 1:02d}T17:00:00+00:00"
                    snapshot = f"snap-{horizon}-{state}-{relation}-{day}"
                    symbol = f"S{day:03d}"
                    annotation = {
                        "schema_version": ANNOTATION_SCHEMA,
                        "phase": "8I-E",
                        "state": "SYNTHETIC_RELIABILITY_RESEARCH_ANNOTATION",
                        "synthetic": True,
                        "snapshot_id": snapshot,
                        "as_of": date,
                        "symbol": symbol,
                        "horizon_sessions": horizon,
                        "generated_at": date,
                        "phase7_explanation_id": "a" * 64,
                        "aggregation_sha256": "b" * 64,
                        "phase7_reliability_state": state,
                        "phase7_core_state": "POSITIVE",
                        "phase7_core_direction": "POSITIVE",
                        "external_direction_state": "POSITIVE",
                        "external_relation_state": relation,
                        "research_role": "PRIMARY",
                        "primary_research_eligible": True,
                        "research_cell": f"{state}|{relation}|{horizon}t",
                        "phase7_reliability_preserved": True,
                        "phase7_stance_preserved": True,
                        "phase7_portfolio_action_preserved": True,
                        "numeric_reliability_score": None,
                        "ordinal_reliability_rank": None,
                        "extended_reliability_state": None,
                        "extended_reliability_enabled": False,
                        "extended_stance_enabled": False,
                        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
                        "portfolio_action_change_authorized": False,
                        "orders_or_trades_authorized": False,
                    }
                    annotation["annotation_sha256"] = digest(annotation)
                    rows.append(annotation)
                    value = 0.03 if relation == "CONFIRMING" else (-0.01 if relation == "CONFLICTING" else 0.01)
                    outcomes.append({
                        "snapshot_id": snapshot,
                        "as_of": date,
                        "symbol": symbol,
                        "horizon_sessions": horizon,
                        f"peer_excess_{horizon}t": value,
                    })
    manifest = freeze_prospective_manifest(
        contract=contract,
        annotations=rows,
        author_identity="synthetic-test",
        authored_at="2027-02-01T00:00:00+00:00",
        synthetic=True,
    )
    return manifest, outcomes


def test_terminal_gate_cannot_open_outcomes_when_full_family_support_is_insufficient() -> None:
    contract, *_ = _contracts()
    manifest, _ = _synthetic_family_manifest(days=4)
    maturity = []
    for row in manifest["rows"]:
        horizon = int(row["horizon_sessions"])
        maturity.append({
            "snapshot_id": row["snapshot_id"],
            "as_of": row["as_of"],
            "symbol": row["symbol"],
            "horizon_sessions": horizon,
            f"label_available_from_{horizon}t": "2027-02-01T00:00:00+00:00",
        })
    gate = terminal_family_gate(
        contract=contract,
        manifest=manifest,
        maturity_rows=maturity,
        research_as_of="2027-03-01T00:00:00+00:00",
        synthetic=True,
    )
    assert gate["schema_version"] == GATE_SCHEMA
    assert gate["ready_for_one_shot_terminal_family_evaluation"] is False
    assert gate["outcomes_open_authorized"] is False
    assert gate["outcomes_read"] is False


def test_synthetic_terminal_analysis_uses_direction_aligned_peer_excess_and_holm_without_promotion() -> None:
    contract, *_ = _contracts()
    manifest, outcomes = _synthetic_family_manifest(days=4)
    gate = {
        "schema_version": GATE_SCHEMA,
        "phase": "8I-E",
        "synthetic": True,
        "manifest_sha256": manifest["manifest_sha256"],
        "research_as_of": "2027-03-01T00:00:00+00:00",
        "family_size": 32,
        "hypothesis_readiness": [{"hypothesis_id": h["hypothesis_id"], "ready": True} for h in contract["primary_hypothesis_family"]["family_members"]],
        "ready_for_one_shot_terminal_family_evaluation": True,
        "outcomes_open_authorized": True,
        "outcomes_read": False,
        "extended_reliability_enabled": False,
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
    }
    gate["gate_sha256"] = digest(gate)
    result = evaluate_synthetic_terminal_family(
        contract=contract,
        manifest=manifest,
        gate=gate,
        outcome_rows=outcomes,
        synthetic=True,
    )
    assert result["schema_version"] == RESULT_SCHEMA
    assert result["family_size"] == 32
    assert result["primary_metric"] == "direction_aligned_peer_excess"
    assert result["multiple_testing_method"] == "Holm"
    assert result["synthetic_tests_are_empirical_evidence"] is False
    assert result["real_empirical_completion"] is False
    assert result["extended_reliability_enabled"] is False
    assert result["extended_stance_enabled"] is False
    assert result["decision_effect"] == "NO_CHANGE_TO_PHASE7_DECISION"
    confirming = next(r for r in result["results"] if "CONFIRMING_MINUS" in r["hypothesis_id"])
    conflicting = next(r for r in result["results"] if "CONFLICTING_MINUS" in r["hypothesis_id"])
    assert confirming["primary_effect"] > 0
    assert conflicting["primary_effect"] < 0
    assert confirming["automatic_promotion"] is False
    assert conflicting["failed_hypothesis_inverted"] is False


def test_current_repository_state_remains_blocked_and_phase7_neutral() -> None:
    contract, p7, direction, agg, dataset, matrix = _contracts()
    state = current_reliability_status(contract, p7, direction, agg, dataset, matrix)
    assert state["state"] == "BLOCKED_BY_8I_B_UPSTREAM_SOURCE_IDENTITY_CONTRACT"
    assert state["real_bound_component_ids"] == []
    assert state["real_reliability_annotations_authorized"] is False
    assert state["real_outcomes_opened"] is False
    assert state["terminal_family_evaluation_complete"] is False
    assert state["phase7_reliability_preserved"] is True
    assert state["extended_reliability_enabled"] is False
    assert state["extended_stance_enabled"] is False
    assert state["decision_effect"] == "NO_CHANGE_TO_PHASE7_DECISION"
    assert state["portfolio_action_change_authorized"] is False
    assert state["orders_or_trades_authorized"] is False


def test_contract_rejects_numeric_score_metric_or_hypothesis_family_drift() -> None:
    contract, p7, direction, agg, dataset, matrix = _contracts()
    broken = copy.deepcopy(contract)
    broken["annotation_model"]["numeric_reliability_score_allowed"] = True
    with pytest.raises(ExternalEvidence8IReliabilityError, match="annotation_policy_leak"):
        validate_reliability_contract(broken, p7, direction, agg, dataset, matrix)
    broken = copy.deepcopy(contract)
    broken["outcome_definition"]["primary_metric"] = "binary_hit_rate"
    with pytest.raises(ExternalEvidence8IReliabilityError, match="primary_metric_drift"):
        validate_reliability_contract(broken, p7, direction, agg, dataset, matrix)
    broken = copy.deepcopy(contract)
    broken["primary_hypothesis_family"]["family_members"] = broken["primary_hypothesis_family"]["family_members"][:-1]
    with pytest.raises(ExternalEvidence8IReliabilityError, match="hypothesis_family_membership_or_order_drift"):
        validate_reliability_contract(broken, p7, direction, agg, dataset, matrix)
