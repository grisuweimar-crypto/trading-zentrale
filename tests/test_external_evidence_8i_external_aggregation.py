from __future__ import annotations

import copy
import json
import subprocess
from pathlib import Path

import pytest

from scanner.research.external_evidence.decision_binding_8i import (
    BINDING_RESULT_SCHEMA,
    BOUND,
    digest as binding_digest,
)
from scanner.research.external_evidence.external_aggregation_8i import (
    ExternalEvidence8IAggregationError,
    INSUFFICIENT,
    MIXED,
    NEGATIVE,
    POSITIVE,
    UNKNOWN,
    aggregate_external_evidence,
    classify_relation,
    current_aggregation_status,
    reduce_direction_states,
    validate_aggregation_contract,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs" / "external_evidence_8i_external_aggregation_design_v1.json"
BINDING_CONTRACT_PATH = ROOT / "configs" / "external_evidence_8i_promotion_provenance_binding_v1.json"
CONFLICT_MATRIX_PATH = ROOT / "configs" / "external_conflict_matrix_v1.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _git_blob_sha(path: str) -> str:
    return subprocess.check_output(["git", "rev-parse", f"HEAD:{path}"], cwd=ROOT, text=True).strip()


def _contracts() -> tuple[dict, dict, dict]:
    return _load(CONTRACT_PATH), _load(BINDING_CONTRACT_PATH), _load(CONFLICT_MATRIX_PATH)


def _binding(
    component_id: str,
    factor_id: str,
    *,
    horizon: int = 20,
    component_type: str = "8g_main_effect",
    parents: list[str] | None = None,
) -> dict:
    row = {
        "schema_version": BINDING_RESULT_SCHEMA,
        "phase": "8I-B",
        "state": BOUND,
        "component_type": component_type,
        "component_id": component_id,
        "factor_id": factor_id,
        "interaction_id_if_applicable": component_id if component_type == "8h_interaction" else None,
        "parent_factor_ids_if_applicable": list(parents or []),
        "horizon_sessions": horizon,
        "external_decision_influence_enabled": False,
        "decision_outcome_access_authorized": False,
        "phase7_mutation_authorized": False,
        "productive_integration_enabled": False,
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
    }
    row["binding_sha256"] = binding_digest(row)
    return row


def _input(
    binding: dict,
    *,
    status: str = "USABLE",
    direction: str | None = POSITIVE,
    horizon: int | None = None,
    unavailable_reason: str | None = None,
) -> dict:
    row = {
        "binding": binding,
        "snapshot_id": "snapshot-2026-09-28T17:00:00Z",
        "symbol": "SYNTH",
        "horizon_sessions": int(binding["horizon_sessions"] if horizon is None else horizon),
        "generated_at": "2026-09-28T17:00:00+00:00",
        "component_observation_status": status,
        "direction_state": direction,
        "direction_adapter_id": "synthetic_adapter",
        "direction_adapter_version": "synthetic-v1",
        "direction_adapter_sha256": "a" * 64,
        "direction_valid_from": "2026-09-28T16:59:00+00:00",
    }
    if unavailable_reason is not None:
        row["unavailable_reason"] = unavailable_reason
    return row


def _aggregate(rows: list[dict], *, core_state: str | None = None) -> dict:
    contract, binding_contract, matrix = _contracts()
    return aggregate_external_evidence(
        contract=contract,
        binding_contract=binding_contract,
        conflict_matrix=matrix,
        component_inputs=rows,
        synthetic=True,
        core_state=core_state,
    )


def test_8i_c_contract_is_design_only_and_binds_exact_parents() -> None:
    contract, binding_contract, matrix = _contracts()
    validate_aggregation_contract(contract, binding_contract, matrix)

    assert contract["schema_version"] == "external_evidence_8i_external_aggregation_design_v1"
    assert contract["phase"] == "8I-C"
    assert contract["status"] == "FROZEN_OUTCOME_BLIND_AGGREGATION_DESIGN_ONLY"
    assert contract["research_only"] is True
    assert contract["shadow_mode"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["phase7_mutation_enabled"] is False
    assert contract["external_state_engine_enabled"] is False
    assert contract["extended_reliability_enabled"] is False
    assert contract["extended_stance_enabled"] is False
    assert contract["portfolio_action_change_enabled"] is False
    assert contract["orders_or_trades_enabled"] is False
    assert contract["real_decision_outcome_read_allowed"] is False

    parents = contract["parent_contracts"]
    assert _git_blob_sha(parents["8i_a"]["path"]) == parents["8i_a"]["git_blob_sha"]
    assert _git_blob_sha(parents["8i_b"]["path"]) == parents["8i_b"]["git_blob_sha"]
    assert _git_blob_sha(parents["conflict_matrix"]["path"]) == parents["conflict_matrix"]["git_blob_sha"]


def test_direction_semantics_are_not_selected_in_8i_c() -> None:
    contract, _, _ = _contracts()
    boundary = contract["component_direction_boundary"]
    assert boundary["8i_c_derives_direction_from_raw_factor_values"] is False
    assert boundary["8i_c_derives_direction_from_model_coefficients"] is False
    assert boundary["8i_c_derives_direction_from_p_values"] is False
    assert boundary["8i_c_selects_zero_or_nonzero_threshold"] is False
    assert boundary["8i_c_selects_sign_convention"] is False
    assert boundary["direction_state_must_be_supplied_by_versioned_outcome_blind_adapter"] is True
    assert boundary["real_direction_adapter_current_state"] == "NOT_YET_FROZEN"


def test_set_reducer_is_order_and_multiplicity_invariant() -> None:
    assert reduce_direction_states([])[0] == INSUFFICIENT
    assert reduce_direction_states([POSITIVE])[0] == POSITIVE
    assert reduce_direction_states([POSITIVE, POSITIVE, POSITIVE])[0] == POSITIVE
    assert reduce_direction_states([NEGATIVE, NEGATIVE])[0] == NEGATIVE
    assert reduce_direction_states([POSITIVE, NEGATIVE])[0] == MIXED
    assert reduce_direction_states([POSITIVE, POSITIVE, NEGATIVE])[0] == MIXED
    assert reduce_direction_states([NEGATIVE, POSITIVE, POSITIVE])[0] == MIXED
    assert reduce_direction_states([POSITIVE, UNKNOWN])[0] == UNKNOWN
    assert reduce_direction_states([UNKNOWN, POSITIVE])[0] == UNKNOWN
    assert reduce_direction_states([POSITIVE, NEGATIVE, UNKNOWN])[0] == MIXED


def test_independent_main_effects_use_set_semantics_not_votes() -> None:
    rates = _input(_binding("rates_policy_x_20t", "rates_policy"), direction=POSITIVE)
    fx = _input(_binding("fx_x_20t", "fx"), direction=NEGATIVE)
    result = _aggregate([rates, fx])
    assert result["external_direction_state"] == MIXED
    assert result["known_direction_set"] == [POSITIVE, NEGATIVE]
    assert result["decision_effect"] == "NO_CHANGE_TO_PHASE7_DECISION"
    assert result["external_decision_influence_enabled"] is False


def test_interaction_and_parents_form_one_dependency_bundle_not_three_votes() -> None:
    rates = _input(_binding("rates_policy_x_20t", "rates_policy"), direction=POSITIVE)
    curve = _input(_binding("yield_curve_x_20t", "yield_curve"), direction=POSITIVE)
    interaction_binding = _binding(
        "rates_policy__yield_curve_x_20t",
        "rates_policy_yield_curve",
        component_type="8h_interaction",
        parents=["rates_policy", "yield_curve"],
    )
    interaction = _input(interaction_binding, direction=NEGATIVE)
    result = _aggregate([rates, curve, interaction])

    assert result["external_direction_state"] == MIXED
    assert len(result["dependency_bundles"]) == 1
    bundle = result["dependency_bundles"][0]
    assert bundle["factor_ids"] == ["rates_policy", "yield_curve"]
    assert set(bundle["component_ids"]) == {
        "rates_policy_x_20t",
        "yield_curve_x_20t",
        "rates_policy__yield_curve_x_20t",
    }
    assert bundle["state"] == MIXED


def test_orphan_interaction_fails_closed() -> None:
    interaction = _input(
        _binding(
            "rates_policy__yield_curve_x_20t",
            "rates_policy_yield_curve",
            component_type="8h_interaction",
            parents=["rates_policy", "yield_curve"],
        ),
        direction=POSITIVE,
    )
    with pytest.raises(ExternalEvidence8IAggregationError, match="orphan_interaction"):
        _aggregate([interaction])


def test_unknown_valid_is_preserved_not_neutralized() -> None:
    rates = _input(_binding("rates_policy_x_20t", "rates_policy"), direction=POSITIVE)
    fx = _input(
        _binding("fx_x_20t", "fx"),
        status="UNKNOWN_VALID",
        direction=UNKNOWN,
    )
    result = _aggregate([rates, fx])
    assert result["external_direction_state"] == UNKNOWN
    assert result["unknown_present"] is True


def test_unavailable_component_is_excluded_with_reason_not_zero_filled() -> None:
    rates = _input(_binding("rates_policy_x_20t", "rates_policy"), direction=POSITIVE)
    fx = _input(
        _binding("fx_x_20t", "fx"),
        status="UNAVAILABLE",
        direction=None,
        unavailable_reason="STALE_AT_SNAPSHOT",
    )
    result = _aggregate([rates, fx])
    assert result["external_direction_state"] == POSITIVE
    assert result["excluded_component_ids_with_reasons"] == {"fx_x_20t": "STALE_AT_SNAPSHOT"}
    assert "fx_x_20t" not in result["included_component_ids"]


def test_all_unavailable_becomes_insufficient_external() -> None:
    rates = _input(
        _binding("rates_policy_x_20t", "rates_policy"),
        status="UNAVAILABLE",
        direction=None,
        unavailable_reason="MISSING_AT_SNAPSHOT",
    )
    result = _aggregate([rates])
    assert result["external_direction_state"] == INSUFFICIENT


def test_cross_horizon_aggregation_is_forbidden() -> None:
    rates = _input(_binding("rates_policy_x_20t", "rates_policy", horizon=20), direction=POSITIVE)
    fx = _input(_binding("fx_x_40t", "fx", horizon=40), direction=POSITIVE)
    with pytest.raises(ExternalEvidence8IAggregationError, match="single_snapshot_symbol_horizon"):
        _aggregate([rates, fx])


def test_binding_horizon_mismatch_is_rejected() -> None:
    rates = _input(_binding("rates_policy_x_20t", "rates_policy", horizon=20), horizon=40)
    with pytest.raises(ExternalEvidence8IAggregationError, match="binding_horizon_mismatch"):
        _aggregate([rates])


def test_forward_outcome_or_decision_columns_are_forbidden() -> None:
    rates = _input(_binding("rates_policy_x_20t", "rates_policy"), direction=POSITIVE)
    rates["peer_excess_20t"] = 0.42
    with pytest.raises(ExternalEvidence8IAggregationError, match="outcome_or_decision_input_forbidden"):
        _aggregate([rates])


def test_direction_valid_from_must_not_be_after_snapshot() -> None:
    rates = _input(_binding("rates_policy_x_20t", "rates_policy"), direction=POSITIVE)
    rates["direction_valid_from"] = "2026-09-28T17:00:01+00:00"
    with pytest.raises(ExternalEvidence8IAggregationError, match="direction_valid_from_after_snapshot"):
        _aggregate([rates])


def test_real_aggregation_is_not_authorized_in_8i_c() -> None:
    contract, binding_contract, matrix = _contracts()
    rates = _input(_binding("rates_policy_x_20t", "rates_policy"), direction=POSITIVE)
    with pytest.raises(ExternalEvidence8IAggregationError, match="real_aggregation_not_authorized"):
        aggregate_external_evidence(
            contract=contract,
            binding_contract=binding_contract,
            conflict_matrix=matrix,
            component_inputs=[rates],
            synthetic=False,
        )


def test_relation_mapping_is_exact_and_descriptive_only() -> None:
    _, _, matrix = _contracts()
    assert classify_relation(core_state="POSITIVE", external_state=POSITIVE, conflict_matrix=matrix) == "CONFIRMING"
    assert classify_relation(core_state="POSITIVE", external_state=NEGATIVE, conflict_matrix=matrix) == "CONFLICTING"
    assert classify_relation(core_state="CONFLICTED", external_state=POSITIVE, conflict_matrix=matrix) == "EXTERNAL_ONLY"
    result = _aggregate([_input(_binding("rates_policy_x_20t", "rates_policy"))], core_state="POSITIVE")
    assert result["relation_state_if_core_state_supplied"] == "CONFIRMING"
    assert result["decision_effect"] == "NO_CHANGE_TO_PHASE7_DECISION"


def test_current_repository_state_remains_fail_closed() -> None:
    contract, binding_contract, matrix = _contracts()
    result = current_aggregation_status(contract, binding_contract, matrix)
    assert result["state"] == "BLOCKED_BY_8I_B_UPSTREAM_SOURCE_IDENTITY_CONTRACT"
    assert result["bound_component_ids"] == []
    assert result["external_direction_state"] == INSUFFICIENT
    assert result["external_evidence_state"] == INSUFFICIENT
    assert result["decision_effect"] == "NO_CHANGE_TO_PHASE7_DECISION"
    assert result["real_aggregation_authorized"] is False
    assert result["external_decision_influence_enabled"] is False
    assert result["phase7_mutation_authorized"] is False
    assert result["portfolio_action_change_authorized"] is False
    assert result["orders_or_trades_authorized"] is False


def test_contract_rejects_any_weighting_or_policy_leak() -> None:
    contract, binding_contract, matrix = _contracts()
    broken = copy.deepcopy(contract)
    broken["state_reducer"]["majority_vote_allowed"] = True
    with pytest.raises(ExternalEvidence8IAggregationError, match="forbidden_reducer_property"):
        validate_aggregation_contract(broken, binding_contract, matrix)

    broken = copy.deepcopy(contract)
    broken["relation_mapping"]["confirming_may_promote_phase7_stance"] = True
    with pytest.raises(ExternalEvidence8IAggregationError, match="relation_policy_leak"):
        validate_aggregation_contract(broken, binding_contract, matrix)
