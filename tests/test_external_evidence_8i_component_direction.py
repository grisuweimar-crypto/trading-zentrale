from __future__ import annotations

import copy
import json
import math
import subprocess
from pathlib import Path

import pytest

from scanner.research.external_evidence.component_direction_8i import (
    ADAPTER_ID,
    ADAPTER_SEMANTICS,
    ADAPTER_VERSION,
    NEGATIVE,
    POSITIVE,
    UNKNOWN,
    UNKNOWN_VALID,
    UNAVAILABLE,
    USABLE,
    ExternalEvidence8IDirectionError,
    current_direction_status,
    derive_component_direction,
    digest,
    validate_direction_contract,
)
from scanner.research.external_evidence.decision_binding_8i import (
    BINDING_RESULT_SCHEMA,
    BOUND,
    digest as binding_digest,
)
from scanner.research.external_evidence.external_aggregation_8i import (
    aggregate_external_evidence,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs" / "external_evidence_8i_component_direction_state_engine_v1.json"
BINDING_CONTRACT_PATH = ROOT / "configs" / "external_evidence_8i_promotion_provenance_binding_v1.json"
AGGREGATION_CONTRACT_PATH = ROOT / "configs" / "external_evidence_8i_external_aggregation_design_v1.json"
CONFLICT_MATRIX_PATH = ROOT / "configs" / "external_conflict_matrix_v1.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _git_blob_sha(path: str) -> str:
    return subprocess.check_output(["git", "rev-parse", f"HEAD:{path}"], cwd=ROOT, text=True).strip()


def _contracts() -> tuple[dict, dict, dict, dict]:
    return (
        _load(CONTRACT_PATH),
        _load(BINDING_CONTRACT_PATH),
        _load(AGGREGATION_CONTRACT_PATH),
        _load(CONFLICT_MATRIX_PATH),
    )


def _model(intercept: float, coefficient: float, name: str = "x") -> dict:
    row = {
        "family": "ridge_linear_regression",
        "alpha": 1.0,
        "fit_intercept": True,
        "intercept": float(intercept),
        "coefficients": {name: float(coefficient)},
        "training_n": 30,
        "training_mse": 1.0,
        "training_mae": 0.8,
        "solver": "solve",
    }
    row["model_sha256"] = digest(row)
    return row


def _model_pair() -> dict:
    return {
        "schema_version": "synthetic_frozen_model_pair_v1",
        "component_id": "rates_policy_x_20t",
        "horizon_sessions": 20,
        "frozen": True,
        "baseline_model": _model(0.10, 0.20),
        "challenger_model": _model(0.11, 0.25),
    }


def _binding(model_pair: dict | None = None, *, valid_from: str = "2026-09-28T16:00:00+00:00") -> dict:
    artifact = _model_pair() if model_pair is None else model_pair
    row = {
        "schema_version": BINDING_RESULT_SCHEMA,
        "phase": "8I-B",
        "state": BOUND,
        "component_type": "8g_main_effect",
        "component_id": "rates_policy_x_20t",
        "factor_id": "rates_policy",
        "interaction_id_if_applicable": None,
        "parent_factor_ids_if_applicable": [],
        "horizon_sessions": 20,
        "component_artifact_hash": digest(artifact),
        "valid_from": valid_from,
        "external_decision_influence_enabled": False,
        "decision_outcome_access_authorized": False,
        "phase7_mutation_authorized": False,
        "productive_integration_enabled": False,
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
    }
    row["binding_sha256"] = binding_digest(row)
    return row


def _prediction(
    binding: dict,
    model_pair: dict,
    *,
    baseline: float | None = 0.10,
    challenger: float | None = 0.20,
    status: str = "AVAILABLE",
    pit_status: str = "PIT_ELIGIBLE",
    unavailable_reason: str | None = None,
    feature_valid_from: str = "2026-09-28T16:30:00+00:00",
) -> dict:
    row = {
        "component_type": "8g_main_effect",
        "component_id": "rates_policy_x_20t",
        "snapshot_id": "snapshot-2026-09-28T17:00:00Z",
        "symbol": "SYNTH",
        "horizon_sessions": 20,
        "generated_at": "2026-09-28T17:00:00+00:00",
        "feature_valid_from": feature_valid_from,
        "input_pit_status": pit_status,
        "target": "peer_excess_20t",
        "prediction_status": status,
        "baseline_model_sha256": model_pair["baseline_model"]["model_sha256"],
        "challenger_model_sha256": model_pair["challenger_model"]["model_sha256"],
        "model_pair_artifact_sha256": digest(model_pair),
        "binding_sha256": binding["binding_sha256"],
        "baseline_prediction": baseline,
        "challenger_prediction": challenger,
    }
    if unavailable_reason is not None:
        row["unavailable_reason"] = unavailable_reason
    return row


def _derive(prediction: dict, binding: dict, model_pair: dict, *, synthetic: bool = True) -> dict:
    contract, binding_contract, aggregation_contract, _ = _contracts()
    return derive_component_direction(
        contract=contract,
        binding_contract=binding_contract,
        aggregation_contract=aggregation_contract,
        binding=binding,
        prediction_record=prediction,
        model_pair_artifact=model_pair,
        synthetic=synthetic,
    )


def test_8i_d_contract_is_outcome_blind_preregistration_only_and_exactly_bound() -> None:
    contract, binding_contract, aggregation_contract, _ = _contracts()
    validate_direction_contract(contract, binding_contract, aggregation_contract)

    assert contract["schema_version"] == "external_evidence_8i_component_direction_state_engine_v1"
    assert contract["phase"] == "8I-D"
    assert contract["status"] == "FROZEN_OUTCOME_BLIND_DIRECTION_STATE_PREREGISTRATION_ONLY"
    assert contract["research_only"] is True
    assert contract["shadow_mode"] is True
    assert contract["real_decision_outcome_read_allowed"] is False
    assert contract["scope"]["real_bound_component_direction_generation_enabled_now"] is False
    assert contract["scope"]["synthetic_contract_execution_allowed"] is True
    assert contract["extended_reliability_enabled"] is False
    assert contract["extended_stance_enabled"] is False
    assert contract["portfolio_action_change_enabled"] is False
    assert contract["orders_or_trades_enabled"] is False

    for parent in contract["parent_contracts"].values():
        assert _git_blob_sha(parent["path"]) == parent["git_blob_sha"]


def test_adapter_semantics_and_hash_are_frozen_without_threshold_or_sign_search() -> None:
    contract, _, _, _ = _contracts()
    adapter = contract["direction_adapter"]
    assert adapter["direction_adapter_id"] == ADAPTER_ID
    assert adapter["direction_adapter_version"] == ADAPTER_VERSION
    assert adapter["semantics"] == ADAPTER_SEMANTICS
    payload = dict(adapter)
    recorded = payload.pop("direction_adapter_sha256")
    assert digest(payload) == recorded
    assert adapter["zero_deadband"] is None
    assert adapter["absolute_or_relative_threshold"] is None
    assert adapter["round_before_sign"] is False
    assert adapter["semantic_factor_sign_assignment"] is False
    assert adapter["raw_factor_value_sign_used"] is False
    assert adapter["coefficient_sign_used"] is False
    assert adapter["p_value_or_significance_used"] is False
    assert adapter["observed_outcome_used_at_runtime"] is False


def test_positive_negative_and_exact_zero_mapping_are_deterministic() -> None:
    pair = _model_pair()
    binding = _binding(pair)

    positive = _derive(_prediction(binding, pair, baseline=0.10, challenger=0.100000000000001), binding, pair)
    assert positive["component_observation_status"] == USABLE
    assert positive["direction_state"] == POSITIVE
    assert positive["prediction_delta"] > 0

    negative = _derive(_prediction(binding, pair, baseline=0.10, challenger=0.09), binding, pair)
    assert negative["component_observation_status"] == USABLE
    assert negative["direction_state"] == NEGATIVE
    assert negative["prediction_delta"] < 0

    zero = _derive(_prediction(binding, pair, baseline=0.10, challenger=0.10), binding, pair)
    assert zero["component_observation_status"] == UNKNOWN_VALID
    assert zero["direction_state"] == UNKNOWN
    assert zero["prediction_delta"] == 0.0
    assert zero["reason_code"] == "ZERO_INCREMENTAL_PREDICTION"


def test_unavailable_is_preserved_and_never_coerced_to_zero_or_neutral() -> None:
    pair = _model_pair()
    binding = _binding(pair)
    prediction = _prediction(
        binding,
        pair,
        baseline=None,
        challenger=None,
        status="UNAVAILABLE",
        pit_status="PIT_INELIGIBLE_STALE",
        unavailable_reason="STALE_AT_SNAPSHOT",
    )
    result = _derive(prediction, binding, pair)
    assert result["component_observation_status"] == UNAVAILABLE
    assert result["direction_state"] is None
    assert result["prediction_delta"] is None
    assert result["unavailable_reason"] == "STALE_AT_SNAPSHOT"


def test_nonfinite_available_prediction_fails_to_unavailable_with_reason() -> None:
    pair = _model_pair()
    binding = _binding(pair)
    result = _derive(_prediction(binding, pair, baseline=math.nan, challenger=0.2), binding, pair)
    assert result["component_observation_status"] == UNAVAILABLE
    assert result["direction_state"] is None
    assert result["prediction_delta"] is None
    assert result["reason_code"] == "NONFINITE_OR_INVALID_MODEL_PREDICTION"


def test_available_prediction_requires_pit_eligible_input() -> None:
    pair = _model_pair()
    binding = _binding(pair)
    prediction = _prediction(binding, pair, pit_status="PIT_INELIGIBLE_STALE")
    with pytest.raises(ExternalEvidence8IDirectionError, match="available_prediction_requires_pit_eligible"):
        _derive(prediction, binding, pair)


def test_feature_and_binding_valid_from_must_not_postdate_snapshot() -> None:
    pair = _model_pair()
    binding = _binding(pair)
    prediction = _prediction(binding, pair, feature_valid_from="2026-09-28T17:00:01+00:00")
    with pytest.raises(ExternalEvidence8IDirectionError, match="feature_valid_from_after_snapshot"):
        _derive(prediction, binding, pair)

    late_binding = _binding(pair, valid_from="2026-09-28T17:00:01+00:00")
    prediction = _prediction(late_binding, pair)
    with pytest.raises(ExternalEvidence8IDirectionError, match="binding_valid_from_after_snapshot"):
        _derive(prediction, late_binding, pair)


def test_target_and_model_pair_identity_are_exact_and_substitution_fails_closed() -> None:
    pair = _model_pair()
    binding = _binding(pair)

    wrong_target = _prediction(binding, pair)
    wrong_target["target"] = "return_20t"
    with pytest.raises(ExternalEvidence8IDirectionError, match="prediction_target_mismatch"):
        _derive(wrong_target, binding, pair)

    substitute = copy.deepcopy(pair)
    substitute["challenger_model"] = _model(0.12, 0.25)
    substitute_record = _prediction(binding, substitute)
    substitute_record["binding_sha256"] = binding["binding_sha256"]
    with pytest.raises(ExternalEvidence8IDirectionError, match="model_pair_not_exact_bound_component_artifact"):
        _derive(substitute_record, binding, substitute)


def test_embedded_model_hash_and_prediction_model_identity_must_verify() -> None:
    pair = _model_pair()
    binding = _binding(pair)
    broken_pair = copy.deepcopy(pair)
    broken_pair["baseline_model"]["intercept"] = 99.0
    with pytest.raises(ExternalEvidence8IDirectionError, match="baseline_model_digest_mismatch"):
        _derive(_prediction(binding, pair), binding, broken_pair)

    wrong_record = _prediction(binding, pair)
    wrong_record["challenger_model_sha256"] = "f" * 64
    with pytest.raises(ExternalEvidence8IDirectionError, match="challenger_model_identity_mismatch"):
        _derive(wrong_record, binding, pair)


def test_realized_outcome_and_decision_fields_are_rejected() -> None:
    pair = _model_pair()
    binding = _binding(pair)
    prediction = _prediction(binding, pair)
    prediction["observed_peer_excess_20t"] = 0.25
    with pytest.raises(ExternalEvidence8IDirectionError, match="forbidden_outcome_or_decision_input"):
        _derive(prediction, binding, pair)

    prediction = _prediction(binding, pair)
    prediction["portfolio_action"] = "ADD"
    with pytest.raises(ExternalEvidence8IDirectionError, match="forbidden_outcome_or_decision_input"):
        _derive(prediction, binding, pair)


def test_real_direction_generation_remains_closed() -> None:
    pair = _model_pair()
    binding = _binding(pair)
    with pytest.raises(ExternalEvidence8IDirectionError, match="real_direction_generation_not_authorized"):
        _derive(_prediction(binding, pair), binding, pair, synthetic=False)


def test_8i_d_output_is_directly_compatible_with_8i_c_synthetic_aggregation() -> None:
    contract, binding_contract, aggregation_contract, matrix = _contracts()
    pair = _model_pair()
    binding = _binding(pair)
    direction = derive_component_direction(
        contract=contract,
        binding_contract=binding_contract,
        aggregation_contract=aggregation_contract,
        binding=binding,
        prediction_record=_prediction(binding, pair, baseline=0.10, challenger=0.20),
        model_pair_artifact=pair,
        synthetic=True,
    )
    assert "portfolio_action" not in " ".join(direction.keys()).lower()
    aggregated = aggregate_external_evidence(
        contract=aggregation_contract,
        binding_contract=binding_contract,
        conflict_matrix=matrix,
        component_inputs=[direction],
        synthetic=True,
    )
    assert aggregated["external_direction_state"] == POSITIVE
    assert aggregated["decision_effect"] == "NO_CHANGE_TO_PHASE7_DECISION"
    assert aggregated["external_decision_influence_enabled"] is False


def test_current_repository_state_remains_blocked_fail_closed_and_phase7_neutral() -> None:
    contract, binding_contract, aggregation_contract, _ = _contracts()
    state = current_direction_status(contract, binding_contract, aggregation_contract)
    assert state["state"] == "BLOCKED_BY_8I_B_UPSTREAM_SOURCE_IDENTITY_CONTRACT"
    assert state["real_bound_component_ids"] == []
    assert state["real_bound_component_direction_generation_authorized"] is False
    assert state["external_direction_state"] == "INSUFFICIENT_EXTERNAL"
    assert state["external_evidence_state"] == "INSUFFICIENT_EXTERNAL"
    assert state["decision_effect"] == "NO_CHANGE_TO_PHASE7_DECISION"
    assert state["external_decision_influence_enabled"] is False
    assert state["phase7_mutation_authorized"] is False
    assert state["extended_reliability_enabled"] is False
    assert state["extended_stance_enabled"] is False
    assert state["portfolio_action_change_authorized"] is False
    assert state["orders_or_trades_authorized"] is False


def test_contract_rejects_threshold_deadband_or_model_substitution_drift() -> None:
    contract, binding_contract, aggregation_contract, _ = _contracts()
    broken = copy.deepcopy(contract)
    broken["direction_adapter"]["zero_deadband"] = 0.01
    with pytest.raises(ExternalEvidence8IDirectionError, match="direction_adapter_digest_mismatch|threshold_or_deadband"):
        validate_direction_contract(broken, binding_contract, aggregation_contract)

    broken = copy.deepcopy(contract)
    broken["model_pair_lineage"]["model_pair_substitution_after_binding_allowed"] = True
    with pytest.raises(ExternalEvidence8IDirectionError, match="model_pair_substitution"):
        validate_direction_contract(broken, binding_contract, aggregation_contract)
