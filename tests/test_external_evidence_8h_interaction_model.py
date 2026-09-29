from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from scanner.research.external_evidence.interaction_eligibility_8h import bind_interaction_eligibility
from scanner.research.external_evidence.interaction_model_8h import (
    DESIGN_FAMILY_CONSTRUCTED,
    WAITING_FOR_SPECS,
    ExternalEvidence8HModelError,
    construct_interaction_design_family,
    validate_model_contract,
)
from scanner.research.external_evidence.interaction_spec_8h import (
    INTERACTION_FREEZE_RESULT_SCHEMA,
    SPECS_FROZEN,
    freeze_interaction_specs,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads((ROOT / "configs" / "external_evidence_8h_interaction_model_construction_v1.json").read_text())
SPEC_CONTRACT = json.loads((ROOT / "configs" / "external_evidence_8h_interaction_spec_freeze_v1.json").read_text())
BINDING_CONTRACT = json.loads((ROOT / "configs" / "external_evidence_8h_interaction_eligibility_binding_v1.json").read_text())
CHALLENGER = json.loads((ROOT / "configs" / "external_evidence_8g_challenger_specs_v1.json").read_text())
SOURCE_REGISTRY = json.loads((ROOT / "configs" / "external_source_registry_v1.json").read_text())


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _git_blob_sha(path: Path) -> str:
    data = path.read_bytes()
    return hashlib.sha1(f"blob {len(data)}\0".encode("ascii") + data).hexdigest()


def _rehash(row: dict, field: str) -> dict:
    out = deepcopy(row)
    out.pop(field, None)
    out[field] = _digest(out)
    return out


def _core_external_spec(
    *,
    spec_id: str = "ix_synthetic_core_external",
    horizon: int = 5,
    core_field: str = "score",
    external_field: str = "rates_policy_level_pct",
) -> dict:
    row = {
        "interaction_spec_id": spec_id,
        "interaction_class": "CORE_X_EXTERNAL",
        "horizon_sessions": horizon,
        "components": [
            {
                "kind": "CORE_NUMERIC",
                "component_id": core_field,
                "resolved_field": core_field,
                "horizon_sessions": horizon,
            },
            {
                "kind": "EXTERNAL_NUMERIC",
                "factor_horizon_id": f"rates_policy_x_{horizon}t",
                "factor_id": "rates_policy",
                "horizon_sessions": horizon,
                "feature_field": external_field,
                "component_identity_sha256": _digest({"field": external_field, "horizon": horizon}),
            },
        ],
        "operator": "ELEMENTWISE_PRODUCT",
        "eligibility_binding_sha256": _digest({"eligibility": horizon}),
        "candidate_manifest_sha256": _digest({"manifest": spec_id}),
        "main_effects_retained": True,
        "exactly_one_new_interaction_term": True,
        "outcome_values_read_during_spec_freeze": False,
        "production_authorized": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
    }
    row["interaction_spec_sha256"] = _digest(row)
    return row


def _external_external_spec(
    *,
    spec_id: str = "ix_synthetic_external_external",
    horizon: int = 5,
) -> dict:
    row = {
        "interaction_spec_id": spec_id,
        "interaction_class": "EXTERNAL_X_EXTERNAL",
        "horizon_sessions": horizon,
        "components": [
            {
                "kind": "EXTERNAL_NUMERIC",
                "factor_horizon_id": f"rates_policy_x_{horizon}t",
                "factor_id": "rates_policy",
                "horizon_sessions": horizon,
                "feature_field": "rates_policy_level_pct",
                "component_identity_sha256": _digest({"field": "rates_policy_level_pct", "horizon": horizon}),
            },
            {
                "kind": "EXTERNAL_NUMERIC",
                "factor_horizon_id": f"fx_x_{horizon}t",
                "factor_id": "fx",
                "horizon_sessions": horizon,
                "feature_field": "fx_log_change",
                "component_identity_sha256": _digest({"field": "fx_log_change", "horizon": horizon}),
            },
        ],
        "operator": "ELEMENTWISE_PRODUCT",
        "eligibility_binding_sha256": _digest({"eligibility": horizon, "two": True}),
        "candidate_manifest_sha256": _digest({"manifest": spec_id}),
        "main_effects_retained": True,
        "exactly_one_new_interaction_term": True,
        "outcome_values_read_during_spec_freeze": False,
        "production_authorized": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
    }
    row["interaction_spec_sha256"] = _digest(row)
    return row


def _freeze(*specs: dict) -> dict:
    ordered = sorted(specs, key=lambda row: row["interaction_spec_id"])
    family = [
        {
            "hypothesis_id": f"{spec['interaction_spec_id']}__peer_excess_{spec['horizon_sessions']}t",
            "interaction_spec_id": spec["interaction_spec_id"],
            "horizon_sessions": spec["horizon_sessions"],
            "primary_outcome": "peer_excess",
        }
        for spec in ordered
    ]
    row = {
        "schema_version": INTERACTION_FREEZE_RESULT_SCHEMA,
        "phase": "8H-C",
        "state": SPECS_FROZEN,
        "eligibility_binding_sha256": _digest({"eligibility": "bound"}),
        "candidate_manifest_sha256": _digest({"manifest": "frozen"}),
        "frozen_at": "2027-07-01T10:30:00Z",
        "frozen_interaction_specs": ordered,
        "confirmatory_family": family,
        "interaction_dataset_model_construction_authorized": True,
        "interaction_outcome_access_authorized": False,
        "empirical_interaction_research_enabled": False,
        "automatic_promotion_authorized": False,
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subphase": "8H-D_DATASET_MODEL_CONSTRUCTION",
        "wait_reason": None,
    }
    row["freeze_sha256"] = _digest(row)
    return row


def _frame(horizon: int = 5) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "snapshot_id": ["s1", "s1", "s2"],
            "as_of": ["2027-07-02T10:00:00Z", "2027-07-02T10:00:00Z", "2027-07-03T10:00:00Z"],
            "generated_at": ["2027-07-02T10:00:00Z", "2027-07-02T10:00:00Z", "2027-07-03T10:00:00Z"],
            "symbol": ["AAA", "BBB", "CCC"],
            "horizon_sessions": [horizon, horizon, horizon],
            "score": [0.5, 1.0, -0.5],
            "rates_policy_level_pct": [2.0, None, -1.0],
            "fx_log_change": [0.2, 0.1, -0.3],
            "other_main_effect": [1.0, 2.0, 3.0],
        }
    )


def test_8h_d_contract_binds_exact_8h_c_and_model_parent_blobs() -> None:
    validate_model_contract(CONTRACT, SPEC_CONTRACT, CHALLENGER)
    parent = CONTRACT["parent_freeze"]
    assert parent["verified_parent_commit"] == "d7e3853f4fae919cb6189155ce3740b95a6bd1af"
    for path_key, sha_key in {
        "8h_c_contract": "8h_c_contract_git_blob_sha",
        "8h_c_implementation": "8h_c_implementation_git_blob_sha",
        "8h_b_contract": "8h_b_contract_git_blob_sha",
        "8g_challenger_specs": "8g_challenger_specs_git_blob_sha",
        "8g_model_contract": "8g_model_contract_git_blob_sha",
        "8g_model_implementation": "8g_model_implementation_git_blob_sha",
        "decision_research_dataset": "decision_research_dataset_git_blob_sha",
    }.items():
        assert _git_blob_sha(ROOT / parent[path_key]) == parent[sha_key]


def test_current_repository_state_waits_with_no_designs_and_no_outcome_access() -> None:
    eligibility = bind_interaction_eligibility(
        contract=BINDING_CONTRACT,
        challenger_specs=CHALLENGER,
        source_registry=SOURCE_REGISTRY,
    )
    freeze = freeze_interaction_specs(
        contract=SPEC_CONTRACT,
        challenger_specs=CHALLENGER,
        eligibility_result=eligibility,
    )
    baselines, challengers, result = construct_interaction_design_family(
        contract=CONTRACT,
        spec_contract=SPEC_CONTRACT,
        challenger_specs=CHALLENGER,
        freeze_result=freeze,
    )
    assert baselines == {}
    assert challengers == {}
    assert result["state"] == WAITING_FOR_SPECS
    assert result["constructed_interaction_designs"] == []
    assert result["8h_e_discovery_evaluation_eligible"] is False
    assert result["interaction_outcome_access_authorized"] is False
    assert result["validation_or_holdout_authorized"] is False


def test_core_x_external_pair_keeps_identical_rows_and_main_effects_and_computes_one_term() -> None:
    spec = _core_external_spec()
    freeze = _freeze(spec)
    frame = _frame()
    main_effects = ["score", "rates_policy_level_pct", "other_main_effect"]
    baselines, challengers, result = construct_interaction_design_family(
        contract=CONTRACT,
        spec_contract=SPEC_CONTRACT,
        challenger_specs=CHALLENGER,
        freeze_result=freeze,
        standardized_frames_by_spec={spec["interaction_spec_id"]: frame},
        main_effect_columns_by_spec={spec["interaction_spec_id"]: main_effects},
    )
    assert result["state"] == DESIGN_FAMILY_CONSTRUCTED
    assert result["8h_e_discovery_evaluation_eligible"] is True
    assert result["interaction_outcome_access_authorized"] is False
    baseline = baselines[spec["interaction_spec_id"]]
    challenger = challengers[spec["interaction_spec_id"]]
    assert len(baseline) == 2
    assert len(challenger) == 2
    assert baseline[["snapshot_id", "as_of", "generated_at", "symbol", "horizon_sessions"]].equals(
        challenger[["snapshot_id", "as_of", "generated_at", "symbol", "horizon_sessions"]]
    )
    assert baseline[main_effects].equals(challenger[main_effects])
    interaction = f"interaction__{spec['interaction_spec_id']}"
    assert list(challenger[interaction]) == pytest.approx([1.0, 0.5])
    assert interaction not in baseline.columns
    receipt = result["constructed_interaction_designs"][0]
    assert receipt["input_rows"] == 3
    assert receipt["paired_rows"] == 2
    assert receipt["dropped_missing_or_nonfinite_rows"] == 1
    assert receipt["outcomes_read"] is False
    assert receipt["estimator_fit"] is False
    assert receipt["losses_computed"] is False


def test_external_x_external_pair_uses_both_external_main_effects() -> None:
    spec = _external_external_spec()
    freeze = _freeze(spec)
    frame = _frame().fillna({"rates_policy_level_pct": 0.0})
    main_effects = ["rates_policy_level_pct", "fx_log_change", "other_main_effect"]
    baselines, challengers, _ = construct_interaction_design_family(
        contract=CONTRACT,
        spec_contract=SPEC_CONTRACT,
        challenger_specs=CHALLENGER,
        freeze_result=freeze,
        standardized_frames_by_spec={spec["interaction_spec_id"]: frame},
        main_effect_columns_by_spec={spec["interaction_spec_id"]: main_effects},
    )
    interaction = f"interaction__{spec['interaction_spec_id']}"
    assert baselines[spec["interaction_spec_id"]][main_effects].equals(
        challengers[spec["interaction_spec_id"]][main_effects]
    )
    assert list(challengers[spec["interaction_spec_id"]][interaction]) == pytest.approx([0.4, 0.0, 0.3])


def test_outcome_or_decision_columns_are_rejected_before_pairing() -> None:
    spec = _core_external_spec()
    freeze = _freeze(spec)
    contaminated = _frame()
    contaminated["peer_excess_5t"] = [0.1, 0.2, 0.3]
    with pytest.raises(ExternalEvidence8HModelError, match="outcome_or_forward_label_columns_forbidden"):
        construct_interaction_design_family(
            contract=CONTRACT,
            spec_contract=SPEC_CONTRACT,
            challenger_specs=CHALLENGER,
            freeze_result=freeze,
            standardized_frames_by_spec={spec["interaction_spec_id"]: contaminated},
            main_effect_columns_by_spec={spec["interaction_spec_id"]: ["score", "rates_policy_level_pct"]},
        )
    decision_contaminated = _frame()
    decision_contaminated["portfolio_action"] = "HOLD"
    with pytest.raises(ExternalEvidence8HModelError, match="forbidden_decision_columns"):
        construct_interaction_design_family(
            contract=CONTRACT,
            spec_contract=SPEC_CONTRACT,
            challenger_specs=CHALLENGER,
            freeze_result=freeze,
            standardized_frames_by_spec={spec["interaction_spec_id"]: decision_contaminated},
            main_effect_columns_by_spec={spec["interaction_spec_id"]: ["score", "rates_policy_level_pct"]},
        )


def test_interaction_component_cannot_be_removed_from_main_effect_model() -> None:
    spec = _core_external_spec()
    freeze = _freeze(spec)
    with pytest.raises(ExternalEvidence8HModelError, match="interaction_component_not_retained_as_main_effect"):
        construct_interaction_design_family(
            contract=CONTRACT,
            spec_contract=SPEC_CONTRACT,
            challenger_specs=CHALLENGER,
            freeze_result=freeze,
            standardized_frames_by_spec={spec["interaction_spec_id"]: _frame()},
            main_effect_columns_by_spec={spec["interaction_spec_id"]: ["score", "other_main_effect"]},
        )


def test_full_frozen_family_is_atomic_and_cannot_be_cherry_picked() -> None:
    first = _core_external_spec(spec_id="ix_first")
    second = _external_external_spec(spec_id="ix_second")
    freeze = _freeze(first, second)
    with pytest.raises(ExternalEvidence8HModelError, match="feature_frame_family_must_equal_frozen_spec_family"):
        construct_interaction_design_family(
            contract=CONTRACT,
            spec_contract=SPEC_CONTRACT,
            challenger_specs=CHALLENGER,
            freeze_result=freeze,
            standardized_frames_by_spec={first["interaction_spec_id"]: _frame()},
            main_effect_columns_by_spec={first["interaction_spec_id"]: ["score", "rates_policy_level_pct"]},
        )


def test_tampered_freeze_or_spec_hash_fails_closed() -> None:
    spec = _core_external_spec()
    freeze = _freeze(spec)
    tampered_freeze = deepcopy(freeze)
    tampered_freeze["frozen_at"] = "2027-07-02T10:30:00Z"
    with pytest.raises(ExternalEvidence8HModelError, match="freeze_result_digest_mismatch"):
        construct_interaction_design_family(
            contract=CONTRACT,
            spec_contract=SPEC_CONTRACT,
            challenger_specs=CHALLENGER,
            freeze_result=tampered_freeze,
        )

    tampered_spec_freeze = deepcopy(freeze)
    tampered_spec_freeze["frozen_interaction_specs"][0]["main_effects_retained"] = False
    tampered_spec_freeze = _rehash(tampered_spec_freeze, "freeze_sha256")
    with pytest.raises(ExternalEvidence8HModelError, match="interaction_spec_digest_mismatch"):
        construct_interaction_design_family(
            contract=CONTRACT,
            spec_contract=SPEC_CONTRACT,
            challenger_specs=CHALLENGER,
            freeze_result=tampered_spec_freeze,
        )


def test_duplicate_identity_and_wrong_horizon_fail_closed() -> None:
    spec = _core_external_spec()
    freeze = _freeze(spec)
    duplicate = pd.concat([_frame(), _frame().iloc[[0]]], ignore_index=True)
    with pytest.raises(ExternalEvidence8HModelError, match="duplicate_row_identity"):
        construct_interaction_design_family(
            contract=CONTRACT,
            spec_contract=SPEC_CONTRACT,
            challenger_specs=CHALLENGER,
            freeze_result=freeze,
            standardized_frames_by_spec={spec["interaction_spec_id"]: duplicate},
            main_effect_columns_by_spec={spec["interaction_spec_id"]: ["score", "rates_policy_level_pct"]},
        )
    wrong_horizon = _frame(horizon=20)
    with pytest.raises(ExternalEvidence8HModelError, match="mixed_or_wrong_horizon"):
        construct_interaction_design_family(
            contract=CONTRACT,
            spec_contract=SPEC_CONTRACT,
            challenger_specs=CHALLENGER,
            freeze_result=freeze,
            standardized_frames_by_spec={spec["interaction_spec_id"]: wrong_horizon},
            main_effect_columns_by_spec={spec["interaction_spec_id"]: ["score", "rates_policy_level_pct"]},
        )


def test_8h_d_never_fits_evaluates_or_authorizes_downstream_decisions() -> None:
    estimator = CONTRACT["estimator_state"]
    outcome = CONTRACT["outcome_boundary"]
    auth = CONTRACT["authorization_boundary"]
    assert estimator["status"] == "NOT_FIT_IN_8H_D"
    assert estimator["fit_allowed_in_8h_d"] is False
    assert estimator["prediction_allowed_in_8h_d"] is False
    assert estimator["loss_computation_allowed_in_8h_d"] is False
    assert outcome["market_outcomes_read_in_8h_d"] is False
    assert outcome["forward_labels_read_in_8h_d"] is False
    assert outcome["peer_excess_read_in_8h_d"] is False
    assert auth["maximum_8h_d_authorization"] == "ELIGIBLE_FOR_8H_E_DISCOVERY_EVALUATION_ONLY"
    assert auth["8h_d_directly_authorizes_interaction_outcome_access"] is False
    assert auth["8h_d_directly_authorizes_validation_or_holdout"] is False
    assert auth["8h_d_directly_authorizes_production"] is False
    assert auth["8h_d_directly_authorizes_phase7_mutation"] is False
    assert auth["8h_d_directly_authorizes_phase8i_integration"] is False
    assert auth["8h_d_directly_authorizes_orders_or_trades"] is False
