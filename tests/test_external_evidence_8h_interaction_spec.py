from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.interaction_eligibility_8h import (
    BOUND_STATE,
    ELIGIBILITY_RESULT_SCHEMA,
    NO_PROMOTED_STATE,
    bind_interaction_eligibility,
)
from scanner.research.external_evidence.interaction_spec_8h import (
    NO_ELIGIBLE_COMPONENTS,
    NO_PREREGISTERED_SPECS,
    SPECS_FROZEN,
    WAITING_FOR_ELIGIBILITY,
    ExternalEvidence8HSpecError,
    freeze_interaction_specs,
    validate_spec_contract,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads((ROOT / "configs" / "external_evidence_8h_interaction_spec_freeze_v1.json").read_text())
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


def _component(factor: str, horizon: int, fields: list[str], source: str) -> dict:
    return {
        "factor_horizon_id": f"{factor}_x_{horizon}t",
        "factor_id": factor,
        "horizon_sessions": horizon,
        "eligibility_state": "ELIGIBLE_FOR_8H_INTERACTION_SPEC_FREEZE_ONLY",
        "factor_spec_sha256": _digest({"factor": factor, "horizon": horizon, "fields": fields}),
        "factor_spec_feature_fields": fields,
        "factor_spec_source_id": source,
        "factor_spec_series_ids": ["synthetic-series"],
        "promotion_completion_sha256": _digest({"promotion": factor, "horizon": horizon}),
        "interaction_outcome_access_authorized": False,
        "production_authorized": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
    }


def _bound(*components: dict) -> dict:
    row = {
        "schema_version": ELIGIBILITY_RESULT_SCHEMA,
        "phase": "8H-B",
        "state": BOUND_STATE,
        "upstream_promotion_review_empirically_complete": True,
        "upstream_completion_sha256": _digest({"upstream": "complete"}),
        "wait_reason": None,
        "eligible_factor_horizon_ids": [c["factor_horizon_id"] for c in components],
        "eligible_components": list(components),
        "interaction_spec_freeze_authorized": True,
        "interaction_outcome_access_authorized": False,
        "empirical_interaction_research_enabled": False,
        "concrete_interaction_specs": [],
        "automatic_candidate_generation_authorized": False,
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "hard_rule": "NO_PROMOTED_FACTORS -> NO_EMPIRICAL_INTERACTION_RESEARCH",
        "next_subphase": "8H-C_OUTCOME_BLIND_INTERACTION_SPEC_FREEZE",
    }
    row["binding_sha256"] = _digest(row)
    return row


def _no_promoted() -> dict:
    row = _bound()
    row["state"] = NO_PROMOTED_STATE
    row["interaction_spec_freeze_authorized"] = False
    return _rehash(row, "binding_sha256")


def _manifest(entries: list[dict], **overrides: object) -> dict:
    row = {
        "schema_version": "external_evidence_8h_interaction_candidate_manifest_v1",
        "phase": "8H-C",
        "author_identity": "synthetic-outcome-blind-test",
        "authored_at": "2027-07-01T10:00:00Z",
        "outcome_values_read": False,
        "economic_theory_used_to_rank_or_select": False,
        "candidate_ranking_used": False,
        "automatic_cartesian_generation_used": False,
        "threshold_search_used": False,
        "sign_search_used": False,
        "hyperparameter_search_used": False,
        "entries": entries,
    }
    row.update(overrides)
    row["manifest_sha256"] = _digest(row)
    return row


def _core_external_entry(core: str = "score", factor: str = "rates_policy", horizon: int = 5, feature: str = "rates_policy_level_pct") -> dict:
    return {
        "interaction_class": "CORE_X_EXTERNAL",
        "horizon_sessions": horizon,
        "components": [
            {"kind": "CORE_NUMERIC", "component_id": core},
            {"kind": "EXTERNAL_NUMERIC", "factor_horizon_id": f"{factor}_x_{horizon}t", "feature_field": feature},
        ],
    }


def test_8h_c_contract_binds_exact_parent_blobs_and_frozen_core_catalog() -> None:
    validate_spec_contract(CONTRACT, CHALLENGER)
    parent = CONTRACT["parent_freeze"]
    assert parent["verified_parent_commit"] == "ba49f3f0540e50c02764263ce82fef9fee9ef523"
    for path_key, sha_key in {
        "8h_a_contract": "8h_a_contract_git_blob_sha",
        "8h_b_contract": "8h_b_contract_git_blob_sha",
        "8h_b_implementation": "8h_b_implementation_git_blob_sha",
        "8g_challenger_specs": "8g_challenger_specs_git_blob_sha",
        "decision_research_dataset": "decision_research_dataset_git_blob_sha",
    }.items():
        assert _git_blob_sha(ROOT / parent[path_key]) == parent[sha_key]
    assert CONTRACT["core_component_policy"]["categorical_core_interactions_in_v1_allowed"] is False
    assert CONTRACT["interaction_math"]["operator"] == "ELEMENTWISE_PRODUCT"
    assert CONTRACT["interaction_math"]["both_underlying_main_effects_remain_present"] is True


def test_current_repository_freeze_remains_empty_and_waiting() -> None:
    eligibility = bind_interaction_eligibility(
        contract=BINDING_CONTRACT,
        challenger_specs=CHALLENGER,
        source_registry=SOURCE_REGISTRY,
    )
    result = freeze_interaction_specs(
        contract=CONTRACT,
        challenger_specs=CHALLENGER,
        eligibility_result=eligibility,
    )
    assert result["state"] == WAITING_FOR_ELIGIBILITY
    assert result["frozen_interaction_specs"] == []
    assert result["confirmatory_family"] == []
    assert result["interaction_dataset_model_construction_authorized"] is False
    assert result["interaction_outcome_access_authorized"] is False
    assert result["empirical_interaction_research_enabled"] is False


def test_manifest_is_forbidden_while_upstream_is_still_waiting() -> None:
    eligibility = bind_interaction_eligibility(
        contract=BINDING_CONTRACT,
        challenger_specs=CHALLENGER,
        source_registry=SOURCE_REGISTRY,
    )
    with pytest.raises(ExternalEvidence8HSpecError, match="manifest_forbidden_while_upstream_waiting"):
        freeze_interaction_specs(
            contract=CONTRACT,
            challenger_specs=CHALLENGER,
            eligibility_result=eligibility,
            candidate_manifest=_manifest([]),
        )


def test_terminal_no_promoted_factors_produces_no_eligible_interactions() -> None:
    result = freeze_interaction_specs(
        contract=CONTRACT,
        challenger_specs=CHALLENGER,
        eligibility_result=_no_promoted(),
    )
    assert result["state"] == NO_ELIGIBLE_COMPONENTS
    assert result["confirmatory_family"] == []
    assert result["interaction_dataset_model_construction_authorized"] is False


def test_bound_eligibility_requires_explicit_outcome_blind_manifest() -> None:
    eligibility = _bound(_component("rates_policy", 5, ["rates_policy_level_pct", "rates_policy_delta_pp"], "FED_H15"))
    with pytest.raises(ExternalEvidence8HSpecError, match="candidate_manifest_required"):
        freeze_interaction_specs(
            contract=CONTRACT,
            challenger_specs=CHALLENGER,
            eligibility_result=eligibility,
        )


def test_empty_explicit_manifest_does_not_invent_candidates() -> None:
    eligibility = _bound(_component("rates_policy", 5, ["rates_policy_level_pct", "rates_policy_delta_pp"], "FED_H15"))
    result = freeze_interaction_specs(
        contract=CONTRACT,
        challenger_specs=CHALLENGER,
        eligibility_result=eligibility,
        candidate_manifest=_manifest([]),
    )
    assert result["state"] == NO_PREREGISTERED_SPECS
    assert result["frozen_interaction_specs"] == []
    assert result["confirmatory_family"] == []


def test_valid_core_x_external_spec_freezes_exactly_one_term_and_full_family_member() -> None:
    eligibility = _bound(_component("rates_policy", 5, ["rates_policy_level_pct", "rates_policy_delta_pp"], "FED_H15"))
    result = freeze_interaction_specs(
        contract=CONTRACT,
        challenger_specs=CHALLENGER,
        eligibility_result=eligibility,
        candidate_manifest=_manifest([_core_external_entry()]),
        frozen_at="2027-07-01T10:30:00Z",
    )
    assert result["state"] == SPECS_FROZEN
    assert len(result["frozen_interaction_specs"]) == 1
    spec = result["frozen_interaction_specs"][0]
    assert spec["interaction_class"] == "CORE_X_EXTERNAL"
    assert spec["horizon_sessions"] == 5
    assert spec["main_effects_retained"] is True
    assert spec["exactly_one_new_interaction_term"] is True
    assert spec["outcome_values_read_during_spec_freeze"] is False
    assert len(result["confirmatory_family"]) == 1
    assert result["confirmatory_family"][0]["hypothesis_id"] == f"{spec['interaction_spec_id']}__peer_excess_5t"
    assert result["interaction_dataset_model_construction_authorized"] is True
    assert result["interaction_outcome_access_authorized"] is False


def test_external_x_external_requires_two_exact_same_horizon_promotions() -> None:
    eligibility = _bound(
        _component("rates_policy", 5, ["rates_policy_level_pct", "rates_policy_delta_pp"], "FED_H15"),
        _component("fx", 5, ["fx_log_usd_per_eur", "fx_log_change"], "ECB_EXR"),
    )
    entry = {
        "interaction_class": "EXTERNAL_X_EXTERNAL",
        "horizon_sessions": 5,
        "components": [
            {"kind": "EXTERNAL_NUMERIC", "factor_horizon_id": "rates_policy_x_5t", "feature_field": "rates_policy_level_pct"},
            {"kind": "EXTERNAL_NUMERIC", "factor_horizon_id": "fx_x_5t", "feature_field": "fx_log_change"},
        ],
    }
    result = freeze_interaction_specs(
        contract=CONTRACT,
        challenger_specs=CHALLENGER,
        eligibility_result=eligibility,
        candidate_manifest=_manifest([entry]),
        frozen_at="2027-07-01T11:00:00Z",
    )
    assert result["state"] == SPECS_FROZEN
    assert result["frozen_interaction_specs"][0]["interaction_class"] == "EXTERNAL_X_EXTERNAL"


def test_cross_horizon_or_unpromoted_external_component_is_rejected() -> None:
    eligibility = _bound(_component("rates_policy", 5, ["rates_policy_level_pct"], "FED_H15"))
    bad_horizon = _core_external_entry(horizon=20)
    with pytest.raises(ExternalEvidence8HSpecError, match="external_component_not_eligible"):
        freeze_interaction_specs(
            contract=CONTRACT,
            challenger_specs=CHALLENGER,
            eligibility_result=eligibility,
            candidate_manifest=_manifest([bad_horizon]),
            frozen_at="2027-07-01T11:00:00Z",
        )


def test_unknown_core_component_and_outcome_tainted_manifest_fail_closed() -> None:
    eligibility = _bound(_component("rates_policy", 5, ["rates_policy_level_pct"], "FED_H15"))
    with pytest.raises(ExternalEvidence8HSpecError, match="core_component_not_allowed"):
        freeze_interaction_specs(
            contract=CONTRACT,
            challenger_specs=CHALLENGER,
            eligibility_result=eligibility,
            candidate_manifest=_manifest([_core_external_entry(core="universal_stance")]),
            frozen_at="2027-07-01T11:00:00Z",
        )
    tainted = _manifest([_core_external_entry()], outcome_values_read=True)
    with pytest.raises(ExternalEvidence8HSpecError, match="manifest_guard_not_false:outcome_values_read"):
        freeze_interaction_specs(
            contract=CONTRACT,
            challenger_specs=CHALLENGER,
            eligibility_result=eligibility,
            candidate_manifest=tainted,
            frozen_at="2027-07-01T11:00:00Z",
        )


def test_commutative_duplicate_specs_are_rejected_instead_of_double_counted() -> None:
    eligibility = _bound(_component("rates_policy", 5, ["rates_policy_level_pct"], "FED_H15"))
    first = _core_external_entry()
    second = deepcopy(first)
    second["components"] = list(reversed(second["components"]))
    with pytest.raises(ExternalEvidence8HSpecError, match="duplicate_or_commutative_duplicate_spec"):
        freeze_interaction_specs(
            contract=CONTRACT,
            challenger_specs=CHALLENGER,
            eligibility_result=eligibility,
            candidate_manifest=_manifest([first, second]),
            frozen_at="2027-07-01T11:00:00Z",
        )


def test_8h_c_never_authorizes_outcome_evaluation_production_or_8i() -> None:
    auth = CONTRACT["authorization_boundary"]
    family = CONTRACT["confirmatory_family_freeze"]
    fresh = CONTRACT["fresh_evidence_boundary"]
    assert auth["maximum_8h_c_authorization"] == "ELIGIBLE_FOR_8H_D_DATASET_MODEL_CONSTRUCTION_ONLY"
    assert auth["8h_c_directly_authorizes_interaction_outcome_evaluation"] is False
    assert auth["8h_c_directly_authorizes_production"] is False
    assert auth["8h_c_directly_authorizes_phase8i_integration"] is False
    assert auth["8h_c_directly_authorizes_orders_or_trades"] is False
    assert family["holm_method"] == "Holm"
    assert family["family_wise_alpha"] == 0.05
    assert family["family_must_freeze_before_first_interaction_outcome_join"] is True
    assert family["family_may_expand_or_shrink_after_outcome_access"] is False
    assert fresh["8g_validation_or_holdout_reusable_as_fresh_8h_confirmation"] is False
    assert fresh["outcome_access_before_spec_and_family_freeze_allowed"] is False
