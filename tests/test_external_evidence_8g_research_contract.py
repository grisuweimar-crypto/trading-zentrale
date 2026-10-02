from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs" / "external_evidence_8g_research_contract_v1.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _git_blob_sha(path: Path) -> str:
    data = path.read_bytes()
    header = f"blob {len(data)}\0".encode("utf-8")
    return hashlib.sha1(header + data).hexdigest()


def test_8g_a_contract_is_outcome_blind_and_research_only() -> None:
    contract = _load(CONTRACT_PATH)

    assert contract["schema_version"] == "external_evidence_8g_research_contract_v1"
    assert contract["phase"] == "8G-A"
    assert contract["status"] == "PRE_REGISTERED_OUTCOME_BLIND_READY_FOR_FACTOR_SPECS"
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["automatic_promotion_enabled"] is False
    assert contract["created_from_outcome_blind_8f_freeze"] is True

    assert all(value is False for value in contract["pre_registration_guards"].values())
    assert contract["8g_a_completion_gate"]["outcome_values_may_be_read_before_gate_passes"] is False


def test_8g_a_contract_is_bound_to_frozen_parent_files() -> None:
    contract = _load(CONTRACT_PATH)
    parent = contract["parent_freeze"]

    frozen_files = {
        "freeze_config": "freeze_config_git_blob_sha",
        "macro_exposure_config": "macro_exposure_config_git_blob_sha",
        "exposure_map": "exposure_map_git_blob_sha",
        "research_dataset_contract": "research_dataset_contract_git_blob_sha",
        "validation_contract": "validation_contract_git_blob_sha",
    }
    for path_key, sha_key in frozen_files.items():
        path = ROOT / parent[path_key]
        assert path.exists(), parent[path_key]
        assert _git_blob_sha(path) == parent[sha_key]

    assert parent["base_commit"] == "a136c959eb9c0d068c0007d0aeb38c3025d4db6d"


def test_8g_a_preserves_8f_freeze_guards_and_factor_states() -> None:
    contract = _load(CONTRACT_PATH)
    freeze = _load(ROOT / contract["parent_freeze"]["freeze_config"])

    assert freeze["schema_version"] == "external_evidence_8f_freeze_v1"
    assert freeze["status"] == "PHASE_8F_SOURCE_AND_EXPOSURE_LAYER_FROZEN_OUTCOME_RESEARCH_PENDING_8G"
    assert freeze["completion"]["status"] == "PASS_8F_COMPLETION"
    assert freeze["guards"]["market_outcomes_read"] is False
    assert freeze["guards"]["market_direction_assigned"] is False
    assert freeze["guards"]["signed_exposure_inference_enabled"] is False
    assert freeze["guards"]["threshold_selection_run"] is False
    assert freeze["guards"]["cross_factor_research_enabled"] is False
    assert freeze["guards"]["phase7_integration_enabled"] is False
    assert freeze["guards"]["production_external_evidence_enabled"] is False

    assert set(contract["factor_eligibility"]) == set(freeze["factor_states"])
    for factor_id, factor_state in freeze["factor_states"].items():
        spec = contract["factor_eligibility"][factor_id]
        assert spec["state_at_8f_freeze"] == factor_state
        assert spec["historical_retrojection_allowed"] is False


def test_8g_a_factor_eligibility_is_fail_closed() -> None:
    contract = _load(CONTRACT_PATH)
    factors = contract["factor_eligibility"]

    for factor_id in ("rates_policy", "yield_curve", "fx"):
        assert factors[factor_id]["research_status"] == "PIT_ELIGIBLE_PROSPECTIVE_ONLY_FROM_RECORDED_VALID_FROM"

    assert factors["inflation"]["research_status"] == "HISTORICAL_PIT_CANDIDATE_REQUIRES_ARCHIVE_AND_PARSER_VALIDATION"
    for factor_id in ("oil", "gas"):
        assert factors[factor_id]["research_status"] == "PROSPECTIVE_ONLY_PENDING_REAL_OBSERVATION"
    for factor_id in ("gold", "silver", "copper"):
        assert factors[factor_id]["research_status"] == "DEFERRED_NO_ORIGINAL_VINTAGE_PROOF"
    for factor_id in ("uranium", "lithium"):
        assert factors[factor_id]["research_status"] == "DEFERRED_PROXY_CHALLENGER_REQUIRES_SEPARATE_FROZEN_SPEC"
        assert factors[factor_id]["relabel_as_physical_spot_allowed"] is False


def test_8g_a_reuses_existing_scanner_outcomes_and_horizons_without_values() -> None:
    contract = _load(CONTRACT_PATH)
    dataset = _load(ROOT / contract["outcomes"]["source_contract"])

    assert contract["outcomes"]["horizons_sessions"] == dataset["horizons_sessions"] == [5, 20, 40, 60]
    assert contract["outcomes"]["primary_outcome"] == "peer_excess"
    assert contract["outcomes"]["available_forward_labels"] == dataset["forward_labels"]
    assert set(contract["outcomes"]["available_forward_labels"]) == {
        "return",
        "peer_excess",
        "adverse_excursion",
        "path_max_drawdown",
    }
    assert "labels, never features" in dataset["label_time_rule"]


def test_8g_a_challengers_are_single_factor_and_frozen_before_outcomes() -> None:
    contract = _load(CONTRACT_PATH)
    challengers = contract["baseline_and_challengers"]

    assert challengers["max_external_factor_families_per_challenger"] == 1
    assert challengers["cross_factor_interactions_allowed"] is False
    assert challengers["external_factor_weight_optimization_allowed"] is False
    assert challengers["outcome_derived_direction_or_sign_allowed"] is False
    assert challengers["outcome_derived_thresholds_allowed"] is False
    assert challengers["factor_specific_spec_required_before_outcome_access"] is True
    assert challengers["multiple_confirmatory_variants_per_factor_allowed"] is False
    assert "split manifest identity" in challengers["factor_specific_spec_must_freeze"]


def test_8g_a_partitions_are_chronological_outcome_blind_and_holdout_safe() -> None:
    contract = _load(CONTRACT_PATH)
    partitions = contract["partitions"]
    allocation = partitions["chronological_allocation"]

    assert sum(allocation.values()) == 1.0
    assert allocation == {
        "discovery_fraction": 0.5,
        "validation_fraction": 0.25,
        "holdout_fraction": 0.25,
    }
    assert partitions["assignment_order"] == ["DISCOVERY", "VALIDATION", "HOLDOUT"]
    assert partitions["split_manifest_required_before_outcome_join"] is True
    assert "outcome values and forward labels are forbidden inputs" in partitions["split_manifest_input"]
    assert partitions["reassignment_after_freeze_allowed"] is False
    assert partitions["holdout_sealed_until_validation_spec_frozen"] is True
    assert partitions["holdout_reuse_for_tuning_allowed"] is False
    assert partitions["post_holdout_change_requires_new_unspent_holdout"] is True


def test_8g_a_dependence_and_minimum_evidence_rules_match_existing_methodology() -> None:
    contract = _load(CONTRACT_PATH)
    overlap = contract["overlap_and_dependence"]
    evidence = contract["minimum_evidence"]
    tests = contract["statistical_tests"]

    assert overlap["overlapping_forward_windows_are_iid"] is False
    assert overlap["primary_uncertainty_method"] == "circular_moving_observation_date_blocks"
    assert overlap["block_length_sessions_rule"] == "2 * evaluated_horizon_sessions"
    assert overlap["non_overlap_sensitivity_required"] is True
    assert overlap["non_overlap_sensitivity_cooldown_sessions_rule"] == "evaluated_horizon_sessions"
    assert overlap["split_boundary_purge_required"] is True

    assert evidence["minimum_paired_n_per_factor_horizon_split"] == 30
    assert evidence["minimum_temporal_support_regions"] == 2
    assert evidence["minimum_required_splits_for_promotion"] == ["VALIDATION", "HOLDOUT"]
    assert evidence["threshold_relaxation_when_underpowered_allowed"] is False

    assert tests["bootstrap_repetitions"] == 1000
    assert tests["confidence_level"] == 0.95
    assert tests["uncertainty_method"] == "circular_moving_observation_date_blocks"
    assert tests["block_length_sessions_rule"] == "2 * evaluated_horizon_sessions"
    assert "iid_row_bootstrap_for_primary_inference" in tests["forbidden"]


def test_8g_a_multiplicity_and_8h_promotion_are_pre_registered() -> None:
    contract = _load(CONTRACT_PATH)
    multiplicity = contract["multiplicity_control"]
    promotion = contract["promotion_to_8h"]

    assert multiplicity["family_membership_frozen_before_outcome_join"] is True
    assert multiplicity["method"] == "Holm"
    assert multiplicity["family_wise_alpha"] == 0.05
    assert multiplicity["secondary_outcomes_in_confirmatory_family"] is False

    assert promotion["automatic_promotion"] is False
    assert promotion["unit"] == "factor_horizon_challenger"
    assert promotion["promotion_candidate_state"] == "PROMOTION_CANDIDATE_FOR_8H_REVIEW"
    assert promotion["adequately_sampled_but_not_confirmed_state"] == "NO_PROMOTION_EVIDENCE"
    assert promotion["underpowered_state"] == "INSUFFICIENT_EVIDENCE"
    assert promotion["pit_failed_or_deferred_state"] == "PIT_INELIGIBLE_OR_DEFERRED"
    assert promotion["rejection_does_not_authorize_inverse_signal"] is True
    assert "separate explicit Phase-8H integration contract" in promotion["8h_requirement"]
