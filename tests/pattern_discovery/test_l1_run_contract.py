import copy
import json
from pathlib import Path

import pytest

from scanner.research.pattern_discovery import (
    DiscoveryRunContractError,
    build_run_manifest,
    load_run_contract,
    manifest_repo_path,
    normalize_preregistration,
    verify_run_manifest,
    write_run_manifest,
)


def preregistration(**overrides):
    payload = {
        "declared_start_at": "2026-10-06T15:00:00+00:00",
        "data_cutoff": "2026-10-06T14:59:00+00:00",
        "data_sources": [
            "artifacts/research/history_metadata.json",
            "artifacts/research/history_analysis.csv",
            "configs/qm_c_hypothesis_registry_v1.json",
        ],
        "pit_rules": [
            "no_future_features",
            "missing_remains_missing",
            "no_retrofit_of_modern_features",
        ],
        "universe_version": "universe-test-v1",
        "feature_library_version": "feature-library-test-v1",
        "allowed_transformations": [
            "delta_1d",
            "delta_5d",
            "threshold_crossing",
        ],
        "pattern_complexity": {
            "min_atomic_conditions": 1,
            "max_atomic_conditions": 3,
        },
        "pattern_types": ["DIRECTIONAL", "RELATIVE_ALPHA"],
        "targets": ["peer_excess_5t_gt_0", "peer_excess_20t_gt_0"],
        "horizons_sessions": [5, 20],
        "baselines": {
            "peer_excess_5t_gt_0": "same_horizon_same_regime_peer_baseline",
            "peer_excess_20t_gt_0": "same_horizon_same_regime_peer_baseline",
        },
        "minimum_criteria": {
            "minimum_raw_n": 40,
            "minimum_temporal_support_regions": 3,
            "minimum_effect_size": 0.02,
            "minimum_baseline_lift": 0.05,
        },
        "search_budget": {
            "max_tested_candidates_total": 5000,
            "max_tested_candidates_per_family": 1000,
        },
        "candidate_budget": {
            "max_frozen_candidates_total": 40,
            "max_frozen_candidates_per_horizon": 15,
        },
        "multiple_testing": {
            "primary_method": "BENJAMINI_HOCHBERG_FDR",
            "parameters": {"fdr_q": 0.05},
        },
        "statistical_primary_method": "cluster_aware_effect_and_probability_estimation",
        "robustness_checks": [
            "moving_block_bootstrap",
            "symbol_concentration",
            "temporal_support",
        ],
        "exclusion_rules": [
            "missing_required_feature",
            "missing_forward_target",
        ],
        "dependency_rules": [
            "overlapping_events_not_independent",
            "duplicate_specs_rejected",
        ],
        "code_version": "commit-test-abc123",
        "determinism": {
            "randomness_allowed": False,
            "seed": None,
        },
    }
    payload.update(overrides)
    return payload


def repo_fixture(tmp_path: Path) -> Path:
    files = {
        "artifacts/research/history_metadata.json": b'{"as_of":"2026-10-06"}\n',
        "artifacts/research/history_analysis.csv": b"Symbol,Score\nAAA,50\n",
        "configs/qm_c_hypothesis_registry_v1.json": b'{"schema_version":"test"}\n',
    }
    for path, content in files.items():
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    return tmp_path


def test_l1_contract_is_research_only_and_requires_full_preregistration():
    contract = load_run_contract()
    required = set(contract["preregistration"]["required_fields"])

    assert contract["phase"] == "L1"
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["execution_allowed"] is False
    assert {
        "declared_start_at",
        "data_cutoff",
        "data_sources",
        "feature_library_version",
        "universe_version",
        "search_budget",
        "candidate_budget",
        "multiple_testing",
        "code_version",
        "determinism",
    }.issubset(required)


def test_incomplete_or_unknown_preregistration_fails_closed():
    missing = preregistration()
    missing.pop("data_cutoff")
    with pytest.raises(
        DiscoveryRunContractError,
        match="preregistration_fields_missing:data_cutoff",
    ):
        normalize_preregistration(missing)

    unknown = preregistration(extra_posthoc_knob="not allowed")
    with pytest.raises(
        DiscoveryRunContractError,
        match="preregistration_unknown_fields:extra_posthoc_knob",
    ):
        normalize_preregistration(unknown)


def test_cutoff_must_not_follow_declared_start():
    raw = preregistration(data_cutoff="2026-10-06T15:01:00+00:00")
    with pytest.raises(
        DiscoveryRunContractError,
        match="data_cutoff_after_declared_start",
    ):
        normalize_preregistration(raw)


def test_complexity_and_budgets_are_bounded():
    raw = preregistration(
        pattern_complexity={"min_atomic_conditions": 1, "max_atomic_conditions": 4}
    )
    with pytest.raises(
        DiscoveryRunContractError,
        match="pattern_complexity_exceeds_l1_initial_limit",
    ):
        normalize_preregistration(raw)

    raw = preregistration(
        search_budget={
            "max_tested_candidates_total": 100,
            "max_tested_candidates_per_family": 101,
        }
    )
    with pytest.raises(
        DiscoveryRunContractError,
        match="search_budget_per_family_exceeds_total",
    ):
        normalize_preregistration(raw)


def test_multiple_testing_plan_is_predeclared_and_validated():
    raw = preregistration(
        multiple_testing={
            "primary_method": "BENJAMINI_HOCHBERG_FDR",
            "parameters": {},
        }
    )
    with pytest.raises(
        DiscoveryRunContractError,
        match="multiple_testing_fdr_q_required",
    ):
        normalize_preregistration(raw)

    normalized = normalize_preregistration(preregistration())
    assert normalized["multiple_testing"] == {
        "primary_method": "BENJAMINI_HOCHBERG_FDR",
        "parameters": {"fdr_q": 0.05},
    }


def test_same_plan_and_same_inputs_produce_same_run_identity(tmp_path):
    root = repo_fixture(tmp_path)
    first = build_run_manifest(preregistration(), repo_root=root)

    reordered = preregistration()
    reordered["data_sources"] = list(reversed(reordered["data_sources"]))
    reordered["pit_rules"] = list(reversed(reordered["pit_rules"]))
    reordered["pattern_types"] = list(reversed(reordered["pattern_types"]))
    reordered["horizons_sessions"] = list(reversed(reordered["horizons_sessions"]))
    second = build_run_manifest(reordered, repo_root=root)

    assert first == second
    assert first["run_id"].startswith("DISC-")
    assert len(first["run_identity_hash"]) == 64
    assert len(first["manifest_hash"]) == 64


def test_changed_input_bytes_change_run_identity(tmp_path):
    root = repo_fixture(tmp_path)
    first = build_run_manifest(preregistration(), repo_root=root)

    (root / "artifacts/research/history_analysis.csv").write_text(
        "Symbol,Score\nAAA,51\n",
        encoding="utf-8",
    )
    second = build_run_manifest(preregistration(), repo_root=root)

    assert second["input_fingerprint_hash"] != first["input_fingerprint_hash"]
    assert second["run_identity_hash"] != first["run_identity_hash"]
    assert second["run_id"] != first["run_id"]


def test_changed_preregistered_semantics_change_run_identity(tmp_path):
    root = repo_fixture(tmp_path)
    first = build_run_manifest(preregistration(), repo_root=root)
    second = build_run_manifest(
        preregistration(
            targets=["peer_excess_5t_gt_0"],
            baselines={
                "peer_excess_5t_gt_0": "same_horizon_same_regime_peer_baseline"
            },
        ),
        repo_root=root,
    )

    assert second["config_hash"] != first["config_hash"]
    assert second["run_id"] != first["run_id"]


def test_missing_or_unapproved_input_fails_closed(tmp_path):
    root = repo_fixture(tmp_path)

    missing = preregistration(
        data_sources=["artifacts/research/does_not_exist.csv"]
    )
    with pytest.raises(
        DiscoveryRunContractError,
        match="input_file_missing:artifacts/research/does_not_exist.csv",
    ):
        build_run_manifest(missing, repo_root=root)

    outside = preregistration(data_sources=["data/raw.csv"])
    with pytest.raises(ValueError, match="read_path_outside_lab_contract"):
        build_run_manifest(outside, repo_root=root)


def test_manifest_is_tamper_evident(tmp_path):
    root = repo_fixture(tmp_path)
    manifest = build_run_manifest(preregistration(), repo_root=root)
    result = verify_run_manifest(manifest)
    assert result["valid"] is True

    tampered = copy.deepcopy(manifest)
    tampered["preregistration"]["minimum_criteria"]["minimum_raw_n"] = 1
    with pytest.raises(DiscoveryRunContractError, match="manifest_hash_mismatch"):
        verify_run_manifest(tampered)


def test_manifest_persists_once_inside_l0_namespace(tmp_path):
    root = repo_fixture(tmp_path)
    manifest = build_run_manifest(preregistration(), repo_root=root)
    target = write_run_manifest(root, manifest)

    expected = root / manifest_repo_path(manifest["run_id"])
    assert target == expected
    saved = json.loads(target.read_text(encoding="utf-8"))
    assert verify_run_manifest(saved)["run_id"] == manifest["run_id"]

    with pytest.raises(
        DiscoveryRunContractError,
        match="frozen_manifest_already_exists",
    ):
        write_run_manifest(root, manifest)


def test_seed_rules_are_fail_closed():
    with pytest.raises(
        DiscoveryRunContractError,
        match="integer_seed_required_when_randomness_allowed",
    ):
        normalize_preregistration(
            preregistration(
                determinism={"randomness_allowed": True, "seed": None}
            )
        )

    normalized = normalize_preregistration(
        preregistration(
            determinism={"randomness_allowed": True, "seed": 20261006}
        )
    )
    assert normalized["determinism"]["seed"] == 20261006


def test_l1_does_not_claim_later_phase_execution():
    contract = load_run_contract()
    boundaries = contract["boundaries"]

    assert boundaries["feature_library_implemented_here"] is False
    assert boundaries["pattern_search_executed_here"] is False
    assert boundaries["candidate_selection_executed_here"] is False
    assert boundaries["multiple_testing_calculated_here"] is False
    assert boundaries["prospective_confirmation_implemented_here"] is False
    assert boundaries["promotion_implemented_here"] is False
    assert boundaries["decision_layer_integration_implemented_here"] is False
