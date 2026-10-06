import json

import pytest

from scanner.research.pattern_discovery import (
    BoundaryViolation,
    PatternDiscoveryBoundary,
    boundary_contract_hash,
    load_boundary_contract,
)


def test_l0_contract_is_research_only_and_productive_paths_are_closed():
    contract = load_boundary_contract()

    assert contract["phase"] == "L0"
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["execution_allowed"] is False
    assert contract["write_policy"]["allowed_prefixes"] == [
        "artifacts/research/pattern_discovery/"
    ]
    assert contract["write_policy"]["productive_artifact_write_allowed"] is False
    assert contract["forbidden_semantics"]["direct_scanner_score_change_allowed"] is False
    assert contract["forbidden_semantics"]["direct_decision_layer_change_allowed"] is False
    assert contract["forbidden_semantics"]["direct_portfolio_action_allowed"] is False


def test_boundary_contract_hash_is_deterministic_and_semantic():
    contract = load_boundary_contract()
    first = boundary_contract_hash(contract)
    second = boundary_contract_hash(json.loads(json.dumps(contract)))

    assert first == second
    assert len(first) == 64

    changed = json.loads(json.dumps(contract))
    changed["status"] = "changed"
    assert boundary_contract_hash(changed) != first


def test_runtime_writes_are_confined_to_pattern_discovery_artifacts():
    boundary = PatternDiscoveryBoundary()

    allowed = boundary.assert_write_path_allowed(
        "artifacts/research/pattern_discovery/discovery_runs/run.json"
    )
    assert allowed.endswith("discovery_runs/run.json")

    for forbidden in (
        "artifacts/research/latest_scanner.csv",
        "artifacts/research/daily_research.json",
        "artifacts/research/timing_patterns_1b_frozen.json",
        "artifacts/research/current_decision_packets_7a.json",
        "artifacts/research/decision_snapshot_w10.json",
        "artifacts/ui/index.html",
        "configs/pattern_discovery/l0_boundary_v1.json",
        "src/scanner/research/decision_layer/input_contract.py",
    ):
        with pytest.raises(BoundaryViolation, match="write_path_outside_lab_namespace"):
            boundary.assert_write_path_allowed(forbidden)


def test_l0_read_policy_is_explicit_and_fail_closed():
    boundary = PatternDiscoveryBoundary()

    assert boundary.assert_read_path_allowed(
        "artifacts/research/history_metadata.json"
    ) == "artifacts/research/history_metadata.json"
    assert boundary.assert_read_path_allowed(
        "configs/qm_c_hypothesis_registry_v1.json"
    ) == "configs/qm_c_hypothesis_registry_v1.json"

    for forbidden in (
        "data/private.csv",
        "artifacts/ui/index.html",
        "src/scanner/domain/scoring_engine/quality/confidence.py",
    ):
        with pytest.raises(BoundaryViolation, match="read_path_outside_lab_contract"):
            boundary.assert_read_path_allowed(forbidden)


def test_path_traversal_and_absolute_paths_fail_closed():
    boundary = PatternDiscoveryBoundary()

    with pytest.raises(BoundaryViolation, match="path_traversal_forbidden"):
        boundary.assert_write_path_allowed(
            "artifacts/research/pattern_discovery/../../latest_scanner.csv"
        )
    with pytest.raises(BoundaryViolation, match="absolute_path_forbidden"):
        boundary.assert_write_path_allowed("/tmp/pattern.json")


def test_productive_decision_semantics_are_rejected_recursively():
    boundary = PatternDiscoveryBoundary()
    boundary.assert_research_payload(
        {
            "pattern_id": "PAT-DRAFT-001",
            "evidence": {"direction_probability": 0.61},
            "status": "DISCOVERY",
        }
    )

    for key in (
        "universal_stance",
        "portfolio_action",
        "trade_decision",
        "order_instruction",
        "buy_signal",
        "sell_signal",
        "position_size",
        "target_weight",
    ):
        with pytest.raises(BoundaryViolation, match="productive_semantics_forbidden"):
            boundary.assert_research_payload(
                {"pattern_id": "PAT-DRAFT-001", "nested": {key: "FORBIDDEN"}}
            )


def test_existing_qm_c_governance_is_reused_instead_of_duplicated():
    boundary = PatternDiscoveryBoundary()
    bindings = boundary.validate_qm_c_bindings()

    assert bindings == {
        "hypothesis_identity": "QM-C1",
        "analysis_plan_freeze": "QM-C2",
        "multiplicity": "QM-C3",
        "sequential_monitoring": "QM-C4",
        "negative_results": "QM-C5",
    }


def test_l0_does_not_claim_later_phase_functionality():
    contract = load_boundary_contract()
    boundaries = contract["boundaries"]

    assert boundaries["pattern_search_implemented_here"] is False
    assert boundaries["feature_library_implemented_here"] is False
    assert boundaries["candidate_registry_implemented_here"] is False
    assert boundaries["prospective_capture_implemented_here"] is False
    assert boundaries["outcome_maturation_implemented_here"] is False
    assert boundaries["rating_implemented_here"] is False
    assert boundaries["promotion_implemented_here"] is False
    assert boundaries["decision_layer_integration_implemented_here"] is False
