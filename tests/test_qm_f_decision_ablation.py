import pytest

from scanner.research.governance.qm_f_decision_ablation import (
    DecisionAblationError,
    compare_policy_results,
    evaluate_policy_path,
    validate_b5_b6_comparability,
    validate_b5_b6_lineage_equivalence,
)


def _step(policy_id, *, before, after, ret=0.05, cost=10.0, eligible=True, tradeable=True, action_available=True, day="2026-09-27"):
    return {
        "policy_id": policy_id,
        "symbol": "TEST",
        "as_of": day,
        "source_snapshot_id": f"snap-{day}",
        "exposure_before": before,
        "exposure_after": after,
        "asset_return": ret,
        "transaction_cost_bps": cost,
        "eligible": eligible,
        "tradeable": tradeable,
        "action_available": action_available,
    }


class _Registry:
    def __init__(self):
        self.nodes = {}

    def add(self, node_id, version_id, content_hash):
        self.nodes[(node_id, version_id)] = {"content_hash": content_hash, "lineage_complete": True}

    def get_node(self, node_id, version_id):
        return dict(self.nodes[(node_id, version_id)])


def test_stateful_path_models_turnover_costs_and_path_continuity():
    steps = [
        _step("B5", before=0.0, after=1.0, ret=0.10, day="2026-09-27"),
        _step("B5", before=1.0, after=0.5, ret=-0.04, day="2026-09-28"),
    ]
    out = evaluate_policy_path(policy_id="B5", steps=steps, estimand="NET_RETURN_DELTA", initial_exposure=0.0)
    assert out["stateful_policy_evaluation"] is True
    assert out["path_divergence_modeled"] is True
    assert out["claim_level_pairing_used_after_divergence"] is False
    assert out["metrics"]["turnover"] == pytest.approx(1.5)
    assert out["research_only"] is True
    assert out["execution_allowed"] is False


def test_policy_value_estimand_requires_explicit_benchmark():
    with pytest.raises(DecisionAblationError, match="realized_policy_value_requires_explicit_benchmark"):
        evaluate_policy_path(policy_id="B5", steps=[_step("B5", before=0.0, after=1.0)], estimand="REALIZED_POLICY_VALUE", initial_exposure=0.0)


def test_b0_flat_and_long_are_distinct_and_fixed():
    flat = evaluate_policy_path(policy_id="B0_FLAT", steps=[_step("B0_FLAT", before=0.0, after=0.0)], estimand="NET_RETURN_DELTA", initial_exposure=0.0)
    long = evaluate_policy_path(policy_id="B0_LONG", steps=[_step("B0_LONG", before=1.0, after=1.0)], estimand="NET_RETURN_DELTA", initial_exposure=1.0)
    assert flat["metrics"]["cumulative_net_return"] == pytest.approx(0.0)
    assert long["metrics"]["cumulative_net_return"] == pytest.approx(0.05)


def test_unavailable_action_cannot_change_state():
    with pytest.raises(DecisionAblationError, match="state_change_when_action_unavailable"):
        evaluate_policy_path(policy_id="B5", steps=[_step("B5", before=0.0, after=1.0, tradeable=False)], estimand="NET_RETURN_DELTA", initial_exposure=0.0)


def test_b5_b6_requires_identical_comparison_inputs_but_allows_path_divergence():
    b5 = [_step("B5", before=0.0, after=1.0)]
    b6 = [_step("B6", before=0.0, after=0.5)]
    result = validate_b5_b6_comparability(b5_steps=b5, b6_steps=b6, initial_exposure_b5=0.0, initial_exposure_b6=0.0)
    assert result["status"] == "COMPARABLE_INPUTS"
    assert result["path_divergence_permitted"] is True

    bad = [_step("B6", before=0.0, after=0.5, cost=11.0)]
    with pytest.raises(DecisionAblationError, match="transaction_cost_bps"):
        validate_b5_b6_comparability(b5_steps=b5, b6_steps=bad, initial_exposure_b5=0.0, initial_exposure_b6=0.0)


def test_b5_b6_lineage_must_match_after_registered_elliott_delta_removed():
    registry = _Registry()
    core_hash = "a" * 64
    elliott_hash = "b" * 64
    registry.add("core", "v1", core_hash)
    registry.add("elliott", "v1", elliott_hash)
    b5 = [{"node_id": "core", "version_id": "v1", "content_hash": core_hash}]
    b6 = [
        {"node_id": "core", "version_id": "v1", "content_hash": core_hash},
        {"node_id": "elliott", "version_id": "v1", "content_hash": elliott_hash},
    ]
    result = validate_b5_b6_lineage_equivalence(
        b5_refs=b5,
        b6_refs=b6,
        b6_elliott_adjustment_refs=[b6[1]],
        registry=registry,
    )
    assert result["status"] == "EQUIVALENT_AFTER_REGISTERED_ELLIOTT_REMOVAL"


def test_incremental_comparison_never_claims_promotion_or_relabels_phase7i():
    left = evaluate_policy_path(policy_id="B5", steps=[_step("B5", before=0.0, after=1.0)], estimand="NET_RETURN_DELTA", initial_exposure=0.0)
    right = evaluate_policy_path(policy_id="B6", steps=[_step("B6", before=0.0, after=0.5)], estimand="NET_RETURN_DELTA", initial_exposure=0.0)
    comparison = compare_policy_results(left, right)
    assert comparison["phase7i_relabelled"] is False
    assert comparison["promotion_claimed"] is False
