import pytest

from scanner.research.governance.qm_g_challenger_evaluation import (
    ChallengerEvaluationError,
    evaluate_challenger_policy_increment,
)

CORE_HASH = "a" * 64
ADJUSTMENT_HASH = "b" * 64
EXTRA_HASH = "c" * 64
VERSION_HASH = "d" * 64


class _Registry:
    def get_challenger(self, challenger_id, challenger_version):
        return {
            "challenger_id": challenger_id,
            "challenger_version": challenger_version,
            "challenger_version_hash": VERSION_HASH,
            "lineage_binding": {
                "core": {"node_id": "core", "version_id": "v1", "content_hash": CORE_HASH},
                "challenger": {"node_id": "adjustment", "version_id": "v1", "content_hash": ADJUSTMENT_HASH},
                "independent_confirmation_claimed": False,
            },
            "boundaries": {
                "sidecar_evidence_only": True,
                "elliott_core_changed": False,
                "hard_rules_changed": False,
                "universal_stance_changed": False,
                "w6_interface_replaced": False,
                "w8_action_matrix_replaced": False,
                "portfolio_action_directly_changed": False,
                "order_or_execution_generated": False,
                "productive_promotion_performed": False,
            },
        }


class _Lineage:
    def __init__(self):
        self.nodes = {
            ("core", "v1"): {"content_hash": CORE_HASH, "lineage_complete": True},
            ("adjustment", "v1"): {"content_hash": ADJUSTMENT_HASH, "lineage_complete": True},
            ("extra", "v1"): {"content_hash": EXTRA_HASH, "lineage_complete": True},
        }

    def get_node(self, node_id, version_id):
        return dict(self.nodes[(node_id, version_id)])


def _readiness(**extra):
    result = {
        "schema_version": "qm_g_elliott_challenger_readiness_v1",
        "valid": True,
        "challenger_id": "scenario-stability",
        "challenger_version": "v1",
        "challenger_version_hash": VERSION_HASH,
        "qm_a_evidence_state": "FROZEN_FOR_CONFIRMATION",
        "productive_integration_enabled": False,
        "execution_allowed": False,
    }
    result.update(extra)
    return result


def _step(*, before, after, day, ret=0.02, cost=10.0):
    return {
        "symbol": "TEST",
        "as_of": day,
        "source_snapshot_id": f"snap-{day}",
        "exposure_before": before,
        "exposure_after": after,
        "asset_return": ret,
        "transaction_cost_bps": cost,
        "eligible": True,
        "tradeable": True,
        "action_available": True,
    }


def _run(core_steps, challenger_steps, core_refs=None, challenger_refs=None, readiness=None, initial_core=0.0, initial_challenger=0.0):
    core_refs = core_refs or [{"node_id": "core", "version_id": "v1", "content_hash": CORE_HASH}]
    challenger_refs = challenger_refs or [
        {"node_id": "core", "version_id": "v1", "content_hash": CORE_HASH},
        {"node_id": "adjustment", "version_id": "v1", "content_hash": ADJUSTMENT_HASH},
    ]
    return evaluate_challenger_policy_increment(
        challenger_id="scenario-stability",
        challenger_version="v1",
        readiness=readiness or _readiness(),
        challenger_registry=_Registry(),
        lineage_registry=_Lineage(),
        core_steps=core_steps,
        challenger_steps=challenger_steps,
        core_lineage_refs=core_refs,
        challenger_lineage_refs=challenger_refs,
        initial_exposure_core=initial_core,
        initial_exposure_challenger=initial_challenger,
        estimand="NET_RETURN_DELTA",
    )


def test_frozen_core_vs_challenger_shadow_uses_stateful_qm_f_without_declaring_winner():
    core = [
        _step(before=0.0, after=1.0, day="2026-09-29"),
        _step(before=1.0, after=1.0, day="2026-09-30"),
    ]
    shadow = [
        _step(before=0.0, after=0.5, day="2026-09-29"),
        _step(before=0.5, after=0.5, day="2026-09-30"),
    ]
    result = _run(core, shadow)
    assert result["arms"]["core"] == "B6_CORE_FROZEN"
    assert result["arms"]["challenger"] == "B6_CHALLENGER_SHADOW"
    assert result["comparability"]["status"] == "COMPARABLE_INPUTS"
    assert result["lineage"]["status"] == "EQUIVALENT_AFTER_REGISTERED_CHALLENGER_REMOVAL"
    assert result["stateful_policy_evaluation"] is True
    assert result["winner_declared"] is False
    assert result["promotion_claimed"] is False
    assert result["productive_integration_enabled"] is False
    assert result["execution_allowed"] is False
    assert result["w6_interface_changed"] is False
    assert result["w8_action_matrix_changed"] is False


def test_cost_or_realized_return_mismatch_blocks_comparison():
    core = [_step(before=0.0, after=1.0, day="2026-09-29")]
    with pytest.raises(ChallengerEvaluationError, match="transaction_cost_bps"):
        _run(core, [_step(before=0.0, after=0.5, day="2026-09-29", cost=11.0)])
    with pytest.raises(ChallengerEvaluationError, match="asset_return"):
        _run(core, [_step(before=0.0, after=0.5, day="2026-09-29", ret=0.03)])


def test_unregistered_lineage_delta_blocks_comparison():
    core = [_step(before=0.0, after=1.0, day="2026-09-29")]
    shadow = [_step(before=0.0, after=0.5, day="2026-09-29")]
    challenger_refs = [
        {"node_id": "core", "version_id": "v1", "content_hash": CORE_HASH},
        {"node_id": "adjustment", "version_id": "v1", "content_hash": ADJUSTMENT_HASH},
        {"node_id": "extra", "version_id": "v1", "content_hash": EXTRA_HASH},
    ]
    with pytest.raises(ChallengerEvaluationError, match="lineage_not_equivalent"):
        _run(core, shadow, challenger_refs=challenger_refs)


def test_registered_adjustment_must_exist_only_in_challenger_arm():
    core = [_step(before=0.0, after=1.0, day="2026-09-29")]
    shadow = [_step(before=0.0, after=0.5, day="2026-09-29")]
    with pytest.raises(ChallengerEvaluationError, match="adjustment_missing"):
        _run(core, shadow, challenger_refs=[{"node_id": "core", "version_id": "v1", "content_hash": CORE_HASH}])
    core_refs = [
        {"node_id": "core", "version_id": "v1", "content_hash": CORE_HASH},
        {"node_id": "adjustment", "version_id": "v1", "content_hash": ADJUSTMENT_HASH},
    ]
    with pytest.raises(ChallengerEvaluationError, match="present_in_frozen_core"):
        _run(core, shadow, core_refs=core_refs)


def test_productive_or_spent_readiness_is_rejected():
    core = [_step(before=0.0, after=1.0, day="2026-09-29")]
    shadow = [_step(before=0.0, after=0.5, day="2026-09-29")]
    with pytest.raises(ChallengerEvaluationError, match="scope_violation"):
        _run(core, shadow, readiness=_readiness(productive_integration_enabled=True))
    with pytest.raises(ChallengerEvaluationError, match="frozen_unspent"):
        _run(core, shadow, readiness=_readiness(qm_a_evidence_state="CONFIRMATORY_EVALUATED"))


def test_starting_exposure_mismatch_is_rejected():
    core = [_step(before=0.0, after=1.0, day="2026-09-29")]
    shadow = [_step(before=0.0, after=0.5, day="2026-09-29")]
    with pytest.raises(ChallengerEvaluationError, match="starting_state_mismatch"):
        _run(core, shadow, initial_core=0.0, initial_challenger=0.5)
