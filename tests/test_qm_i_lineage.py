import json

import pytest

from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.governance.qm_a import GovernanceLedger
from scanner.research.governance.qm_c import HypothesisRegistry
from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry
from scanner.research.governance.qm_c_families_multiplicity import FamilyMultiplicityRegistry
from scanner.research.governance.qm_c_negative_results import NegativeResultRegistry
from scanner.research.governance.qm_c_sequential_monitoring import SequentialMonitoringRegistry
from scanner.research.governance.qm_i_lineage import LineageError, LineageRegistry, content_hash, load_qm_i_contract


def node(node_id, node_type, *, version="v1", complete=True):
    return {
        "node_id": node_id,
        "version_id": version,
        "node_type": node_type,
        "content_hash": content_hash({"id": node_id, "version": version}),
        "lineage_complete": complete,
        "as_of": "2026-09-30T17:00:00Z",
        "metadata": {},
    }


def add(registry, record):
    registry.register_node(record=record, actor_id="tester", actor_role="researcher")
    return record["node_id"], record["version_id"]


def link(registry, edge_id, source, target, relation="DERIVED_FROM"):
    registry.register_edge(
        record={
            "edge_id": edge_id,
            "from_node_id": source[0],
            "from_version_id": source[1],
            "to_node_id": target[0],
            "to_version_id": target[1],
            "relation": relation,
            "material_for_ancestry": True,
        },
        actor_id="tester",
        actor_role="researcher",
    )


def test_contract_is_research_only_and_current_qm_c_chain_is_representable():
    contract = load_qm_i_contract()
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["execution_allowed"] is False
    assert contract["qm_c_integration"]["handoff_schema"] == "ba_qm2_handoff_v2"
    assert contract["qm_c_integration"]["reuse_monitoring_plan_identity"] is True
    required = {"RAW_SOURCE", "FEATURE", "INDICATOR", "SCORE", "CLAIM", "CALIBRATION", "DECISION", "WATCH", "HYPOTHESIS", "ANALYSIS_PLAN", "CONTROL_PLAN", "MONITORING_PLAN", "RESULT"}
    assert required.issubset(set(contract["node"]["node_types"]))
    assert contract["principles"]["missing_lineage_is_not_independence"] is True
    assert contract["principles"]["qm_h_capa_authority_preserved"] is True


def test_typed_path_is_hash_chained_and_detects_direct_dependency(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    types = ["RAW_SOURCE", "FEATURE", "INDICATOR", "SCORE", "CLAIM", "CALIBRATION", "DECISION", "WATCH"]
    refs = [add(registry, node(f"N-{i}", kind)) for i, kind in enumerate(types)]
    for i in range(len(refs) - 1):
        link(registry, f"E-{i}", refs[i], refs[i + 1])
    verification = registry.verify_integrity()
    assert verification["valid"] is True
    assert verification["node_version_count"] == len(types)
    ancestry = registry.analyze_ancestry(left_node_id=refs[0][0], left_version_id=refs[0][1], right_node_id=refs[-1][0], right_version_id=refs[-1][1])
    assert ancestry["classification"] == "DIRECT_DEPENDENCY"
    assert ancestry["direct_path"][0] == "N-0::v1"
    assert ancestry["direct_path"][-1] == f"N-{len(types)-1}::v1"


def test_common_ancestry_is_review_trigger_not_automatic_defect(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    raw = add(registry, node("RAW", "RAW_SOURCE"))
    feature = add(registry, node("FEATURE", "FEATURE"))
    left = add(registry, node("CLAIM-A", "CLAIM"))
    right = add(registry, node("CLAIM-B", "CLAIM"))
    link(registry, "E-RF", raw, feature)
    link(registry, "E-FA", feature, left)
    link(registry, "E-FB", feature, right)
    ancestry = registry.analyze_ancestry(left_node_id=left[0], left_version_id=left[1], right_node_id=right[0], right_version_id=right[1])
    assert ancestry["classification"] == "COMMON_ANCESTRY"
    assert "FEATURE::v1" in ancestry["common_ancestors"]
    review = registry.double_counting_review(combination_id="C-1", evidence_nodes=[{"node_id": left[0], "version_id": left[1]}, {"node_id": right[0], "version_id": right[1]}], purports_independent=False)
    assert review["status"] == "REVIEW_REQUIRED"
    assert review["triggers"][0]["trigger"] == "COMMON_ANCESTRY_REVIEW_REQUIRED"
    assert review["common_ancestry_is_automatic_error"] is False
    assert review["automatic_weight_change_performed"] is False


def test_incomplete_lineage_fails_closed_for_independence(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    left = add(registry, node("CLAIM-A", "CLAIM", complete=False))
    right = add(registry, node("CLAIM-B", "CLAIM", complete=False))
    assert registry.analyze_ancestry(left_node_id=left[0], left_version_id=left[1], right_node_id=right[0], right_version_id=right[1])["classification"] == "UNKNOWN_INCOMPLETE_LINEAGE"
    with pytest.raises(LineageError, match="independent_supported_contradicted_by_ancestry"):
        registry.register_independence_claim(
            record={"independence_claim_id": "IC-1", "version_id": "v1", "left_node_id": left[0], "left_version_id": left[1], "right_node_id": right[0], "right_version_id": right[1], "status": "INDEPENDENT_SUPPORTED", "review_reference": "review-1", "rationale": "must fail"},
            actor_id="tester",
            actor_role="reviewer",
        )


def test_supported_independence_requires_complete_separate_roots_and_review(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    raw_a = add(registry, node("RAW-A", "RAW_SOURCE"))
    raw_b = add(registry, node("RAW-B", "RAW_SOURCE"))
    left = add(registry, node("CLAIM-A", "CLAIM"))
    right = add(registry, node("CLAIM-B", "CLAIM"))
    link(registry, "E-A", raw_a, left)
    link(registry, "E-B", raw_b, right)
    with pytest.raises(LineageError, match="independent_supported_requires_review_reference"):
        registry.register_independence_claim(record={"independence_claim_id": "IC-1", "version_id": "v1", "left_node_id": left[0], "left_version_id": left[1], "right_node_id": right[0], "right_version_id": right[1], "status": "INDEPENDENT_SUPPORTED", "review_reference": None, "rationale": "missing review"}, actor_id="tester", actor_role="reviewer")
    registry.register_independence_claim(record={"independence_claim_id": "IC-1", "version_id": "v1", "left_node_id": left[0], "left_version_id": left[1], "right_node_id": right[0], "right_version_id": right[1], "status": "INDEPENDENT_SUPPORTED", "review_reference": "review-1", "rationale": "complete disjoint lineage"}, actor_id="tester", actor_role="reviewer")
    review = registry.double_counting_review(combination_id="C-2", evidence_nodes=[{"node_id": left[0], "version_id": left[1]}, {"node_id": right[0], "version_id": right[1]}], purports_independent=True)
    assert review["status"] == "CLEAR"


def test_new_provenance_can_make_old_independence_claim_stale(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    raw_a = add(registry, node("RAW-A", "RAW_SOURCE"))
    raw_b = add(registry, node("RAW-B", "RAW_SOURCE"))
    left = add(registry, node("CLAIM-A", "CLAIM"))
    right = add(registry, node("CLAIM-B", "CLAIM"))
    link(registry, "E-A", raw_a, left)
    link(registry, "E-B", raw_b, right)
    registry.register_independence_claim(record={"independence_claim_id": "IC-1", "version_id": "v1", "left_node_id": left[0], "left_version_id": left[1], "right_node_id": right[0], "right_version_id": right[1], "status": "INDEPENDENT_SUPPORTED", "review_reference": "review-1", "rationale": "initially disjoint"}, actor_id="tester", actor_role="reviewer")
    common = add(registry, node("RAW-COMMON", "RAW_SOURCE"))
    link(registry, "E-CA", common, raw_a)
    link(registry, "E-CB", common, raw_b)
    verification = registry.verify_integrity()
    assert verification["stale_independence_claims"][0]["current_classification"] == "COMMON_ANCESTRY"


def test_material_cycle_is_rejected(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    raw = add(registry, node("RAW", "RAW_SOURCE"))
    feature = add(registry, node("FEATURE", "FEATURE"))
    link(registry, "E-1", raw, feature)
    with pytest.raises(LineageError, match="lineage_cycle_forbidden"):
        link(registry, "E-2", feature, raw)


def test_phase7_claim_ref_is_material_direct_lineage(tmp_path):
    packet = build_input_packet(
        symbol="TEST",
        as_of="2026-09-30T17:00:00Z",
        source_snapshot_id="snapshot-1",
        evidence=[
            {"family": "selection", "claim_id": "SEL-1", "as_of": "2026-09-30T17:00:00Z", "available_from": "2026-09-30T17:00:00Z", "source_version": "v1", "coverage_state": "available", "maturity_state": "robust", "pit_state": "verified", "integration_mode": "research_only", "payload": {"direction": "positive"}},
            {"family": "probability", "claim_id": "PROB-1", "claim_ref": "SEL-1", "as_of": "2026-09-30T17:00:00Z", "available_from": "2026-09-30T17:00:00Z", "source_version": "v1", "coverage_state": "available", "maturity_state": "robust", "pit_state": "verified", "integration_mode": "research_only", "payload": {"horizon_sessions": 20, "probability": 0.63}},
        ],
    )
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    registry.register_phase7_packet(packet, actor_id="tester", actor_role="researcher")
    ancestry = registry.analyze_ancestry(left_node_id="SEL-1", left_version_id="v1", right_node_id="PROB-1", right_version_id="v1")
    assert ancestry["classification"] == "DIRECT_DEPENDENCY"



def _portfolio_action_with_w6(*, complete_provenance: bool):
    output_id = "d" * 64
    source_provenance = None
    timeframe_degrees = [{
        "output_id": output_id,
        "timeframe": "daily",
        "degree": "intermediate",
        "current_wave_stage": "wave_4",
        "primary_scenario_id": "scenario-1",
    }]
    if complete_provenance:
        timeframe_degrees[0]["lineage_features"] = {
            "elliott_structure": "e" * 64,
            "fibonacci_geometry": "f" * 64,
            "swing_routing": "1" * 64,
            "uncertainty_state": "2" * 64,
        }
        source_provenance = {
            "source_capture_id": "capture-2026-10-05",
            "snapshot_id": "snapshot-2026-10-05",
            "source_hashes": {
                "market_ohlcv_sha256": "a" * 64,
                "daily_research_sha256": "b" * 64,
            },
            "validation_source": {
                "adapter": "stage4_compact_aggregate_to_frozen_6g_v1",
                "stage4_result_hash": "c" * 64,
                "source_commit": "3" * 40,
                "price_source_sha256": "4" * 64,
            },
        }
    return {
        "schema_version": "decision_portfolio_action_v1",
        "phase": "7F",
        "symbol": "TEST",
        "as_of": "2026-10-05T10:00:00Z",
        "source_snapshot_id": "snapshot-2026-10-05",
        "universal_stance_context": {
            "raw_state": "positive",
            "raw_direction": "positive",
            "preserved": True,
        },
        "transition_context": {
            "status": "stable_confirmed",
            "stable_directional_anchor": "positive",
            "pending_direction": None,
            "stable_anchor_is_current_stance": True,
            "preserved": True,
        },
        "position_context": {
            "schema_version": "decision_position_snapshot_v1",
            "source_snapshot_id": "position-2026-10-05",
            "as_of": "2026-10-05T09:59:00Z",
            "position_state": "long",
            "quantity": 1,
            "currency": "USD",
            "average_entry_price": 100.0,
            "current_price": 110.0,
            "add_capacity_state": "unknown",
            "remaining_adds": None,
        },
        "pnl_context": {
            "unrealized_return_pct": 10.0,
            "unrealized_pnl": 10.0,
            "currency": "USD",
            "complete": True,
            "used_for_stance_direction": False,
            "used_for_action_direction": False,
        },
        "portfolio_action": {
            "state": "REDUCE_REVIEW",
            "reason_code": "test_w6_lineage",
            "review_only": True,
            "execution_allowed": False,
            "order_instruction": None,
        },
        "swing_management": {
            "mode": "position_reduce_review",
            "source": "elliott_vnext_6h",
            "source_output_id": output_id,
            "review_contexts": ["profit_protection_review"],
            "review_contexts_are_actions": False,
            "context_conflict": False,
            "adjustment": "positive_stance_swing_reduction_review",
            "elliott_changed_stance_direction": False,
            "w6": {
                "source_commit": "2" * 40,
                "elliott_output_as_of": "2026-10-05",
                "source_output_ids": [output_id],
                "output_count": 1,
                "timeframe_degrees": timeframe_degrees,
                "source_provenance": source_provenance,
            },
        },
        "cost_context": {
            "cost_sensitive_action": True,
            "transaction_cost_bps": None,
            "cost_model_present": False,
            "net_benefit_claim_made": False,
            "missing_cost_model_blocks_net_benefit_claim": True,
        },
        "semantics": {
            "position_state_changed_universal_stance": False,
            "pnl_changed_universal_stance": False,
            "pnl_changed_action_direction": False,
            "elliott_is_directional_vote": False,
            "elliott_review_context_is_order": False,
            "weighted_super_score_used": False,
            "position_sizing_computed": False,
            "target_weight_computed": False,
            "score_used_as_price_proxy": False,
            "broker_order_generated": False,
        },
        "validation": {
            "status": "prospective_unconfirmed",
            "research_only": True,
            "portfolio_action_rule_empirically_validated": False,
            "swing_action_edge_empirically_validated": False,
            "future_mature_outcomes_required": True,
            "productive_integration_enabled": False,
            "execution_allowed": False,
            "promotion_eligible": False,
        },
    }


def test_w6_complete_prospective_provenance_reaches_raw_data_through_features(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    decision = add(registry, node("DECISION-W6", "DECISION", complete=False))
    action = _portfolio_action_with_w6(complete_provenance=True)
    registry.register_phase7_portfolio_action(
        action,
        action_id="ACTION-W6",
        version_id="v1",
        decision_id=decision[0],
        decision_version_id=decision[1],
        actor_id="tester",
        actor_role="reviewer",
    )

    output_id = "d" * 64
    market_id = "elliott:market_ohlcv:" + ("a" * 64)
    context = registry.get_node(
        output_id,
        "elliott_vnext_6h:capture:capture-2026-10-05",
    )
    assert context["lineage_complete"] is True
    assert context["metadata"]["upstream_elliott_binding_complete"] is True

    for feature_name, feature_hash in {
        "elliott_structure": "e" * 64,
        "fibonacci_geometry": "f" * 64,
        "swing_routing": "1" * 64,
        "uncertainty_state": "2" * 64,
    }.items():
        feature = registry.get_node(
            f"elliott:{output_id}:{feature_name}",
            feature_hash,
        )
        assert feature["node_type"] == "FEATURE"
        assert feature["lineage_complete"] is True

    validation = registry.get_node(
        "elliott:stage4_validation:" + ("c" * 64),
        "stage4_compact_aggregate_to_frozen_6g_v1",
    )
    assert validation["node_type"] == "CALIBRATION"
    assert validation["lineage_complete"] is True

    validation_ancestry = registry.analyze_ancestry(
        left_node_id="elliott:validation_price_history:" + ("4" * 64),
        left_version_id="4" * 64,
        right_node_id="ACTION-W6",
        right_version_id="v1",
    )
    assert validation_ancestry["classification"] == "DIRECT_DEPENDENCY"

    ancestry = registry.analyze_ancestry(
        left_node_id=market_id,
        left_version_id="a" * 64,
        right_node_id="ACTION-W6",
        right_version_id="v1",
    )
    assert ancestry["classification"] == "DIRECT_DEPENDENCY"
    assert market_id + "::" + ("a" * 64) == ancestry["direct_path"][0]
    assert ancestry["direct_path"][-1] == "ACTION-W6::v1"


def test_w6_missing_exact_provenance_remains_lineage_incomplete(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    decision = add(registry, node("DECISION-W6", "DECISION", complete=False))
    action = _portfolio_action_with_w6(complete_provenance=False)
    registry.register_phase7_portfolio_action(
        action,
        action_id="ACTION-W6",
        version_id="v1",
        decision_id=decision[0],
        decision_version_id=decision[1],
        actor_id="tester",
        actor_role="reviewer",
    )

    context = registry.get_node("d" * 64, "elliott_vnext_6h")
    assert context["lineage_complete"] is False
    assert context["metadata"]["upstream_elliott_binding_complete"] is False
    with pytest.raises(LineageError, match="lineage_node_not_registered"):
        registry.get_node(
            "elliott:market_ohlcv:" + ("a" * 64),
            "a" * 64,
        )


def test_current_qm_c_rejected_result_ids_and_hashes_are_reused_without_rekeying(tmp_path):
    hypotheses = HypothesisRegistry(tmp_path / "hypotheses.jsonl")
    plans = AnalysisPlanRegistry(tmp_path / "plans.jsonl")
    controls = FamilyMultiplicityRegistry(tmp_path / "controls.jsonl")
    monitoring = SequentialMonitoringRegistry(tmp_path / "monitoring.jsonl")
    results = NegativeResultRegistry(tmp_path / "results.jsonl")
    qm_a = GovernanceLedger(tmp_path / "qm_a.jsonl")
    hypotheses.register_hypothesis(record={"hypothesis_id": "H-1", "hypothesis_version": "v1", "research_question": "Does X improve alpha?", "hypothesis_statement": "X improves alpha.", "hypothesis_family_id": "HF-1", "research_mode": "CONFIRMATION", "qm_a_analysis_id": "A-1", "universe_requirement": "OBSERVED_SCANNER_UNIVERSE"}, actor_id="tester", actor_role="researcher")
    hypothesis = hypotheses.get_hypothesis("H-1", "v1")
    hypotheses.transition(hypothesis_id="H-1", hypothesis_version="v1", to_state="REJECTED", actor_id="tester", actor_role="researcher", reason="pre-evaluation rejection")
    results.register_result(
        record={
            "result_id": "R-1",
            "result_version": "v1",
            "hypothesis_id": "H-1",
            "hypothesis_version": "v1",
            "hypothesis_version_hash": hypothesis["hypothesis_version_hash"],
            "evidence_scope": "NO_OUTCOME_EVIDENCE",
            "outcome_classification": "REJECTED_PRE_EVALUATION",
            "conclusion": "Rejected before outcome inspection.",
            "evidence_artifact_hash": None,
            "analysis_plan_id": None,
            "analysis_plan_version": None,
            "analysis_plan_hash": None,
            "control_plan_id": None,
            "control_plan_version": None,
            "control_plan_hash": None,
            "monitoring_plan_id": None,
            "monitoring_plan_version": None,
            "monitoring_plan_hash": None,
            "qm_a_analysis_id": None,
            "qm_a_version_id": None,
        },
        hypothesis_registry=hypotheses,
        analysis_plan_registry=plans,
        control_registry=controls,
        monitoring_registry=monitoring,
        qm_a_ledger=qm_a,
        actor_id="tester",
        actor_role="researcher",
    )
    result = results.get_result("R-1", "v1")
    lineage = LineageRegistry(tmp_path / "lineage.jsonl")
    imported = lineage.register_qm_c_result_chain(result_id="R-1", result_version="v1", hypothesis_registry=hypotheses, analysis_plan_registry=plans, control_registry=controls, monitoring_registry=monitoring, result_registry=results, actor_id="tester", actor_role="researcher")
    assert imported["nodes"] == [{"node_id": "H-1", "version_id": "v1"}, {"node_id": "R-1", "version_id": "v1"}]
    assert lineage.get_node("H-1", "v1")["content_hash"] == hypothesis["hypothesis_version_hash"]
    assert lineage.get_node("R-1", "v1")["content_hash"] == result["result_hash"]


def test_tampered_registry_fails_closed(tmp_path):
    path = tmp_path / "lineage.jsonl"
    registry = LineageRegistry(path)
    add(registry, node("RAW", "RAW_SOURCE"))
    event = json.loads(path.read_text(encoding="utf-8"))
    event["payload"]["record"]["node_type"] = "CLAIM"
    path.write_text(json.dumps(event) + "\n", encoding="utf-8")
    with pytest.raises(LineageError, match="lineage_entry_hash_invalid"):
        registry.verify_integrity()
