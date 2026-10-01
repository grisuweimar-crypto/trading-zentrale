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
        record={"result_id": "R-1", "result_version": "v1", "hypothesis_id": "H-1", "hypothesis_version": "v1", "hypothesis_version_hash": hypothesis["hypothesis_version_hash"], "evidence_scope": "NO_OUTCOME_EVIDENCE", "outcome_classification": "REJECTED_PRE_EVALUATION", "conclusion": "Rejected before outcome inspection."},
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
