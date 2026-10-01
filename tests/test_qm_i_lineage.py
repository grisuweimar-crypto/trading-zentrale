import json
from pathlib import Path

import pytest

from scanner.research.decision_layer.depot_watch import build_depot_watch
from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.decision_layer.universal_stance import compute_universal_stance
from scanner.research.governance.qm_a import GovernanceLedger
from scanner.research.governance.qm_c import HypothesisRegistry
from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry
from scanner.research.governance.qm_c_multiplicity import MultiplicityMonitoringRegistry
from scanner.research.governance.qm_c_results import ResultRegistry
from scanner.research.governance.qm_i_lineage import (
    LineageError,
    LineageRegistry,
    content_hash,
    load_qm_i_contract,
)


def node(node_id, node_type, *, version="v1", complete=True, payload=None):
    payload = payload if payload is not None else {"id": node_id, "version": version}
    return {
        "node_id": node_id,
        "version_id": version,
        "node_type": node_type,
        "content_hash": content_hash(payload),
        "lineage_complete": complete,
        "as_of": "2026-09-30T17:00:00Z",
        "metadata": {},
    }


def edge(edge_id, source, target, relation="DERIVED_FROM", *, material=True):
    return {
        "edge_id": edge_id,
        "from_node_id": source[0],
        "from_version_id": source[1],
        "to_node_id": target[0],
        "to_version_id": target[1],
        "relation": relation,
        "material_for_ancestry": material,
    }


def add(registry, record):
    registry.register_node(record=record, actor_id="tester", actor_role="researcher")
    return (record["node_id"], record["version_id"])


def link(registry, record):
    registry.register_edge(record=record, actor_id="tester", actor_role="researcher")


def test_contract_is_research_only_and_contains_masterplan_lineage_types():
    contract = load_qm_i_contract()
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["execution_allowed"] is False
    required = {"RAW_SOURCE", "FEATURE", "INDICATOR", "SCORE", "CLAIM", "CALIBRATION", "DECISION", "WATCH"}
    assert required.issubset(set(contract["node"]["node_types"]))
    assert contract["principles"]["missing_lineage_is_not_independence"] is True
    assert contract["principles"]["common_ancestry_is_review_trigger_not_automatic_defect"] is True
    assert contract["boundaries"]["scanner_weights_changed"] is False
    assert contract["boundaries"]["decision_layer_semantics_changed"] is False


def test_full_raw_to_watch_path_is_typed_versioned_and_hash_chained(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    types = ["RAW_SOURCE", "FEATURE", "INDICATOR", "SCORE", "CLAIM", "CALIBRATION", "DECISION", "WATCH"]
    refs = []
    for index, node_type in enumerate(types):
        refs.append(add(registry, node(f"N-{index}-{node_type}", node_type)))
    for index in range(len(refs) - 1):
        link(registry, edge(f"E-{index}", refs[index], refs[index + 1]))

    verification = registry.verify_integrity()
    assert verification["valid"] is True
    assert verification["node_version_count"] == len(types)
    assert verification["edge_count"] == len(types) - 1
    assert verification["head_hash"]

    ancestry = registry.analyze_ancestry(
        left_node_id=refs[0][0], left_version_id=refs[0][1],
        right_node_id=refs[-1][0], right_version_id=refs[-1][1],
    )
    assert ancestry["classification"] == "DIRECT_DEPENDENCY"
    assert ancestry["direct_path"][0] == f"{refs[0][0]}::{refs[0][1]}"
    assert ancestry["direct_path"][-1] == f"{refs[-1][0]}::{refs[-1][1]}"


def test_common_ancestry_returns_shared_nodes_and_paths_but_not_automatic_defect(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    raw = add(registry, node("RAW-YAHOO-001", "RAW_SOURCE"))
    feature = add(registry, node("FEATURE-RS3M", "FEATURE"))
    claim_a = add(registry, node("CLAIM-SELECTION", "CLAIM"))
    claim_b = add(registry, node("CLAIM-PROBABILITY", "CLAIM"))
    link(registry, edge("E-RAW-F", raw, feature))
    link(registry, edge("E-F-A", feature, claim_a))
    link(registry, edge("E-F-B", feature, claim_b))

    ancestry = registry.analyze_ancestry(
        left_node_id=claim_a[0], left_version_id=claim_a[1],
        right_node_id=claim_b[0], right_version_id=claim_b[1],
    )
    assert ancestry["classification"] == "COMMON_ANCESTRY"
    assert f"{feature[0]}::{feature[1]}" in ancestry["common_ancestors"]
    assert ancestry["paths"]

    review = registry.double_counting_review(
        combination_id="DECISION-COMBINATION-1",
        evidence_nodes=[{"node_id": claim_a[0], "version_id": claim_a[1]}, {"node_id": claim_b[0], "version_id": claim_b[1]}],
        purports_independent=False,
    )
    assert review["status"] == "REVIEW_REQUIRED"
    assert review["triggers"][0]["trigger"] == "COMMON_ANCESTRY_REVIEW_REQUIRED"
    assert review["common_ancestry_is_automatic_error"] is False
    assert review["automatic_weight_change_performed"] is False


def test_missing_lineage_never_becomes_supported_independence(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    left = add(registry, node("CLAIM-A", "CLAIM", complete=False))
    right = add(registry, node("CLAIM-B", "CLAIM", complete=False))
    ancestry = registry.analyze_ancestry(
        left_node_id=left[0], left_version_id=left[1],
        right_node_id=right[0], right_version_id=right[1],
    )
    assert ancestry["classification"] == "UNKNOWN_INCOMPLETE_LINEAGE"

    with pytest.raises(LineageError, match="independent_supported_contradicted_by_ancestry"):
        registry.register_independence_claim(
            record={
                "independence_claim_id": "IC-1", "version_id": "v1",
                "left_node_id": left[0], "left_version_id": left[1],
                "right_node_id": right[0], "right_version_id": right[1],
                "status": "INDEPENDENT_SUPPORTED",
                "review_reference": "review-001",
                "rationale": "Must fail because lineage is incomplete.",
            },
            actor_id="tester", actor_role="reviewer",
        )

    review = registry.double_counting_review(
        combination_id="UNKNOWN-COMBINATION",
        evidence_nodes=[{"node_id": left[0], "version_id": left[1]}, {"node_id": right[0], "version_id": right[1]}],
        purports_independent=True,
    )
    assert review["status"] == "REVIEW_REQUIRED"
    assert review["triggers"][0]["trigger"] == "INCOMPLETE_LINEAGE_REVIEW_REQUIRED"


def test_supported_independence_requires_complete_separate_lineage_and_review_reference(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    raw_a = add(registry, node("RAW-A", "RAW_SOURCE"))
    raw_b = add(registry, node("RAW-B", "RAW_SOURCE"))
    claim_a = add(registry, node("CLAIM-A", "CLAIM"))
    claim_b = add(registry, node("CLAIM-B", "CLAIM"))
    link(registry, edge("E-A", raw_a, claim_a))
    link(registry, edge("E-B", raw_b, claim_b))

    assert registry.analyze_ancestry(
        left_node_id=claim_a[0], left_version_id=claim_a[1],
        right_node_id=claim_b[0], right_version_id=claim_b[1],
    )["classification"] == "NO_COMMON_ANCESTRY_DETECTED"

    with pytest.raises(LineageError, match="independent_supported_requires_review_reference"):
        registry.register_independence_claim(
            record={
                "independence_claim_id": "IC-1", "version_id": "v1",
                "left_node_id": claim_a[0], "left_version_id": claim_a[1],
                "right_node_id": claim_b[0], "right_version_id": claim_b[1],
                "status": "INDEPENDENT_SUPPORTED", "review_reference": None,
                "rationale": "No review reference.",
            }, actor_id="tester", actor_role="reviewer"
        )

    registry.register_independence_claim(
        record={
            "independence_claim_id": "IC-1", "version_id": "v1",
            "left_node_id": claim_a[0], "left_version_id": claim_a[1],
            "right_node_id": claim_b[0], "right_version_id": claim_b[1],
            "status": "INDEPENDENT_SUPPORTED", "review_reference": "review-001",
            "rationale": "Separate complete roots and explicit review.",
        }, actor_id="tester", actor_role="reviewer"
    )
    review = registry.double_counting_review(
        combination_id="SUPPORTED-COMBINATION",
        evidence_nodes=[{"node_id": claim_a[0], "version_id": claim_a[1]}, {"node_id": claim_b[0], "version_id": claim_b[1]}],
        purports_independent=True,
    )
    assert review["status"] == "CLEAR"
    assert review["triggers"] == []


def test_new_lineage_can_make_old_independence_claim_stale_without_erasing_history(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    raw_a = add(registry, node("RAW-A", "RAW_SOURCE"))
    raw_b = add(registry, node("RAW-B", "RAW_SOURCE"))
    claim_a = add(registry, node("CLAIM-A", "CLAIM"))
    claim_b = add(registry, node("CLAIM-B", "CLAIM"))
    link(registry, edge("E-A", raw_a, claim_a))
    link(registry, edge("E-B", raw_b, claim_b))
    registry.register_independence_claim(
        record={
            "independence_claim_id": "IC-1", "version_id": "v1",
            "left_node_id": claim_a[0], "left_version_id": claim_a[1],
            "right_node_id": claim_b[0], "right_version_id": claim_b[1],
            "status": "INDEPENDENT_SUPPORTED", "review_reference": "review-001",
            "rationale": "Supported before additional ancestry became known.",
        }, actor_id="tester", actor_role="reviewer"
    )

    common = add(registry, node("RAW-COMMON", "RAW_SOURCE"))
    link(registry, edge("E-COMMON-A", common, raw_a))
    link(registry, edge("E-COMMON-B", common, raw_b))

    verification = registry.verify_integrity()
    assert verification["valid"] is True
    assert verification["stale_independence_claims"] == [{
        "independence_claim_id": "IC-1", "version_id": "v1", "current_classification": "COMMON_ANCESTRY"
    }]
    review = registry.double_counting_review(
        combination_id="STALE-CLAIM-COMBINATION",
        evidence_nodes=[{"node_id": claim_a[0], "version_id": claim_a[1]}, {"node_id": claim_b[0], "version_id": claim_b[1]}],
        purports_independent=True,
    )
    assert review["triggers"][0]["trigger"] == "INDEPENDENCE_CLAIM_CONTRADICTION_REVIEW_REQUIRED"


def test_material_lineage_cycle_is_rejected(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    raw = add(registry, node("RAW", "RAW_SOURCE"))
    feature = add(registry, node("FEATURE", "FEATURE"))
    score = add(registry, node("SCORE", "SCORE"))
    link(registry, edge("E-1", raw, feature))
    link(registry, edge("E-2", feature, score))
    with pytest.raises(LineageError, match="lineage_cycle_forbidden"):
        link(registry, edge("E-3", score, raw))


def phase7_packet():
    return build_input_packet(
        symbol="TEST",
        as_of="2026-09-30T17:00:00Z",
        source_snapshot_id="daily-snapshot-2026-09-30",
        evidence=[
            {
                "family": "selection",
                "claim_id": "SEL-TEST-001",
                "as_of": "2026-09-30T17:00:00Z",
                "available_from": "2026-09-30T17:00:00Z",
                "source_version": "selection-v1",
                "coverage_state": "available",
                "maturity_state": "robust",
                "pit_state": "verified",
                "integration_mode": "research_only",
                "payload": {"direction": "positive"},
            },
            {
                "family": "probability",
                "claim_id": "PROB-TEST-001",
                "claim_ref": "SEL-TEST-001",
                "as_of": "2026-09-30T17:00:00Z",
                "available_from": "2026-09-30T17:00:00Z",
                "source_version": "probability-v1",
                "coverage_state": "available",
                "maturity_state": "robust",
                "pit_state": "verified",
                "integration_mode": "research_only",
                "payload": {"horizon_sessions": 20, "probability": 0.63},
            },
        ],
    )


def test_phase7_adapter_reuses_claim_ids_and_turns_claim_ref_into_direct_lineage(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    packet = phase7_packet()
    imported = registry.register_phase7_packet(packet, actor_id="tester", actor_role="researcher")
    assert imported["source_snapshot"]["node_id"] == "daily-snapshot-2026-09-30"
    assert {ref["node_id"] for ref in imported["claims"]} == {"SEL-TEST-001", "PROB-TEST-001"}
    ancestry = registry.analyze_ancestry(
        left_node_id="SEL-TEST-001", left_version_id="selection-v1",
        right_node_id="PROB-TEST-001", right_version_id="probability-v1",
    )
    assert ancestry["classification"] == "DIRECT_DEPENDENCY"

    review = registry.double_counting_review(
        combination_id="selection-plus-own-probability",
        evidence_nodes=[
            {"node_id": "SEL-TEST-001", "version_id": "selection-v1"},
            {"node_id": "PROB-TEST-001", "version_id": "probability-v1"},
        ],
        purports_independent=True,
    )
    assert review["triggers"][0]["trigger"] == "DIRECT_DEPENDENCY_REVIEW_REQUIRED"


def test_phase7_stance_uses_explicit_decision_id_and_claim_lineage(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    packet = phase7_packet()
    registry.register_phase7_packet(packet, actor_id="tester", actor_role="researcher")
    stance = compute_universal_stance(packet)
    saved = registry.register_phase7_stance(
        stance,
        decision_id="DECISION-TEST-20260930",
        version_id="v1",
        actor_id="tester",
        actor_role="researcher",
    )
    assert saved["node_type"] == "DECISION"
    assert saved["node_id"] == "DECISION-TEST-20260930"
    ancestry = registry.analyze_ancestry(
        left_node_id="SEL-TEST-001", left_version_id="selection-v1",
        right_node_id="DECISION-TEST-20260930", right_version_id="v1",
    )
    assert ancestry["classification"] == "DIRECT_DEPENDENCY"


def test_phase7_watch_reuses_native_watch_id_without_guessing(tmp_path):
    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    watch = build_depot_watch(
        {"snapshot_id": "daily-empty", "as_of": "2026-09-30T17:00:00Z", "symbols": {}, "universe_size": 0},
        {"schema_version": "decision_depot_position_book_v1", "source_snapshot_id": "positions-empty", "as_of": "2026-09-30T16:00:00Z", "positions": []},
        {"schema_version": "decision_chain_bundle_set_v1", "bundles": []},
    )
    saved = registry.register_phase7_watch(watch, parent_nodes=[], actor_id="tester", actor_role="researcher")
    assert saved["node_id"] == watch["watch_id"]
    assert saved["node_type"] == "WATCH"
    assert saved["content_hash"] == content_hash(watch)


def test_qm_c_adapter_preserves_exact_stable_ids_and_hashes_for_rejected_result(tmp_path):
    hypotheses = HypothesisRegistry(tmp_path / "hypotheses.jsonl")
    plans = AnalysisPlanRegistry(tmp_path / "plans.jsonl")
    controls = MultiplicityMonitoringRegistry(tmp_path / "controls.jsonl")
    results = ResultRegistry(tmp_path / "results.jsonl")
    qm_a = GovernanceLedger(tmp_path / "qm_a.jsonl")
    hypotheses.register_hypothesis(
        record={
            "hypothesis_id": "H-LINEAGE-001",
            "hypothesis_version": "v1",
            "research_question": "Does the registered lineage test pattern improve alpha?",
            "hypothesis_statement": "The registered lineage test pattern improves forward alpha.",
            "hypothesis_family_id": "HF-LINEAGE",
            "research_mode": "CONFIRMATION",
            "qm_a_analysis_id": "A-LINEAGE-001",
            "universe_requirement": "OBSERVED_SCANNER_UNIVERSE",
        },
        actor_id="tester", actor_role="researcher"
    )
    hypothesis = hypotheses.get_hypothesis("H-LINEAGE-001", "v1")
    hypotheses.transition(
        hypothesis_id="H-LINEAGE-001", hypothesis_version="v1", to_state="REJECTED",
        actor_id="tester", actor_role="researcher", reason="Rejected before evaluation for lineage test."
    )
    results.register_result(
        record={
            "result_id": "R-LINEAGE-001", "result_version": "v1",
            "hypothesis_id": hypothesis["hypothesis_id"],
            "hypothesis_version": hypothesis["hypothesis_version"],
            "hypothesis_version_hash": hypothesis["hypothesis_version_hash"],
            "evidence_scope": "NO_OUTCOME_EVIDENCE",
            "outcome_classification": "REJECTED_PRE_EVALUATION",
            "conclusion": "Rejected before outcome inspection.",
            "evidence_artifact_hash": None,
            "analysis_plan_id": None, "analysis_plan_version": None, "analysis_plan_hash": None,
            "control_plan_id": None, "control_plan_version": None, "control_plan_hash": None,
            "qm_a_analysis_id": None, "qm_a_version_id": None,
        },
        hypothesis_registry=hypotheses, analysis_plan_registry=plans,
        control_registry=controls, qm_a_ledger=qm_a,
        actor_id="tester", actor_role="researcher"
    )
    result = results.get_result("R-LINEAGE-001", "v1")

    lineage = LineageRegistry(tmp_path / "lineage.jsonl")
    imported = lineage.register_qm_c_result_chain(
        result_id="R-LINEAGE-001", result_version="v1",
        hypothesis_registry=hypotheses, analysis_plan_registry=plans,
        control_registry=controls, result_registry=results,
        actor_id="tester", actor_role="researcher"
    )
    assert imported["nodes"] == [
        {"node_id": "H-LINEAGE-001", "version_id": "v1"},
        {"node_id": "R-LINEAGE-001", "version_id": "v1"},
    ]
    assert lineage.get_node("H-LINEAGE-001", "v1")["content_hash"] == hypothesis["hypothesis_version_hash"]
    assert lineage.get_node("R-LINEAGE-001", "v1")["content_hash"] == result["result_hash"]


def test_tampered_lineage_registry_fails_closed(tmp_path):
    path = tmp_path / "lineage.jsonl"
    registry = LineageRegistry(path)
    add(registry, node("RAW", "RAW_SOURCE"))
    event = json.loads(path.read_text(encoding="utf-8"))
    event["payload"]["record"]["node_type"] = "CLAIM"
    path.write_text(json.dumps(event) + "\n", encoding="utf-8")
    with pytest.raises(LineageError, match="lineage_entry_hash_invalid"):
        registry.verify_integrity()
