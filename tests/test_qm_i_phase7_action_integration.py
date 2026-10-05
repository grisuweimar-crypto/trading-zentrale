from scanner.research.decision_layer.depot_action_policy import apply_depot_action_policy
from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.decision_layer.phase7_state_history import (
    attach_state_history_to_7f,
    build_state_history_context,
)
from scanner.research.decision_layer.portfolio_action import compute_portfolio_action
from scanner.research.decision_layer.state_transition import build_state_transition_history
from scanner.research.decision_layer.universal_stance import compute_universal_stance
from scanner.research.governance.qm_i_lineage import LineageRegistry


def test_phase7_decision_to_portfolio_action_lineage_uses_explicit_ids(tmp_path):
    packet = build_input_packet(
        symbol="TEST",
        as_of="2026-09-30T17:00:00Z",
        source_snapshot_id="daily-action-snapshot",
        evidence=[
            {
                "family": "selection",
                "claim_id": "SEL-ACTION-001",
                "as_of": "2026-09-30T17:00:00Z",
                "available_from": "2026-09-30T17:00:00Z",
                "source_version": "selection-v1",
                "coverage_state": "available",
                "maturity_state": "robust",
                "pit_state": "verified",
                "integration_mode": "research_only",
                "payload": {"direction": "positive"},
            }
        ],
    )
    stance = compute_universal_stance(packet)
    transition = build_state_transition_history([stance])
    action = compute_portfolio_action(
        transition,
        {
            "schema_version": "decision_position_snapshot_v1",
            "symbol": "TEST",
            "source_snapshot_id": "position-snapshot-001",
            "as_of": "2026-09-30T16:00:00Z",
            "position_state": "flat",
            "quantity": 0,
            "market_value": 0,
            "currency": "EUR",
            "average_entry_price": None,
            "current_price": None,
            "transaction_cost_bps": None,
            "can_add": None,
            "remaining_adds": None,
        },
    )

    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    registry.register_phase7_packet(packet, actor_id="tester", actor_role="researcher")
    registry.register_phase7_stance(
        stance,
        decision_id="DECISION-ACTION-001",
        version_id="v1",
        actor_id="tester",
        actor_role="researcher",
    )
    saved = registry.register_phase7_portfolio_action(
        action,
        action_id="PORTFOLIO-ACTION-001",
        version_id="v1",
        decision_id="DECISION-ACTION-001",
        decision_version_id="v1",
        actor_id="tester",
        actor_role="researcher",
    )
    assert saved["node_type"] == "PORTFOLIO_ACTION"

    decision_ancestry = registry.analyze_ancestry(
        left_node_id="DECISION-ACTION-001",
        left_version_id="v1",
        right_node_id="PORTFOLIO-ACTION-001",
        right_version_id="v1",
    )
    assert decision_ancestry["classification"] == "DIRECT_DEPENDENCY"

    position = registry.get_node("position-snapshot-001", "2026-09-30T16:00:00Z")
    assert position["node_type"] == "POSITION_SNAPSHOT"
    assert position["lineage_complete"] is True
    position_ancestry = registry.analyze_ancestry(
        left_node_id="position-snapshot-001",
        left_version_id="2026-09-30T16:00:00Z",
        right_node_id="PORTFOLIO-ACTION-001",
        right_version_id="v1",
    )
    assert position_ancestry["classification"] == "DIRECT_DEPENDENCY"


def test_w8_state_history_claim_is_material_portfolio_action_parent(tmp_path):
    old_packet = build_input_packet(
        symbol="TEST",
        as_of="2026-10-03T17:00:00Z",
        source_snapshot_id="snapshot-old",
        evidence=[
            {
                "family": "selection",
                "claim_id": "SEL-W8-OLD",
                "as_of": "2026-10-03T17:00:00Z",
                "available_from": "2026-10-03T17:00:00Z",
                "source_version": "selection-v1-old",
                "coverage_state": "available",
                "maturity_state": "robust",
                "pit_state": "verified",
                "integration_mode": "research_only",
                "payload": {"direction": "positive"},
            }
        ],
    )
    current_packet = build_input_packet(
        symbol="TEST",
        as_of="2026-10-04T17:00:00Z",
        source_snapshot_id="snapshot-current",
        evidence=[
            {
                "family": "selection",
                "claim_id": "SEL-W8-CURRENT",
                "as_of": "2026-10-04T17:00:00Z",
                "available_from": "2026-10-04T17:00:00Z",
                "source_version": "selection-v1-current",
                "coverage_state": "available",
                "maturity_state": "robust",
                "pit_state": "verified",
                "integration_mode": "research_only",
                "payload": {"direction": "positive"},
            },
            {
                "family": "risk",
                "claim_id": "RISK-PATH-W8-CURRENT",
                "as_of": "2026-10-04T17:00:00Z",
                "available_from": "2026-10-04T17:00:00Z",
                "source_version": "scanner-path-v1",
                "coverage_state": "available",
                "maturity_state": "not_applicable",
                "pit_state": "verified",
                "integration_mode": "research_only",
                "payload": {
                    "context_type": "scanner_path_state_v1",
                    "overextension_active": True,
                    "last_overextension_date": "2026-10-04",
                    "last_overextension_rs3m": 0.20,
                    "recent_overextension": True,
                    "sessions_since_last_overextension": 0,
                    "sequence_state": "overextension_with_fading_dynamics",
                    "review_state": "profit_protection_review",
                    "review_is_trade_decision": False,
                    "execution_allowed": False,
                    "deterioration": {
                        "rs3m_falling_5t": True,
                        "score_falling_5t": True,
                        "rank_worsening_5t": False,
                        "trend200_falling_5t": True,
                        "r_code_downgrade": False,
                    },
                },
            },
        ],
    )

    old_stance = compute_universal_stance(old_packet)
    stance = compute_universal_stance(current_packet)
    transition = build_state_transition_history([old_stance, stance])
    action = compute_portfolio_action(
        transition,
        {
            "schema_version": "decision_position_snapshot_v1",
            "symbol": "TEST",
            "source_snapshot_id": "position-w8",
            "as_of": "2026-10-04T16:00:00Z",
            "position_state": "long",
            "quantity": 10,
            "market_value": 1000,
            "currency": "EUR",
            "average_entry_price": 90.0,
            "current_price": 100.0,
            "transaction_cost_bps": 10.0,
            "can_add": False,
            "remaining_adds": 0,
        },
    )
    state_history = build_state_history_context(
        current_packet,
        {
            "current": {},
            "dynamics": {},
            "persistence": {},
            "classification": {"overextension_warning": True},
            "historical_matches": {},
        },
    )
    action = attach_state_history_to_7f(action, state_history)
    action = apply_depot_action_policy(action, state_history)
    assert action["portfolio_action"]["state"] == "REDUCE_REVIEW"
    assert action["depot_action_policy"]["action_changed"] is True

    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    registry.register_phase7_packet(
        current_packet, actor_id="tester", actor_role="researcher"
    )
    registry.register_phase7_stance(
        stance,
        decision_id="DECISION-W8-001",
        version_id="v1",
        actor_id="tester",
        actor_role="researcher",
    )
    saved = registry.register_phase7_portfolio_action(
        action,
        action_id="PORTFOLIO-ACTION-W8-001",
        version_id="v1",
        decision_id="DECISION-W8-001",
        decision_version_id="v1",
        actor_id="tester",
        actor_role="researcher",
    )
    assert saved["metadata"]["phase"] == "7F/W8"
    assert saved["metadata"]["w8_action_policy_evaluated"] is True

    ancestry = registry.analyze_ancestry(
        left_node_id="RISK-PATH-W8-CURRENT",
        left_version_id="scanner-path-v1",
        right_node_id="PORTFOLIO-ACTION-W8-001",
        right_version_id="v1",
    )
    assert ancestry["classification"] == "DIRECT_DEPENDENCY"


def test_w6_elliott_review_context_is_material_portfolio_action_parent(tmp_path):
    packet = build_input_packet(
        symbol="TEST",
        as_of="2026-10-04T17:00:00Z",
        source_snapshot_id="snapshot-w6-current",
        evidence=[
            {
                "family": "selection",
                "claim_id": "SEL-W6-CURRENT",
                "as_of": "2026-10-04T17:00:00Z",
                "available_from": "2026-10-04T17:00:00Z",
                "source_version": "selection-v1-current",
                "coverage_state": "available",
                "maturity_state": "robust",
                "pit_state": "verified",
                "integration_mode": "research_only",
                "payload": {"direction": "positive"},
            }
        ],
    )
    stance = compute_universal_stance(packet)
    transition = build_state_transition_history([stance])
    action = compute_portfolio_action(
        transition,
        {
            "schema_version": "decision_position_snapshot_v1",
            "symbol": "TEST",
            "source_snapshot_id": "position-w6",
            "as_of": "2026-10-04T16:00:00Z",
            "position_state": "long",
            "quantity": 10,
            "market_value": 1000,
            "currency": "EUR",
            "average_entry_price": 90.0,
            "current_price": 100.0,
            "transaction_cost_bps": 10.0,
            "can_add": False,
            "remaining_adds": 0,
        },
        swing_context={
            "source": "elliott_vnext_6h",
            "source_output_id": "elliott-w6-output-001",
            "as_of": "2026-10-04T16:30:00Z",
            "review_contexts": ["profit_protection_review"],
            "routing_is_trade_decision": False,
            "research_only": True,
        },
    )
    assert action["portfolio_action"]["state"] == "REDUCE_REVIEW"
    assert action["swing_management"]["adjustment"] == "positive_stance_swing_reduction_review"

    registry = LineageRegistry(tmp_path / "lineage.jsonl")
    registry.register_phase7_packet(packet, actor_id="tester", actor_role="researcher")
    registry.register_phase7_stance(
        stance,
        decision_id="DECISION-W6-001",
        version_id="v1",
        actor_id="tester",
        actor_role="researcher",
    )
    registry.register_phase7_portfolio_action(
        action,
        action_id="PORTFOLIO-ACTION-W6-001",
        version_id="v1",
        decision_id="DECISION-W6-001",
        decision_version_id="v1",
        actor_id="tester",
        actor_role="researcher",
    )

    context = registry.get_node("elliott-w6-output-001", "elliott_vnext_6h")
    assert context["node_type"] == "DECISION_CONTEXT"
    assert context["lineage_complete"] is False
    ancestry = registry.analyze_ancestry(
        left_node_id="elliott-w6-output-001",
        left_version_id="elliott_vnext_6h",
        right_node_id="PORTFOLIO-ACTION-W6-001",
        right_version_id="v1",
    )
    assert ancestry["classification"] == "DIRECT_DEPENDENCY"
