from scanner.research.decision_layer.input_contract import build_input_packet
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
