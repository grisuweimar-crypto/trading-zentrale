from __future__ import annotations

from copy import deepcopy

import pytest

from scanner.research.governance.qm_h_capa import CapaLedger
from scanner.research.governance.qm_j_negative_controls import (
    NegativeControlError,
    destroyed_information_control,
    evaluate_falsification,
    irrelevant_feature_control,
    load_qm_j_contract,
    permuted_feature_control,
    placebo_evidence_control,
    pseudo_event_control,
    pseudo_signal_control,
    register_qm_h_falsification_finding,
    shifted_data_control,
    validate_control_artifact,
)


def _rows():
    return [
        {"symbol": "AAA", "as_of": "2026-09-01", "feature": 10.0, "signal": 1.0, "outcome": 0.01},
        {"symbol": "AAA", "as_of": "2026-09-02", "feature": 20.0, "signal": 2.0, "outcome": -0.02},
        {"symbol": "AAA", "as_of": "2026-09-03", "feature": 30.0, "signal": 3.0, "outcome": 0.03},
        {"symbol": "BBB", "as_of": "2026-09-01", "feature": 40.0, "signal": 4.0, "outcome": -0.01},
        {"symbol": "BBB", "as_of": "2026-09-02", "feature": 50.0, "signal": 5.0, "outcome": 0.02},
        {"symbol": "BBB", "as_of": "2026-09-03", "feature": 60.0, "signal": 6.0, "outcome": 0.00},
    ]


def _plan(direction="HIGHER_IS_BETTER", margin=0.01):
    return {
        "plan_id": "QM-J-PLAN-001",
        "plan_version": "v1",
        "frozen_at": "2026-10-01T10:00:00+00:00",
        "outcome_visibility_at_freeze": "NONE",
        "primary_metric": "alpha",
        "metric_direction": direction,
        "similarity_margin": margin,
    }


def _result(result_id, value, context="a" * 64, n=100):
    return {
        "result_id": result_id,
        "metric_name": "alpha",
        "metric_value": value,
        "comparison_context_hash": context,
        "n_observations": n,
    }


def test_contract_is_research_only_and_covers_masterplan_levels():
    contract = load_qm_j_contract()
    assert contract["business_area"] == "BA-QM7"
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["execution_allowed"] is False
    assert set(contract["negative_control_types"]) == {
        "DATA", "FEATURE", "RESEARCH", "DECISION_LAYER", "END_TO_END"
    }


def test_shifted_data_is_entity_local_past_only_and_does_not_mutate_source():
    rows = _rows()
    original = deepcopy(rows)
    artifact = shifted_data_control(
        rows,
        source_snapshot_id="snap-1",
        value_fields=["feature"],
        entity_fields=["symbol"],
        shift_by=1,
    )
    assert rows == original
    out = artifact["records"]
    assert out[0]["feature"] is None
    assert out[1]["feature"] == 10.0
    assert out[2]["feature"] == 20.0
    assert out[3]["feature"] is None
    assert out[4]["feature"] == 40.0
    assert out[5]["feature"] == 50.0
    assert artifact["transform_definition"]["uses_future_rows"] is False
    assert artifact["may_replace_productive_data"] is False


def test_feature_controls_are_deterministic_outcome_blind_and_preserve_source():
    rows = _rows()
    original = deepcopy(rows)
    a = permuted_feature_control(
        rows,
        source_snapshot_id="snap-2",
        feature_fields=["feature"],
        identity_fields=["symbol", "as_of"],
        seed="frozen-seed",
    )
    b = permuted_feature_control(
        rows,
        source_snapshot_id="snap-2",
        feature_fields=["feature"],
        identity_fields=["symbol", "as_of"],
        seed="frozen-seed",
    )
    assert rows == original
    assert a["control_content_hash"] == b["control_content_hash"]
    assert sorted(row["feature"] for row in a["records"]) == sorted(row["feature"] for row in rows)
    assert [row["feature"] for row in a["records"]] != [row["feature"] for row in rows]
    assert [row["outcome"] for row in a["records"]] == [row["outcome"] for row in rows]

    irrelevant = irrelevant_feature_control(
        rows,
        source_snapshot_id="snap-2",
        identity_fields=["symbol", "as_of"],
        feature_name="irrelevant_qmj",
        seed="irrelevant-seed",
    )
    assert all(0.0 <= row["irrelevant_qmj"] <= 1.0 for row in irrelevant["records"])
    assert all("irrelevant_qmj" not in row for row in rows)


def test_research_and_decision_placebos_are_sidecar_only():
    rows = _rows()
    pseudo_signal = pseudo_signal_control(
        rows,
        source_snapshot_id="snap-3",
        identity_fields=["symbol", "as_of"],
        signal_field="pseudo_signal",
        seed="signal-seed",
    )
    pseudo_event = pseudo_event_control(
        rows,
        source_snapshot_id="snap-3",
        identity_fields=["symbol", "as_of"],
        event_field="pseudo_event",
        seed="event-seed",
    )
    placebo = placebo_evidence_control(
        rows,
        source_snapshot_id="snap-3",
        identity_fields=["symbol", "as_of"],
        evidence_field="placebo_evidence",
        seed="placebo-seed",
    )
    assert pseudo_signal["level"] == "RESEARCH"
    assert pseudo_event["control_type"] == "PSEUDO_EVENT"
    assert placebo["level"] == "DECISION_LAYER"
    assert all(row["placebo_evidence"]["directional_vote_allowed"] is False for row in placebo["records"])
    assert placebo["may_enter_live_decision_path"] is False


def test_end_to_end_control_destroys_predictive_alignment_but_protects_outcomes():
    rows = _rows()
    original = deepcopy(rows)
    artifact = destroyed_information_control(
        rows,
        source_snapshot_id="snap-e2e",
        predictive_fields=["feature", "signal"],
        identity_fields=["symbol", "as_of"],
        protected_fields=["symbol", "as_of", "outcome"],
        seed="e2e-seed",
    )
    assert rows == original
    assert [row["outcome"] for row in artifact["records"]] == [row["outcome"] for row in rows]
    assert [row["symbol"] for row in artifact["records"]] == [row["symbol"] for row in rows]
    assert [row["feature"] for row in artifact["records"]] != [row["feature"] for row in rows]
    assert artifact["transform_definition"]["isolated_pipeline_required"] is True


def test_artifact_hash_or_productive_boundary_tampering_fails_closed():
    artifact = pseudo_signal_control(
        _rows(),
        source_snapshot_id="snap-x",
        identity_fields=["symbol", "as_of"],
        signal_field="pseudo_signal",
        seed="seed",
    )
    bad = deepcopy(artifact)
    bad["records"][0]["pseudo_signal"] = "POSITIVE"
    with pytest.raises(NegativeControlError, match="content_hash_mismatch"):
        validate_control_artifact(bad)

    bad = deepcopy(artifact)
    bad["may_enter_live_decision_path"] = True
    with pytest.raises(NegativeControlError, match="boundary_invalid"):
        validate_control_artifact(bad)


def test_falsification_stops_promotion_when_placebo_is_within_frozen_margin():
    evaluation = evaluate_falsification(
        plan=_plan(margin=0.01),
        real_result=_result("real", 0.10),
        control_results=[_result("placebo-weak", 0.02), _result("placebo-near", 0.095)],
    )
    assert evaluation["status"] == "PROMOTION_STOP_INVESTIGATION_CAPA_REQUIRED"
    assert evaluation["promotion_blocked_by_qm_j"] is True
    assert evaluation["investigation_required"] is True
    assert evaluation["capa_required"] is True
    assert evaluation["promotion_performed"] is False
    assert [row["control_result_id"] for row in evaluation["triggered_controls"]] == ["placebo-near"]
    assert set(evaluation["diagnostic_classes_if_triggered"]) == {
        "LEAKAGE", "OVERFITTING", "MULTIPLE_TESTING", "DEPENDENCE_ERROR", "PIPELINE_BIAS"
    }


def test_falsification_does_not_promote_when_controls_are_weak():
    evaluation = evaluate_falsification(
        plan=_plan(margin=0.01),
        real_result=_result("real", 0.10),
        control_results=[_result("placebo-1", 0.01), _result("placebo-2", 0.04)],
    )
    assert evaluation["status"] == "NEGATIVE_CONTROLS_NOT_SIMILARLY_STRONG"
    assert evaluation["promotion_blocked_by_qm_j"] is False
    assert evaluation["promotion_performed"] is False


def test_lower_is_better_direction_is_supported_without_reinterpreting_metric():
    evaluation = evaluate_falsification(
        plan=_plan(direction="LOWER_IS_BETTER", margin=0.005),
        real_result=_result("real", 0.10),
        control_results=[_result("placebo", 0.103)],
    )
    assert evaluation["promotion_blocked_by_qm_j"] is True


def test_falsification_plan_must_be_frozen_before_outcome_and_comparable():
    bad_plan = _plan()
    bad_plan["outcome_visibility_at_freeze"] = "ROW_LEVEL"
    with pytest.raises(NegativeControlError, match="frozen_pre_outcome"):
        evaluate_falsification(
            plan=bad_plan,
            real_result=_result("real", 0.10),
            control_results=[_result("p", 0.01)],
        )

    with pytest.raises(NegativeControlError, match="comparison_context_mismatch"):
        evaluate_falsification(
            plan=_plan(),
            real_result=_result("real", 0.10),
            control_results=[_result("p", 0.01, context="b" * 64)],
        )

    with pytest.raises(NegativeControlError, match="observation_count_mismatch"):
        evaluate_falsification(
            plan=_plan(),
            real_result=_result("real", 0.10),
            control_results=[_result("p", 0.01, n=99)],
        )


def test_triggered_falsification_registers_qm_h_promotion_blocking_finding(tmp_path):
    evaluation = evaluate_falsification(
        plan=_plan(margin=0.01),
        real_result=_result("real", 0.10),
        control_results=[_result("placebo", 0.10)],
    )
    ledger = CapaLedger(tmp_path / "qm_h.jsonl")
    result = register_qm_h_falsification_finding(
        evaluation,
        ledger=ledger,
        actor_id="qm-j",
        actor_role="RESEARCH_GOVERNANCE",
    )
    finding = ledger.get_finding(result["finding_id"])
    assert finding["category"] == "METHODOLOGY_FINDING"
    assert finding["evidence_impact"] == "PROMOTION_BLOCKED"
    assert finding["status"] == "OPEN"
    assert result["capa_required"] is True

    repeated = register_qm_h_falsification_finding(
        evaluation,
        ledger=ledger,
        actor_id="qm-j",
        actor_role="RESEARCH_GOVERNANCE",
    )
    assert repeated["already_registered"] is True


def test_qm_h_finding_cannot_be_created_for_non_triggering_control():
    evaluation = evaluate_falsification(
        plan=_plan(),
        real_result=_result("real", 0.10),
        control_results=[_result("placebo", 0.01)],
    )
    with pytest.raises(NegativeControlError, match="requires_triggered"):
        register_qm_h_falsification_finding(
            evaluation,
            ledger=object(),
            actor_id="qm-j",
            actor_role="RESEARCH_GOVERNANCE",
        )
