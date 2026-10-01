"""Fail-closed governance guard for QM-H-QMJ-PHASE1A-LAG1-001.

This module does not alter scanner, Selection, Decision Layer or portfolio
semantics. It only prevents the spent Phase-1A level-score evidence from being
used for promotion after the frozen Lag-1 negative control triggered.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


SCHEMA_VERSION = "qm_j_phase1a_selection_freshness_gate_v1"
DEFAULT_CONFIG_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "qm_j_phase1a_selection_freshness_gate_v1.json"
)


class SelectionFreshnessGateError(ValueError):
    pass


def load_gate(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONFIG_PATH
    try:
        value = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SelectionFreshnessGateError(f"freshness_gate_unreadable:{target}") from exc
    if not isinstance(value, dict) or value.get("schema_version") != SCHEMA_VERSION:
        raise SelectionFreshnessGateError("freshness_gate_schema_invalid")
    validate_gate(value)
    return value


def validate_gate(value: Mapping[str, Any]) -> dict[str, Any]:
    if value.get("schema_version") != SCHEMA_VERSION:
        raise SelectionFreshnessGateError("freshness_gate_schema_invalid")
    if value.get("module") != "QM-J" or value.get("business_area") != "BA-QM7":
        raise SelectionFreshnessGateError("freshness_gate_scope_invalid")
    if value.get("finding_id") != "QM-H-QMJ-PHASE1A-LAG1-001":
        raise SelectionFreshnessGateError("freshness_gate_finding_invalid")
    if value.get("capa_id") != "QM-H-CAPA-QMJ-PHASE1A-LAG1-001":
        raise SelectionFreshnessGateError("freshness_gate_capa_invalid")
    if value.get("status") != "IMPLEMENTED_GOVERNANCE_GUARD_PENDING_EFFECTIVENESS_VERIFICATION":
        raise SelectionFreshnessGateError("freshness_gate_status_invalid")
    if value.get("research_only") is not True:
        raise SelectionFreshnessGateError("freshness_gate_must_be_research_only")
    if value.get("productive_integration_enabled") is not False or value.get("execution_allowed") is not False:
        raise SelectionFreshnessGateError("freshness_gate_productive_or_execution_scope_invalid")

    blocked = value.get("blocked_evidence")
    if not isinstance(blocked, Mapping):
        raise SelectionFreshnessGateError("freshness_gate_blocked_evidence_required")
    if blocked.get("application") != "PHASE1A_SELECTION_LEVEL_SCORE_20_SESSION":
        raise SelectionFreshnessGateError("freshness_gate_application_invalid")
    if blocked.get("falsification_result_hash") != "a646d5eb4397f32d60f922ddd333e30cf61562cde8b5b1a6d1f791d3e380f483":
        raise SelectionFreshnessGateError("freshness_gate_falsification_hash_invalid")
    if blocked.get("investigation_result_hash") != "27ae7c66a2e931a5008301d88f96b73539917fe6798852218a846455d7817a23":
        raise SelectionFreshnessGateError("freshness_gate_investigation_hash_invalid")
    if blocked.get("evidence_impact") != "PROMOTION_BLOCKED" or blocked.get("promotion_allowed") is not False:
        raise SelectionFreshnessGateError("freshness_gate_promotion_block_missing")

    root_cause = value.get("root_cause_basis")
    if not isinstance(root_cause, Mapping):
        raise SelectionFreshnessGateError("freshness_gate_root_cause_required")
    if root_cause.get("classification") != "TEMPORAL_REDUNDANCY_IN_SELECTION_LEVEL_SCORE":
        raise SelectionFreshnessGateError("freshness_gate_root_cause_classification_invalid")

    corrective = value.get("corrective_guard")
    if not isinstance(corrective, Mapping):
        raise SelectionFreshnessGateError("freshness_gate_corrective_guard_required")
    if corrective.get("existing_phase1a_level_score_evidence_may_support_promotion") is not False:
        raise SelectionFreshnessGateError("freshness_gate_existing_evidence_must_remain_blocked")
    if corrective.get("existing_falsification_trigger_may_be_reinterpreted_as_validation") is not False:
        raise SelectionFreshnessGateError("freshness_gate_trigger_reinterpretation_forbidden")
    for key in (
        "production_scanner_score_or_weights_changed",
        "decision_layer_changed",
        "depot_watch_changed",
    ):
        if corrective.get(key) is not False:
            raise SelectionFreshnessGateError(f"freshness_gate_corrective_boundary_invalid:{key}")

    future = value.get("future_promotion_requirements")
    if not isinstance(future, Mapping):
        raise SelectionFreshnessGateError("freshness_gate_future_requirements_required")
    required_true = (
        "prospective_unspent_evidence_required",
        "analysis_plan_must_be_preregistered_before_outcome_visibility",
        "past_only_lag_or_persistence_negative_control_required",
        "real_and_lag_control_must_use_identical_eligible_grid",
        "incremental_freshness_estimand_required",
        "overlap_and_dependence_handling_must_be_declared",
        "multiplicity_handling_must_be_declared_if_multiple_freshness_tests_are_used",
        "acceptance_rule_must_be_frozen_before_outcome_visibility",
    )
    for key in required_true:
        if future.get(key) is not True:
            raise SelectionFreshnessGateError(f"freshness_gate_future_requirement_missing:{key}")
    for key in (
        "acceptance_threshold_defined_by_this_capa",
        "post_trigger_threshold_selection_allowed",
        "historical_spent_evidence_may_verify_effectiveness",
    ):
        if future.get(key) is not False:
            raise SelectionFreshnessGateError(f"freshness_gate_posthoc_or_spent_evidence_forbidden:{key}")

    release = value.get("release_guard")
    if not isinstance(release, Mapping):
        raise SelectionFreshnessGateError("freshness_gate_release_guard_required")
    if release.get("qm_h_effectiveness_verified_required") is not True:
        raise SelectionFreshnessGateError("freshness_gate_effectiveness_verification_required")
    if release.get("evidence_disposition_reference_required") is not True:
        raise SelectionFreshnessGateError("freshness_gate_disposition_reference_required")
    if release.get("automatic_release_from_promotion_block") is not False:
        raise SelectionFreshnessGateError("freshness_gate_automatic_release_forbidden")
    if release.get("automatic_promotion_allowed") is not False:
        raise SelectionFreshnessGateError("freshness_gate_automatic_promotion_forbidden")

    boundaries = value.get("boundaries")
    if not isinstance(boundaries, Mapping):
        raise SelectionFreshnessGateError("freshness_gate_boundaries_required")
    required_false = (
        "scanner_logic_changed",
        "scanner_weights_changed",
        "selection_signal_changed",
        "timing_signal_changed",
        "decision_semantics_changed",
        "portfolio_action_changed",
        "orders_generated",
        "execution_enabled",
        "promotion_performed",
    )
    for key in required_false:
        if boundaries.get(key) is not False:
            raise SelectionFreshnessGateError(f"freshness_gate_boundary_violation:{key}")
    return dict(value)


def assert_evidence_promotion_blocked(result_hash: str, gate: Mapping[str, Any] | None = None) -> dict[str, Any]:
    value = validate_gate(gate or load_gate())
    blocked = value["blocked_evidence"]
    if str(result_hash) != str(blocked["falsification_result_hash"]):
        raise SelectionFreshnessGateError("freshness_gate_result_hash_not_registered")
    return {
        "schema_version": "qm_j_selection_freshness_promotion_guard_v1",
        "finding_id": value["finding_id"],
        "capa_id": value["capa_id"],
        "result_hash": str(result_hash),
        "evidence_impact": "PROMOTION_BLOCKED",
        "promotion_allowed": False,
        "effectiveness_verification_pending": True,
        "automatic_release_allowed": False,
    }
