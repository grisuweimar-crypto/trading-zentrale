"""BA-QM8 end-to-end Scanner audit contract foundation.

This module adds no investment logic. It validates that the complete Scanner-vNext
information-path contract is explicit, fail-closed, lineage-aware and preserves
all open governance blocks before BA-QM8 runtime manipulation tests are added.

BA-QM8 engineering closure is intentionally NOT claimed by this foundation.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.qm_i_lineage import load_qm_i_contract
from scanner.research.governance.qm_j_closure import (
    validate_closure_file as validate_ba_qm7_closure_file,
)
from scanner.research.governance.qm_j_selection_freshness_gate import (
    load_gate,
    validate_gate,
)


SCHEMA_VERSION = "ba_qm8_scanner_e2e_audit_v1"
EXPECTED_STAGES = (
    "DATA",
    "SCANNER",
    "SELECTION",
    "TIMING",
    "PROBABILITY",
    "RISK",
    "CONFIDENCE",
    "LEARNING",
    "ELLIOTT",
    "DECISION_LAYER",
    "EXTERNAL_EVIDENCE",
)
EXPECTED_ERROR_CLASSES = frozenset(
    {
        "LEAKAGE",
        "RETROJECTION",
        "DOUBLE_COUNTING",
        "SEMANTIC_DRIFT",
        "MISSING_AS_NEUTRAL",
        "UNCONTROLLED_MULTIPLICITY",
        "HIDDEN_EVIDENCE_REUSE",
    }
)
EXPECTED_TRANSITIONS = tuple(zip(EXPECTED_STAGES, EXPECTED_STAGES[1:]))
EXPECTED_REGRESSION_DEPENDENCIES = frozenset(
    {
        "BA-QM7/QM-J",
        "QM-H",
        "QM-I",
        "W11",
        "W12/Decision Watch",
        "External Evidence",
    }
)
EXPECTED_BOUNDARIES = frozenset(
    {
        "scanner_logic_changed",
        "scanner_weights_changed",
        "selection_signal_changed",
        "timing_signal_changed",
        "probability_semantics_changed",
        "risk_semantics_changed",
        "confidence_semantics_changed",
        "learning_semantics_changed",
        "elliott_semantics_changed",
        "decision_semantics_changed",
        "portfolio_action_changed",
        "external_evidence_promoted",
        "orders_generated",
        "execution_enabled",
        "productive_integration_enabled",
        "empirical_promotion_performed",
        "open_capa_closed_by_ba_qm8",
    }
)

_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_CONFIG_PATH = _ROOT / "configs" / "ba_qm8_scanner_e2e_audit_v1.json"
EXTERNAL_CONTRACT_PATH = _ROOT / "configs" / "external_evidence_8_contract_v1.json"
EXTERNAL_8C_COMPLETION_PATH = (
    _ROOT / "artifacts" / "research" / "external_evidence_8c_completion.json"
)


class BAQM8AuditError(ValueError):
    """Raised when the BA-QM8 foundation would become permissive or ambiguous."""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BAQM8AuditError(f"ba_qm8_input_unreadable:{path}") from exc
    if not isinstance(value, dict):
        raise BAQM8AuditError(f"ba_qm8_input_must_be_object:{path}")
    return value


def _require_false(mapping: Mapping[str, Any], key: str, label: str) -> None:
    if mapping.get(key) is not False:
        raise BAQM8AuditError(f"{label}_must_be_false:{key}")


def _require_true(mapping: Mapping[str, Any], key: str, label: str) -> None:
    if mapping.get(key) is not True:
        raise BAQM8AuditError(f"{label}_must_be_true:{key}")


def load_contract(path: str | Path | None = None) -> dict[str, Any]:
    result = _read_json(Path(path) if path is not None else DEFAULT_CONFIG_PATH)
    return validate_contract(result)


def validate_contract(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise BAQM8AuditError("ba_qm8_contract_must_be_object")
    result = dict(payload)

    if result.get("schema_version") != SCHEMA_VERSION:
        raise BAQM8AuditError("ba_qm8_contract_schema_invalid")
    if result.get("business_area") != "BA-QM8":
        raise BAQM8AuditError("ba_qm8_business_area_invalid")
    if result.get("status") != "CONTRACT_FOUNDATION_ACTIVE":
        raise BAQM8AuditError("ba_qm8_foundation_status_invalid")
    if result.get("engineering_status") != "IN_PROGRESS":
        raise BAQM8AuditError("ba_qm8_must_remain_in_progress")
    if result.get("transition_guard_status") != "IMPLEMENTED_FAIL_CLOSED_ENGINE":
        raise BAQM8AuditError("ba_qm8_transition_guard_status_invalid")
    if result.get("real_stage_adapter_status") != "IMPLEMENTED_WITH_OPEN_GAPS":
        raise BAQM8AuditError("ba_qm8_real_stage_adapter_status_invalid")
    if result.get("data_scanner_provenance_status") != "IMPLEMENTED_AWAITING_FIRST_PROSPECTIVE_SNAPSHOT":
        raise BAQM8AuditError("ba_qm8_data_scanner_provenance_status_invalid")
    if result.get("real_transition_observation_status") != "IMPLEMENTED_CURRENT_SNAPSHOT_9_OF_10_DATA_BLOCKED":
        raise BAQM8AuditError("ba_qm8_real_transition_observation_status_invalid")
    if result.get("research_only") is not True:
        raise BAQM8AuditError("ba_qm8_research_only_required")
    _require_false(result, "productive_integration_enabled", "ba_qm8")
    _require_false(result, "execution_allowed", "ba_qm8")
    _require_false(result, "investment_logic_changed", "ba_qm8")

    stages = result.get("stage_chain")
    if not isinstance(stages, list):
        raise BAQM8AuditError("ba_qm8_stage_chain_required")
    stage_ids = [str(row.get("stage_id") or "") for row in stages if isinstance(row, Mapping)]
    if len(stage_ids) != len(stages) or tuple(stage_ids) != EXPECTED_STAGES:
        raise BAQM8AuditError("ba_qm8_stage_chain_invalid")
    if len(set(stage_ids)) != len(stage_ids):
        raise BAQM8AuditError("ba_qm8_stage_ids_must_be_unique")

    for row in stages:
        assert isinstance(row, Mapping)
        stage = str(row["stage_id"])
        if not str(row.get("semantic_role") or "").strip():
            raise BAQM8AuditError(f"ba_qm8_stage_semantic_role_required:{stage}")
        identity = row.get("required_identity_fields")
        time_fields = row.get("required_time_fields")
        if not isinstance(identity, list) or not identity or not all(str(v).strip() for v in identity):
            raise BAQM8AuditError(f"ba_qm8_stage_identity_fields_required:{stage}")
        if not isinstance(time_fields, list) or not time_fields or not all(str(v).strip() for v in time_fields):
            raise BAQM8AuditError(f"ba_qm8_stage_time_fields_required:{stage}")
        if row.get("lineage_required") is not True:
            raise BAQM8AuditError(f"ba_qm8_stage_lineage_required:{stage}")
        missing_policy = str(row.get("missing_policy") or "")
        if not missing_policy.startswith("FAIL_CLOSED") or "NEUTRAL" == missing_policy:
            raise BAQM8AuditError(f"ba_qm8_stage_missing_policy_not_fail_closed:{stage}")

    transition_contract = result.get("transition_contract")
    if not isinstance(transition_contract, Mapping):
        raise BAQM8AuditError("ba_qm8_transition_contract_required")
    classes = transition_contract.get("required_error_classes")
    if not isinstance(classes, list) or frozenset(map(str, classes)) != EXPECTED_ERROR_CLASSES:
        raise BAQM8AuditError("ba_qm8_error_class_set_invalid")
    if len(classes) != len(EXPECTED_ERROR_CLASSES):
        raise BAQM8AuditError("ba_qm8_error_classes_must_be_unique")

    transitions = transition_contract.get("transitions")
    if not isinstance(transitions, list):
        raise BAQM8AuditError("ba_qm8_transitions_required")
    observed_pairs: list[tuple[str, str]] = []
    for row in transitions:
        if not isinstance(row, Mapping):
            raise BAQM8AuditError("ba_qm8_transition_must_be_object")
        pair = (str(row.get("from") or ""), str(row.get("to") or ""))
        observed_pairs.append(pair)
        checks = row.get("checks")
        if not isinstance(checks, list) or frozenset(map(str, checks)) != EXPECTED_ERROR_CLASSES:
            raise BAQM8AuditError(
                "ba_qm8_transition_error_coverage_invalid:" + "->".join(pair)
            )
        if len(checks) != len(EXPECTED_ERROR_CLASSES):
            raise BAQM8AuditError(
                "ba_qm8_transition_error_checks_must_be_unique:" + "->".join(pair)
            )
    if tuple(observed_pairs) != EXPECTED_TRANSITIONS:
        raise BAQM8AuditError("ba_qm8_transition_chain_invalid")

    lineage = result.get("lineage_contract")
    if not isinstance(lineage, Mapping):
        raise BAQM8AuditError("ba_qm8_lineage_contract_required")
    if lineage.get("qm_i_schema") != "qm_i_evidence_lineage_v1":
        raise BAQM8AuditError("ba_qm8_qm_i_schema_invalid")
    for key in (
        "stable_upstream_ids_must_be_reused",
        "common_ancestry_requires_review",
        "all_stages_require_lineage",
        "external_evidence_is_separate_root_family",
    ):
        _require_true(lineage, key, "ba_qm8_lineage")
    for key in (
        "missing_lineage_is_independence",
        "historical_lineage_backfilled_by_guessing",
        "direct_reference_is_independent_confirmation",
    ):
        _require_false(lineage, key, "ba_qm8_lineage")

    external = result.get("external_evidence_contract")
    if not isinstance(external, Mapping):
        raise BAQM8AuditError("ba_qm8_external_evidence_contract_required")
    if external.get("schema_version") != "external_evidence_8_contract_v1":
        raise BAQM8AuditError("ba_qm8_external_schema_invalid")
    if external.get("position") != "POST_DECISION_RESEARCH_SIDECAR":
        raise BAQM8AuditError("ba_qm8_external_position_invalid")
    _require_true(external, "retrojection_forbidden", "ba_qm8_external")
    for key in (
        "may_rewrite_historical_scanner_state",
        "may_rewrite_historical_decision_state",
        "may_rewrite_selection",
        "may_rewrite_timing",
        "may_rewrite_probability",
        "may_rewrite_risk",
        "may_rewrite_confidence",
        "may_rewrite_elliott",
        "may_replace_portfolio_action",
        "may_generate_order",
    ):
        _require_false(external, key, "ba_qm8_external")

    governance = result.get("governance_guard")
    if not isinstance(governance, Mapping):
        raise BAQM8AuditError("ba_qm8_governance_guard_required")
    _require_true(governance, "ba_qm7_closure_required", "ba_qm8_governance")
    if governance.get("finding_id") != "QM-H-QMJ-PHASE1A-LAG1-001":
        raise BAQM8AuditError("ba_qm8_lag1_finding_identity_invalid")
    if governance.get("capa_id") != "QM-H-CAPA-QMJ-PHASE1A-LAG1-001":
        raise BAQM8AuditError("ba_qm8_lag1_capa_identity_invalid")
    if governance.get("evidence_impact") != "PROMOTION_BLOCKED":
        raise BAQM8AuditError("ba_qm8_promotion_block_not_preserved")
    if governance.get("effectiveness_verification") != "PENDING_PROSPECTIVE_UNSPENT_EVIDENCE":
        raise BAQM8AuditError("ba_qm8_effectiveness_state_invalid")
    for key in (
        "green_ba_qm8_may_release_promotion_block",
        "green_w11_may_release_promotion_block",
        "green_watch_may_release_promotion_block",
        "automatic_promotion_allowed",
    ):
        _require_false(governance, key, "ba_qm8_governance")

    regressions = result.get("regression_dependencies")
    if (
        not isinstance(regressions, list)
        or frozenset(map(str, regressions)) != EXPECTED_REGRESSION_DEPENDENCIES
        or len(regressions) != len(EXPECTED_REGRESSION_DEPENDENCIES)
    ):
        raise BAQM8AuditError("ba_qm8_regression_dependencies_invalid")

    boundaries = result.get("boundaries")
    if not isinstance(boundaries, Mapping) or frozenset(map(str, boundaries)) != EXPECTED_BOUNDARIES:
        raise BAQM8AuditError("ba_qm8_boundary_set_invalid")
    for key in EXPECTED_BOUNDARIES:
        _require_false(boundaries, key, "ba_qm8_boundary")

    return result


def validate_dependencies(
    contract: Mapping[str, Any],
    *,
    ba_qm7_closure: Mapping[str, Any],
    freshness_gate: Mapping[str, Any],
    qm_i_contract: Mapping[str, Any],
    external_contract: Mapping[str, Any],
    external_8c_completion: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind the foundation to the current governance stack without promotion."""
    validated = validate_contract(contract)

    if ba_qm7_closure.get("engineering_status") != "COMPLETE":
        raise BAQM8AuditError("ba_qm8_requires_completed_ba_qm7")
    if ba_qm7_closure.get("all_negative_controls_passed") is not False:
        raise BAQM8AuditError("ba_qm8_requires_preserved_ba_qm7_trigger")
    if ba_qm7_closure.get("next_mandatory_work_package") != "BA-QM8 – End-to-End Scanner Audit":
        raise BAQM8AuditError("ba_qm8_ba_qm7_handoff_invalid")
    open_capa = ba_qm7_closure.get("open_capa")
    if not isinstance(open_capa, Mapping):
        raise BAQM8AuditError("ba_qm8_open_capa_required")
    if open_capa.get("finding_id") != "QM-H-QMJ-PHASE1A-LAG1-001":
        raise BAQM8AuditError("ba_qm8_open_capa_finding_invalid")
    if open_capa.get("capa_id") != "QM-H-CAPA-QMJ-PHASE1A-LAG1-001":
        raise BAQM8AuditError("ba_qm8_open_capa_id_invalid")
    if open_capa.get("evidence_impact") != "PROMOTION_BLOCKED":
        raise BAQM8AuditError("ba_qm8_open_capa_promotion_block_missing")
    if open_capa.get("effectiveness_verification") != "PENDING_PROSPECTIVE_UNSPENT_EVIDENCE":
        raise BAQM8AuditError("ba_qm8_open_capa_effectiveness_state_invalid")
    if open_capa.get("automatic_release_allowed") is not False:
        raise BAQM8AuditError("ba_qm8_open_capa_auto_release_forbidden")

    gate = validate_gate(freshness_gate)
    blocked = gate.get("blocked_evidence") or {}
    if blocked.get("evidence_impact") != "PROMOTION_BLOCKED" or blocked.get("promotion_allowed") is not False:
        raise BAQM8AuditError("ba_qm8_freshness_gate_not_blocked")
    release = gate.get("release_guard") or {}
    if release.get("automatic_release_from_promotion_block") is not False:
        raise BAQM8AuditError("ba_qm8_freshness_auto_release_forbidden")

    if qm_i_contract.get("schema_version") != "qm_i_evidence_lineage_v1":
        raise BAQM8AuditError("ba_qm8_qm_i_contract_invalid")
    principles = qm_i_contract.get("principles")
    if not isinstance(principles, Mapping):
        raise BAQM8AuditError("ba_qm8_qm_i_principles_required")
    _require_true(principles, "stable_upstream_ids_must_be_reused", "ba_qm8_qm_i")
    _require_true(principles, "missing_lineage_is_not_independence", "ba_qm8_qm_i")
    _require_true(principles, "direct_reference_is_not_independent_confirmation", "ba_qm8_qm_i")
    boundaries = qm_i_contract.get("boundaries")
    if not isinstance(boundaries, Mapping):
        raise BAQM8AuditError("ba_qm8_qm_i_boundaries_required")
    _require_false(boundaries, "historical_lineage_backfilled_by_guessing", "ba_qm8_qm_i")
    _require_false(boundaries, "empirical_promotion_performed", "ba_qm8_qm_i")

    if external_contract.get("schema_version") != "external_evidence_8_contract_v1":
        raise BAQM8AuditError("ba_qm8_external_dependency_schema_invalid")
    pit = external_contract.get("pit_contract")
    semantics = external_contract.get("external_evidence_semantics")
    status_model = external_contract.get("status_model")
    if not isinstance(pit, Mapping) or not isinstance(semantics, Mapping) or not isinstance(status_model, Mapping):
        raise BAQM8AuditError("ba_qm8_external_dependency_sections_missing")
    _require_true(pit, "current_value_retrojection_forbidden", "ba_qm8_external_dependency")
    for key in (
        "unknown_may_be_treated_as_neutral",
        "low_coverage_may_be_treated_as_neutral",
        "stale_may_be_treated_as_neutral",
        "conflicting_sources_may_be_treated_as_neutral",
    ):
        _require_false(status_model, key, "ba_qm8_external_dependency")
    for key in (
        "may_rewrite_selection",
        "may_rewrite_timing",
        "may_rewrite_probability",
        "may_rewrite_risk",
        "may_rewrite_confidence",
        "may_rewrite_elliott",
        "may_directly_replace_portfolio_action",
        "may_generate_order",
    ):
        _require_false(semantics, key, "ba_qm8_external_dependency")

    if external_8c_completion.get("schema_version") != "external_evidence_8c_completion_v1":
        raise BAQM8AuditError("ba_qm8_external_8c_completion_invalid")
    hard = external_8c_completion.get("hard_boundaries")
    if not isinstance(hard, Mapping):
        raise BAQM8AuditError("ba_qm8_external_8c_boundaries_required")
    for key in (
        "market_outcomes_read_for_8C_I",
        "production_external_evidence_enabled",
        "phase7_integration_enabled",
        "market_direction_assigned",
    ):
        _require_false(hard, key, "ba_qm8_external_8c")

    return {
        "schema_version": "ba_qm8_scanner_e2e_audit_foundation_receipt_v1",
        "status": "PASSED_CONTRACT_FOUNDATION",
        "engineering_status": "IN_PROGRESS",
        "transition_guard_status": "IMPLEMENTED_FAIL_CLOSED_ENGINE",
        "real_stage_adapter_status": "IMPLEMENTED_WITH_OPEN_GAPS",
        "data_scanner_provenance_status": "IMPLEMENTED_AWAITING_FIRST_PROSPECTIVE_SNAPSHOT",
        "real_transition_observation_status": "IMPLEMENTED_CURRENT_SNAPSHOT_9_OF_10_DATA_BLOCKED",
        "stage_count": len(EXPECTED_STAGES),
        "transition_count": len(EXPECTED_TRANSITIONS),
        "error_class_count": len(EXPECTED_ERROR_CLASSES),
        "all_transitions_cover_all_error_classes": True,
        "qm_i_lineage_bound": True,
        "external_evidence_fail_closed": True,
        "lag1_finding_id": "QM-H-QMJ-PHASE1A-LAG1-001",
        "lag1_capa_id": "QM-H-CAPA-QMJ-PHASE1A-LAG1-001",
        "evidence_impact": "PROMOTION_BLOCKED",
        "effectiveness_verification_pending": True,
        "automatic_release_allowed": False,
        "empirical_promotion_performed": False,
        "closure_claimed": False,
        "next_step": validated["next_step"],
    }


def validate_foundation_file(path: str | Path | None = None) -> dict[str, Any]:
    contract = load_contract(path)
    ba_qm7 = validate_ba_qm7_closure_file()
    freshness = load_gate()
    qm_i = load_qm_i_contract()
    external = _read_json(EXTERNAL_CONTRACT_PATH)
    external_8c = _read_json(EXTERNAL_8C_COMPLETION_PATH)
    return validate_dependencies(
        contract,
        ba_qm7_closure=ba_qm7,
        freshness_gate=freshness,
        qm_i_contract=qm_i,
        external_contract=external,
        external_8c_completion=external_8c,
    )
