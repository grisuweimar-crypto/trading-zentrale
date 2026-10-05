"""BA-QM11 Gesamtsystem-Audit closure validator.

Quality-management only.  The audit verifies system-level contracts and live
research runtime invariants; it does not change scanner, research, decision,
portfolio-action or execution semantics.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.qm_h_capa import CapaLedger

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_CONTRACT = ROOT / "configs" / "ba_qm11_system_audit_v1.json"
EXPECTED_DIMENSIONS = (
    "ARCHITECTURE",
    "INFORMATION_FLOW",
    "DECISION_LOGIC",
    "UNCERTAINTY",
    "MISSINGNESS",
    "FALSIFIABILITY",
    "LEARNING",
    "OPERATIONALIZATION",
    "PURPOSE_FIDELITY",
)


class BaQm11AuditError(ValueError):
    pass


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BaQm11AuditError(f"json_unreadable:{path}") from exc
    if not isinstance(value, dict):
        raise BaQm11AuditError(f"json_object_required:{path}")
    return value


def _require_false(payload: Mapping[str, Any], *fields: str) -> None:
    for field in fields:
        if payload.get(field) is not False:
            raise BaQm11AuditError(f"unsafe_true_or_missing:{field}")


def validate_contract(payload: Mapping[str, Any]) -> dict[str, Any]:
    if payload.get("schema_version") != "ba_qm11_system_audit_v1":
        raise BaQm11AuditError("ba_qm11_schema_invalid")
    if payload.get("business_area") != "BA-QM11":
        raise BaQm11AuditError("ba_qm11_identity_invalid")
    if payload.get("status") != "COMPLETE" or payload.get("engineering_status") != "COMPLETE":
        raise BaQm11AuditError("ba_qm11_not_complete")
    if payload.get("result") != "PASS" or payload.get("scope") != "QUALITY_MANAGEMENT_ONLY":
        raise BaQm11AuditError("ba_qm11_scope_or_result_invalid")
    if payload.get("closure_claimed") is not True:
        raise BaQm11AuditError("ba_qm11_closure_not_claimed")
    _require_false(
        payload,
        "research_logic_changed",
        "decision_logic_changed",
        "investment_logic_changed",
        "execution_enabled",
        "empirical_promotion_performed",
    )

    dimensions = payload.get("dimensions")
    if not isinstance(dimensions, Mapping) or tuple(dimensions.keys()) != EXPECTED_DIMENSIONS:
        raise BaQm11AuditError("ba_qm11_dimensions_invalid")
    for dimension in EXPECTED_DIMENSIONS:
        row = dimensions.get(dimension)
        if not isinstance(row, Mapping) or row.get("status") != "PASS":
            raise BaQm11AuditError(f"ba_qm11_dimension_not_pass:{dimension}")

    findings = payload.get("findings")
    if not isinstance(findings, list) or [row.get("finding_id") for row in findings] != [
        "BA-QM11-F01",
        "BA-QM11-F02",
    ]:
        raise BaQm11AuditError("ba_qm11_findings_invalid")
    if any(row.get("state") != "CLOSED_EFFECTIVE" for row in findings):
        raise BaQm11AuditError("ba_qm11_finding_not_closed_effective")
    if findings[0].get("evidence_impact") != "EVIDENCE_REVIEW_REQUIRED":
        raise BaQm11AuditError("ba_qm11_f01_evidence_impact_invalid")
    if findings[1].get("evidence_impact") != "PROMOTION_BLOCKED":
        raise BaQm11AuditError("ba_qm11_f02_evidence_impact_invalid")

    blocks = payload.get("independent_blocks")
    if not isinstance(blocks, Mapping):
        raise BaQm11AuditError("ba_qm11_independent_blocks_missing")
    lag1 = blocks.get("lag1")
    if not isinstance(lag1, Mapping):
        raise BaQm11AuditError("ba_qm11_lag1_block_missing")
    if lag1.get("evidence_impact") != "PROMOTION_BLOCKED":
        raise BaQm11AuditError("ba_qm11_lag1_promotion_block_lost")
    if lag1.get("ba_qm11_may_release") is not False:
        raise BaQm11AuditError("ba_qm11_may_not_release_lag1")

    done = payload.get("definition_of_done")
    if not isinstance(done, Mapping):
        raise BaQm11AuditError("ba_qm11_definition_of_done_missing")
    required_done = (
        "nine_system_dimensions_assessed",
        "concrete_findings_reproduced",
        "capa_only_for_proven_defects_or_methodology_risk",
        "evidence_impact_assessed",
        "regression_protection_added",
        "remaining_risks_documented",
        "lag1_block_preserved",
        "qm_h_findings_and_capas_registered",
    )
    if any(done.get(field) is not True for field in required_done):
        raise BaQm11AuditError("ba_qm11_definition_of_done_incomplete")
    if payload.get("next_mandatory_work_package") != "BA-QM12 – Konsolidierung & produktiver QM-Betrieb":
        raise BaQm11AuditError("ba_qm11_next_package_invalid")
    return dict(payload)


def audit_current_system(root: Path = ROOT) -> dict[str, Any]:
    contract = validate_contract(_read(root / "configs" / "ba_qm11_system_audit_v1.json"))

    for name in (
        "ba_qm8_scanner_e2e_audit_v1.json",
        "ba_qm9_end_application_audit_v1.json",
        "ba_qm10_production_operations_qm_v1.json",
    ):
        predecessor = _read(root / "configs" / name)
        if predecessor.get("status") != "COMPLETE":
            raise BaQm11AuditError(f"predecessor_not_complete:{name}")
        if predecessor.get("engineering_status") != "COMPLETE":
            raise BaQm11AuditError(f"predecessor_engineering_not_complete:{name}")

    qmi = _read(root / "configs" / "qm_i_evidence_lineage_v1.json")
    phase7 = qmi.get("phase7_integration")
    if not isinstance(phase7, Mapping):
        raise BaQm11AuditError("qm_i_phase7_integration_missing")
    for field in (
        "w8_state_history_claim_is_material_action_parent",
        "w8_changed_action_requires_registered_state_history",
        "w6_elliott_context_is_material_action_parent",
        "w6_context_lineage_incomplete_until_upstream_elliott_binding",
    ):
        if phase7.get(field) is not True:
            raise BaQm11AuditError(f"qm_i_ba_qm11_guard_missing:{field}")
    node_types = set((qmi.get("node") or {}).get("node_types") or [])
    if "DECISION_CONTEXT" not in node_types:
        raise BaQm11AuditError("qm_i_decision_context_type_missing")

    w8 = _read(root / "configs" / "decision_depot_action_policy_v1.json")
    if w8.get("change_classification") != "OUTCOME_DRIVEN_RESEARCH_CHANGE":
        raise BaQm11AuditError("w8_change_classification_invalid")
    governance = w8.get("governance")
    validation = w8.get("validation")
    if not isinstance(governance, Mapping) or not isinstance(validation, Mapping):
        raise BaQm11AuditError("w8_governance_missing")
    if governance.get("source_case_is_spent_for_independent_confirmation") is not True:
        raise BaQm11AuditError("w8_source_case_not_spent")
    if governance.get("prospective_unspent_from") != "2026-10-02":
        raise BaQm11AuditError("w8_prospective_boundary_invalid")
    if validation.get("empirically_validated") is not False or validation.get("promotion_eligible") is not False:
        raise BaQm11AuditError("w8_validation_state_unsafe")

    promotion = _read(root / "configs" / "decision_validation_promotion_v1.json")
    if promotion.get("frozen_on") != "2026-09-25":
        raise BaQm11AuditError("7i_original_freeze_date_changed")
    if "post_freeze_action_policy" in promotion:
        raise BaQm11AuditError("7i_frozen_contract_retroactively_mutated")
    frozen_required_layers = set(
        (promotion.get("shadow_trace_summary") or {}).get("required_captured_layers") or []
    )
    if frozen_required_layers != {"7D", "7E", "7F", "7G", "7H"}:
        raise BaQm11AuditError("7i_frozen_trace_layers_changed")

    runtime = _read(root / "artifacts" / "research" / "watch_runtime" / "public_long_reference.json")
    diagnostics = runtime.get("diagnostics")
    if not isinstance(diagnostics, Mapping):
        raise BaQm11AuditError("runtime_diagnostics_missing")
    expected_live = contract["live_system_evidence"]
    if int(diagnostics.get("bundle_count", -1)) != int(expected_live["bundle_count"]):
        raise BaQm11AuditError("runtime_bundle_count_changed_from_closure_evidence")
    if list(diagnostics.get("missing_current_packet_symbols") or []):
        raise BaQm11AuditError("runtime_missing_current_packets")
    if diagnostics.get("phase8_external_evidence_activated") is not False:
        raise BaQm11AuditError("runtime_external_evidence_unexpectedly_active")
    if diagnostics.get("private_position_data_persisted") is not False:
        raise BaQm11AuditError("runtime_private_position_data_persisted")
    if diagnostics.get("scanner_scalar_fallback_used") is not False:
        raise BaQm11AuditError("runtime_scanner_scalar_fallback_used")
    changed = list(diagnostics.get("w8_changed_action_symbols") or [])
    if len(changed) != int(expected_live["w8_changed_action_count"]):
        raise BaQm11AuditError("runtime_w8_changed_action_count_mismatch")

    ledger = CapaLedger(root / "artifacts" / "research" / "qm" / "qm_h_capa_ledger.jsonl")
    lag1 = ledger.get_finding("QM-H-QMJ-PHASE1A-LAG1-001")
    if lag1.get("status") != "IMPLEMENTED" or lag1.get("evidence_impact") != "PROMOTION_BLOCKED":
        raise BaQm11AuditError("lag1_block_state_changed")

    finding_1 = ledger.get_finding("QM-H-BA-QM11-F01")
    if (
        finding_1.get("status") != "CLOSED"
        or finding_1.get("evidence_impact") != "EVIDENCE_REVIEW_REQUIRED"
        or finding_1.get("capa_id") != "QM-H-CAPA-BA-QM11-F01"
    ):
        raise BaQm11AuditError("ba_qm11_f01_qm_h_state_invalid")
    finding_2 = ledger.get_finding("QM-H-BA-QM11-F02")
    if (
        finding_2.get("status") != "CLOSED"
        or finding_2.get("evidence_impact") != "PROMOTION_BLOCKED"
        or finding_2.get("capa_id") != "QM-H-CAPA-BA-QM11-F02"
    ):
        raise BaQm11AuditError("ba_qm11_f02_qm_h_state_invalid")

    return {
        "schema_version": "ba_qm11_system_audit_receipt_v1",
        "status": "BA_QM11_ENGINEERING_COMPLETE",
        "result": "PASS",
        "dimension_count": len(EXPECTED_DIMENSIONS),
        "dimensions_passed": len(EXPECTED_DIMENSIONS),
        "finding_count": len(contract["findings"]),
        "findings_closed_effective": len(contract["findings"]),
        "qm_h_findings_closed": 2,
        "w8_live_changed_action_count": len(changed),
        "w8_empirical_promotion_eligible": False,
        "lag1_evidence_impact": lag1["evidence_impact"],
        "ba_qm11_may_release_lag1_block": False,
        "next_mandatory_work_package": contract["next_mandatory_work_package"],
    }
