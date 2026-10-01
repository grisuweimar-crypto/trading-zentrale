"""QM-H closure and integration validation against QM-A/B/C and BA-QM2."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.qm_c6_closure import validate_ba_qm2_handoff, validate_six_step_manifests
from scanner.research.governance.qm_h_capa import load_qm_h_contract

ROOT = Path(__file__).resolve().parents[4]


class QMHClosureError(ValueError):
    """Raised when QM-H closure or upstream integration invariants fail."""


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise QMHClosureError(f"required_json_unreadable:{path}") from exc
    if not isinstance(value, dict):
        raise QMHClosureError(f"json_object_required:{path}")
    return value


def validate_qm_h_closure(root: str | Path | None = None) -> dict[str, Any]:
    base = Path(root) if root is not None else ROOT
    validate_six_step_manifests(base)
    ba_qm2 = validate_ba_qm2_handoff(base)["handoff"]
    contract = load_qm_h_contract(base / "configs/qm_h_capa_v1.json")
    closure = _load(base / "configs/qm_h_closure_v1.json")
    qm_b = _load(base / "configs/qm_b_closure_v1.json")

    if closure.get("schema_version") != "qm_h_closure_v1":
        raise QMHClosureError("qm_h_closure_schema_invalid")
    if closure.get("qm_axis") != "QM-H" or closure.get("engineering_status") != "COMPLETE":
        raise QMHClosureError("qm_h_engineering_closure_invalid")
    if closure.get("continuous_control_status") != "ACTIVE":
        raise QMHClosureError("qm_h_continuous_control_not_active")
    if closure.get("research_only") is not True:
        raise QMHClosureError("qm_h_must_be_research_only")
    if closure.get("productive_integration_enabled") is not False or closure.get("execution_allowed") is not False:
        raise QMHClosureError("qm_h_productive_or_execution_scope_forbidden")
    if closure.get("empirical_promotion_claimed") is not False:
        raise QMHClosureError("qm_h_empirical_promotion_forbidden")

    principles = contract.get("principles", {})
    required_true = ["append_only_audit_trail", "hash_chained_events", "no_retroactive_event_invention", "qm_a_remains_evidence_consumption_authority", "qm_h_may_not_mutate_qm_a_state", "qm_b_external_gaps_do_not_reopen_engineering", "missing_external_evidence_remains_fail_closed", "stable_upstream_identities_are_reused_without_rekeying"]
    if any(principles.get(key) is not True for key in required_true):
        raise QMHClosureError("qm_h_principle_guard_missing")

    expected_categories = ["DEFECT", "NEAR_MISS", "METHODOLOGY_FINDING", "EXTERNAL_EVIDENCE_GAP"]
    if contract.get("finding_categories") != expected_categories:
        raise QMHClosureError("qm_h_finding_categories_invalid")

    expected_identity_contract = {
        "QM_A_ANALYSIS": ba_qm2["stable_lineage_identities"]["qm_a_analysis"],
        "QM_B_UNIVERSE": ba_qm2["stable_lineage_identities"]["qm_b_universe"],
        "QM_C_HYPOTHESIS": ba_qm2["stable_lineage_identities"]["hypothesis"],
        "QM_C_ANALYSIS_PLAN": ba_qm2["stable_lineage_identities"]["analysis_plan"],
        "QM_C_MULTIPLICITY_CONTROL": ba_qm2["stable_lineage_identities"]["multiplicity_control"],
        "QM_C_SEQUENTIAL_MONITORING": ba_qm2["stable_lineage_identities"]["sequential_monitoring"],
        "QM_C_RESULT": ba_qm2["stable_lineage_identities"]["result"],
    }
    declared_identity_contract = contract.get("identity_reference_contract", {})
    for kind, fields in expected_identity_contract.items():
        if declared_identity_contract.get(kind) != fields:
            raise QMHClosureError(f"qm_h_identity_contract_mismatch:{kind}")

    qm_b_blockers = {str(row.get("id")): str(row.get("state")) for row in qm_b.get("external_blockers", []) if isinstance(row, Mapping)}
    qm_h_blockers = {str(row.get("blocker_id")): str(row.get("state")) for row in contract.get("known_qm_b_external_blockers", []) if isinstance(row, Mapping)}
    if qm_h_blockers != qm_b_blockers:
        raise QMHClosureError("qm_h_qm_b_external_blocker_contract_mismatch")
    if any(state != "UNKNOWN_FAIL_CLOSED" for state in qm_h_blockers.values()):
        raise QMHClosureError("qm_h_qm_b_external_blocker_must_remain_fail_closed")

    scope = closure.get("scope_boundary", {})
    if scope.get("qm_a_authority_duplicated") is not False:
        raise QMHClosureError("qm_h_must_not_duplicate_qm_a")
    if scope.get("qm_a_state_mutation_by_qm_h_permitted") is not False:
        raise QMHClosureError("qm_h_must_not_mutate_qm_a")
    if scope.get("qm_b_engineering_reopened") is not False:
        raise QMHClosureError("qm_h_must_not_reopen_qm_b")
    if scope.get("historical_findings_backfilled_without_evidence") is not False:
        raise QMHClosureError("qm_h_must_not_invent_historical_findings")
    if scope.get("ba_qm3_implemented_by_this_closure") is not False:
        raise QMHClosureError("qm_h_must_not_implement_ba_qm3")
    if scope.get("next_step_after_user_authorization") != "BA-QM3 / QM-I":
        raise QMHClosureError("qm_h_next_step_invalid")

    if ba_qm2.get("engineering_governance_status") != "COMPLETE":
        raise QMHClosureError("ba_qm2_must_remain_complete")
    if ba_qm2.get("qm_b", {}).get("external_blockers_reopen_engineering") is not False:
        raise QMHClosureError("ba_qm2_qm_b_reopen_guard_invalid")
    if ba_qm2.get("scope_boundary", {}).get("ba_qm3_implemented_by_this_handoff") is not False:
        raise QMHClosureError("ba_qm2_scope_boundary_invalid")

    return {"valid": True, "display_status": closure["display_status"], "finding_categories": expected_categories, "known_qm_b_external_blockers": sorted(qm_h_blockers), "next_step_after_user_authorization": scope["next_step_after_user_authorization"], "closure": closure}
