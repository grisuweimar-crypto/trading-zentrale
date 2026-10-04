"""BA-QM10 fail-closed production/operations closure gate.

The gate evaluates quality-management evidence only. It never changes Scanner,
Research, Decision, portfolio-action or execution semantics.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import scanner.research.governance.ba_qm10_production_operations as operations_module
from scanner.research.governance.ba_qm10_production_operations import (
    EXPECTED_CHECKS,
    load_contract,
)


SCHEMA_VERSION = "ba_qm10_closure_gate_v1"
_ROOT = Path(__file__).resolve().parents[4]


class BAQM10ClosureError(ValueError):
    pass


def _state_map(rows: object, id_key: str) -> dict[str, str]:
    if not isinstance(rows, list):
        raise BAQM10ClosureError("ba_qm10_closure_state_rows_required")
    result: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise BAQM10ClosureError("ba_qm10_closure_state_row_invalid")
        item_id = str(row.get(id_key) or "")
        state = str(row.get("state") or "")
        if not item_id or not state or item_id in result:
            raise BAQM10ClosureError("ba_qm10_closure_state_identity_invalid")
        result[item_id] = state
    return result


def evaluate_ba_qm10_closure(root: str | Path = _ROOT) -> dict[str, Any]:
    root = Path(root).resolve()
    contract = load_contract(root / "configs" / "ba_qm10_production_operations_qm_v1.json")
    audit = operations_module.audit_current_operations(root)

    if contract.get("scope") != "QUALITY_MANAGEMENT_ONLY":
        raise BAQM10ClosureError("ba_qm10_closure_scope_invalid")
    if audit.get("scope") != "QUALITY_MANAGEMENT_ONLY":
        raise BAQM10ClosureError("ba_qm10_audit_scope_invalid")
    if audit.get("false_decision_observed") is not False:
        raise BAQM10ClosureError("ba_qm10_false_decision_observed")
    if audit.get("all_observed_failures_fail_closed") is not True:
        raise BAQM10ClosureError("ba_qm10_fail_closed_evidence_missing")
    if audit.get("research_logic_changed") is not False:
        raise BAQM10ClosureError("ba_qm10_research_logic_changed")
    if audit.get("decision_logic_changed") is not False:
        raise BAQM10ClosureError("ba_qm10_decision_logic_changed")
    if audit.get("investment_logic_changed") is not False:
        raise BAQM10ClosureError("ba_qm10_investment_logic_changed")
    if audit.get("execution_enabled") is not False:
        raise BAQM10ClosureError("ba_qm10_execution_enabled")
    if audit.get("lag1_evidence_impact") != "PROMOTION_BLOCKED":
        raise BAQM10ClosureError("ba_qm10_lag1_block_missing")
    if audit.get("ba_qm10_may_release_lag1_block") is not False:
        raise BAQM10ClosureError("ba_qm10_lag1_release_forbidden")

    assessment = contract.get("current_assessment")
    if not isinstance(assessment, Mapping):
        raise BAQM10ClosureError("ba_qm10_assessment_required")
    if tuple(assessment.keys()) != EXPECTED_CHECKS:
        raise BAQM10ClosureError("ba_qm10_assessment_coverage_invalid")

    finding_states = _state_map(contract.get("findings"), "finding_id")
    risk_states = _state_map(contract.get("static_risks_to_verify"), "risk_id")

    blockers: list[str] = []
    for dimension in EXPECTED_CHECKS:
        state = str(assessment.get(dimension) or "")
        if not state.startswith("PASS"):
            blockers.append(f"check:{dimension}:{state or 'MISSING'}")
    for finding_id, state in finding_states.items():
        if state != "CLOSED_EFFECTIVE":
            blockers.append(f"finding:{finding_id}:{state}")
    for risk_id, state in risk_states.items():
        if state != "CLOSED_EFFECTIVE":
            blockers.append(f"risk:{risk_id}:{state}")

    if audit.get("scanner_publication_race_risk_open") is True:
        blockers.append("audit:scanner_publication_race_risk_open")
    if int(audit.get("open_finding_count") or 0) != 0:
        blockers.append(f"audit:open_finding_count:{audit.get('open_finding_count')}")

    eligible = not blockers
    return {
        "schema_version": SCHEMA_VERSION,
        "status": (
            "ELIGIBLE_FOR_BA_QM10_ENGINEERING_CLOSURE"
            if eligible
            else "PENDING_OPERATIONAL_EFFECTIVENESS"
        ),
        "required_check_count": len(EXPECTED_CHECKS),
        "finding_states": finding_states,
        "risk_states": risk_states,
        "closure_blockers": blockers,
        "engineering_closure_eligible": eligible,
        "engineering_closure_performed": False,
        "false_decision_observed": False,
        "research_logic_changed": False,
        "decision_logic_changed": False,
        "investment_logic_changed": False,
        "execution_enabled": False,
        "empirical_promotion_performed": False,
        "lag1_finding_id": contract["dependencies"]["lag1_finding_id"],
        "lag1_capa_id": contract["dependencies"]["lag1_capa_id"],
        "lag1_evidence_impact": "PROMOTION_BLOCKED",
        "ba_qm10_may_release_lag1_block": False,
        "automatic_release_allowed": False,
    }
