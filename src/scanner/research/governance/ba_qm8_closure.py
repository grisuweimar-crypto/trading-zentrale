"""BA-QM8 fail-closed engineering closure gate.

Engineering closure is allowed only when the current real snapshot has complete
stage bindings and all 10 adjacent transitions pass the seven-class guard.
A BA-QM8 closure never releases the independent Lag-1 CAPA promotion block.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from scanner.research.governance.ba_qm8_end_to_end import validate_foundation_file
from scanner.research.governance.ba_qm8_real_stage_bindings import audit_real_stage_bindings
from scanner.research.governance.ba_qm8_real_transitions import audit_real_transitions
from scanner.research.governance.qm_j_closure import (
    validate_closure_file as validate_ba_qm7_closure_file,
)
from scanner.research.governance.qm_j_selection_freshness_gate import load_gate, validate_gate


SCHEMA_VERSION = "ba_qm8_closure_gate_v1"
_ROOT = Path(__file__).resolve().parents[4]


class BAQM8ClosureError(ValueError):
    pass


def evaluate_ba_qm8_closure(root: str | Path = _ROOT) -> dict[str, Any]:
    root = Path(root).resolve()

    foundation = validate_foundation_file()
    ba_qm7 = validate_ba_qm7_closure_file()
    freshness = validate_gate(load_gate())
    stages = audit_real_stage_bindings(root)
    transitions = audit_real_transitions(root)

    if foundation.get("engineering_status") != "IN_PROGRESS":
        raise BAQM8ClosureError("ba_qm8_foundation_lifecycle_invalid")
    if foundation.get("closure_claimed") is not False:
        raise BAQM8ClosureError("ba_qm8_foundation_must_not_preclaim_closure")
    if foundation.get("evidence_impact") != "PROMOTION_BLOCKED":
        raise BAQM8ClosureError("ba_qm8_lag1_promotion_block_missing")
    if foundation.get("automatic_release_allowed") is not False:
        raise BAQM8ClosureError("ba_qm8_automatic_release_forbidden")

    open_capa = ba_qm7.get("open_capa")
    if not isinstance(open_capa, Mapping):
        raise BAQM8ClosureError("ba_qm8_open_capa_required")
    if open_capa.get("finding_id") != "QM-H-QMJ-PHASE1A-LAG1-001":
        raise BAQM8ClosureError("ba_qm8_open_capa_finding_invalid")
    if open_capa.get("capa_id") != "QM-H-CAPA-QMJ-PHASE1A-LAG1-001":
        raise BAQM8ClosureError("ba_qm8_open_capa_id_invalid")
    if open_capa.get("evidence_impact") != "PROMOTION_BLOCKED":
        raise BAQM8ClosureError("ba_qm8_open_capa_not_promotion_blocked")
    if open_capa.get("effectiveness_verification") != "PENDING_PROSPECTIVE_UNSPENT_EVIDENCE":
        raise BAQM8ClosureError("ba_qm8_open_capa_effectiveness_state_invalid")
    if open_capa.get("automatic_release_allowed") is not False:
        raise BAQM8ClosureError("ba_qm8_open_capa_auto_release_forbidden")

    blocked_evidence = freshness.get("blocked_evidence")
    release_guard = freshness.get("release_guard")
    if not isinstance(blocked_evidence, Mapping) or not isinstance(release_guard, Mapping):
        raise BAQM8ClosureError("ba_qm8_freshness_sections_missing")
    if blocked_evidence.get("evidence_impact") != "PROMOTION_BLOCKED":
        raise BAQM8ClosureError("ba_qm8_freshness_promotion_block_missing")
    if blocked_evidence.get("promotion_allowed") is not False:
        raise BAQM8ClosureError("ba_qm8_freshness_promotion_forbidden")
    if release_guard.get("automatic_release_from_promotion_block") is not False:
        raise BAQM8ClosureError("ba_qm8_freshness_auto_release_forbidden")

    stage_complete = (
        stages.get("status") == "REAL_STAGE_BINDINGS_PASS"
        and stages.get("closure_eligible") is True
        and not stages.get("closure_blockers")
    )
    transition_complete = (
        transitions.get("status") == "REAL_TRANSITIONS_PASS"
        and transitions.get("transition_pass_count") == 10
        and transitions.get("transition_blocked_count") == 0
        and transitions.get("closure_eligible") is True
        and transitions.get("all_executed_transitions_cover_all_seven_classes") is True
    )

    eligible = stage_complete and transition_complete
    blockers: list[str] = []
    if not stage_complete:
        blockers.extend(
            f"stage:{value}"
            for value in (stages.get("closure_blockers") or ["real_stage_bindings_incomplete"])
        )
    if not transition_complete:
        blocked = transitions.get("blocked_transitions") or []
        if blocked:
            blockers.extend(
                f"transition:{item.get('from_stage')}->{item.get('to_stage')}:{item.get('reason')}"
                for item in blocked
                if isinstance(item, Mapping)
            )
        else:
            blockers.append("transition:real_transition_audit_incomplete")

    return {
        "schema_version": SCHEMA_VERSION,
        "status": (
            "ELIGIBLE_FOR_BA_QM8_ENGINEERING_CLOSURE"
            if eligible
            else "PENDING_PROSPECTIVE_SNAPSHOT"
        ),
        "snapshot_id": stages.get("snapshot_id"),
        "stage_bindings_complete": stage_complete,
        "real_transitions_complete": transition_complete,
        "transition_pass_count": transitions.get("transition_pass_count"),
        "transition_blocked_count": transitions.get("transition_blocked_count"),
        "closure_blockers": blockers,
        "engineering_closure_eligible": eligible,
        "engineering_closure_performed": False,
        "empirical_validation_claimed": False,
        "empirical_promotion_performed": False,
        "lag1_finding_id": "QM-H-QMJ-PHASE1A-LAG1-001",
        "lag1_capa_id": "QM-H-CAPA-QMJ-PHASE1A-LAG1-001",
        "lag1_evidence_impact": "PROMOTION_BLOCKED",
        "lag1_effectiveness_verification": "PENDING_PROSPECTIVE_UNSPENT_EVIDENCE",
        "ba_qm8_may_release_lag1_block": False,
        "automatic_release_allowed": False,
    }
