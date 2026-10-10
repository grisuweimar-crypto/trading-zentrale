"""Read-only CYCLE-DIR CY-08 preflight; never a promotion or admission engine.

L12/L13/L14 remain the sole authorities for actual reviews, admissions,
shadow evaluation, and operations. The preflight cannot certify evidence.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


CONTRACTS = {
    "L12": ("configs/pattern_discovery/l12_promotion_gate_v1.json", "pattern_discovery_l12_promotion_gate_v1"),
    "L13": ("configs/pattern_discovery/l13_decision_challenger_v1.json", "pattern_discovery_l13_decision_challenger_v1"),
    "L14": ("configs/pattern_discovery/l14_continuous_operations_v1.json", "pattern_discovery_l14_continuous_operations_v1"),
}


def _read_json(root: Path, path: str) -> dict[str, Any] | None:
    try:
        value = json.loads((root / path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def audit_cy08(root: Path) -> dict[str, Any]:
    """Assess prerequisites without loading pattern results or writing artifacts.

    This check deliberately stops at REVIEW_REQUIRED: actual L12 eligibility,
    reviewer approval and L13 admission must be assessed by their own engines.
    """
    root = Path(root)
    blockers: list[str] = []
    contract_versions: dict[str, str | None] = {}
    contracts: dict[str, dict[str, Any]] = {}
    for phase, (path, version) in CONTRACTS.items():
        doc = _read_json(root, path)
        contract_versions[phase] = doc.get("schema_version") if doc else None
        if (doc is None or doc.get("schema_version") != version
                or doc.get("research_only") is not True
                or doc.get("productive_integration_enabled") is not False
                or doc.get("execution_allowed") is not False):
            blockers.append(f"{phase}_CONTRACT_INVALID_OR_UNSAFE")
        else:
            contracts[phase] = doc

    l12 = contracts.get("L12", {})
    if l12 and (l12.get("eligibility", {}).get("required_l9_result_class") != "SUPPORTED"
                or not {"A", "B"}.issubset(set(l12.get("eligibility", {}).get("allowed_ratings", [])))
                or set(l12.get("integration", {}).get("allowed_modes", [])) != {"ANNOTATION_ONLY", "SHADOW_CHALLENGER"}
                or l12.get("review", {}).get("approval_requires_eligibility") is not True
                or l12.get("review", {}).get("approval_requires_explicit_integration_contract") is not True):
        blockers.append("L12_REVIEW_SAFETY_MISMATCH")

    l13 = contracts.get("L13", {})
    if l13 and (l13.get("inputs", {}).get("required_l12_status") != "ADMITTED"
                or l13.get("inputs", {}).get("required_l12_current_validation_status") != "CURRENT"
                or l13.get("principles", {}).get("only_same_snapshot_l7_claims_allowed") is not True
                or l13.get("ablation", {}).get("paired_same_snapshot_required") is not True
                or l13.get("ablation", {}).get("same_horizon_required") is not True
                or l13.get("ablation", {}).get("same_target_scope_required") is not True
                or l13.get("ablation", {}).get("comparison") != "EXISTING_TIMING_VS_EXISTING_TIMING_PLUS_PROMOTED_PATTERNS"):
        blockers.append("L13_PAIRED_SHADOW_SAFETY_MISMATCH")
    # Generic L13 is gross. The separately versioned CY-08 companion is the
    # *required* net-cost ablation. Do not silently treat old L13 results as net.
    net = _read_json(root, "configs/cycle_direction/cy08_net_cost_v1.json")
    net_file = root / "src/scanner/research/pattern_discovery/cycle_cy08_net_ablation.py"
    if (net is None or net.get("schema_version") != "cycle_direction_cy08_net_cost_v1"
            or net.get("research_only") is not True
            or net.get("productive_integration_enabled") is not False
            or net.get("execution_allowed") is not False
            or net.get("no_auto_promotion") is not True
            or net.get("paired_comparison") != "EXISTING_TIMING_VS_EXISTING_TIMING_PLUS_PROMOTED_PATTERNS"
            or net.get("costs_bps_roundtrip") != [10, 20, 50]
            or net.get("primary_cost_bps_roundtrip") != 20
            or net.get("stress_cost_bps_roundtrip") != 50
            or not net_file.is_file()):
        blockers.append("L13_NET_COST_ABLATION_NOT_IMPLEMENTED")

    l14 = contracts.get("L14", {})
    if l14 and (l14.get("principles", {}).get("automatic_promotion_forbidden") is not True
                or l14.get("extraordinary_discovery", {}).get("requires_explicit_request") is not True
                or l14.get("extraordinary_discovery", {}).get("requires_new_l1_preregistration") is not True
                or l14.get("boundaries", {}).get("productive_decision_change_performed") is not False):
        blockers.append("L14_OPERATIONS_SAFETY_MISMATCH")

    design = _read_json(root, "configs/cycle_direction/cy05_preregistered_design_v1.json")
    if (design is None or design.get("schema_version") != "cycle_direction_cy05_research_design_v1"
            or design.get("research_only") is not True):
        blockers.append("CY05_DESIGN_INVALID_OR_MISSING")
    elif design.get("evidence_status", {}).get("frozen_l1_run_manifest_created") is not True:
        blockers.append("CY05_L1_RUN_FREEZE_NOT_DOCUMENTED")

    history = _read_json(root, "artifacts/cycle_history/manifest.json")
    if history is None or history.get("schema_version") != "cycle_observations_cy03_v1":
        blockers.append("CY03_MANIFEST_MISSING_OR_INVALID")
    else:
        if not isinstance(history.get("research_eligible"), int) or history["research_eligible"] < 1:
            blockers.append("CY03_NO_RESEARCH_ELIGIBLE_OBSERVATIONS")
        if not isinstance(history.get("provisional_lags"), dict) or not isinstance(history["provisional_lags"].get("5"), int) or history["provisional_lags"]["5"] < 1:
            blockers.append("CY03_NO_5OBS_DIRECTION_HISTORY")

    return {
        "schema_version": "cycle_direction_cy08_readiness_v1",
        "package": "CY-08",
        "read_only": True,
        "promotion_performed": False,
        "shadow_ablation_performed": False,
        "productive_decision_modified": False,
        "contract_versions": contract_versions,
        "cy03_snapshot_count": history.get("snapshots") if history else None,
        "cy03_research_eligible": history.get("research_eligible") if history else None,
        "cy03_provisional_lag5": history.get("provisional_lags", {}).get("5") if history else None,
        "blockers": sorted(set(blockers)),
        "status": "BLOCKED" if blockers else "REVIEW_REQUIRED",
        "next_gate": "Exact independently verified CY-07 L9/L10/L6/L5 evidence and explicit L12 reviewer decision; never automatic admission",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    args = parser.parse_args()
    print(json.dumps(audit_cy08(args.repo_root), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
