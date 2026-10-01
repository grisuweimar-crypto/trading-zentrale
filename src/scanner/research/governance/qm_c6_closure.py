"""QM-C6 full six-step closure and BA-QM2 handoff validation."""
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import re
import unicodedata
from typing import Any, Mapping

from scanner.research.governance.qm_a import GovernanceLedger
from scanner.research.governance.qm_c import HypothesisRegistry
from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry
from scanner.research.governance.qm_c_families_multiplicity import FamilyMultiplicityRegistry
from scanner.research.governance.qm_c_sequential_monitoring import SequentialMonitoringRegistry
from scanner.research.governance.qm_c_negative_results import NegativeResultRegistry

ROOT = Path(__file__).resolve().parents[4]


class QMC6ClosureError(ValueError):
    pass


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise QMC6ClosureError(f"required_json_unreadable:{path}") from exc
    if not isinstance(value, dict):
        raise QMC6ClosureError(f"json_object_required:{path}")
    return value


def _hash(value: Any) -> str:
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8")).hexdigest()


def _norm(text: Any) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(text or "")).casefold()).strip()


def hypothesis_duplicate_fingerprint(hypothesis: Mapping[str, Any]) -> str:
    return _hash({
        "research_question": _norm(hypothesis.get("research_question")),
        "hypothesis_statement": _norm(hypothesis.get("hypothesis_statement")),
        "research_mode": str(hypothesis.get("research_mode") or "").upper(),
        "universe_requirement": str(hypothesis.get("universe_requirement") or "").upper(),
    })


def detect_duplicate_hypotheses(hypothesis_registry: HypothesisRegistry) -> list[dict[str, Any]]:
    hypothesis_registry.verify_integrity()
    _, hypotheses = hypothesis_registry._load_and_validate()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for hypothesis in hypotheses.values():
        grouped.setdefault(hypothesis_duplicate_fingerprint(hypothesis), []).append(hypothesis)
    findings = []
    for fingerprint, members in sorted(grouped.items()):
        ids = sorted({str(row["hypothesis_id"]) for row in members})
        if len(ids) < 2:
            continue
        findings.append({
            "finding_id": "QM-C-DUP-" + fingerprint[:16].upper(),
            "classification": "POTENTIAL_EXACT_SEMANTIC_DUPLICATE",
            "fingerprint": fingerprint,
            "hypothesis_ids": ids,
            "members": [{"hypothesis_id": row["hypothesis_id"], "hypothesis_version": row["hypothesis_version"], "hypothesis_version_hash": row["hypothesis_version_hash"]} for row in sorted(members, key=lambda x: (x["hypothesis_id"], x["hypothesis_version"]))],
            "automatic_merge_performed": False,
            "requires_review": True,
        })
    return findings


def validate_six_step_manifests(root: str | Path | None = None) -> dict[str, Any]:
    base = Path(root) if root is not None else ROOT
    files = {
        "QM-C1": base / "configs/qm_c1_closure_v1.json",
        "QM-C2": base / "configs/qm_c2_closure_v1.json",
        "QM-C3": base / "configs/qm_c3_closure_v2.json",
        "QM-C4": base / "configs/qm_c4_closure_v2.json",
        "QM-C5": base / "configs/qm_c5_closure_v1.json",
        "QM-C6": base / "configs/qm_c6_closure_v1.json",
    }
    manifests = {name: _load(path) for name, path in files.items()}
    c6 = manifests["QM-C6"]
    expected = c6.get("required_work_packages")
    if not isinstance(expected, Mapping) or list(expected) != ["QM-C1", "QM-C2", "QM-C3", "QM-C4", "QM-C5", "QM-C6"]:
        raise QMC6ClosureError("six_step_work_package_order_invalid")
    for name, status in expected.items():
        manifest = manifests[name]
        if manifest.get("work_package") != name:
            raise QMC6ClosureError(f"work_package_identity_mismatch:{name}")
        if manifest.get("display_status") != status:
            raise QMC6ClosureError(f"work_package_status_mismatch:{name}")
        if manifest.get("engineering_status") != "COMPLETE":
            raise QMC6ClosureError(f"work_package_not_complete:{name}")
        if manifest.get("research_only") is not True or manifest.get("productive_integration_enabled") is not False:
            raise QMC6ClosureError(f"work_package_scope_invalid:{name}")
    if c6.get("qm_c_axis_status") != "COMPLETE" or c6.get("empirical_promotion_claimed") is not False:
        raise QMC6ClosureError("qm_c6_axis_status_invalid")
    return {"valid": True, "manifests": manifests, "display_status": c6["display_status"]}


def audit_qm_c_system(*, hypothesis_registry: HypothesisRegistry, analysis_plan_registry: AnalysisPlanRegistry, control_registry: FamilyMultiplicityRegistry, monitoring_registry: SequentialMonitoringRegistry, result_registry: NegativeResultRegistry, qm_a_ledger: GovernanceLedger) -> dict[str, Any]:
    c1 = hypothesis_registry.verify_integrity(); c2 = analysis_plan_registry.verify_integrity(); c3 = control_registry.verify_integrity(); c4 = monitoring_registry.verify_integrity(); c5 = result_registry.verify_integrity()
    _, hypotheses = hypothesis_registry._load_and_validate(); _, plans = analysis_plan_registry._load_and_validate(); _, controls = control_registry._load(); _, monitoring = monitoring_registry._load(); _, results = result_registry._load()
    errors: list[str] = []
    for key, plan in plans.items():
        hkey = f"{plan['hypothesis_id']}::{plan['hypothesis_version']}"; h = hypotheses.get(hkey)
        if h is None: errors.append(f"plan_missing_hypothesis:{key}:{hkey}")
        elif h["hypothesis_version_hash"] != plan["hypothesis_version_hash"]: errors.append(f"plan_hypothesis_hash_mismatch:{key}")
    for key, control in controls.items():
        for member in control["family_members"]:
            hkey=f"{member['hypothesis_id']}::{member['hypothesis_version']}"; pkey=f"{member['analysis_plan_id']}::{member['analysis_plan_version']}"
            h=hypotheses.get(hkey); p=plans.get(pkey)
            if h is None or h.get("hypothesis_version_hash")!=member["hypothesis_version_hash"]: errors.append(f"control_hypothesis_binding_invalid:{key}:{hkey}")
            if p is None or p.get("analysis_plan_hash")!=member["analysis_plan_hash"]: errors.append(f"control_plan_binding_invalid:{key}:{pkey}")
            try: qm_a_ledger.get_analysis(member["qm_a_analysis_id"], member["qm_a_version_id"])
            except Exception: errors.append(f"control_qm_a_binding_missing:{key}:{member['qm_a_analysis_id']}::{member['qm_a_version_id']}")
    for key, monitor in monitoring.items():
        ckey=f"{monitor['control_plan_id']}::{monitor['control_plan_version']}"; control=controls.get(ckey)
        if control is None or control.get("control_plan_hash")!=monitor["control_plan_hash"]: errors.append(f"monitoring_control_binding_invalid:{key}:{ckey}")
    for key, result in results.items():
        hkey=f"{result['hypothesis_id']}::{result['hypothesis_version']}"; h=hypotheses.get(hkey)
        if h is None or h.get("hypothesis_version_hash")!=result["hypothesis_version_hash"]: errors.append(f"result_hypothesis_binding_invalid:{key}:{hkey}")
        if result["analysis_plan_id"] is not None:
            pkey=f"{result['analysis_plan_id']}::{result['analysis_plan_version']}"; p=plans.get(pkey)
            if p is None or p.get("analysis_plan_hash")!=result["analysis_plan_hash"]: errors.append(f"result_plan_binding_invalid:{key}:{pkey}")
        if result["control_plan_id"] is not None:
            ckey=f"{result['control_plan_id']}::{result['control_plan_version']}"; control=controls.get(ckey)
            if control is None or control.get("control_plan_hash")!=result["control_plan_hash"]: errors.append(f"result_control_binding_invalid:{key}:{ckey}")
        if result["monitoring_plan_id"] is not None:
            mkey=f"{result['monitoring_plan_id']}::{result['monitoring_plan_version']}"; monitor=monitoring.get(mkey)
            if monitor is None or monitor.get("monitoring_plan_hash")!=result["monitoring_plan_hash"]: errors.append(f"result_monitoring_binding_invalid:{key}:{mkey}")
        if result["qm_a_analysis_id"] is not None:
            try: qm_a_ledger.get_analysis(result["qm_a_analysis_id"], result["qm_a_version_id"])
            except Exception: errors.append(f"result_qm_a_binding_missing:{key}")
    duplicates = detect_duplicate_hypotheses(hypothesis_registry)
    return {
        "schema_version": "qm_c6_full_system_audit_v1",
        "status": "PASS" if not errors else "FAIL",
        "integrity": {"QM-C1":c1,"QM-C2":c2,"QM-C3":c3,"QM-C4":c4,"QM-C5":c5},
        "counts": {"hypothesis_versions":len(hypotheses),"analysis_plan_versions":len(plans),"control_plan_versions":len(controls),"monitoring_plan_versions":len(monitoring),"result_versions":len(results),"duplicate_findings":len(duplicates)},
        "cross_reference_errors": errors,
        "duplicate_findings": duplicates,
        "duplicate_findings_are_auto_merged": False,
    }


def validate_ba_qm2_handoff(root: str | Path | None = None) -> dict[str, Any]:
    base=Path(root) if root is not None else ROOT; validate_six_step_manifests(base)
    handoff=_load(base/"configs/ba_qm2_handoff_v2.json"); qm_b=_load(base/"configs/qm_b_closure_v1.json")
    if handoff.get("schema_version")!="ba_qm2_handoff_v2": raise QMC6ClosureError("ba_qm2_handoff_schema_invalid")
    if handoff.get("qm_c",{}).get("work_packages_complete") != ["QM-C1","QM-C2","QM-C3","QM-C4","QM-C5","QM-C6"]: raise QMC6ClosureError("ba_qm2_handoff_six_step_list_invalid")
    if handoff.get("strict_historical_promotion_status") != qm_b.get("strict_historical_promotion_status"): raise QMC6ClosureError("ba_qm2_qm_b_promotion_status_mismatch")
    blockers=[row.get("blocker_id") for row in qm_b.get("external_blockers",[]) if isinstance(row,Mapping)]
    declared=handoff.get("qm_b",{}).get("external_blockers")
    if declared != blockers: raise QMC6ClosureError("ba_qm2_qm_b_blocker_mismatch")
    if handoff.get("empirical_promotion_claimed") is not False: raise QMC6ClosureError("ba_qm2_empirical_promotion_forbidden")
    return {"valid":True,"display_status":handoff.get("display_status"),"handoff":handoff}
