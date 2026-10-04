"""BA-QM10 production and operations quality-management audit.

This module audits operational wiring only. It does not alter scanner research,
Decision-Layer semantics, portfolio actions, execution or empirical promotion.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import yaml

from scanner.reports.daily_research import validate_daily_research
from scanner.research.decision_layer.w10_orchestration import validate_sealed_manifest
from scanner.research.governance.ba_qm8_closure import evaluate_ba_qm8_closure
from scanner.research.governance.ba_qm9_end_application_audit import (
    evaluate_ba_qm9_closure,
)


SCHEMA_VERSION = "ba_qm10_production_operations_qm_v1"
_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_CONTRACT = _ROOT / "configs" / "ba_qm10_production_operations_qm_v1.json"

EXPECTED_CHECKS = (
    "SCANNER_AUTORUN",
    "GITHUB_ACTIONS",
    "PUBLICATION",
    "WORKFLOW_DEPENDENCIES",
    "RETRY",
    "IDEMPOTENCY",
    "ARTIFACT_PROVENANCE",
    "CANONICAL_FILE_SELECTION",
    "STALE_ARTIFACT_DETECTION",
    "RACE_CONDITIONS",
    "PARTIAL_RUNS",
    "RECOVERY",
    "FAILED_UPDATES",
)
EXPECTED_FINDINGS = ("BA-QM10-F01", "BA-QM10-F02", "BA-QM10-F03")


class BAQM10AuditError(ValueError):
    pass


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BAQM10AuditError(f"ba_qm10_input_unreadable:{path}") from exc
    if not isinstance(value, dict):
        raise BAQM10AuditError(f"ba_qm10_input_must_be_object:{path}")
    return value


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        value = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    except OSError as exc:
        raise BAQM10AuditError(f"ba_qm10_workflow_unreadable:{path}") from exc
    if not isinstance(value, dict):
        raise BAQM10AuditError(f"ba_qm10_workflow_must_be_object:{path}")
    return value


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise BAQM10AuditError(f"ba_qm10_mapping_required:{field}")
    return value


def validate_contract(value: Mapping[str, Any]) -> dict[str, Any]:
    if value.get("schema_version") != SCHEMA_VERSION:
        raise BAQM10AuditError("ba_qm10_schema_invalid")
    if value.get("business_area") != "BA-QM10":
        raise BAQM10AuditError("ba_qm10_business_area_invalid")
    if value.get("name") != "Produktions- und Betriebs-QM":
        raise BAQM10AuditError("ba_qm10_name_invalid")
    if value.get("status") != "IN_PROGRESS":
        raise BAQM10AuditError("ba_qm10_status_invalid")
    if value.get("scope") != "QUALITY_MANAGEMENT_ONLY":
        raise BAQM10AuditError("ba_qm10_scope_invalid")
    for key in (
        "research_logic_changed",
        "decision_logic_changed",
        "investment_logic_changed",
        "execution_enabled",
        "empirical_promotion_performed",
        "closure_claimed",
    ):
        if value.get(key) is not False:
            raise BAQM10AuditError(f"ba_qm10_must_be_false:{key}")
    if tuple(value.get("required_checks") or ()) != EXPECTED_CHECKS:
        raise BAQM10AuditError("ba_qm10_required_checks_invalid")

    assessment = _mapping(value.get("current_assessment"), "current_assessment")
    if tuple(assessment.keys()) != EXPECTED_CHECKS:
        raise BAQM10AuditError("ba_qm10_assessment_coverage_invalid")

    findings = value.get("findings")
    if not isinstance(findings, list):
        raise BAQM10AuditError("ba_qm10_findings_must_be_list")
    ids = tuple(str(row.get("finding_id") or "") for row in findings if isinstance(row, Mapping))
    if ids != EXPECTED_FINDINGS:
        raise BAQM10AuditError("ba_qm10_finding_registry_invalid")
    for row in findings:
        if not isinstance(row, Mapping):
            raise BAQM10AuditError("ba_qm10_finding_must_be_object")
        if row.get("state") != "OPEN_CAPA_REQUIRED":
            raise BAQM10AuditError(f"ba_qm10_finding_state_invalid:{row.get('finding_id')}")
        effect = _mapping(row.get("effect"), f"effect:{row.get('finding_id')}")
        if effect.get("decision_safety") != "FAIL_CLOSED":
            raise BAQM10AuditError(f"ba_qm10_finding_not_fail_closed:{row.get('finding_id')}")
        if row.get("finding_id") == "BA-QM10-F01" and effect.get("false_watch_accepted") is not False:
            raise BAQM10AuditError("ba_qm10_f01_false_watch_guard_invalid")

    deps = _mapping(value.get("dependencies"), "dependencies")
    if deps.get("ba_qm8_complete") is not True or deps.get("ba_qm9_complete") is not True:
        raise BAQM10AuditError("ba_qm10_predecessor_dependency_invalid")
    if deps.get("lag1_evidence_impact") != "PROMOTION_BLOCKED":
        raise BAQM10AuditError("ba_qm10_lag1_block_missing")
    if deps.get("ba_qm10_may_release_lag1_block") is not False:
        raise BAQM10AuditError("ba_qm10_lag1_release_forbidden")
    return dict(value)


def load_contract(path: str | Path = DEFAULT_CONTRACT) -> dict[str, Any]:
    return validate_contract(_read_json(Path(path)))


def _workflow(root: Path, name: str) -> dict[str, Any]:
    return _read_yaml(root / ".github" / "workflows" / name)


def audit_current_operations(root: str | Path = _ROOT) -> dict[str, Any]:
    root = Path(root).resolve()
    contract = load_contract(root / "configs" / "ba_qm10_production_operations_qm_v1.json")

    ba_qm8 = evaluate_ba_qm8_closure(root)
    ba_qm9 = evaluate_ba_qm9_closure(root)
    if ba_qm8.get("status") != "BA_QM8_ENGINEERING_COMPLETE":
        raise BAQM10AuditError("ba_qm10_requires_ba_qm8_complete")
    if ba_qm9.get("status") != "BA_QM9_ENGINEERING_COMPLETE":
        raise BAQM10AuditError("ba_qm10_requires_ba_qm9_complete")

    daily = validate_daily_research(root)
    snapshot_id = str(daily.get("snapshot_id") or "")
    w10 = validate_sealed_manifest(
        _read_json(root / "artifacts" / "research" / "decision_snapshot_w10.json"),
        expected_snapshot_id=snapshot_id,
    )

    scanner = _workflow(root, "run_scanner.yml")
    phase2 = _workflow(root, "probability_calibration_2.yml")
    decision = _workflow(root, "decision_watch_pipeline.yml")
    runtime = _workflow(root, "decision_watch_runtime.yml")
    symbols = _workflow(root, "decision_watch_symbol_views.yml")

    scanner_text = (root / ".github" / "workflows" / "run_scanner.yml").read_text(encoding="utf-8")
    decision_text = (root / ".github" / "workflows" / "decision_watch_pipeline.yml").read_text(encoding="utf-8")
    runtime_text = (root / ".github" / "workflows" / "decision_watch_runtime.yml").read_text(encoding="utf-8")
    symbols_text = (root / ".github" / "workflows" / "decision_watch_symbol_views.yml").read_text(encoding="utf-8")
    phase2_text = (root / ".github" / "workflows" / "probability_calibration_2.yml").read_text(encoding="utf-8")

    scanner_on = _mapping(scanner.get("on"), "scanner.on")
    scanner_schedules = scanner_on.get("schedule")
    if not isinstance(scanner_schedules, list) or len(scanner_schedules) != 3:
        raise BAQM10AuditError("scanner_retry_schedule_invalid")
    scanner_concurrency = _mapping(scanner.get("concurrency"), "scanner.concurrency")
    if scanner_concurrency.get("cancel-in-progress") != "false":
        raise BAQM10AuditError("scanner_concurrency_must_serialize_without_cancel")

    provenance = daily.get("scanner_input_provenance")
    if not isinstance(provenance, Mapping) or provenance.get("complete") is not True:
        raise BAQM10AuditError("scanner_provenance_not_complete")
    if provenance.get("historical_backfill") is not False:
        raise BAQM10AuditError("scanner_provenance_historical_backfill_forbidden")

    decision_on = _mapping(decision.get("on"), "decision.on")
    decision_workflow_run = _mapping(decision_on.get("workflow_run"), "decision.workflow_run")
    decision_upstreams = tuple(decision_workflow_run.get("workflows") or ())
    expected_decision_upstreams = (
        "Phase 2 Probability Calibration",
        "Phase 5E Adaptive Shadow and Promotion",
        "Module 6 Elliott Prospective Shadow",
    )
    if decision_upstreams != expected_decision_upstreams:
        raise BAQM10AuditError("decision_upstream_trigger_set_changed")

    runtime_on = _mapping(runtime.get("on"), "runtime.on")
    runtime_workflow_run = _mapping(runtime_on.get("workflow_run"), "runtime.workflow_run")
    if tuple(runtime_workflow_run.get("workflows") or ()) != ("Decision Watch Integration Pipeline",):
        raise BAQM10AuditError("runtime_must_follow_decision_integration")

    symbol_on = _mapping(symbols.get("on"), "symbols.on")
    symbol_workflow_run = _mapping(symbol_on.get("workflow_run"), "symbols.workflow_run")
    symbol_upstreams = tuple(symbol_workflow_run.get("workflows") or ())
    if symbol_upstreams != (
        "Decision Watch Runtime Publish",
        "Module 6 Elliott Prospective Shadow",
    ):
        raise BAQM10AuditError("symbol_view_upstream_trigger_set_changed")

    phase2_concurrency = _mapping(phase2.get("concurrency"), "phase2.concurrency")
    if phase2_concurrency.get("cancel-in-progress") != "true":
        raise BAQM10AuditError("phase2_latest_snapshot_concurrency_guard_missing")

    runtime_manifest_path = root / "artifacts" / "research" / "watch_runtime" / "manifest.json"
    runtime_snapshot_id = None
    runtime_current = False
    if runtime_manifest_path.exists():
        runtime_manifest = _read_json(runtime_manifest_path)
        runtime_snapshot_id = str(runtime_manifest.get("snapshot_id") or "")
        runtime_current = runtime_snapshot_id == snapshot_id

    static = {
        "scanner_three_retry_slots": True,
        "scanner_serialized": True,
        "scanner_provenance_required": "SCANNER_REQUIRE_PROVENANCE: '1'" in scanner_text,
        "scanner_records_success_marker_only_after_validation": (
            scanner_text.find("Research-Kohaerenz vor Veroeffentlichung pruefen")
            < scanner_text.find("autorun_state.py --record")
            < scanner_text.find("git add artifacts/")
        ),
        "scanner_incomplete_run_exits_failure": "Scannerlauf unvollstaendig" in scanner_text,
        "scanner_explicit_main_advance_refusal_guard": "main advanced" in scanner_text,
        "phase2_fallback_schedule_present": "cron: '45 22 * * *'" in phase2_text,
        "phase2_freshness_gate_present": "Decide whether calibration is stale" in phase2_text,
        "decision_same_snapshot_guard_present": "--require-snapshot-match" in decision_text,
        "decision_stale_publication_refusal_present": "refuse stale current publication" in decision_text,
        "decision_any_single_upstream_can_trigger": len(decision_upstreams) == 3,
        "runtime_validates_sealed_w10": "Validate sealed W10 and authoritative scanner snapshot" in runtime_text,
        "runtime_stale_publication_refusal_present": "refuse stale publication" in runtime_text,
        "runtime_hard_two_mb_shard_limit_present": "max_size < 2_000_000" in runtime_text,
        "symbol_views_validate_runtime_identity": "Validate symbol-view identity and privacy" in symbols_text,
        "symbol_views_can_trigger_on_elliott_before_runtime": "Module 6 Elliott Prospective Shadow" in symbol_upstreams,
    }

    findings = {row["finding_id"]: row["state"] for row in contract["findings"]}
    check_status = dict(contract["current_assessment"])

    return {
        "schema_version": "ba_qm10_current_operations_audit_v1",
        "status": "OPEN_FINDINGS_CAPA_REQUIRED",
        "scope": "QUALITY_MANAGEMENT_ONLY",
        "snapshot_id": snapshot_id,
        "snapshot_as_of": daily.get("as_of"),
        "w10_status": w10.get("status"),
        "runtime_snapshot_id": runtime_snapshot_id,
        "runtime_matches_current_snapshot": runtime_current,
        "required_check_count": len(EXPECTED_CHECKS),
        "check_status": check_status,
        "static_guards": static,
        "open_findings": findings,
        "open_finding_count": len(findings),
        "scanner_publication_race_risk_open": not static["scanner_explicit_main_advance_refusal_guard"],
        "false_decision_observed": False,
        "all_observed_failures_fail_closed": True,
        "research_logic_changed": False,
        "decision_logic_changed": False,
        "investment_logic_changed": False,
        "execution_enabled": False,
        "lag1_evidence_impact": "PROMOTION_BLOCKED",
        "ba_qm10_may_release_lag1_block": False,
        "closure_eligible": False,
    }
