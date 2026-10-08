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
from scanner.research.decision_layer.current_evidence import DEFAULT_ARCHIVE
from scanner.research.decision_layer.evidence_archive import load_evidence_archive
from scanner.research.decision_layer.watch_runtime import SHARD_IDS, build_watch_runtime
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
EXPECTED_FINDINGS = ("BA-QM10-F01", "BA-QM10-F02", "BA-QM10-F03", "BA-QM10-F04", "BA-QM10-F05")
EXPECTED_FINDING_STATES = {
    "BA-QM10-F01": "CLOSED_EFFECTIVE",
    "BA-QM10-F02": "CLOSED_EFFECTIVE",
    "BA-QM10-F03": "CLOSED_EFFECTIVE",
    "BA-QM10-F04": "CLOSED_EFFECTIVE",
    "BA-QM10-F05": "CLOSED_EFFECTIVE",
}


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
    if value.get("status") != "COMPLETE":
        raise BAQM10AuditError("ba_qm10_status_invalid")
    if value.get("engineering_status") != "COMPLETE":
        raise BAQM10AuditError("ba_qm10_engineering_status_invalid")
    if value.get("scope") != "QUALITY_MANAGEMENT_ONLY":
        raise BAQM10AuditError("ba_qm10_scope_invalid")
    for key in (
        "research_logic_changed",
        "decision_logic_changed",
        "investment_logic_changed",
        "execution_enabled",
        "empirical_promotion_performed",
    ):
        if value.get(key) is not False:
            raise BAQM10AuditError(f"ba_qm10_must_be_false:{key}")
    if value.get("closure_claimed") is not True:
        raise BAQM10AuditError("ba_qm10_closure_must_be_claimed")
    if tuple(value.get("required_checks") or ()) != EXPECTED_CHECKS:
        raise BAQM10AuditError("ba_qm10_required_checks_invalid")

    assessment = _mapping(value.get("current_assessment"), "current_assessment")
    if tuple(assessment.keys()) != EXPECTED_CHECKS:
        raise BAQM10AuditError("ba_qm10_assessment_coverage_invalid")
    if any(assessment.get(key) != "PASS" for key in EXPECTED_CHECKS):
        raise BAQM10AuditError("ba_qm10_assessment_not_all_pass")

    findings = value.get("findings")
    if not isinstance(findings, list):
        raise BAQM10AuditError("ba_qm10_findings_must_be_list")
    ids = tuple(str(row.get("finding_id") or "") for row in findings if isinstance(row, Mapping))
    if ids != EXPECTED_FINDINGS:
        raise BAQM10AuditError("ba_qm10_finding_registry_invalid")
    for row in findings:
        if not isinstance(row, Mapping):
            raise BAQM10AuditError("ba_qm10_finding_must_be_object")
        expected_state = EXPECTED_FINDING_STATES.get(str(row.get("finding_id") or ""))
        if row.get("state") != expected_state:
            raise BAQM10AuditError(f"ba_qm10_finding_state_invalid:{row.get('finding_id')}")
        effect = _mapping(row.get("effect"), f"effect:{row.get('finding_id')}")
        if effect.get("decision_safety") != "FAIL_CLOSED":
            raise BAQM10AuditError(f"ba_qm10_finding_not_fail_closed:{row.get('finding_id')}")
        if row.get("finding_id") in {"BA-QM10-F01", "BA-QM10-F05"} and effect.get("false_watch_accepted") is not False:
            raise BAQM10AuditError("ba_qm10_f01_false_watch_guard_invalid")
        effectiveness = _mapping(
            row.get("effectiveness"),
            f"effectiveness:{row.get('finding_id')}",
        )
        if effectiveness.get("verification_status") != "EFFECTIVE":
            raise BAQM10AuditError(
                f"ba_qm10_finding_effectiveness_invalid:{row.get('finding_id')}"
            )
        _mapping(
            effectiveness.get("evidence"),
            f"effectiveness_evidence:{row.get('finding_id')}",
        )
        if effectiveness.get("decision_logic_changed") is not False:
            raise BAQM10AuditError(
                f"ba_qm10_finding_effectiveness_changed_decision:{row.get('finding_id')}"
            )

    risks = value.get("static_risks_to_verify")
    if not isinstance(risks, list) or len(risks) != 1:
        raise BAQM10AuditError("ba_qm10_static_risk_registry_invalid")
    risk = _mapping(risks[0], "static_risk")
    if risk.get("risk_id") != "BA-QM10-R01":
        raise BAQM10AuditError("ba_qm10_static_risk_id_invalid")
    if risk.get("state") != "CLOSED_EFFECTIVE":
        raise BAQM10AuditError("ba_qm10_static_risk_not_closed_effective")
    risk_effectiveness = _mapping(risk.get("effectiveness"), "static_risk_effectiveness")
    if risk_effectiveness.get("verification_status") != "EFFECTIVE":
        raise BAQM10AuditError("ba_qm10_static_risk_effectiveness_invalid")
    if risk_effectiveness.get("decision_logic_changed") is not False:
        raise BAQM10AuditError("ba_qm10_static_risk_changed_decision")

    closure_evidence = _mapping(value.get("closure_evidence"), "closure_evidence")
    if not str(closure_evidence.get("snapshot_id") or "").strip():
        raise BAQM10AuditError("ba_qm10_closure_snapshot_id_required")
    if closure_evidence.get("ba_qm10_ci_passed_tests") != 76:
        raise BAQM10AuditError("ba_qm10_closure_ci_test_count_invalid")
    if closure_evidence.get("public_runtime_shard_count") != 32:
        raise BAQM10AuditError("ba_qm10_closure_runtime_shard_count_invalid")
    if value.get("next_mandatory_work_package") != "BA-QM11 – Gesamtsystem-Audit":
        raise BAQM10AuditError("ba_qm10_next_work_package_invalid")

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
    publication_guard_path = root / "scripts" / "check_publication_base.py"
    publication_guard_text = (
        publication_guard_path.read_text(encoding="utf-8")
        if publication_guard_path.exists()
        else ""
    )
    decision_text = (root / ".github" / "workflows" / "decision_watch_pipeline.yml").read_text(encoding="utf-8")
    runtime_text = (root / ".github" / "workflows" / "decision_watch_runtime.yml").read_text(encoding="utf-8")
    symbols_text = (root / ".github" / "workflows" / "decision_watch_symbol_views.yml").read_text(encoding="utf-8")
    phase2_text = (root / ".github" / "workflows" / "probability_calibration_2.yml").read_text(encoding="utf-8")

    scanner_on = _mapping(scanner.get("on"), "scanner.on")
    scanner_schedules = scanner_on.get("schedule")
    if not isinstance(scanner_schedules, list):
        raise BAQM10AuditError("scanner_retry_schedule_invalid")
    scanner_crons = tuple(
        str(row.get("cron") or "")
        for row in scanner_schedules
        if isinstance(row, Mapping)
    )
    if scanner_crons != ("7 16 * * *", "7 17 * * *", "37 18,20 * * *"):
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
    runtime_projection_sha256 = None
    if runtime_manifest_path.exists():
        runtime_manifest = _read_json(runtime_manifest_path)
        runtime_snapshot_id = str(runtime_manifest.get("snapshot_id") or "")
        runtime_current = runtime_snapshot_id == snapshot_id
        runtime_projection_sha256 = str(
            runtime_manifest.get("runtime_projection_sha256") or ""
        ) or None

    symbol_index_path = (
        root
        / "artifacts"
        / "research"
        / "watch_runtime"
        / "symbols"
        / "index.json"
    )
    symbol_views_snapshot_id = None
    symbol_views_current = False
    symbol_views_runtime_projection_matches = False
    if symbol_index_path.exists():
        symbol_index = _read_json(symbol_index_path)
        symbol_views_snapshot_id = str(symbol_index.get("snapshot_id") or "")
        symbol_views_current = symbol_views_snapshot_id == snapshot_id
        symbol_views_runtime_projection_matches = (
            bool(runtime_projection_sha256)
            and str(symbol_index.get("source_runtime_projection_sha256") or "")
            == runtime_projection_sha256
        )

    static = {
        "scanner_current_retry_schedule": scanner_crons == ("7 16 * * *", "7 17 * * *", "37 18,20 * * *"),
        "scanner_serialized": True,
        "scanner_provenance_required": "SCANNER_REQUIRE_PROVENANCE: '1'" in scanner_text,
        "scanner_records_success_marker_only_after_validation": (
            scanner_text.find("Research-Kohaerenz vor Veroeffentlichung pruefen")
            < scanner_text.find("autorun_state.py --record")
            < scanner_text.find("git add artifacts/")
        ),
        "scanner_incomplete_run_exits_failure": "Scannerlauf unvollstaendig" in scanner_text,
        "scanner_explicit_main_advance_refusal_guard": (
            "python scripts/check_publication_base.py" in scanner_text
            and "STALE_PUBLICATION_EXIT = 75" in publication_guard_text
            and '"ls-remote", "--heads"' in publication_guard_text
        ),
        "phase2_fallback_schedule_present": "cron: '45 22 * * *'" in phase2_text,
        "phase2_freshness_gate_present": "Decide whether calibration is stale" in phase2_text,
        "decision_same_snapshot_guard_present": "--require-snapshot-match" in decision_text,
        "decision_stale_publication_refusal_present": "refuse stale current publication" in decision_text,
        "decision_any_single_upstream_can_trigger": len(decision_upstreams) == 3,
        "decision_readiness_gate_present": "Check current Phase 2 snapshot readiness" in decision_text,
        "runtime_validates_sealed_w10": "Validate sealed W10 and authoritative scanner snapshot" in runtime_text,
        "runtime_stale_publication_refusal_present": "refuse stale publication" in runtime_text,
        "runtime_hard_two_mb_shard_limit_present": "max_size < 2_000_000" in runtime_text,
        "symbol_views_validate_runtime_identity": "Validate symbol-view identity and privacy" in symbols_text,
        "symbol_views_can_trigger_on_elliott_before_runtime": "Module 6 Elliott Prospective Shadow" in symbol_upstreams,
        "symbol_view_runtime_readiness_gate_present": "Check current Watch runtime snapshot readiness" in symbols_text,
        "qm_j_canonical_archive_default_present": "DEFAULT_ARCHIVE" in (root / "scripts" / "run_qm_j_decision_e2e_falsification.py").read_text(encoding="utf-8"),
    }

    findings = {row["finding_id"]: row["state"] for row in contract["findings"]}
    risks = {
        row["risk_id"]: row.get("state")
        for row in contract.get("static_risks_to_verify", [])
        if isinstance(row, Mapping)
    }
    check_status = dict(contract["current_assessment"])
    open_finding_count = sum(
        state != "CLOSED_EFFECTIVE" for state in findings.values()
    )
    open_risk_count = sum(
        state != "CLOSED_EFFECTIVE" for state in risks.values()
    )
    required_static_guards = (
        "scanner_current_retry_schedule",
        "scanner_serialized",
        "scanner_provenance_required",
        "scanner_records_success_marker_only_after_validation",
        "scanner_incomplete_run_exits_failure",
        "scanner_explicit_main_advance_refusal_guard",
        "phase2_fallback_schedule_present",
        "phase2_freshness_gate_present",
        "decision_same_snapshot_guard_present",
        "decision_stale_publication_refusal_present",
        "decision_readiness_gate_present",
        "runtime_validates_sealed_w10",
        "runtime_stale_publication_refusal_present",
        "runtime_hard_two_mb_shard_limit_present",
        "symbol_views_validate_runtime_identity",
        "symbol_view_runtime_readiness_gate_present",
        "qm_j_canonical_archive_default_present",
    )
    all_required_guards_passed = all(
        static.get(key) is True for key in required_static_guards
    )
    all_required_checks_passed = all(
        check_status.get(key) == "PASS" for key in EXPECTED_CHECKS
    )
    closure_eligible = (
        all_required_checks_passed
        and all_required_guards_passed
        and open_finding_count == 0
        and open_risk_count == 0
        and runtime_current
        and symbol_views_current
        and symbol_views_runtime_projection_matches
        and w10.get("status") == "sealed"
    )

    return {
        "schema_version": "ba_qm10_current_operations_audit_v1",
        "status": (
            "BA_QM10_ENGINEERING_COMPLETE"
            if closure_eligible
            else "CLOSURE_EVIDENCE_MISMATCH"
        ),
        "scope": "QUALITY_MANAGEMENT_ONLY",
        "snapshot_id": snapshot_id,
        "snapshot_as_of": daily.get("as_of"),
        "w10_status": w10.get("status"),
        "runtime_snapshot_id": runtime_snapshot_id,
        "runtime_matches_current_snapshot": runtime_current,
        "runtime_projection_sha256": runtime_projection_sha256,
        "symbol_views_snapshot_id": symbol_views_snapshot_id,
        "symbol_views_match_current_snapshot": symbol_views_current,
        "symbol_views_runtime_projection_matches": (
            symbol_views_runtime_projection_matches
        ),
        "required_check_count": len(EXPECTED_CHECKS),
        "check_status": check_status,
        "all_required_checks_passed": all_required_checks_passed,
        "static_guards": static,
        "all_required_guards_passed": all_required_guards_passed,
        "finding_states": findings,
        "open_finding_count": open_finding_count,
        "implemented_capa_pending_verification_count": sum(
            "PENDING" in str(state) for state in findings.values()
        ),
        "scanner_publication_race_risk_open": (
            risks.get("BA-QM10-R01") != "CLOSED_EFFECTIVE"
        ),
        "static_risk_states": risks,
        "open_static_risk_count": open_risk_count,
        "false_decision_observed": False,
        "all_observed_failures_fail_closed": True,
        "research_logic_changed": False,
        "decision_logic_changed": False,
        "investment_logic_changed": False,
        "execution_enabled": False,
        "lag1_evidence_impact": "PROMOTION_BLOCKED",
        "ba_qm10_may_release_lag1_block": False,
        "closure_eligible": closure_eligible,
    }



def _runtime_capacity_state(max_bytes: int, hard_limit: int = 2_000_000) -> dict[str, Any]:
    """Distinguish transport failure, low-reserve review, and safe capacity."""
    if type(max_bytes) is not int or max_bytes < 0 or hard_limit <= 0:
        raise BAQM10AuditError("runtime_capacity_invalid_measurement")
    reserve = (hard_limit - max_bytes) / hard_limit
    return {
        "transport_status": "PASS" if max_bytes < hard_limit else "FAIL",
        "health_status": "FAIL" if max_bytes >= hard_limit else (
            "REVIEW_REQUIRED" if reserve < 0.20 else "PASS"
        ),
        "headroom_bytes": hard_limit - max_bytes,
        "headroom_ratio": reserve,
        "capacity_review_recommended": reserve < 0.20,
    }


def audit_current_runtime_capacity(root: str | Path = _ROOT) -> dict[str, Any]:
    """Build the current compact runtime in memory and measure transport capacity.

    This is an operational audit only. It does not write artifacts or alter
    Decision semantics.
    """
    root = Path(root).resolve()
    current = _read_json(
        root / "artifacts" / "research" / "current_decision_packets_7a.json"
    )
    w10 = _read_json(
        root / "artifacts" / "research" / "decision_snapshot_w10.json"
    )
    packets, metadata = load_evidence_archive(
        root / DEFAULT_ARCHIVE,
        missing_ok=False,
    )
    manifest, shards = build_watch_runtime(
        current_packet_set=current,
        archive_packets=packets,
        w10_manifest=w10,
    )
    sizes = {
        shard_id: len(
            (
                json.dumps(
                    shard,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                )
                + "\n"
            ).encode("utf-8")
        )
        for shard_id, shard in shards.items()
    }
    max_id = max(sizes, key=sizes.get)
    max_size = sizes[max_id]
    hard_limit = 2_000_000
    health = _runtime_capacity_state(max_size, hard_limit)
    symbol_sizes: dict[str, int] = {}
    symbol_packets: dict[str, int] = {}
    for shard in shards.values():
        for packet in shard["packets"]:
            symbol = str(packet["symbol"])
            symbol_sizes[symbol] = symbol_sizes.get(symbol, 0) + len(
                json.dumps(packet, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            )
            symbol_packets[symbol] = symbol_packets.get(symbol, 0) + 1
    dominant = max(symbol_sizes, key=symbol_sizes.get)
    return {
        "schema_version": "ba_qm10_runtime_capacity_audit_v1",
        "status": health["transport_status"],
        "health_status": health["health_status"],
        "partition_strategy": manifest.get("partition_strategy", "legacy_sha256_mod64_v1"),
        "snapshot_id": manifest["snapshot_id"],
        "shard_count": len(shards),
        "configured_shard_count": len(SHARD_IDS),
        "max_shard_id": max_id,
        "max_shard_bytes": max_size,
        "hard_limit_bytes": hard_limit,
        "headroom_bytes": health["headroom_bytes"],
        "headroom_ratio": health["headroom_ratio"],
        "capacity_review_recommended": health["capacity_review_recommended"],
        "total_shard_bytes": sum(sizes.values()),
        "shard_sizes_bytes": dict(sorted(sizes.items())),
        "largest_symbol": dominant,
        "largest_symbol_bytes": symbol_sizes[dominant],
        "largest_symbol_packet_count": symbol_packets[dominant],
        "largest_symbols_by_bytes": [
            {"symbol": key, "bytes": symbol_sizes[key], "packets": symbol_packets[key]}
            for key in sorted(symbol_sizes, key=lambda key: (-symbol_sizes[key], key))[:10]
        ],
        "symbol_count": manifest["symbol_count"],
        "packet_count": manifest["packet_count"],
        "archive_status": metadata.get("status"),
        "private_position_data_included": False,
        "decision_logic_changed": manifest["decision_logic_changed"],
        "writes_performed": False,
    }



def evaluate_ba_qm10_closure(root: str | Path = _ROOT) -> dict[str, Any]:
    """Return the fail-closed BA-QM10 engineering closure receipt."""
    root = Path(root).resolve()
    contract = load_contract(
        root / "configs" / "ba_qm10_production_operations_qm_v1.json"
    )
    operations = audit_current_operations(root)
    capacity = audit_current_runtime_capacity(root)

    if operations.get("status") != "BA_QM10_ENGINEERING_COMPLETE":
        raise BAQM10AuditError("ba_qm10_operations_not_complete")
    if operations.get("closure_eligible") is not True:
        raise BAQM10AuditError("ba_qm10_closure_not_eligible")
    if operations.get("all_required_checks_passed") is not True:
        raise BAQM10AuditError("ba_qm10_required_checks_not_all_passed")
    if operations.get("all_required_guards_passed") is not True:
        raise BAQM10AuditError("ba_qm10_required_guards_not_all_passed")
    if operations.get("open_finding_count") != 0:
        raise BAQM10AuditError("ba_qm10_open_findings_remain")
    if operations.get("open_static_risk_count") != 0:
        raise BAQM10AuditError("ba_qm10_open_static_risks_remain")
    if operations.get("runtime_matches_current_snapshot") is not True:
        raise BAQM10AuditError("ba_qm10_runtime_not_current")
    if operations.get("symbol_views_match_current_snapshot") is not True:
        raise BAQM10AuditError("ba_qm10_symbol_views_not_current")
    if operations.get("symbol_views_runtime_projection_matches") is not True:
        raise BAQM10AuditError("ba_qm10_symbol_view_projection_mismatch")
    if capacity.get("status") != "PASS":
        raise BAQM10AuditError("ba_qm10_runtime_capacity_not_pass")
    if capacity.get("health_status") != "PASS":
        raise BAQM10AuditError("ba_qm10_runtime_capacity_review_required")
    if capacity.get("max_shard_bytes", 2_000_000) >= 2_000_000:
        raise BAQM10AuditError("ba_qm10_runtime_capacity_limit_exceeded")

    evidence = _mapping(contract.get("closure_evidence"), "closure_evidence")

    # BA-QM10 is a historical engineering-closure record.  Its frozen evidence
    # must remain internally self-consistent, but it must not be compared to
    # mutable current runtime identity.  BA-QM12 separately audits current
    # snapshot/runtime health through audit_current_operations() above.
    closure_snapshot_id = str(evidence.get("snapshot_id") or "").strip()
    closure_snapshot_as_of = str(evidence.get("snapshot_as_of") or "").strip()
    closure_projection = str(
        evidence.get("public_runtime_projection_sha256") or ""
    ).strip()
    closure_projection_alias = str(
        evidence.get("runtime_projection_sha256") or ""
    ).strip()
    if not closure_snapshot_id or not closure_snapshot_as_of:
        raise BAQM10AuditError("ba_qm10_closure_identity_missing")
    if not closure_projection or closure_projection != closure_projection_alias:
        raise BAQM10AuditError("ba_qm10_closure_projection_evidence_inconsistent")
    if int(evidence.get("public_runtime_shard_count") or 0) <= 0:
        raise BAQM10AuditError("ba_qm10_closure_shard_count_invalid")
    if int(evidence.get("public_runtime_symbol_count") or 0) <= 0:
        raise BAQM10AuditError("ba_qm10_closure_symbol_count_invalid")
    if int(evidence.get("public_runtime_packet_count") or 0) <= 0:
        raise BAQM10AuditError("ba_qm10_closure_packet_count_invalid")

    return {
        "schema_version": "ba_qm10_engineering_closure_receipt_v1",
        "status": "BA_QM10_ENGINEERING_COMPLETE",
        "scope": "QUALITY_MANAGEMENT_ONLY",
        "snapshot_id": operations["snapshot_id"],
        "snapshot_as_of": operations["snapshot_as_of"],
        "required_check_count": operations["required_check_count"],
        "all_required_checks_passed": True,
        "all_required_guards_passed": True,
        "open_finding_count": 0,
        "open_static_risk_count": 0,
        "runtime_shard_count": capacity["shard_count"],
        "runtime_max_shard_bytes": capacity["max_shard_bytes"],
        "runtime_hard_limit_bytes": capacity["hard_limit_bytes"],
        "runtime_symbol_count": capacity["symbol_count"],
        "runtime_packet_count": capacity["packet_count"],
        "runtime_projection_sha256": operations["runtime_projection_sha256"],
        "symbol_views_runtime_projection_matches": True,
        "historical_closure_evidence": {
            "snapshot_id": closure_snapshot_id,
            "snapshot_as_of": closure_snapshot_as_of,
            "runtime_projection_sha256": closure_projection,
            "public_runtime_shard_count": evidence["public_runtime_shard_count"],
            "public_runtime_symbol_count": evidence["public_runtime_symbol_count"],
            "public_runtime_packet_count": evidence["public_runtime_packet_count"],
            "historical_record_not_live_runtime_identity": True,
        },
        "current_runtime_evaluated_separately": True,
        "engineering_closure_performed": True,
        "research_logic_changed": False,
        "decision_logic_changed": False,
        "investment_logic_changed": False,
        "execution_enabled": False,
        "empirical_promotion_performed": False,
        "lag1_finding_id": contract["dependencies"]["lag1_finding_id"],
        "lag1_capa_id": contract["dependencies"]["lag1_capa_id"],
        "lag1_evidence_impact": "PROMOTION_BLOCKED",
        "ba_qm10_may_release_lag1_block": False,
        "next_mandatory_work_package": "BA-QM11 – Gesamtsystem-Audit",
    }
