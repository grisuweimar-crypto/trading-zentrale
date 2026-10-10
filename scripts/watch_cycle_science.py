"""CYCLE-DIR scientific evidence watchdog. READ ONLY, never research promotion.

A dashboard of verified *technical* observations and explicitly missing scientific
proof. The CY-03 v1 archive can NEVER become a released sample by changing
an issue state or a manifest count; that needs a reviewed new release contract.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from scanner.research.pattern_discovery.cycle_cy05_adapter import inspect_cycle_archive

ROOT = "artifacts/research/pattern_discovery"
REQUIRED_LAGS = ("1", "5", "10")
PREREG_PATH = "configs/cycle_direction/cy05_preregistered_design_v1.json"
ORIGINAL_SNAPSHOT = "f409c312-0349-4b29-8dab-3ecdbd9463b3"


def _json(root: Path, name: str) -> dict[str, Any]:
    path = root / name
    if not path.is_file() or path.is_symlink():
        raise ValueError("missing_or_symlink:" + name)
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("not_object:" + name)
    return value


def _count(path: Path, pattern: str) -> int:
    if not path.is_dir():
        return 0
    # Count only names, never ingest these files as scientifically trustworthy.
    return sum(1 for entry in path.glob(pattern) if entry.is_file() and not entry.is_symlink())


def audit_scientific_readiness(
    root: str | Path, *, observed_at: datetime | None = None,
    issue_269_state: str = "UNKNOWN",
) -> dict[str, Any]:
    """Fail closed on archive integrity; no automatic research release."""
    repo = Path(root)
    utc = observed_at or datetime.now(timezone.utc)
    if utc.tzinfo is None:
        raise ValueError("observed_at_timezone_required")
    utc = utc.astimezone(timezone.utc)
    issue = str(issue_269_state).upper()
    if issue not in {"OPEN", "CLOSED", "UNKNOWN"}:
        raise ValueError("invalid_issue_state")

    report: dict[str, Any] = {
        "schema_version": "cycle_dir_science_watch_v1",
        "plan_id": "CYCLE-DIR-2026-10-09-v1",
        "checked_at_utc": utc.isoformat().replace("+00:00", "Z"),
        "research_only": True,
        "read_only": True,
        "research_release_performed": False,
        "discovery_performed": False,
        "promotion_performed": False,
        "production_effect": False,
        "issue_269_state": issue,
        "gates": {},
        "observations": {},
        "blockers": [],
        "alerts": [],
        "status": "BLOCKED",
    }
    gates = report["gates"]
    blockers = report["blockers"]
    alerts = report["alerts"]

    try:
        inspection = inspect_cycle_archive(repo)  # audits ledger, CSV/mask/coverage and every archived bar hash
    except Exception as exc:
        # Integrity exceptions are not waived because upstream job is green.
        report["status"] = "INTEGRITY_FAILURE"
        report["integrity_error"] = type(exc).__name__ + ": " + str(exc)[:300]
        blockers.append("CY03_ARCHIVE_INTEGRITY_FAILURE")
        gates["CY03_internal_archive"] = "FAIL"
        return report

    gates["CY03_internal_archive"] = "PASS_INTERNAL_ONLY"
    report["observations"].update({
        "snapshot_count": inspection["snapshots"],
        "observation_count": inspection["observations"],
        "first_observation_as_of": None,
        "latest_as_of": inspection["as_of"],
        "internally_replayable_rows": inspection["internally_replayable"],
        "excluded_rows": inspection["excluded"],
        "lag_status_counts": inspection["lag_status_counts"],
        "research_eligible_count": inspection["research_eligible"],
        "external_pit_verified": inspection["independent_external_pit_verified"],
    })
    from scanner.reports.cycle_history import ROOT as CYCLE_ROOT, read_ledger
    ledger = read_ledger(repo / CYCLE_ROOT / "observations.csv")
    snapshot_ids = {row["snapshot_id"] for row in ledger}
    if ORIGINAL_SNAPSHOT not in snapshot_ids:
        report["status"] = "INTEGRITY_FAILURE"
        blockers.append("CY03_ORIGINAL_SNAPSHOT_REMOVED")
        gates["CY03_original_anchor"] = "FAIL"
        return report
    report["observations"]["first_observation_as_of"] = min(row["as_of"] for row in ledger)
    days = sorted({row["as_of"] for row in ledger})
    report["observations"]["distinct_scanner_dates"] = len(days)
    report["observations"]["original_snapshot_retained"] = True
    gates["CY03_original_anchor"] = "PASS"

    today = utc.astimezone(ZoneInfo("Europe/Berlin")).date()
    from datetime import date
    last = date.fromisoformat(inspection["as_of"])
    age = (today - last).days
    report["observations"]["age_calendar_days"] = age
    if age < 0:
        report["status"] = "INTEGRITY_FAILURE"
        blockers.append("CY03_AS_OF_IN_FUTURE")
        gates["CY03_freshness"] = "FAIL"
        return report
    gates["CY03_freshness"] = "RECENT" if age <= 3 else "STALE"
    if age > 3:
        alerts.append("CY03_NO_RECENT_ARCHIVE_PUBLICATION_OVER_3_DAYS")

    # Explicitly NOT a research verification. The v1 research authority is
    # hard-blocked in both cycle_history and the CY-05 adapter.
    gates["CY02_external_source_PIT"] = "NOT_VERIFIED"
    blockers.append("CY02_ISSUE_269_EXTERNAL_PROVIDER_PIT_NOT_VERIFIED")
    blockers.append("CY03_V1_HAS_NO_RESEARCH_RELEASE_CONTRACT")
    if issue == "CLOSED":
        alerts.append("ISSUE_269_CLOSED_BUT_SOURCE_PIT_RELEASE_NOT_CERTIFIED")
    elif issue == "UNKNOWN":
        alerts.append("ISSUE_269_STATE_UNAVAILABLE")
    for lag in REQUIRED_LAGS:
        n = inspection["lag_status_counts"][lag].get("PROVISIONAL_CHAIN", 0)
        report["observations"]["provisional_lag_" + lag + "obs"] = n
        gates["CY03_lag_" + lag + "obs"] = "PROVISIONAL_ONLY" if n > 0 else "NO_CHAIN"
        if n == 0:
            blockers.append("CY03_NO_" + lag + "OBS_CHAIN")
    if inspection["research_eligible"] != 0 or inspection["research_released"] is not False:
        report["status"] = "INTEGRITY_FAILURE"
        blockers.append("CY03_V1_FALSE_RESEARCH_RELEASE")
        return report
    gates["CY03_research_release"] = "BLOCKED_BY_VERSIONED_V1_CONTRACT"

    design = _json(repo, PREREG_PATH)
    if (design.get("schema_version") != "cycle_direction_cy05_research_design_v1"
            or design.get("execution_allowed") is not False
            or design.get("research_only") is not True):
        report["status"] = "INTEGRITY_FAILURE"
        blockers.append("CY05_DESIGN_CONTRACT_INVALID")
        return report
    freeze = design.get("evidence_status") or {}
    if freeze.get("frozen_l1_run_manifest_created") is True:
        alerts.append("CY05_DESIGN_FLAG_ALONE_CANNOT_PROVE_REAL_L1_FREEZE")
    manifests = _count(repo / ROOT / "discovery_runs", "*/manifest.json")
    freezes = _count(repo / ROOT / "discovery_runs", "*/l5_freeze_snapshot.json")
    gates["CY05_L1_real_run_freeze"] = "MISSING" if not manifests else "ARTIFACTS_REQUIRE_PIT_AUDIT"
    gates["CY06_L5_candidate_freeze"] = "MISSING" if not freezes else "ARTIFACTS_REQUIRE_CYCLE_BINDING_REVIEW"
    report["observations"]["l1_run_manifest_files_unverified"] = manifests
    report["observations"]["l5_freeze_files_unverified"] = freezes
    if not manifests:
        blockers.append("CY05_NO_ACTUAL_L1_FROZEN_PRE_RUN")
    if not freezes:
        blockers.append("CY06_NO_QUALIFIED_L5_CYCLE_PATTERN")

    traces = [
        ("CY07_L7_prospective_claims", "prospective_claims.jsonl"),
        ("CY07_L8_matured_outcomes", "matured_outcomes.jsonl"),
        ("CY07_L9_confirmation", "confirmation_looks.jsonl"),
        ("CY07_L10_rating", "pattern_rating_history.jsonl"),
        ("CY08_L12_promotion", "promotion/promotion_registry.jsonl"),
    ]
    for stage, filename in traces:
        file = repo / ROOT / filename
        exists = file.is_file() and not file.is_symlink() and file.stat().st_size > 0
        gates[stage] = "NONE" if not exists else "ARTIFACT_PRESENT_NOT_CYCLE_CERTIFIED"
        if exists:
            alerts.append(stage + "_NEEDS_EXACT_PATTERN_AND_PROSPECTIVE_VERIFICATION")
    # L14 generic READY_WITH_WORK cannot be a Cycle-Pattern admission.
    try:
        l14 = _json(repo, ROOT + "/operations/latest.json")
        report["observations"]["generic_l14_status"] = l14.get("cycle_status")
    except (OSError, ValueError, json.JSONDecodeError):
        report["observations"]["generic_l14_status"] = "NOT_AVAILABLE"
    gates["CY08_L12_reviewer_approval"] = "NOT_CYCLE_CERTIFIED"
    gates["CY08_L13_shadow_net_evaluation"] = "NOT_EVALUATED"
    blockers.extend([
        "CY07_NO_CERTIFIED_PROSPECTIVE_L9_SUPPORTED_L10_A_OR_B",
        "CY08_NO_EXPLICIT_L12_REVIEW_FOR_EXACT_CYCLE_PATTERN",
    ])
    report["blockers"] = sorted(set(blockers))
    report["alerts"] = sorted(set(alerts))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--issue-269-state", default="UNKNOWN", choices=("OPEN", "CLOSED", "UNKNOWN"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = audit_scientific_readiness(args.repo_root, issue_269_state=args.issue_269_state)
    serialized = json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized, encoding="utf-8")
    print(serialized, end="")
    return 2 if result["status"] == "INTEGRITY_FAILURE" else 0


if __name__ == "__main__":
    raise SystemExit(main())
