"""Fail-closed QM-B gate for historical taxonomy/classification integrity."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


class HistoricalTaxonomyIntegrityError(ValueError):
    pass


def load_contract(path: str | Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HistoricalTaxonomyIntegrityError("contract_unreadable") from exc
    if payload.get("schema_version") != "qm_b_historical_taxonomy_integrity_v1":
        raise HistoricalTaxonomyIntegrityError("contract_schema_invalid")
    rules = payload.get("rules") or {}
    required_false = (
        "current_taxonomy_may_define_historical_sample_membership",
        "current_taxonomy_may_retroactively_enrich_historical_rows",
        "missing_historical_taxonomy_may_be_filled_from_current_metadata",
    )
    if any(rules.get(key) is not False for key in required_false):
        raise HistoricalTaxonomyIntegrityError("retrojection_rule_invalid")
    if rules.get("new_candidate_requires_explicit_review") is not True:
        raise HistoricalTaxonomyIntegrityError("fail_closed_review_rule_required")
    return payload


def evaluate(scan: Mapping[str, Any], contract: Mapping[str, Any]) -> dict[str, Any]:
    if scan.get("schema_version") != "qm_b_historical_taxonomy_scan_v1":
        raise HistoricalTaxonomyIntegrityError("scan_schema_invalid")
    raw_dispositions = contract.get("dispositions")
    if not isinstance(raw_dispositions, list):
        raise HistoricalTaxonomyIntegrityError("dispositions_required")

    dispositions: dict[str, Mapping[str, Any]] = {}
    for row in raw_dispositions:
        if not isinstance(row, Mapping):
            raise HistoricalTaxonomyIntegrityError("disposition_row_invalid")
        path = str(row.get("path") or "").strip()
        status = str(row.get("status") or "").strip()
        reason = str(row.get("reason") or "").strip()
        if not path or path in dispositions:
            raise HistoricalTaxonomyIntegrityError("disposition_path_invalid_or_duplicate")
        if status not in {"SAFE", "CURRENT_TAXONOMY_DEPENDENCY", "CURRENT_METADATA_RETROJECTION", "REVIEW_REQUIRED"}:
            raise HistoricalTaxonomyIntegrityError(f"disposition_status_invalid:{path}")
        if not reason:
            raise HistoricalTaxonomyIntegrityError(f"disposition_reason_required:{path}")
        dispositions[path] = row

    candidates = scan.get("candidates") or []
    candidate_paths = {str(row.get("path") or "") for row in candidates if isinstance(row, Mapping)}
    disposition_paths = set(dispositions)
    unreviewed = sorted(candidate_paths - disposition_paths)
    stale = sorted(disposition_paths - candidate_paths)
    statuses = {path: str(dispositions[path].get("status")) for path in sorted(candidate_paths & disposition_paths)}
    confirmed_dependency = sorted(path for path, status in statuses.items() if status == "CURRENT_TAXONOMY_DEPENDENCY")
    confirmed_retrojection = sorted(path for path, status in statuses.items() if status == "CURRENT_METADATA_RETROJECTION")
    review_required = sorted(path for path, status in statuses.items() if status == "REVIEW_REQUIRED")
    safe = sorted(path for path, status in statuses.items() if status == "SAFE")

    expected_dependencies = int(contract.get("confirmed_current_taxonomy_dependencies", -1))
    expected_retrojections = int(contract.get("confirmed_current_metadata_retrojections", -1))
    count_mismatch = (
        expected_dependencies != len(confirmed_dependency)
        or expected_retrojections != len(confirmed_retrojection)
    )
    gate = "PASS" if not (unreviewed or stale or review_required or confirmed_dependency or confirmed_retrojection or count_mismatch) else "FAIL"
    return {
        "schema_version": "qm_b_historical_taxonomy_integrity_result_v1",
        "gate_status": gate,
        "files_scanned": int(scan.get("files_scanned") or 0),
        "candidate_count": len(candidate_paths),
        "high_priority_count": int(scan.get("high_priority_count") or 0),
        "safe_count": len(safe),
        "unreviewed_candidates": unreviewed,
        "stale_dispositions": stale,
        "review_required": review_required,
        "confirmed_current_taxonomy_dependencies": confirmed_dependency,
        "confirmed_current_metadata_retrojections": confirmed_retrojection,
        "count_mismatch": count_mismatch,
        "historical_retrojection_permitted": False,
        "current_taxonomy_may_define_historical_samples": False,
    }


def require_pass(result: Mapping[str, Any]) -> None:
    if result.get("gate_status") != "PASS":
        raise HistoricalTaxonomyIntegrityError("historical_taxonomy_gate_failed")
