from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping


EVIDENCE_SCHEMA = "external_evidence_8d_sec_insider_bulk_v1"
VALIDATION_CONFIG_SCHEMA = "external_evidence_8d_insider_validation_v1"
PACKAGE_SCHEMA = "external_evidence_8d_insider_validation_package_v1"
RESULT_SCHEMA = "external_evidence_8d_insider_validation_result_v1"


class SecInsiderValidationError(ValueError):
    """Raised when the Phase 8D-B3 validation contract is violated."""


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _load_json(path: Path) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise SecInsiderValidationError(f"cannot decode JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise SecInsiderValidationError(f"JSON root must be an object: {path}")
    return payload, _sha256_bytes(raw)


def _validation_id(row: Mapping[str, Any]) -> str:
    fields = (
        str(row.get("issuer_cik") or ""),
        str(row.get("accession_number") or ""),
        str(row.get("transaction_key") or ""),
        str(row.get("transaction_code") or ""),
    )
    if any(not value for value in fields):
        raise SecInsiderValidationError(
            "validation row requires issuer_cik, accession_number, transaction_key and transaction_code"
        )
    return hashlib.sha256("|".join(fields).encode("utf-8")).hexdigest()


def _hash_rank(seed: str, validation_id: str, stratum: str) -> str:
    return hashlib.sha256(f"{seed}|{stratum}|{validation_id}".encode("utf-8")).hexdigest()


def _deterministic_sample(
    rows: Iterable[Mapping[str, Any]], *, seed: str, stratum: str, maximum: int
) -> list[dict[str, Any]]:
    if maximum <= 0:
        raise SecInsiderValidationError("sample maximum must be positive")
    prepared: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        vid = _validation_id(row)
        row["validation_id"] = vid
        prepared.append(row)
    prepared.sort(key=lambda row: _hash_rank(seed, row["validation_id"], stratum))
    return prepared[:maximum]


def _validate_outcome_blind_evidence(evidence: Mapping[str, Any]) -> None:
    if evidence.get("schema_version") != EVIDENCE_SCHEMA:
        raise SecInsiderValidationError("unsupported SEC insider evidence schema")
    guards = evidence.get("guards") or {}
    required_false = (
        "market_outcomes_read",
        "market_direction_assigned",
        "threshold_selection_run",
        "phase7_integration_enabled",
        "production_external_evidence_enabled",
        "historical_selection_research_enabled",
    )
    for field in required_false:
        if guards.get(field) is not False:
            raise SecInsiderValidationError(
                f"validation refuses evidence unless guards.{field} is false"
            )
    if guards.get("exact_accession_join_required") is not True:
        raise SecInsiderValidationError("exact accession join guard must be enabled")
    if guards.get("exact_issuer_cik_join_required") is not True:
        raise SecInsiderValidationError("exact issuer CIK join guard must be enabled")
    if guards.get("current_ticker_used_as_historical_identity") is not False:
        raise SecInsiderValidationError("current ticker may not be historical identity")


def _validate_config(config: Mapping[str, Any]) -> None:
    if config.get("schema_version") != VALIDATION_CONFIG_SCHEMA:
        raise SecInsiderValidationError("unsupported insider validation config schema")
    if config.get("status") != "PRE_REGISTERED_BEFORE_REAL_SEC_INSIDER_EVALUATION":
        raise SecInsiderValidationError("validation config is not in pre-registered state")
    if config.get("market_outcomes_may_be_read") is not False:
        raise SecInsiderValidationError("validation config may not permit market outcomes")
    gate = config.get("research_gate") or {}
    for field in (
        "outcome_research_enabled",
        "production_external_evidence_enabled",
        "phase7_integration_enabled",
        "interaction_research_enabled",
    ):
        if gate.get(field) is not False:
            raise SecInsiderValidationError(f"research gate {field} must be false")


def prepare_insider_validation_package(
    *, evidence_path: Path, config_path: Path
) -> dict[str, Any]:
    evidence, evidence_sha = _load_json(evidence_path)
    config, config_sha = _load_json(config_path)
    _validate_outcome_blind_evidence(evidence)
    _validate_config(config)

    rows = evidence.get("rows")
    if not isinstance(rows, list):
        raise SecInsiderValidationError("SEC insider evidence rows must be an array")
    normalized_rows = [dict(row) for row in rows if isinstance(row, Mapping)]

    scope = config.get("scope") or {}
    target_codes = {str(value) for value in scope.get("target_transaction_codes") or []}
    high_status = str(scope.get("high_precision_candidate_status") or "")
    quarantine_statuses = {
        str(value) for value in scope.get("excluded_or_quarantined_statuses") or []
    }
    if target_codes != {"P", "S"} or not high_status:
        raise SecInsiderValidationError("unexpected frozen P/S validation scope")

    invalid_codes = sorted(
        {
            str(row.get("transaction_code") or "")
            for row in normalized_rows
            if str(row.get("transaction_code") or "") not in target_codes
        }
    )
    if invalid_codes:
        raise SecInsiderValidationError(
            f"evidence contains non-P/S rows despite B2 contract: {invalid_codes}"
        )

    seed = str((config.get("sampling") or {}).get("seed") or "")
    candidate_cfg = (config.get("sampling") or {}).get("candidate_audit") or {}
    quarantine_cfg = (config.get("sampling") or {}).get("quarantine_audit") or {}
    provenance_cfg = (config.get("sampling") or {}).get("provenance_audit") or {}

    selected_by_id: dict[str, dict[str, Any]] = {}

    def add_selected(row: Mapping[str, Any], audit_class: str) -> None:
        vid = _validation_id(row)
        existing = selected_by_id.get(vid)
        if existing is None:
            existing = dict(row)
            existing["validation_id"] = vid
            existing["audit_classes"] = []
            selected_by_id[vid] = existing
        if audit_class not in existing["audit_classes"]:
            existing["audit_classes"].append(audit_class)

    candidate_counts: Counter[str] = Counter()
    for code in sorted(target_codes):
        candidates = [
            row
            for row in normalized_rows
            if str(row.get("candidate_status") or "") == high_status
            and str(row.get("transaction_code") or "") == code
        ]
        sample = _deterministic_sample(
            candidates,
            seed=seed,
            stratum=f"candidate:{code}",
            maximum=int(candidate_cfg.get("per_transaction_code_max") or 0),
        )
        candidate_counts[code] = len(candidates)
        for row in sample:
            add_selected(row, f"CANDIDATE_{code}")

    quarantine_counts: Counter[str] = Counter()
    for status in sorted(quarantine_statuses):
        candidates = [
            row for row in normalized_rows if str(row.get("candidate_status") or "") == status
        ]
        quarantine_counts[status] = len(candidates)
        if not candidates:
            continue
        sample = _deterministic_sample(
            candidates,
            seed=seed,
            stratum=f"quarantine:{status}",
            maximum=int(quarantine_cfg.get("per_status_max") or 0),
        )
        for row in sample:
            add_selected(row, f"QUARANTINE_{status}")

    by_accession: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in normalized_rows:
        accession = str(row.get("accession_number") or "")
        if accession:
            by_accession[accession].append(row)
    ranked_accessions = sorted(
        by_accession,
        key=lambda accession: hashlib.sha256(
            f"{seed}|provenance|{accession}".encode("utf-8")
        ).hexdigest(),
    )
    accession_max = int(provenance_cfg.get("distinct_accession_max") or 0)
    for accession in ranked_accessions[:accession_max]:
        rows_for_accession = sorted(
            by_accession[accession], key=lambda row: _validation_id(row)
        )
        if rows_for_accession:
            add_selected(rows_for_accession[0], "PROVENANCE")

    audit_rows = list(selected_by_id.values())
    for row in audit_rows:
        row["audit_classes"] = sorted(row["audit_classes"])
        row["truth_label"] = ""
        row["review_status"] = "PENDING"
        row["market_outcomes_seen"] = "FALSE"
        row["notes"] = ""
        for field in (config.get("annotation") or {}).get("critical_field_checks") or []:
            row[str(field)] = ""

    audit_rows.sort(key=lambda row: row["validation_id"])
    distinct_issuers = {
        str(row.get("issuer_cik") or "") for row in normalized_rows if row.get("issuer_cik")
    }
    high_precision_rows = [
        row for row in normalized_rows if str(row.get("candidate_status") or "") == high_status
    ]
    status_counts = Counter(str(row.get("candidate_status") or "UNKNOWN") for row in normalized_rows)

    return {
        "schema_version": PACKAGE_SCHEMA,
        "phase": "8D_B3_insider_validation",
        "status": "READY_FOR_OUTCOME_BLIND_HUMAN_AUDIT",
        "config_sha256": config_sha,
        "evidence_sha256": evidence_sha,
        "source_quarter": evidence.get("source_quarter"),
        "source_url": evidence.get("source_url"),
        "source_zip_sha256": evidence.get("source_zip_sha256"),
        "population": {
            "evidence_row_count": len(normalized_rows),
            "high_precision_candidate_count": len(high_precision_rows),
            "distinct_issuer_count": len(distinct_issuers),
            "candidate_count_by_code": dict(sorted(candidate_counts.items())),
            "candidate_status_counts": dict(sorted(status_counts.items())),
            "quarantine_population_by_status": dict(sorted(quarantine_counts.items())),
            "distinct_accession_count": len(by_accession),
        },
        "sample": {
            "audit_row_count": len(audit_rows),
            "candidate_sample_by_code": {
                code: sum(
                    1 for row in audit_rows if f"CANDIDATE_{code}" in row["audit_classes"]
                )
                for code in sorted(target_codes)
            },
            "provenance_accession_sample_count": sum(
                1 for row in audit_rows if "PROVENANCE" in row["audit_classes"]
            ),
        },
        "rows": audit_rows,
        "guards": {
            "deterministic_sampling": True,
            "market_outcomes_read": False,
            "market_direction_assigned": False,
            "threshold_selection_run": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
        },
    }


def write_validation_package(
    package: Mapping[str, Any], *, json_path: Path, annotation_csv_path: Path
) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(dict(package), indent=2, sort_keys=True), encoding="utf-8")

    rows = list(package.get("rows") or [])
    if not rows:
        annotation_csv_path.parent.mkdir(parents=True, exist_ok=True)
        annotation_csv_path.write_text("", encoding="utf-8")
        return
    preferred = [
        "validation_id",
        "audit_classes",
        "scanner_symbol",
        "issuer_cik",
        "issuer_name",
        "accession_number",
        "document_type",
        "amendment",
        "transaction_key",
        "transaction_date",
        "transaction_code",
        "acquired_disposed_code",
        "transaction_shares",
        "transaction_price_per_share",
        "candidate_status",
        "aff10b5one",
        "equity_swap_involved",
        "published_at",
        "valid_from",
        "pit_status",
        "reporting_owners",
        "truth_label",
        "review_status",
        "market_outcomes_seen",
    ]
    all_fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in all_fields:
                all_fields.append(key)
    fields = [field for field in preferred if field in all_fields] + [
        field for field in all_fields if field not in preferred
    ]

    annotation_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with annotation_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for raw in rows:
            row = dict(raw)
            for key, value in list(row.items()):
                if isinstance(value, (list, dict)):
                    row[key] = json.dumps(value, sort_keys=True)
            writer.writerow(row)


def _wilson_lower(successes: int, total: int, z: float = 1.959963984540054) -> float | None:
    if total <= 0:
        return None
    p = successes / total
    denominator = 1.0 + (z * z / total)
    centre = p + (z * z / (2.0 * total))
    margin = z * math.sqrt((p * (1.0 - p) / total) + (z * z / (4.0 * total * total)))
    return (centre - margin) / denominator


def _truth_expected_for_code(code: str) -> str:
    if code == "P":
        return "VALID_P_TRANSACTION"
    if code == "S":
        return "VALID_S_TRANSACTION"
    raise SecInsiderValidationError(f"unexpected transaction code in audit: {code}")


def evaluate_insider_annotations(
    *, package_path: Path, annotation_csv_path: Path, config_path: Path
) -> dict[str, Any]:
    package, package_sha = _load_json(package_path)
    config, config_sha = _load_json(config_path)
    _validate_config(config)
    if package.get("schema_version") != PACKAGE_SCHEMA:
        raise SecInsiderValidationError("unsupported insider validation package schema")
    guards = package.get("guards") or {}
    if guards.get("market_outcomes_read") is not False:
        raise SecInsiderValidationError("validation package is outcome-contaminated")

    expected_rows = {
        str(row.get("validation_id") or ""): dict(row)
        for row in package.get("rows") or []
        if isinstance(row, Mapping)
    }
    if not expected_rows or "" in expected_rows:
        raise SecInsiderValidationError("validation package has invalid row identities")

    with annotation_csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        annotations = [dict(row) for row in csv.DictReader(handle)]
    by_id: dict[str, dict[str, str]] = {}
    for annotation in annotations:
        vid = str(annotation.get("validation_id") or "").strip()
        if not vid or vid not in expected_rows:
            raise SecInsiderValidationError(f"annotation has unknown validation_id: {vid!r}")
        if vid in by_id:
            raise SecInsiderValidationError(f"duplicate annotation validation_id: {vid}")
        by_id[vid] = annotation

    reviewed: list[tuple[dict[str, Any], dict[str, str]]] = []
    market_outcome_violations = 0
    for vid, source in expected_rows.items():
        annotation = by_id.get(vid)
        if annotation is None:
            continue
        if str(annotation.get("review_status") or "").strip().upper() != "REVIEWED":
            continue
        if str(annotation.get("market_outcomes_seen") or "").strip().upper() != "FALSE":
            market_outcome_violations += 1
        reviewed.append((source, annotation))

    high_status = str((config.get("scope") or {}).get("high_precision_candidate_status") or "")
    candidate_reviewed = [
        pair for pair in reviewed if str(pair[0].get("candidate_status") or "") == high_status
    ]
    certain_candidate = [
        pair
        for pair in candidate_reviewed
        if str(pair[1].get("truth_label") or "").strip().upper() != "UNCERTAIN"
    ]
    scope_successes = 0
    by_code_reviewed: Counter[str] = Counter()
    distinct_issuers: set[str] = set()
    for source, annotation in certain_candidate:
        code = str(source.get("transaction_code") or "")
        by_code_reviewed[code] += 1
        distinct_issuers.add(str(source.get("issuer_cik") or ""))
        if str(annotation.get("truth_label") or "").strip().upper() == _truth_expected_for_code(code):
            scope_successes += 1

    check_fields = [
        str(value) for value in (config.get("annotation") or {}).get("critical_field_checks") or []
    ]
    critical_success = 0
    critical_total = 0
    provenance_fields = {
        "issuer_cik_correct",
        "accession_number_correct",
        "valid_from_correct",
        "reporting_owner_link_correct",
    }
    provenance_success = 0
    provenance_total = 0
    for _, annotation in reviewed:
        for field in check_fields:
            value = str(annotation.get(field) or "").strip().upper()
            if value == "UNCERTAIN" or not value:
                continue
            if value not in {"TRUE", "FALSE"}:
                raise SecInsiderValidationError(
                    f"invalid annotation check value {value!r} for {field}"
                )
            critical_total += 1
            if value == "TRUE":
                critical_success += 1
            if field in provenance_fields:
                provenance_total += 1
                if value == "TRUE":
                    provenance_success += 1

    uncertain_count = sum(
        1
        for _, annotation in reviewed
        if str(annotation.get("truth_label") or "").strip().upper() == "UNCERTAIN"
    )
    uncertain_rate = uncertain_count / len(reviewed) if reviewed else None
    scope_total = len(certain_candidate)
    gates = config.get("promotion_gates") or {}
    scope_lower = _wilson_lower(scope_successes, scope_total)
    critical_lower = _wilson_lower(critical_success, critical_total)
    provenance_lower = _wilson_lower(provenance_success, provenance_total)

    requirements = {
        "minimum_labeled_high_precision_candidates": scope_total
        >= int(gates.get("minimum_labeled_high_precision_candidates") or 0),
        "minimum_distinct_issuers_in_labeled_candidates": len(distinct_issuers)
        >= int(gates.get("minimum_distinct_issuers_in_labeled_candidates") or 0),
        "minimum_labeled_purchases": by_code_reviewed["P"]
        >= int(gates.get("minimum_labeled_purchases") or 0),
        "minimum_labeled_sales": by_code_reviewed["S"]
        >= int(gates.get("minimum_labeled_sales") or 0),
        "scope_precision": scope_lower is not None
        and scope_lower >= float(gates.get("scope_precision_wilson_95_lower_bound_min") or 1.0),
        "critical_field_accuracy": critical_lower is not None
        and critical_lower
        >= float(gates.get("critical_field_accuracy_wilson_95_lower_bound_min") or 1.0),
        "provenance_accuracy": provenance_lower is not None
        and provenance_lower >= float(gates.get("provenance_accuracy_wilson_95_lower_bound_min") or 1.0),
        "maximum_uncertain_annotation_rate": uncertain_rate is not None
        and uncertain_rate <= float(gates.get("maximum_uncertain_annotation_rate") or 0.0),
        "market_outcome_violation_count": market_outcome_violations
        <= int(gates.get("market_outcome_violation_count_max") or 0),
    }

    sample_minima = all(
        requirements[key]
        for key in (
            "minimum_labeled_high_precision_candidates",
            "minimum_distinct_issuers_in_labeled_candidates",
            "minimum_labeled_purchases",
            "minimum_labeled_sales",
        )
    )
    if not sample_minima:
        decision = str(gates.get("low_coverage_behavior") or "LOW_COVERAGE_NOT_PROMOTABLE")
    elif all(requirements.values()):
        decision = "PASS_SOURCE_SEMANTICS_VALIDATION"
    else:
        decision = str(gates.get("failed_accuracy_behavior") or "REMAIN_CHALLENGER")

    return {
        "schema_version": RESULT_SCHEMA,
        "phase": "8D_B3_insider_validation",
        "decision": decision,
        "package_sha256": package_sha,
        "config_sha256": config_sha,
        "reviewed_row_count": len(reviewed),
        "high_precision_reviewed_certain_count": scope_total,
        "distinct_issuer_count": len(distinct_issuers),
        "reviewed_candidate_count_by_code": dict(sorted(by_code_reviewed.items())),
        "scope_precision": {
            "successes": scope_successes,
            "total": scope_total,
            "wilson_95_lower_bound": scope_lower,
        },
        "critical_field_accuracy": {
            "successes": critical_success,
            "total": critical_total,
            "wilson_95_lower_bound": critical_lower,
        },
        "provenance_accuracy": {
            "successes": provenance_success,
            "total": provenance_total,
            "wilson_95_lower_bound": provenance_lower,
        },
        "uncertain_annotation_rate": uncertain_rate,
        "market_outcome_violation_count": market_outcome_violations,
        "requirements": requirements,
        "guards": {
            "market_outcomes_read_by_pipeline": False,
            "market_direction_assigned": False,
            "threshold_selection_run": False,
            "phase7_integration_enabled": False,
            "production_external_evidence_enabled": False,
        },
    }


def write_validation_result(payload: Mapping[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True), encoding="utf-8")
