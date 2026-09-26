from __future__ import annotations

import hashlib
import math
from collections import defaultdict
from typing import Any, Iterable, Mapping


class SemanticValidationError(ValueError):
    """Raised when 8C-I validation inputs violate the preregistered contract."""


def anchor_id(row: Mapping[str, Any]) -> str:
    parts = [
        str(row.get("family") or ""),
        str(row.get("symbol") or ""),
        str(row.get("accession_number") or ""),
        str(row.get("excerpt_sha256") or row.get("evidence_excerpt_sha256") or ""),
    ]
    if any(not part for part in parts):
        raise SemanticValidationError("anchor_id requires family, symbol, accession_number and excerpt hash")
    return "|".join(parts)


def _stable_rank(seed: str, value: str) -> str:
    return hashlib.sha256(f"{seed}|{value}".encode("utf-8")).hexdigest()


def deterministic_sample(
    rows: Iterable[Mapping[str, Any]], *, seed: str, per_family_max: int
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in rows:
        row = dict(raw)
        family = str(row.get("family") or "").upper()
        if not family:
            raise SemanticValidationError("Every sampled row requires family")
        row["anchor_id"] = anchor_id(row)
        grouped[family].append(row)

    output: list[dict[str, Any]] = []
    for family in sorted(grouped):
        ranked = sorted(
            grouped[family],
            key=lambda row: (_stable_rank(seed, row["anchor_id"]), row["anchor_id"]),
        )
        output.extend(ranked[: max(int(per_family_max), 0)])
    return output


def wilson_lower(successes: int, total: int, *, z: float = 1.959963984540054) -> float | None:
    if total <= 0:
        return None
    if successes < 0 or successes > total:
        raise SemanticValidationError("successes must be between 0 and total")
    p = successes / total
    z2 = z * z
    denominator = 1.0 + z2 / total
    centre = p + z2 / (2.0 * total)
    margin = z * math.sqrt((p * (1.0 - p) + z2 / (4.0 * total)) / total)
    return (centre - margin) / denominator


def _candidate_truth_is_target(family: str, truth_label: str) -> bool:
    expected = {
        "DIVIDEND": "REGULAR_QUARTERLY_DIVIDEND_DECLARATION",
        "BUYBACK": "NEW_AUTHORIZATION",
    }
    return truth_label == expected.get(family)


def _field_equal(observed: Any, truth: Any) -> bool:
    if isinstance(observed, (int, float)) and isinstance(truth, (int, float)):
        return math.isclose(float(observed), float(truth), rel_tol=1e-9, abs_tol=1e-12)
    return str(observed) == str(truth)


def evaluate_family(
    *,
    family: str,
    candidate_rows: Iterable[Mapping[str, Any]],
    annotations: Iterable[Mapping[str, Any]],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    family = family.upper()
    candidates = [dict(row) for row in candidate_rows if str(row.get("family") or "").upper() == family]
    annotation_index = {str(row.get("anchor_id") or ""): dict(row) for row in annotations}
    gates = contract["promotion_gates"]
    critical_fields = list((contract.get("critical_fields") or {}).get(family) or [])

    labeled = 0
    correct = 0
    uncertain = 0
    distinct_issuers: set[str] = set()
    field_total = 0
    field_correct = 0
    provenance_violations = 0
    direction_violations = 0
    missing_annotations = 0

    for candidate in candidates:
        cid = anchor_id(candidate)
        annotation = annotation_index.get(cid)
        if annotation is None:
            missing_annotations += 1
            continue
        if annotation.get("market_outcomes_seen") not in {False, "false", "False", 0}:
            raise SemanticValidationError("Annotation indicates market outcomes were visible")
        truth_label = str(annotation.get("truth_label") or "")
        if truth_label == "UNCERTAIN":
            uncertain += 1
            continue
        labeled += 1
        if _candidate_truth_is_target(family, truth_label):
            correct += 1
        distinct_issuers.add(str(candidate.get("cik") or candidate.get("symbol") or ""))

        for field in critical_fields:
            truth_key = f"truth_{field}"
            if truth_key not in annotation:
                continue
            field_total += 1
            if _field_equal(candidate.get(field), annotation.get(truth_key)):
                field_correct += 1

        if str(candidate.get("market_direction") or "UNKNOWN") != "UNKNOWN":
            direction_violations += 1
        required_provenance = [
            "accession_number",
            "source_valid_from",
            "source_document",
            "evidence_excerpt_sha256",
            "parser_version",
        ]
        if any(not candidate.get(field) for field in required_provenance):
            provenance_violations += 1

    precision_lb = wilson_lower(correct, labeled)
    field_lb = wilson_lower(field_correct, field_total)
    denominator = labeled + uncertain
    uncertain_rate = (uncertain / denominator) if denominator else None

    checks = {
        "minimum_labeled_emitted_candidates": labeled >= int(gates["minimum_labeled_emitted_candidates"]),
        "minimum_distinct_issuers": len(distinct_issuers) >= int(gates["minimum_distinct_issuers_in_labeled_candidates"]),
        "precision_wilson": precision_lb is not None and precision_lb >= float(gates["precision_wilson_95_lower_bound_min"]),
        "critical_field_accuracy_wilson": field_lb is not None and field_lb >= float(gates["critical_field_accuracy_wilson_95_lower_bound_min"]),
        "uncertain_rate": uncertain_rate is not None and uncertain_rate <= float(gates["maximum_uncertain_annotation_rate"]),
        "market_direction_violations": direction_violations <= int(gates["market_direction_violation_count_max"]),
        "provenance_violations": provenance_violations <= int(gates["provenance_violation_count_max"]),
        "all_candidates_annotated": missing_annotations == 0,
    }

    return {
        "family": family,
        "candidate_count": len(candidates),
        "labeled_candidate_count": labeled,
        "uncertain_count": uncertain,
        "missing_annotation_count": missing_annotations,
        "distinct_issuer_count": len(distinct_issuers),
        "correct_target_count": correct,
        "precision": (correct / labeled) if labeled else None,
        "precision_wilson_95_lower": precision_lb,
        "critical_field_comparisons": field_total,
        "critical_field_correct": field_correct,
        "critical_field_accuracy": (field_correct / field_total) if field_total else None,
        "critical_field_accuracy_wilson_95_lower": field_lb,
        "uncertain_annotation_rate": uncertain_rate,
        "market_direction_violation_count": direction_violations,
        "provenance_violation_count": provenance_violations,
        "checks": checks,
        "promotion_status": "PASS" if all(checks.values()) else "REMAIN_CHALLENGER",
        "outcome_research": "NOT_RUN",
    }


def evaluate_all(
    *,
    candidate_rows: Iterable[Mapping[str, Any]],
    annotations: Iterable[Mapping[str, Any]],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    candidates = [dict(row) for row in candidate_rows]
    annotations_list = [dict(row) for row in annotations]
    families = sorted({str(row.get("family") or "").upper() for row in candidates if row.get("family")})
    results = [
        evaluate_family(
            family=family,
            candidate_rows=candidates,
            annotations=annotations_list,
            contract=contract,
        )
        for family in families
    ]
    return {
        "schema_version": "external_evidence_8c_semantic_validation_result_v1",
        "families": results,
        "all_families_pass": bool(results) and all(row["promotion_status"] == "PASS" for row in results),
        "market_outcomes_read": False,
        "production_external_evidence_enabled": False,
        "phase7_integration_enabled": False,
    }
