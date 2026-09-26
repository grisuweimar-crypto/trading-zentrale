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
        raise SemanticValidationError(
            "anchor_id requires family, symbol, accession_number and excerpt hash"
        )
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


def wilson_lower(
    successes: int, total: int, *, z: float = 1.959963984540054
) -> float | None:
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
        return math.isclose(
            float(observed), float(truth), rel_tol=1e-9, abs_tol=1e-12
        )
    return str(observed) == str(truth)


def _annotation_index(annotations: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for raw in annotations:
        row = dict(raw)
        cid = str(row.get("anchor_id") or "")
        if not cid:
            raise SemanticValidationError("Every annotation requires anchor_id")
        if cid in result:
            raise SemanticValidationError(f"Duplicate annotation anchor_id: {cid}")
        result[cid] = row
    return result


def _assert_outcome_blind(annotation: Mapping[str, Any]) -> None:
    if annotation.get("market_outcomes_seen") not in {False, "false", "False", 0}:
        raise SemanticValidationError("Annotation indicates market outcomes were visible")


def evaluate_family(
    *,
    family: str,
    candidate_rows: Iterable[Mapping[str, Any]],
    annotations: Iterable[Mapping[str, Any]],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    family = family.upper()
    candidates = [
        dict(row)
        for row in candidate_rows
        if str(row.get("family") or "").upper() == family
    ]
    annotation_index = _annotation_index(annotations)
    gates = contract["promotion_gates"]
    critical_fields = list(
        (contract.get("critical_fields") or {}).get(family) or []
    )

    candidate_issuers = {
        str(row.get("cik") or row.get("symbol") or "") for row in candidates
    }
    candidate_issuers.discard("")

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
        _assert_outcome_blind(annotation)
        truth_label = str(annotation.get("truth_label") or "")
        if truth_label == "UNCERTAIN":
            uncertain += 1
            continue
        labeled += 1
        is_target = _candidate_truth_is_target(family, truth_label)
        if is_target:
            correct += 1
        issuer = str(candidate.get("cik") or candidate.get("symbol") or "")
        if issuer:
            distinct_issuers.add(issuer)

        # Critical-field accuracy is defined only for true target events. A NOT_TARGET
        # candidate is a precision false positive and has no valid target-field truth
        # tuple to score against.
        if is_target:
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
        "minimum_labeled_emitted_candidates": labeled
        >= int(gates["minimum_labeled_emitted_candidates"]),
        "minimum_distinct_issuers": len(distinct_issuers)
        >= int(gates["minimum_distinct_issuers_in_labeled_candidates"]),
        "precision_wilson": precision_lb is not None
        and precision_lb >= float(gates["precision_wilson_95_lower_bound_min"]),
        "critical_field_accuracy_wilson": field_lb is not None
        and field_lb
        >= float(gates["critical_field_accuracy_wilson_95_lower_bound_min"]),
        "uncertain_rate": uncertain_rate is not None
        and uncertain_rate <= float(gates["maximum_uncertain_annotation_rate"]),
        "market_direction_violations": direction_violations
        <= int(gates["market_direction_violation_count_max"]),
        "provenance_violations": provenance_violations
        <= int(gates["provenance_violation_count_max"]),
        "all_candidates_annotated": missing_annotations == 0,
    }

    intrinsic_low_coverage = (
        len(candidates) < int(gates["minimum_labeled_emitted_candidates"])
        or len(candidate_issuers)
        < int(gates["minimum_distinct_issuers_in_labeled_candidates"])
    )
    if intrinsic_low_coverage:
        promotion_status = str(
            gates.get("low_coverage_behavior") or "LOW_COVERAGE_NOT_PROMOTABLE"
        )
    elif all(checks.values()):
        promotion_status = "PASS"
    else:
        promotion_status = str(
            gates.get("failed_precision_behavior") or "REMAIN_CHALLENGER"
        )

    return {
        "family": family,
        "candidate_count": len(candidates),
        "candidate_distinct_issuer_count": len(candidate_issuers),
        "labeled_candidate_count": labeled,
        "uncertain_count": uncertain,
        "missing_annotation_count": missing_annotations,
        "distinct_issuer_count": len(distinct_issuers),
        "correct_target_count": correct,
        "precision": (correct / labeled) if labeled else None,
        "precision_wilson_95_lower": precision_lb,
        "critical_field_comparisons": field_total,
        "critical_field_correct": field_correct,
        "critical_field_accuracy": (
            field_correct / field_total if field_total else None
        ),
        "critical_field_accuracy_wilson_95_lower": field_lb,
        "uncertain_annotation_rate": uncertain_rate,
        "market_direction_violation_count": direction_violations,
        "provenance_violation_count": provenance_violations,
        "checks": checks,
        "intrinsic_low_coverage": intrinsic_low_coverage,
        "promotion_status": promotion_status,
        "outcome_research": "NOT_RUN",
    }


def evaluate_anchor_audit(
    *,
    anchor_rows: Iterable[Mapping[str, Any]],
    annotations: Iterable[Mapping[str, Any]],
    candidate_rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Evaluate the preregistered independent anchor audit descriptively only.

    The first 8C-I validation explicitly does not use recall as a promotion gate. This
    function therefore reports target prevalence, emitted targets and false negatives
    without changing family promotion status.
    """
    anchors = [dict(row) for row in anchor_rows]
    annotations_index = _annotation_index(annotations)
    emitted_ids = {anchor_id(row) for row in candidate_rows}
    families = sorted(
        {
            str(row.get("family") or "").upper()
            for row in anchors
            if row.get("family")
        }
    )
    results: list[dict[str, Any]] = []

    for family in families:
        family_rows = [
            row
            for row in anchors
            if str(row.get("family") or "").upper() == family
        ]
        labeled = 0
        uncertain = 0
        missing = 0
        target_count = 0
        emitted_target_count = 0
        false_negative_count = 0
        non_target_count = 0

        for row in family_rows:
            cid = anchor_id(row)
            annotation = annotations_index.get(cid)
            if annotation is None:
                missing += 1
                continue
            _assert_outcome_blind(annotation)
            truth_label = str(annotation.get("truth_label") or "")
            if truth_label == "UNCERTAIN":
                uncertain += 1
                continue
            labeled += 1
            if _candidate_truth_is_target(family, truth_label):
                target_count += 1
                if cid in emitted_ids:
                    emitted_target_count += 1
                else:
                    false_negative_count += 1
            else:
                non_target_count += 1

        results.append(
            {
                "family": family,
                "sample_count": len(family_rows),
                "labeled_count": labeled,
                "uncertain_count": uncertain,
                "missing_annotation_count": missing,
                "target_count": target_count,
                "non_target_count": non_target_count,
                "emitted_target_count": emitted_target_count,
                "false_negative_count": false_negative_count,
                "descriptive_recall": (
                    emitted_target_count / target_count if target_count else None
                ),
                "all_anchors_annotated": missing == 0,
                "promotion_effect": "NONE_DESCRIPTIVE_ONLY",
            }
        )

    return {
        "schema_version": "external_evidence_8c_anchor_audit_result_v1",
        "families": results,
        "recall_gate": "DESCRIPTIVE_ONLY_IN_FIRST_VALIDATION",
        "market_outcomes_read": False,
        "promotion_effect": "NONE",
    }


def evaluate_all(
    *,
    candidate_rows: Iterable[Mapping[str, Any]],
    annotations: Iterable[Mapping[str, Any]],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    candidates = [dict(row) for row in candidate_rows]
    annotations_list = [dict(row) for row in annotations]
    families = sorted(
        {
            str(row.get("family") or "").upper()
            for row in candidates
            if row.get("family")
        }
    )
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
        "all_families_pass": bool(results)
        and all(row["promotion_status"] == "PASS" for row in results),
        "market_outcomes_read": False,
        "production_external_evidence_enabled": False,
        "phase7_integration_enabled": False,
    }
