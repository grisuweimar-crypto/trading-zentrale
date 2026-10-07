"""Phase L12 Promotion Gate for Pattern Discovery Lab v2.

L12 creates explicit, version-bound and reversible admission decisions.
It never activates productive Decision Layer, portfolio or execution effects.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from .boundary import PatternDiscoveryBoundary
from .confirmation_engine import verify_confirmation_look
from .dependency_graph import verify_dependency_graph
from .rating_engine import verify_rating_history

SCHEMA_VERSION = "pattern_discovery_l12_promotion_gate_v1"
REVIEW_SCHEMA_VERSION = "pattern_discovery_l12_promotion_review_v1"
DECISION_SCHEMA_VERSION = "pattern_discovery_l12_promotion_decision_v1"
REGISTRY_EVENT_SCHEMA_VERSION = "pattern_discovery_l12_promotion_registry_event_v1"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "l12_promotion_gate_v1.json"
)


class PromotionGateError(ValueError):
    """Raised when an L12 promotion invariant is violated."""


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    return sha256(_canonical(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise PromotionGateError(f"value_required:{field}")
    return result


def _sha256(value: Any, field: str) -> str:
    text = _text(value, field).lower()
    if len(text) != 64 or any(ch not in "0123456789abcdef" for ch in text):
        raise PromotionGateError(f"sha256_required:{field}")
    return text


def _timestamp(value: Any, field: str) -> str:
    text = _text(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    if len(normalized) == 10:
        normalized += "T00:00:00+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise PromotionGateError(f"invalid_timestamp:{field}") from exc
    if parsed.tzinfo is None:
        raise PromotionGateError(f"timezone_required:{field}")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def load_promotion_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PromotionGateError(
            f"promotion_contract_unreadable:{target}"
        ) from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != SCHEMA_VERSION
    ):
        raise PromotionGateError("promotion_contract_schema_invalid")
    if payload.get("research_only") is not True:
        raise PromotionGateError("promotion_contract_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise PromotionGateError("promotion_productive_integration_forbidden")
    if payload.get("execution_allowed") is not False:
        raise PromotionGateError("promotion_execution_forbidden")
    principles = payload.get("principles") or {}
    for field in (
        "rating_alone_never_promotes",
        "only_exact_pattern_version_can_be_admitted",
        "separate_review_required",
        "prospective_confirmation_required",
        "no_vote_counting",
        "admission_is_reversible",
        "l13_required_before_any_decision_layer_effect",
    ):
        if principles.get(field) is not True:
            raise PromotionGateError(f"promotion_principle_missing:{field}")
    return payload


def promotion_contract_hash(
    contract: Mapping[str, Any] | None = None,
) -> str:
    value = dict(contract) if contract is not None else load_promotion_contract()
    return _hash(value)


def _assert_boundaries(payload: Mapping[str, Any]) -> None:
    boundaries = payload.get("boundaries") or {}
    for field in (
        "l5_pattern_mutation_performed",
        "l9_confirmation_mutation_performed",
        "l10_rating_mutation_performed",
        "scanner_score_change_performed",
        "timing_change_performed",
        "decision_layer_integration_performed",
        "portfolio_or_execution_effect_created",
    ):
        if boundaries.get(field) is not False:
            raise PromotionGateError(f"promotion_boundary_invalid:{field}")
    PatternDiscoveryBoundary().assert_research_payload(payload)


def _verify_frozen_pattern(pattern: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(pattern, Mapping):
        raise PromotionGateError("frozen_pattern_must_be_object")
    if pattern.get("schema_version") != "pattern_discovery_l5_frozen_pattern_v1":
        raise PromotionGateError("frozen_pattern_schema_invalid")
    if pattern.get("research_only") is not True:
        raise PromotionGateError("frozen_pattern_research_only_guard_missing")
    if pattern.get("productive_integration_enabled") is not False:
        raise PromotionGateError("frozen_pattern_productive_integration_forbidden")
    if pattern.get("execution_allowed") is not False:
        raise PromotionGateError("frozen_pattern_execution_forbidden")

    stored = _sha256(pattern.get("frozen_record_hash"), "frozen_record_hash")
    body = dict(pattern)
    body.pop("frozen_record_hash", None)
    if _hash(body) != stored:
        raise PromotionGateError("frozen_pattern_hash_mismatch")
    spec = pattern.get("pattern_spec")
    if not isinstance(spec, Mapping):
        raise PromotionGateError("frozen_pattern_spec_missing")
    spec_hash = _sha256(pattern.get("pattern_spec_hash"), "pattern_spec_hash")
    if _hash(spec) != spec_hash:
        raise PromotionGateError("frozen_pattern_spec_hash_mismatch")

    semantics = spec.get("semantics") or {}
    forecast = spec.get("forecast") or {}
    handoff = pattern.get("qm_c_handoff") or {}
    hypothesis = handoff.get("hypothesis_record") or {}
    try:
        horizon = int(forecast.get("horizon_sessions"))
    except (TypeError, ValueError) as exc:
        raise PromotionGateError("horizon_sessions_invalid") from exc
    if horizon <= 0:
        raise PromotionGateError("horizon_sessions_must_be_positive")

    identity = {
        "pattern_id": _text(pattern.get("pattern_id"), "pattern_id"),
        "pattern_version": _text(pattern.get("pattern_version"), "pattern_version"),
        "pattern_spec_hash": spec_hash,
        "frozen_record_hash": stored,
        "pattern_family_id": _text(
            hypothesis.get("hypothesis_family_id"),
            "qm_c_handoff.hypothesis_family_id",
        ),
        "pattern_type": _text(semantics.get("pattern_type"), "pattern_type"),
        "expected_direction": _text(
            forecast.get("expected_direction"), "expected_direction"
        ),
        "target_id": _text(forecast.get("target_id"), "target_id"),
        "horizon_sessions": horizon,
        "baseline": _text(forecast.get("baseline"), "baseline"),
    }
    PatternDiscoveryBoundary().assert_research_payload(pattern)
    return identity


def _identity_key(identity: Mapping[str, Any]) -> str:
    return (
        f"{_text(identity.get('pattern_id'), 'pattern_id')}::"
        f"{_text(identity.get('pattern_version'), 'pattern_version')}::"
        f"{_text(identity.get('pattern_spec_hash'), 'pattern_spec_hash')}"
    )


def _verify_rating_binding(
    history: Mapping[str, Any],
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    verify_rating_history(history)
    for field in ("pattern_id", "pattern_version", "pattern_spec_hash"):
        if history.get(field) != identity[field]:
            raise PromotionGateError(f"rating_identity_mismatch:{field}")
    return {
        "current_rating": _text(history.get("current_rating"), "current_rating"),
        "history_hash": _sha256(history.get("history_hash"), "history_hash"),
    }


def _applicable_l9(
    reports: Sequence[Mapping[str, Any]],
    identity: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in reports:
        report = dict(raw)
        verify_confirmation_look(report)
        look_hash = _sha256(report.get("look_hash"), "l9.look_hash")
        if look_hash in seen:
            raise PromotionGateError(f"duplicate_l9_look_hash:{look_hash}")
        seen.add(look_hash)
        evaluated_at = _timestamp(report.get("evaluated_at"), "l9.evaluated_at")
        governance = report.get("qm_governance") or {}
        for result in report.get("pattern_results") or []:
            if (
                result.get("pattern_id") == identity["pattern_id"]
                and result.get("pattern_version") == identity["pattern_version"]
            ):
                if result.get("pattern_spec_hash") != identity["pattern_spec_hash"]:
                    raise PromotionGateError("l9_pattern_spec_hash_mismatch")
                rows.append(
                    {
                        "look_hash": look_hash,
                        "confirmation_look_id": report.get("confirmation_look_id"),
                        "confirmation_evidence_hash": report.get(
                            "confirmation_evidence_hash"
                        ),
                        "evaluated_at": evaluated_at,
                        "family_decision": report.get("family_decision"),
                        "is_final_look": governance.get("is_final_look"),
                        "monitoring_plan_id": governance.get("monitoring_plan_id"),
                        "monitoring_plan_version": governance.get(
                            "monitoring_plan_version"
                        ),
                        "look_id": governance.get("look_id"),
                        "result_class": result.get("result_class"),
                        "result_reasons": list(result.get("result_reasons") or []),
                        "prospective_evidence": result.get("prospective_evidence"),
                    }
                )
    rows.sort(
        key=lambda row: (
            row["evaluated_at"],
            str(row.get("confirmation_look_id") or ""),
        )
    )
    return rows


def _latest_terminal_supported(
    reports: Sequence[Mapping[str, Any]],
    identity: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> dict[str, Any] | None:
    terminal_decisions = set(
        contract["eligibility"]["terminal_family_decisions"]
    )
    terminal_rows = [
        row
        for row in _applicable_l9(reports, identity)
        if row.get("is_final_look") is True
        or row.get("family_decision") in terminal_decisions
    ]
    if not terminal_rows:
        return None
    latest = terminal_rows[-1]
    if (
        latest.get("result_class")
        != contract["eligibility"]["required_l9_result_class"]
    ):
        return None
    return latest


def _dependency_summary(
    graph: Mapping[str, Any],
    identity: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    verify_dependency_graph(graph)
    nodes = {
        str(node.get("node_id")): node
        for node in graph.get("nodes") or []
        if isinstance(node, Mapping)
    }
    target = [
        node
        for node in nodes.values()
        if node.get("pattern_id") == identity["pattern_id"]
        and node.get("pattern_version") == identity["pattern_version"]
        and node.get("pattern_spec_hash") == identity["pattern_spec_hash"]
    ]
    if len(target) != 1:
        raise PromotionGateError("l6_exact_pattern_node_required")
    node = target[0]
    rank = {"NONE": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
    highest = "NONE"
    relationships: list[dict[str, Any]] = []
    for edge in graph.get("edges") or []:
        if edge.get("source_node_id") == node["node_id"]:
            other_id = str(edge.get("target_node_id"))
        elif edge.get("target_node_id") == node["node_id"]:
            other_id = str(edge.get("source_node_id"))
        else:
            continue
        other = nodes.get(other_id)
        if other is None:
            raise PromotionGateError("l6_dependency_other_node_missing")
        severity = _text(edge.get("dependency_severity"), "dependency_severity")
        if severity not in rank:
            raise PromotionGateError(f"dependency_severity_unknown:{severity}")
        if rank[severity] > rank[highest]:
            highest = severity
        relationships.append(
            {
                "other_pattern_id": other.get("pattern_id"),
                "other_pattern_version": other.get("pattern_version"),
                "relationship": edge.get("relationship"),
                "dependency_severity": severity,
                "feature_overlap": edge.get("feature_overlap"),
                "event_overlap": edge.get("event_overlap"),
            }
        )
    relationships.sort(
        key=lambda row: (
            -rank[row["dependency_severity"]],
            str(row.get("other_pattern_id") or ""),
        )
    )
    high = set(contract["dependency_handling"]["high_severities"])
    return {
        "graph_id": graph.get("graph_id"),
        "graph_hash": _sha256(graph.get("graph_hash"), "graph_hash"),
        "highest_severity": highest,
        "high_dependency_present": highest in high,
        "duplicate_present": any(
            row.get("relationship") == "DUPLICATE" for row in relationships
        ),
        "relationships": relationships,
    }


def _normalize_policy(
    requested: Mapping[str, Any],
    dependency: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    if not isinstance(requested, Mapping):
        raise PromotionGateError("requested_policy_must_be_object")
    integration = _text(
        requested.get("integration_mode"), "integration_mode"
    ).upper()
    dependency_mode = _text(
        requested.get("dependency_handling"), "dependency_handling"
    ).upper()
    conflict = _text(
        requested.get("conflict_handling"), "conflict_handling"
    ).upper()

    if integration not in set(contract["integration"]["allowed_modes"]):
        raise PromotionGateError(f"integration_mode_not_allowed:{integration}")
    if dependency_mode not in set(
        contract["dependency_handling"]["allowed_modes"]
    ):
        raise PromotionGateError(
            f"dependency_handling_not_allowed:{dependency_mode}"
        )
    if conflict not in set(contract["conflict_handling"]["allowed_modes"]):
        raise PromotionGateError(f"conflict_handling_not_allowed:{conflict}")

    blockers: list[str] = []
    independent = dependency_mode == "INDEPENDENT_IF_NO_HIGH_DEPENDENCY"
    if dependency.get("high_dependency_present") and independent:
        blockers.append("HIGH_DEPENDENCY_REQUIRES_NONINDEPENDENT_HANDLING")
    if dependency.get("duplicate_present") and independent:
        blockers.append("DUPLICATE_REQUIRES_NONINDEPENDENT_HANDLING")

    return {
        "integration_mode": integration,
        "directional_authority": contract["integration"][
            "mode_directional_authority"
        ][integration],
        "probability_attachment_mode": contract["probability_attachment"][
            "source"
        ],
        "dependency_handling": dependency_mode,
        "conflict_handling": conflict,
        "modifier_contract": "SEPARATE_CONTRACT_REQUIRED_FOR_MODIFIERS",
        "vote_counting_allowed": False,
        "productive_timing_evidence_active": False,
        "decision_layer_effect_active": False,
        "requires_l13_before_activation": True,
    }, blockers


def _probability_attachment(
    terminal: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = terminal.get("prospective_evidence")
    if not isinstance(evidence, Mapping):
        raise PromotionGateError("l9_prospective_evidence_required")
    values = {
        field: evidence.get(field)
        for field in contract["probability_attachment"]["fields"]
    }
    if values.get("direction_probability") is None:
        raise PromotionGateError("l9_direction_probability_required")
    return {
        "source": contract["probability_attachment"]["source"],
        "l9_look_hash": terminal["look_hash"],
        "confirmation_evidence_hash": terminal.get(
            "confirmation_evidence_hash"
        ),
        "evaluated_at": terminal["evaluated_at"],
        "monitoring_plan_id": terminal.get("monitoring_plan_id"),
        "monitoring_plan_version": terminal.get("monitoring_plan_version"),
        "look_id": terminal.get("look_id"),
        "result_class": terminal.get("result_class"),
        "values": values,
        "recalibration_performed": False,
        "discovery_probability_used": False,
    }


def build_promotion_review(
    frozen_pattern: Mapping[str, Any],
    rating_history: Mapping[str, Any],
    confirmation_reports: Sequence[Mapping[str, Any]],
    dependency_graph: Mapping[str, Any],
    *,
    requested_policy: Mapping[str, Any],
    opened_at: str,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Create an OPEN review. Rating B/A alone never promotes."""
    spec = dict(contract) if contract is not None else load_promotion_contract()
    identity = _verify_frozen_pattern(frozen_pattern)
    rating = _verify_rating_binding(rating_history, identity)
    dependency = _dependency_summary(dependency_graph, identity, spec)
    policy, policy_blockers = _normalize_policy(
        requested_policy, dependency, spec
    )
    terminal = _latest_terminal_supported(
        confirmation_reports, identity, spec
    )

    blockers = list(policy_blockers)
    if rating["current_rating"] not in set(
        spec["eligibility"]["allowed_ratings"]
    ):
        blockers.append(
            f"RATING_NOT_PROMOTION_ELIGIBLE:{rating['current_rating']}"
        )
    pattern_type_upper = identity["pattern_type"].upper()
    if any(
        token in pattern_type_upper
        for token in spec["eligibility"]["disallowed_pattern_type_tokens"]
    ):
        blockers.append("MODIFIER_REQUIRES_SEPARATE_ADMISSION_CONTRACT")
    if terminal is None:
        blockers.append("TERMINAL_SUPPORTED_L9_CONFIRMATION_REQUIRED")

    probability = (
        _probability_attachment(terminal, spec)
        if terminal is not None
        else None
    )
    core: dict[str, Any] = {
        "schema_version": REVIEW_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L12",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "review_status": "OPEN",
        "opened_at": _timestamp(opened_at, "opened_at"),
        "pattern_identity": identity,
        "promotion_eligibility": {
            "status": "ELIGIBLE_FOR_REVIEW" if not blockers else "BLOCKED",
            "blockers": sorted(set(blockers)),
            "rating": rating["current_rating"],
            "requires_separate_review": True,
            "automatic_promotion_performed": False,
        },
        "requested_integration_contract": policy,
        "probability_attachment": probability,
        "dependency_snapshot": dependency,
        "conflict_contract": {
            "handling": policy["conflict_handling"],
            "fusion_performed_in_l12": False,
            "l13_relation_graph_required_before_fusion": True,
        },
        "modifier_contract": {
            "pattern_type": identity["pattern_type"],
            "separate_modifier_admission_required": True,
            "modifier_counted_as_directional_vote": False,
        },
        "evidence_bindings": {
            "frozen_record_hash": identity["frozen_record_hash"],
            "rating_history_hash": rating["history_hash"],
            "l9_look_hash": (
                terminal["look_hash"] if terminal is not None else None
            ),
            "l6_graph_hash": dependency["graph_hash"],
        },
        "boundaries": {
            "l5_pattern_mutation_performed": False,
            "l9_confirmation_mutation_performed": False,
            "l10_rating_mutation_performed": False,
            "scanner_score_change_performed": False,
            "timing_change_performed": False,
            "decision_layer_integration_performed": False,
            "portfolio_or_execution_effect_created": False,
        },
        "l12_contract_hash": promotion_contract_hash(spec),
    }
    _assert_boundaries(core)
    core["review_id"] = "PRV-" + _hash(core)[:24].upper()
    core["review_hash"] = _hash(core)
    return core


def verify_promotion_review(
    review: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = dict(contract) if contract is not None else load_promotion_contract()
    if not isinstance(review, Mapping):
        raise PromotionGateError("promotion_review_must_be_object")
    if review.get("schema_version") != REVIEW_SCHEMA_VERSION:
        raise PromotionGateError("promotion_review_schema_invalid")
    if review.get("review_status") != "OPEN":
        raise PromotionGateError("promotion_review_must_be_open")
    if review.get("research_only") is not True:
        raise PromotionGateError("promotion_review_research_only_guard_missing")
    if review.get("productive_integration_enabled") is not False:
        raise PromotionGateError(
            "promotion_review_productive_integration_forbidden"
        )
    if review.get("execution_allowed") is not False:
        raise PromotionGateError("promotion_review_execution_forbidden")
    if review.get("l12_contract_hash") != promotion_contract_hash(spec):
        raise PromotionGateError("promotion_review_contract_hash_mismatch")
    _identity_key(review.get("pattern_identity") or {})
    policy = review.get("requested_integration_contract") or {}
    if policy.get("integration_mode") not in set(
        spec["integration"]["allowed_modes"]
    ):
        raise PromotionGateError("promotion_review_integration_mode_invalid")
    if policy.get("vote_counting_allowed") is not False:
        raise PromotionGateError("promotion_review_vote_counting_forbidden")
    if policy.get("decision_layer_effect_active") is not False:
        raise PromotionGateError("promotion_review_decision_effect_forbidden")
    stored = _sha256(review.get("review_hash"), "review_hash")
    body = dict(review)
    body.pop("review_hash", None)
    if _hash(body) != stored:
        raise PromotionGateError("promotion_review_hash_mismatch")
    id_body = dict(body)
    id_body.pop("review_id", None)
    expected_id = "PRV-" + _hash(id_body)[:24].upper()
    if review.get("review_id") != expected_id:
        raise PromotionGateError("promotion_review_id_mismatch")
    _assert_boundaries(review)
    return {
        "valid": True,
        "review_id": review["review_id"],
        "review_hash": stored,
        "eligibility_status": review["promotion_eligibility"]["status"],
    }


def record_promotion_decision(
    review: Mapping[str, Any],
    *,
    decision: str,
    reviewer_id: str,
    reviewer_role: str,
    reviewed_at: str,
    reason_codes: Sequence[str],
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Record explicit approve/reject/defer review outcome."""
    spec = dict(contract) if contract is not None else load_promotion_contract()
    verify_promotion_review(review, contract=spec)
    normalized = _text(decision, "decision").upper()
    if normalized not in set(spec["review"]["allowed_decisions"]):
        raise PromotionGateError(f"promotion_decision_not_allowed:{normalized}")
    reasons = sorted({_text(code, "reason_code") for code in reason_codes})
    if not reasons:
        raise PromotionGateError("promotion_decision_reason_codes_required")
    if (
        normalized == "APPROVE"
        and review["promotion_eligibility"]["status"] != "ELIGIBLE_FOR_REVIEW"
    ):
        raise PromotionGateError("promotion_approval_requires_eligibility")

    status = {
        "APPROVE": "ADMITTED",
        "REJECT": "REJECTED",
        "DEFER": "DEFERRED",
    }[normalized]
    admission = None
    if normalized == "APPROVE":
        identity = review["pattern_identity"]
        policy = review["requested_integration_contract"]
        probability = review.get("probability_attachment")
        if not isinstance(probability, Mapping):
            raise PromotionGateError(
                "promotion_approval_requires_probability_attachment"
            )
        admission = {
            "admitted_pattern_id": identity["pattern_id"],
            "admitted_pattern_version": identity["pattern_version"],
            "admitted_pattern_spec_hash": identity["pattern_spec_hash"],
            "admitted_pattern_family_id": identity["pattern_family_id"],
            "pattern_type": identity["pattern_type"],
            "direction": identity["expected_direction"],
            "target_id": identity["target_id"],
            "horizon_sessions": identity["horizon_sessions"],
            "baseline": identity["baseline"],
            "integration_mode": policy["integration_mode"],
            "directional_authority": policy["directional_authority"],
            "probability_attachment": dict(probability),
            "dependency_handling": policy["dependency_handling"],
            "conflict_handling": policy["conflict_handling"],
            "modifier_contract": policy["modifier_contract"],
            "vote_counting_allowed": False,
            "productive_timing_evidence_active": False,
            "decision_layer_effect_active": False,
            "requires_l13_before_activation": True,
            "reversible": True,
        }

    core: dict[str, Any] = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L12",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "review_id": review["review_id"],
        "review_hash": review["review_hash"],
        "pattern_identity": dict(review["pattern_identity"]),
        "decision": normalized,
        "promotion_status": status,
        "reviewed_at": _timestamp(reviewed_at, "reviewed_at"),
        "reviewer_id": _text(reviewer_id, "reviewer_id"),
        "reviewer_role": _text(reviewer_role, "reviewer_role"),
        "reason_codes": reasons,
        "admission_contract": admission,
        "evidence_bindings": dict(review["evidence_bindings"]),
        "boundaries": dict(review["boundaries"]),
        "l12_contract_hash": promotion_contract_hash(spec),
    }
    _assert_boundaries(core)
    core["decision_id"] = "PRD-" + _hash(core)[:24].upper()
    core["decision_hash"] = _hash(core)
    return core


def verify_promotion_decision(
    decision: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = dict(contract) if contract is not None else load_promotion_contract()
    if not isinstance(decision, Mapping):
        raise PromotionGateError("promotion_decision_must_be_object")
    if decision.get("schema_version") != DECISION_SCHEMA_VERSION:
        raise PromotionGateError("promotion_decision_schema_invalid")
    if decision.get("research_only") is not True:
        raise PromotionGateError(
            "promotion_decision_research_only_guard_missing"
        )
    if decision.get("productive_integration_enabled") is not False:
        raise PromotionGateError(
            "promotion_decision_productive_integration_forbidden"
        )
    if decision.get("execution_allowed") is not False:
        raise PromotionGateError("promotion_decision_execution_forbidden")
    normalized = _text(decision.get("decision"), "decision")
    expected_status = {
        "APPROVE": "ADMITTED",
        "REJECT": "REJECTED",
        "DEFER": "DEFERRED",
    }.get(normalized)
    if expected_status is None or decision.get("promotion_status") != expected_status:
        raise PromotionGateError("promotion_decision_status_invalid")
    if not decision.get("reason_codes"):
        raise PromotionGateError("promotion_decision_reason_codes_missing")
    if decision.get("l12_contract_hash") != promotion_contract_hash(spec):
        raise PromotionGateError("promotion_decision_contract_hash_mismatch")

    if normalized == "APPROVE":
        admission = decision.get("admission_contract")
        if not isinstance(admission, Mapping):
            raise PromotionGateError("promotion_admission_contract_missing")
        identity = decision.get("pattern_identity") or {}
        expected = {
            "admitted_pattern_id": identity.get("pattern_id"),
            "admitted_pattern_version": identity.get("pattern_version"),
            "admitted_pattern_spec_hash": identity.get("pattern_spec_hash"),
        }
        for field, value in expected.items():
            if admission.get(field) != value:
                raise PromotionGateError(
                    f"promotion_exact_version_binding_mismatch:{field}"
                )
        if admission.get("vote_counting_allowed") is not False:
            raise PromotionGateError("promotion_vote_counting_forbidden")
        if admission.get("decision_layer_effect_active") is not False:
            raise PromotionGateError(
                "promotion_decision_layer_effect_forbidden"
            )
        if admission.get("requires_l13_before_activation") is not True:
            raise PromotionGateError("promotion_l13_gate_required")
    elif decision.get("admission_contract") is not None:
        raise PromotionGateError("nonapproval_cannot_have_admission_contract")

    stored = _sha256(decision.get("decision_hash"), "decision_hash")
    body = dict(decision)
    body.pop("decision_hash", None)
    if _hash(body) != stored:
        raise PromotionGateError("promotion_decision_hash_mismatch")
    id_body = dict(body)
    id_body.pop("decision_id", None)
    if decision.get("decision_id") != "PRD-" + _hash(id_body)[:24].upper():
        raise PromotionGateError("promotion_decision_id_mismatch")
    _assert_boundaries(decision)
    return {
        "valid": True,
        "decision_id": decision["decision_id"],
        "promotion_status": decision["promotion_status"],
        "decision_hash": stored,
    }


def validate_admission_current(
    decision: Mapping[str, Any],
    frozen_pattern: Mapping[str, Any],
    rating_history: Mapping[str, Any],
    confirmation_reports: Sequence[Mapping[str, Any]],
    dependency_graph: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Fail closed when an admitted review is stale against current evidence."""
    spec = dict(contract) if contract is not None else load_promotion_contract()
    verify_promotion_decision(decision, contract=spec)
    if decision.get("promotion_status") != "ADMITTED":
        return {
            "valid": False,
            "status": "NOT_ADMITTED",
            "reasons": ["PROMOTION_STATUS_NOT_ADMITTED"],
        }
    identity = _verify_frozen_pattern(frozen_pattern)
    if _identity_key(identity) != _identity_key(
        decision.get("pattern_identity") or {}
    ):
        raise PromotionGateError("admission_pattern_identity_mismatch")
    rating = _verify_rating_binding(rating_history, identity)
    dependency = _dependency_summary(dependency_graph, identity, spec)
    terminal = _latest_terminal_supported(
        confirmation_reports, identity, spec
    )
    bindings = decision.get("evidence_bindings") or {}
    reasons: list[str] = []
    if rating["current_rating"] not in set(
        spec["eligibility"]["allowed_ratings"]
    ):
        reasons.append(
            f"CURRENT_RATING_NOT_ELIGIBLE:{rating['current_rating']}"
        )
    if rating["history_hash"] != bindings.get("rating_history_hash"):
        reasons.append("RATING_HISTORY_CHANGED_SINCE_REVIEW")
    if dependency["graph_hash"] != bindings.get("l6_graph_hash"):
        reasons.append("DEPENDENCY_GRAPH_CHANGED_SINCE_REVIEW")
    current_l9 = terminal["look_hash"] if terminal is not None else None
    if current_l9 != bindings.get("l9_look_hash"):
        reasons.append("CONFIRMATION_EVIDENCE_CHANGED_SINCE_REVIEW")
    if terminal is None:
        reasons.append(
            "TERMINAL_SUPPORTED_CONFIRMATION_NO_LONGER_AVAILABLE"
        )
    return {
        "valid": not reasons,
        "status": "CURRENT" if not reasons else "STALE_REVIEW_REQUIRED",
        "reasons": sorted(set(reasons)),
        "pattern_id": identity["pattern_id"],
        "pattern_version": identity["pattern_version"],
        "decision_id": decision["decision_id"],
    }


def review_repo_path(
    review: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = dict(contract) if contract is not None else load_promotion_contract()
    identity = review.get("pattern_identity") or {}
    return str(spec["storage"]["review_path_template"]).format(
        pattern_id=_text(identity.get("pattern_id"), "pattern_id"),
        pattern_version=_text(
            identity.get("pattern_version"), "pattern_version"
        ),
        review_id=_text(review.get("review_id"), "review_id"),
    )


def promotion_registry_repo_path(
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = dict(contract) if contract is not None else load_promotion_contract()
    return str(spec["storage"]["registry_path"])


def persist_promotion_review(
    repo_root: str | Path,
    review: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
    boundary: PatternDiscoveryBoundary | None = None,
) -> dict[str, Any]:
    spec = dict(contract) if contract is not None else load_promotion_contract()
    verify_promotion_review(review, contract=spec)
    guard = boundary or PatternDiscoveryBoundary()
    root = Path(repo_root).resolve()
    repo_path = review_repo_path(review, contract=spec)
    guard.assert_write_path_allowed(repo_path)
    path = (root / repo_path).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise PromotionGateError("promotion_review_path_outside_repo") from exc
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(
        dict(review),
        indent=2,
        sort_keys=True,
        ensure_ascii=True,
        allow_nan=False,
    ) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != text:
            raise PromotionGateError("promotion_review_identity_collision")
        return {"valid": True, "idempotent": True, "review_path": repo_path}
    path.write_text(text, encoding="utf-8", newline="\n")
    return {"valid": True, "idempotent": False, "review_path": repo_path}


class PromotionRegistry:
    """Append-only hash-chained promotion decision and reversal registry."""

    def __init__(
        self,
        path: str | Path,
        *,
        contract: Mapping[str, Any] | None = None,
    ) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.contract = (
            dict(contract)
            if contract is not None
            else load_promotion_contract()
        )

    def _read(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        result: list[dict[str, Any]] = []
        for line_number, line in enumerate(
            self.path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise PromotionGateError(
                    f"promotion_registry_invalid_json:{line_number}"
                ) from exc
            if not isinstance(value, dict):
                raise PromotionGateError(
                    f"promotion_registry_event_not_object:{line_number}"
                )
            result.append(value)
        return result

    def _verify_chain(
        self,
        events: Sequence[Mapping[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        previous_hash: str | None = None
        states: dict[str, dict[str, Any]] = {}
        seen_ids: set[str] = set()
        for sequence, raw in enumerate(events, start=1):
            event = dict(raw)
            if event.get("schema_version") != REGISTRY_EVENT_SCHEMA_VERSION:
                raise PromotionGateError(
                    f"promotion_registry_schema_invalid:{sequence}"
                )
            if event.get("sequence") != sequence:
                raise PromotionGateError(
                    f"promotion_registry_sequence_invalid:{sequence}"
                )
            if event.get("previous_event_hash") != previous_hash:
                raise PromotionGateError(
                    f"promotion_registry_previous_hash_invalid:{sequence}"
                )
            stored = _sha256(
                event.get("entry_hash"),
                f"promotion_registry.entry_hash.{sequence}",
            )
            body = dict(event)
            body.pop("entry_hash", None)
            if _hash(body) != stored:
                raise PromotionGateError(
                    f"promotion_registry_entry_hash_invalid:{sequence}"
                )
            event_id = _text(event.get("event_id"), "event_id")
            if event_id in seen_ids:
                raise PromotionGateError(
                    "promotion_registry_duplicate_event_id"
                )
            seen_ids.add(event_id)
            event_type = _text(event.get("event_type"), "event_type")
            identity = event.get("pattern_identity")
            if not isinstance(identity, Mapping):
                raise PromotionGateError(
                    "promotion_registry_pattern_identity_missing"
                )
            key = _identity_key(identity)
            current = states.get(key)

            if event_type == "PROMOTION_DECISION_RECORDED":
                decision = event.get("decision")
                if not isinstance(decision, Mapping):
                    raise PromotionGateError(
                        "promotion_registry_decision_missing"
                    )
                verify_promotion_decision(
                    decision, contract=self.contract
                )
                if _identity_key(decision["pattern_identity"]) != key:
                    raise PromotionGateError(
                        "promotion_registry_decision_identity_mismatch"
                    )
                if (
                    current is not None
                    and current["status"] == "ADMITTED"
                    and current.get("decision_id") != decision["decision_id"]
                ):
                    raise PromotionGateError(
                        "promotion_registry_requires_reversal_before_new_decision"
                    )
                states[key] = {
                    "status": decision["promotion_status"],
                    "decision_id": decision["decision_id"],
                    "decision_hash": decision["decision_hash"],
                    "review_id": decision["review_id"],
                    "last_event_hash": stored,
                }
            elif event_type == "PROMOTION_STATE_REVERSED":
                if current is None or current.get("status") != "ADMITTED":
                    raise PromotionGateError(
                        "promotion_reversal_requires_admitted_state"
                    )
                reversal = event.get("reversal")
                if not isinstance(reversal, Mapping):
                    raise PromotionGateError(
                        "promotion_reversal_payload_missing"
                    )
                action = _text(reversal.get("action"), "reversal.action")
                if action not in set(
                    self.contract["reversibility"][
                        "allowed_terminal_actions"
                    ]
                ):
                    raise PromotionGateError(
                        f"promotion_reversal_action_invalid:{action}"
                    )
                reasons = reversal.get("reason_codes")
                if not isinstance(reasons, list) or not reasons:
                    raise PromotionGateError(
                        "promotion_reversal_reason_codes_required"
                    )
                allowed = set(
                    self.contract["reversibility"]["reason_codes"]
                )
                if any(str(reason) not in allowed for reason in reasons):
                    raise PromotionGateError(
                        "promotion_reversal_reason_code_invalid"
                    )
                states[key] = {
                    **current,
                    "status": (
                        "DEMOTED" if action == "DEMOTE" else "ROLLED_BACK"
                    ),
                    "last_event_hash": stored,
                }
            else:
                raise PromotionGateError(
                    f"promotion_registry_event_type_unknown:{event_type}"
                )
            previous_hash = stored
        return states

    def _append(
        self,
        *,
        event_type: str,
        identity: Mapping[str, Any],
        recorded_at: str,
        actor_id: str,
        actor_role: str,
        payload_field: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock_fd: int | None = None
        try:
            try:
                lock_fd = os.open(
                    self.lock_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o644,
                )
            except FileExistsError as exc:
                raise PromotionGateError(
                    "promotion_registry_lock_already_held"
                ) from exc
            events = self._read()
            self._verify_chain(events)
            event: dict[str, Any] = {
                "schema_version": REGISTRY_EVENT_SCHEMA_VERSION,
                "sequence": len(events) + 1,
                "event_type": event_type,
                "recorded_at": _timestamp(recorded_at, "recorded_at"),
                "actor_id": _text(actor_id, "actor_id"),
                "actor_role": _text(actor_role, "actor_role"),
                "pattern_identity": dict(identity),
                payload_field: dict(payload),
                "previous_event_hash": (
                    events[-1]["entry_hash"] if events else None
                ),
            }
            event["event_id"] = "PRE-" + _hash(event)[:24].upper()
            event["entry_hash"] = _hash(event)
            self._verify_chain([*events, event])
            fd = os.open(
                self.path,
                os.O_CREAT | os.O_APPEND | os.O_WRONLY,
                0o644,
            )
            try:
                os.write(fd, (_canonical(event) + "\n").encode("utf-8"))
                os.fsync(fd)
            finally:
                os.close(fd)
            return event
        finally:
            if lock_fd is not None:
                os.close(lock_fd)
                try:
                    self.lock_path.unlink()
                except FileNotFoundError:
                    pass

    def record_decision(
        self,
        decision: Mapping[str, Any],
    ) -> dict[str, Any]:
        verify_promotion_decision(decision, contract=self.contract)
        events = self._read()
        self._verify_chain(events)
        for event in events:
            existing = event.get("decision")
            if (
                isinstance(existing, Mapping)
                and existing.get("decision_id") == decision.get("decision_id")
            ):
                if dict(existing) != dict(decision):
                    raise PromotionGateError(
                        "promotion_decision_identity_collision"
                    )
                return {
                    "valid": True,
                    "idempotent": True,
                    "event_id": event["event_id"],
                    "entry_hash": event["entry_hash"],
                }
        event = self._append(
            event_type="PROMOTION_DECISION_RECORDED",
            identity=decision["pattern_identity"],
            recorded_at=decision["reviewed_at"],
            actor_id=decision["reviewer_id"],
            actor_role=decision["reviewer_role"],
            payload_field="decision",
            payload=decision,
        )
        return {
            "valid": True,
            "idempotent": False,
            "event_id": event["event_id"],
            "entry_hash": event["entry_hash"],
        }

    def reverse(
        self,
        *,
        pattern_identity: Mapping[str, Any],
        action: str,
        reason_codes: Sequence[str],
        actor_id: str,
        actor_role: str,
        observed_at: str,
        evidence_hash: str,
    ) -> dict[str, Any]:
        normalized = _text(action, "action").upper()
        if normalized not in set(
            self.contract["reversibility"]["allowed_terminal_actions"]
        ):
            raise PromotionGateError(
                f"promotion_reversal_action_not_allowed:{normalized}"
            )
        allowed = set(self.contract["reversibility"]["reason_codes"])
        reasons = sorted({_text(x, "reason_code") for x in reason_codes})
        if not reasons:
            raise PromotionGateError(
                "promotion_reversal_reason_codes_required"
            )
        if any(reason not in allowed for reason in reasons):
            raise PromotionGateError(
                "promotion_reversal_reason_code_not_allowed"
            )
        event = self._append(
            event_type="PROMOTION_STATE_REVERSED",
            identity=pattern_identity,
            recorded_at=observed_at,
            actor_id=actor_id,
            actor_role=actor_role,
            payload_field="reversal",
            payload={
                "action": normalized,
                "reason_codes": reasons,
                "evidence_hash": _sha256(evidence_hash, "evidence_hash"),
            },
        )
        return {
            "valid": True,
            "status": (
                "DEMOTED" if normalized == "DEMOTE" else "ROLLED_BACK"
            ),
            "event_id": event["event_id"],
            "entry_hash": event["entry_hash"],
        }

    def current_status(
        self,
        pattern_identity: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        states = self._verify_chain(self._read())
        value = states.get(_identity_key(pattern_identity))
        return dict(value) if value is not None else None

    def verify_integrity(self) -> dict[str, Any]:
        events = self._read()
        states = self._verify_chain(events)
        return {
            "valid": True,
            "event_count": len(events),
            "head_hash": events[-1]["entry_hash"] if events else None,
            "states": {
                key: value["status"]
                for key, value in sorted(states.items())
            },
        }
