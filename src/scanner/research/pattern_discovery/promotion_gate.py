"""Phase L12 Pattern Promotion Gate.

Explicit, version-bound, reversible admission governance. L12 never activates
productive scanner, Decision Layer, portfolio, or execution behavior.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from .boundary import PatternDiscoveryBoundary
from .confirmation_engine import verify_confirmation_look
from .dependency_graph import verify_dependency_graph
from .rating_engine import verify_rating_history

SCHEMA_VERSION = "pattern_discovery_l12_promotion_gate_v1"
REVIEW_SCHEMA_VERSION = "pattern_discovery_l12_promotion_review_v1"
DECISION_SCHEMA_VERSION = "pattern_discovery_l12_promotion_decision_v1"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "l12_promotion_gate_v1.json"
)


class PromotionGateError(ValueError):
    """Raised when an L12 promotion invariant is violated."""


def _json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    return sha256(_json(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise PromotionGateError(f"value_required:{field}")
    return result


def _sha(value: Any, field: str) -> str:
    text = _text(value, field).lower()
    if len(text) != 64 or any(c not in "0123456789abcdef" for c in text):
        raise PromotionGateError(f"sha256_required:{field}")
    return text


def _time(value: Any, field: str) -> str:
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
        raise PromotionGateError(f"promotion_contract_unreadable:{target}") from exc
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
    for key in (
        "rating_alone_never_promotes",
        "only_exact_pattern_version_can_be_admitted",
        "separate_review_required",
        "prospective_confirmation_required",
        "no_vote_counting",
        "admission_is_reversible",
        "l13_required_before_any_decision_layer_effect",
    ):
        if payload.get("principles", {}).get(key) is not True:
            raise PromotionGateError(f"promotion_principle_missing:{key}")
    return payload


def promotion_contract_hash(
    contract: Mapping[str, Any] | None = None,
) -> str:
    return _hash(
        dict(contract) if contract is not None else load_promotion_contract()
    )


def _identity(pattern: Mapping[str, Any]) -> dict[str, Any]:
    if (
        not isinstance(pattern, Mapping)
        or pattern.get("schema_version")
        != "pattern_discovery_l5_frozen_pattern_v1"
    ):
        raise PromotionGateError("frozen_pattern_schema_invalid")
    if pattern.get("research_only") is not True:
        raise PromotionGateError("frozen_pattern_research_only_guard_missing")
    if pattern.get("productive_integration_enabled") is not False:
        raise PromotionGateError("frozen_pattern_productive_integration_forbidden")
    if pattern.get("execution_allowed") is not False:
        raise PromotionGateError("frozen_pattern_execution_forbidden")
    frozen_hash = _sha(pattern.get("frozen_record_hash"), "frozen_record_hash")
    body = dict(pattern)
    body.pop("frozen_record_hash", None)
    if _hash(body) != frozen_hash:
        raise PromotionGateError("frozen_pattern_hash_mismatch")
    spec = pattern.get("pattern_spec")
    if not isinstance(spec, Mapping):
        raise PromotionGateError("frozen_pattern_spec_missing")
    spec_hash = _sha(pattern.get("pattern_spec_hash"), "pattern_spec_hash")
    if _hash(spec) != spec_hash:
        raise PromotionGateError("frozen_pattern_spec_hash_mismatch")
    semantics = spec.get("semantics") or {}
    forecast = spec.get("forecast") or {}
    hypothesis = (
        (pattern.get("qm_c_handoff") or {}).get("hypothesis_record") or {}
    )
    try:
        horizon = int(forecast.get("horizon_sessions"))
    except (TypeError, ValueError) as exc:
        raise PromotionGateError("horizon_sessions_invalid") from exc
    if horizon <= 0:
        raise PromotionGateError("horizon_sessions_must_be_positive")
    result = {
        "pattern_id": _text(pattern.get("pattern_id"), "pattern_id"),
        "pattern_version": _text(pattern.get("pattern_version"), "pattern_version"),
        "pattern_spec_hash": spec_hash,
        "frozen_record_hash": frozen_hash,
        "pattern_family_id": _text(
            hypothesis.get("hypothesis_family_id"), "hypothesis_family_id"
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
    return result


def _key(identity: Mapping[str, Any]) -> str:
    return "::".join(
        (
            _text(identity.get("pattern_id"), "pattern_id"),
            _text(identity.get("pattern_version"), "pattern_version"),
            _text(identity.get("pattern_spec_hash"), "pattern_spec_hash"),
        )
    )


def _rating(
    history: Mapping[str, Any], identity: Mapping[str, Any]
) -> dict[str, str]:
    verify_rating_history(history)
    for field in ("pattern_id", "pattern_version", "pattern_spec_hash"):
        if history.get(field) != identity[field]:
            raise PromotionGateError(f"rating_identity_mismatch:{field}")
    return {
        "rating": _text(history.get("current_rating"), "current_rating"),
        "history_hash": _sha(history.get("history_hash"), "history_hash"),
    }


def _terminal_l9(
    reports: Sequence[Mapping[str, Any]],
    identity: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> dict[str, Any] | None:
    terminal_decisions = set(
        contract["eligibility"]["terminal_family_decisions"]
    )
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in reports:
        report = dict(raw)
        verify_confirmation_look(report)
        look_hash = _sha(report.get("look_hash"), "l9.look_hash")
        if look_hash in seen:
            raise PromotionGateError(f"duplicate_l9_look_hash:{look_hash}")
        seen.add(look_hash)
        governance = report.get("qm_governance") or {}
        terminal = (
            governance.get("is_final_look") is True
            or report.get("family_decision") in terminal_decisions
        )
        if not terminal:
            continue
        evaluated_at = _time(report.get("evaluated_at"), "l9.evaluated_at")
        for row in report.get("pattern_results") or []:
            if (
                row.get("pattern_id") == identity["pattern_id"]
                and row.get("pattern_version") == identity["pattern_version"]
            ):
                if row.get("pattern_spec_hash") != identity["pattern_spec_hash"]:
                    raise PromotionGateError("l9_pattern_spec_hash_mismatch")
                rows.append(
                    {
                        "evaluated_at": evaluated_at,
                        "look_hash": look_hash,
                        "confirmation_evidence_hash": report.get(
                            "confirmation_evidence_hash"
                        ),
                        "family_decision": report.get("family_decision"),
                        "monitoring_plan_id": governance.get("monitoring_plan_id"),
                        "monitoring_plan_version": governance.get(
                            "monitoring_plan_version"
                        ),
                        "look_id": governance.get("look_id"),
                        "result_class": row.get("result_class"),
                        "prospective_evidence": row.get("prospective_evidence"),
                    }
                )
    if not rows:
        return None
    rows.sort(key=lambda item: (item["evaluated_at"], item["look_hash"]))
    latest = rows[-1]
    if (
        latest["result_class"]
        != contract["eligibility"]["required_l9_result_class"]
    ):
        return None
    return latest


def _dependency(
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
    targets = [
        node
        for node in nodes.values()
        if node.get("pattern_id") == identity["pattern_id"]
        and node.get("pattern_version") == identity["pattern_version"]
        and node.get("pattern_spec_hash") == identity["pattern_spec_hash"]
    ]
    if len(targets) != 1:
        raise PromotionGateError("l6_exact_pattern_node_required")
    node = targets[0]
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
        highest = severity if rank[severity] > rank[highest] else highest
        provenance = edge.get("provenance") or {}
        structural = provenance.get("structural") or {}
        empirical = provenance.get("empirical") or {}
        relationships.append(
            {
                "other_pattern_id": other.get("pattern_id"),
                "other_pattern_version": other.get("pattern_version"),
                "relationship": edge.get("relationship"),
                "dependency_severity": severity,
                "feature_overlap": structural.get("feature_overlap"),
                "condition_overlap": structural.get("condition_overlap"),
                "event_overlap": empirical.get("event_overlap"),
            }
        )
    high = set(contract["dependency_handling"]["high_severities"])
    return {
        "graph_id": graph.get("graph_id"),
        "graph_hash": _sha(graph.get("graph_hash"), "graph_hash"),
        "highest_severity": highest,
        "high_dependency_present": highest in high,
        "duplicate_present": any(
            item.get("relationship") == "DUPLICATE"
            for item in relationships
        ),
        "relationships": relationships,
    }


def _policy(
    requested: Mapping[str, Any],
    dependency: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    if not isinstance(requested, Mapping):
        raise PromotionGateError("requested_policy_must_be_object")
    integration = _text(
        requested.get("integration_mode"), "integration_mode"
    ).upper()
    dep_mode = _text(
        requested.get("dependency_handling"), "dependency_handling"
    ).upper()
    conflict = _text(
        requested.get("conflict_handling"), "conflict_handling"
    ).upper()
    if integration not in set(contract["integration"]["allowed_modes"]):
        raise PromotionGateError(f"integration_mode_not_allowed:{integration}")
    if dep_mode not in set(contract["dependency_handling"]["allowed_modes"]):
        raise PromotionGateError(
            f"dependency_handling_not_allowed:{dep_mode}"
        )
    if conflict not in set(contract["conflict_handling"]["allowed_modes"]):
        raise PromotionGateError(f"conflict_handling_not_allowed:{conflict}")
    blockers: list[str] = []
    if dep_mode == "INDEPENDENT_IF_NO_HIGH_DEPENDENCY":
        if dependency["high_dependency_present"]:
            blockers.append(
                "HIGH_DEPENDENCY_REQUIRES_NONINDEPENDENT_HANDLING"
            )
        if dependency["duplicate_present"]:
            blockers.append("DUPLICATE_REQUIRES_NONINDEPENDENT_HANDLING")
    return {
        "integration_mode": integration,
        "directional_authority": contract["integration"][
            "mode_directional_authority"
        ][integration],
        "dependency_handling": dep_mode,
        "conflict_handling": conflict,
        "probability_attachment_mode": contract[
            "probability_attachment"
        ]["source"],
        "modifier_contract": "SEPARATE_CONTRACT_REQUIRED_FOR_MODIFIERS",
        "vote_counting_allowed": False,
        "productive_timing_evidence_active": False,
        "decision_layer_effect_active": False,
        "requires_l13_before_activation": True,
    }, blockers


def _probability(
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
    if values["direction_probability"] is None:
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
        "result_class": terminal["result_class"],
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
    spec = dict(contract) if contract is not None else load_promotion_contract()
    identity = _identity(frozen_pattern)
    rating = _rating(rating_history, identity)
    dependency = _dependency(dependency_graph, identity, spec)
    policy, blockers = _policy(requested_policy, dependency, spec)
    terminal = _terminal_l9(confirmation_reports, identity, spec)
    if rating["rating"] not in set(spec["eligibility"]["allowed_ratings"]):
        blockers.append(f"RATING_NOT_PROMOTION_ELIGIBLE:{rating['rating']}")
    if any(
        token in identity["pattern_type"].upper()
        for token in spec["eligibility"]["disallowed_pattern_type_tokens"]
    ):
        blockers.append("MODIFIER_REQUIRES_SEPARATE_ADMISSION_CONTRACT")
    if terminal is None:
        blockers.append("LATEST_TERMINAL_L9_RESULT_MUST_BE_SUPPORTED")
    core: dict[str, Any] = {
        "schema_version": REVIEW_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L12",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "review_status": "OPEN",
        "opened_at": _time(opened_at, "opened_at"),
        "pattern_identity": identity,
        "promotion_eligibility": {
            "status": "ELIGIBLE_FOR_REVIEW" if not blockers else "BLOCKED",
            "blockers": sorted(set(blockers)),
            "rating": rating["rating"],
            "automatic_promotion_performed": False,
            "separate_review_required": True,
        },
        "requested_integration_contract": policy,
        "probability_attachment": (
            _probability(terminal, spec) if terminal is not None else None
        ),
        "dependency_snapshot": dependency,
        "conflict_contract": {
            "handling": policy["conflict_handling"],
            "fusion_performed_in_l12": False,
            "l13_relation_graph_required_before_fusion": True,
        },
        "modifier_contract": {
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
    PatternDiscoveryBoundary().assert_research_payload(core)
    core["review_id"] = "PRV-" + _hash(core)[:24].upper()
    core["review_hash"] = _hash(core)
    return core


def verify_promotion_review(
    review: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = dict(contract) if contract is not None else load_promotion_contract()
    if (
        not isinstance(review, Mapping)
        or review.get("schema_version") != REVIEW_SCHEMA_VERSION
    ):
        raise PromotionGateError("promotion_review_schema_invalid")
    if review.get("review_status") != "OPEN":
        raise PromotionGateError("promotion_review_must_be_open")
    if review.get("l12_contract_hash") != promotion_contract_hash(spec):
        raise PromotionGateError("promotion_review_contract_hash_mismatch")
    policy = review.get("requested_integration_contract") or {}
    if policy.get("vote_counting_allowed") is not False:
        raise PromotionGateError("promotion_review_vote_counting_forbidden")
    if policy.get("decision_layer_effect_active") is not False:
        raise PromotionGateError("promotion_review_decision_effect_forbidden")
    stored = _sha(review.get("review_hash"), "review_hash")
    body = dict(review)
    body.pop("review_hash", None)
    if _hash(body) != stored:
        raise PromotionGateError("promotion_review_hash_mismatch")
    id_body = dict(body)
    id_body.pop("review_id", None)
    if review.get("review_id") != "PRV-" + _hash(id_body)[:24].upper():
        raise PromotionGateError("promotion_review_id_mismatch")
    PatternDiscoveryBoundary().assert_research_payload(review)
    return {
        "valid": True,
        "review_id": review["review_id"],
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
    spec = dict(contract) if contract is not None else load_promotion_contract()
    verify_promotion_review(review, contract=spec)
    normalized = _text(decision, "decision").upper()
    if normalized not in set(spec["review"]["allowed_decisions"]):
        raise PromotionGateError(f"promotion_decision_not_allowed:{normalized}")
    reasons = sorted({_text(x, "reason_code") for x in reason_codes})
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
        "reviewed_at": _time(reviewed_at, "reviewed_at"),
        "reviewer_id": _text(reviewer_id, "reviewer_id"),
        "reviewer_role": _text(reviewer_role, "reviewer_role"),
        "reason_codes": reasons,
        "admission_contract": admission,
        "evidence_bindings": dict(review["evidence_bindings"]),
        "boundaries": dict(review["boundaries"]),
        "l12_contract_hash": promotion_contract_hash(spec),
    }
    PatternDiscoveryBoundary().assert_research_payload(core)
    core["decision_id"] = "PRD-" + _hash(core)[:24].upper()
    core["decision_hash"] = _hash(core)
    return core


def verify_promotion_decision(
    decision: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = dict(contract) if contract is not None else load_promotion_contract()
    if (
        not isinstance(decision, Mapping)
        or decision.get("schema_version") != DECISION_SCHEMA_VERSION
    ):
        raise PromotionGateError("promotion_decision_schema_invalid")
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
        for field, expected in (
            ("admitted_pattern_id", identity.get("pattern_id")),
            ("admitted_pattern_version", identity.get("pattern_version")),
            ("admitted_pattern_spec_hash", identity.get("pattern_spec_hash")),
        ):
            if admission.get(field) != expected:
                raise PromotionGateError(
                    f"promotion_exact_version_binding_mismatch:{field}"
                )
        if admission.get("vote_counting_allowed") is not False:
            raise PromotionGateError("promotion_vote_counting_forbidden")
        if admission.get("decision_layer_effect_active") is not False:
            raise PromotionGateError("promotion_decision_layer_effect_forbidden")
        if admission.get("requires_l13_before_activation") is not True:
            raise PromotionGateError("promotion_l13_gate_required")
    elif decision.get("admission_contract") is not None:
        raise PromotionGateError("nonapproval_cannot_have_admission_contract")
    stored = _sha(decision.get("decision_hash"), "decision_hash")
    body = dict(decision)
    body.pop("decision_hash", None)
    if _hash(body) != stored:
        raise PromotionGateError("promotion_decision_hash_mismatch")
    id_body = dict(body)
    id_body.pop("decision_id", None)
    if decision.get("decision_id") != "PRD-" + _hash(id_body)[:24].upper():
        raise PromotionGateError("promotion_decision_id_mismatch")
    PatternDiscoveryBoundary().assert_research_payload(decision)
    return {
        "valid": True,
        "decision_id": decision["decision_id"],
        "promotion_status": decision["promotion_status"],
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
    spec = dict(contract) if contract is not None else load_promotion_contract()
    verify_promotion_decision(decision, contract=spec)
    if decision.get("promotion_status") != "ADMITTED":
        return {
            "valid": False,
            "status": "NOT_ADMITTED",
            "reasons": ["PROMOTION_STATUS_NOT_ADMITTED"],
        }
    identity = _identity(frozen_pattern)
    if _key(identity) != _key(decision.get("pattern_identity") or {}):
        raise PromotionGateError("admission_pattern_identity_mismatch")
    rating = _rating(rating_history, identity)
    dependency = _dependency(dependency_graph, identity, spec)
    terminal = _terminal_l9(confirmation_reports, identity, spec)
    bindings = decision.get("evidence_bindings") or {}
    reasons: list[str] = []
    if rating["rating"] not in set(spec["eligibility"]["allowed_ratings"]):
        reasons.append(f"CURRENT_RATING_NOT_ELIGIBLE:{rating['rating']}")
    if rating["history_hash"] != bindings.get("rating_history_hash"):
        reasons.append("RATING_HISTORY_CHANGED_SINCE_REVIEW")
    if dependency["graph_hash"] != bindings.get("l6_graph_hash"):
        reasons.append("DEPENDENCY_GRAPH_CHANGED_SINCE_REVIEW")
    current_l9 = terminal["look_hash"] if terminal is not None else None
    if current_l9 != bindings.get("l9_look_hash"):
        reasons.append("CONFIRMATION_EVIDENCE_CHANGED_SINCE_REVIEW")
    if terminal is None:
        reasons.append("LATEST_TERMINAL_L9_RESULT_NOT_SUPPORTED")
    return {
        "valid": not reasons,
        "status": "CURRENT" if not reasons else "STALE_REVIEW_REQUIRED",
        "reasons": sorted(set(reasons)),
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


def persist_promotion_review(
    repo_root: str | Path,
    review: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = dict(contract) if contract is not None else load_promotion_contract()
    verify_promotion_review(review, contract=spec)
    root = Path(repo_root).resolve()
    repo_path = review_repo_path(review, contract=spec)
    PatternDiscoveryBoundary().assert_write_path_allowed(repo_path)
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
    if path.exists() and path.read_text(encoding="utf-8") != text:
        raise PromotionGateError("promotion_review_identity_collision")
    if not path.exists():
        path.write_text(text, encoding="utf-8", newline="\n")
    return {"valid": True, "review_path": repo_path}
