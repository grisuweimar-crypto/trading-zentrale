"""Phase L13 Decision-Layer Challenger Integration.

Promoted Pattern Discovery v2 evidence is compared in a shadow challenger
against the existing Decision Layer. Existing Decision semantics are read-only.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import math
import random
from pathlib import Path
from statistics import mean
from typing import Any, Mapping, Sequence

from scanner.research.decision_layer.input_contract import validate_input_packet
from scanner.research.decision_layer.relation_graph import packet_relation_graph
from scanner.research.decision_layer.universal_stance import compute_universal_stance

from .boundary import PatternDiscoveryBoundary
from .outcome_maturation import verify_matured_outcome
from .promotion_gate import (
    validate_admission_current,
    verify_promotion_decision,
)
from .promotion_registry import PromotionRegistry
from .prospective_capture import verify_prospective_claim

SCHEMA_VERSION = "pattern_discovery_l13_decision_challenger_v1"
TRACE_SCHEMA_VERSION = "pattern_discovery_l13_challenger_trace_v1"
EVALUATION_SCHEMA_VERSION = "pattern_discovery_l13_incremental_evaluation_v1"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "l13_decision_challenger_v1.json"
)


class ChallengerIntegrationError(ValueError):
    """Raised when an L13 challenger invariant is violated."""


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
        raise ChallengerIntegrationError(f"value_required:{field}")
    return result


def _timestamp(value: Any, field: str) -> str:
    text = _text(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    if len(normalized) == 10:
        normalized += "T00:00:00+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ChallengerIntegrationError(f"invalid_timestamp:{field}") from exc
    if parsed.tzinfo is None:
        raise ChallengerIntegrationError(f"timezone_required:{field}")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def load_challenger_contract(
    path: str | Path | None = None,
) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ChallengerIntegrationError(
            f"challenger_contract_unreadable:{target}"
        ) from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != SCHEMA_VERSION
    ):
        raise ChallengerIntegrationError("challenger_contract_schema_invalid")
    if payload.get("research_only") is not True:
        raise ChallengerIntegrationError("challenger_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise ChallengerIntegrationError(
            "challenger_productive_integration_forbidden"
        )
    if payload.get("execution_allowed") is not False:
        raise ChallengerIntegrationError("challenger_execution_forbidden")
    principles = payload.get("principles") or {}
    for field in (
        "baseline_decision_layer_is_immutable",
        "shadow_challenger_only",
        "no_immediate_action_change",
        "no_numeric_vote_counting",
        "correlated_patterns_must_not_be_double_counted",
        "dependency_aware_fusion_required",
        "horizons_never_mixed",
        "incremental_value_must_be_explicit",
        "regular_integration_requires_separate_positive_review",
    ):
        if principles.get(field) is not True:
            raise ChallengerIntegrationError(
                f"challenger_principle_missing:{field}"
            )
    return payload


def challenger_contract_hash(
    contract: Mapping[str, Any] | None = None,
) -> str:
    return _hash(
        dict(contract) if contract is not None else load_challenger_contract()
    )


def _direction(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if text == "positive":
        return "positive"
    if text == "negative":
        return "negative"
    if text.upper() == "POSITIVE":
        return "positive"
    if text.upper() == "NEGATIVE":
        return "negative"
    return None


def _state(directions: set[str]) -> str:
    if directions == {"positive"}:
        return "positive"
    if directions == {"negative"}:
        return "negative"
    if directions == {"positive", "negative"}:
        return "conflicted"
    return "insufficient_evidence"


def _timing_baseline(
    packet: Mapping[str, Any],
    relation: Mapping[str, Any],
    horizon: int,
) -> dict[str, Any]:
    eligible = set(relation.get("eligible_directional_claim_ids") or [])
    claims: list[dict[str, Any]] = []
    for row in packet.get("evidence") or []:
        if not isinstance(row, Mapping) or row.get("family") != "timing":
            continue
        payload = row.get("payload") or {}
        try:
            row_horizon = int(payload.get("horizon_sessions"))
        except (TypeError, ValueError):
            continue
        if row_horizon != horizon or row.get("claim_id") not in eligible:
            continue
        direction = _direction(payload.get("direction"))
        claims.append(
            {
                "claim_id": row.get("claim_id"),
                "pattern_id": payload.get("pattern_id"),
                "direction": direction,
            }
        )
    directions = {
        str(item["direction"])
        for item in claims
        if item.get("direction") in {"positive", "negative"}
    }
    return {
        "state": _state(directions),
        "direction": next(iter(directions)) if len(directions) == 1 else None,
        "claim_ids": [str(item["claim_id"]) for item in claims],
        "claims": claims,
        "numeric_vote_counting_used": False,
    }


def _admission_key(decision: Mapping[str, Any]) -> str:
    identity = decision.get("pattern_identity") or {}
    return "::".join(
        (
            _text(identity.get("pattern_id"), "pattern_id"),
            _text(identity.get("pattern_version"), "pattern_version"),
            _text(identity.get("pattern_spec_hash"), "pattern_spec_hash"),
        )
    )


def _validate_admission_context(
    context: Mapping[str, Any],
    registry: PromotionRegistry,
) -> tuple[str, dict[str, Any]]:
    if not isinstance(context, Mapping):
        raise ChallengerIntegrationError("admission_context_must_be_object")
    decision = context.get("decision")
    if not isinstance(decision, Mapping):
        raise ChallengerIntegrationError("admission_decision_required")
    verify_promotion_decision(decision)
    if decision.get("promotion_status") != "ADMITTED":
        raise ChallengerIntegrationError("l13_requires_admitted_l12_decision")
    admission = decision.get("admission_contract") or {}
    if admission.get("integration_mode") != "SHADOW_CHALLENGER":
        raise ChallengerIntegrationError(
            "l13_requires_shadow_challenger_admission"
        )
    if admission.get("directional_authority") != "SHADOW_ONLY":
        raise ChallengerIntegrationError(
            "l13_requires_shadow_only_directional_authority"
        )
    if admission.get("decision_layer_effect_active") is not False:
        raise ChallengerIntegrationError(
            "l12_admission_decision_effect_must_be_false"
        )
    if admission.get("requires_l13_before_activation") is not True:
        raise ChallengerIntegrationError("l12_l13_gate_missing")

    current = registry.current_status(decision["pattern_identity"])
    if (
        not isinstance(current, Mapping)
        or current.get("status") != "ADMITTED"
        or current.get("decision_id") != decision.get("decision_id")
    ):
        raise ChallengerIntegrationError(
            "l12_registry_state_not_currently_admitted"
        )
    required = (
        "frozen_pattern",
        "rating_history",
        "confirmation_reports",
        "dependency_graph",
    )
    if any(field not in context for field in required):
        raise ChallengerIntegrationError(
            "admission_current_context_incomplete"
        )
    check = validate_admission_current(
        decision,
        context["frozen_pattern"],
        context["rating_history"],
        context["confirmation_reports"],
        context["dependency_graph"],
    )
    if check.get("status") != "CURRENT" or check.get("valid") is not True:
        raise ChallengerIntegrationError(
            "l12_admission_stale_review_required"
        )
    return _admission_key(decision), {
        "decision": dict(decision),
        "dependency_graph": context["dependency_graph"],
        "current_validation": dict(check),
    }


def _claim_key(claim: Mapping[str, Any]) -> str:
    pattern = claim.get("pattern") or {}
    return "::".join(
        (
            _text(pattern.get("pattern_id"), "claim.pattern_id"),
            _text(pattern.get("pattern_version"), "claim.pattern_version"),
            _text(pattern.get("pattern_spec_hash"), "claim.pattern_spec_hash"),
        )
    )


def _active_claims(
    claims: Sequence[Mapping[str, Any]],
    *,
    admissions: Mapping[str, Mapping[str, Any]],
    symbol: str,
    snapshot_id: str,
    packet_as_of: str,
    horizon: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    active: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    seen: set[str] = set()
    packet_time = _timestamp(packet_as_of, "packet_as_of")
    for raw in claims:
        verify_prospective_claim(raw)
        claim = dict(raw)
        claim_id = _text(claim.get("claim_id"), "claim_id")
        if claim_id in seen:
            raise ChallengerIntegrationError(
                f"duplicate_l7_claim_id:{claim_id}"
            )
        seen.add(claim_id)
        match = claim.get("match") or {}
        forecast = claim.get("forecast") or {}
        if (
            match.get("symbol") != symbol
            or match.get("snapshot_id") != snapshot_id
            or int(forecast.get("horizon_sessions") or -1) != horizon
        ):
            continue
        if _timestamp(match.get("observation_as_of"), "claim.observation_as_of") != packet_time:
            raise ChallengerIntegrationError(
                f"l7_claim_packet_asof_mismatch:{claim_id}"
            )
        key = _claim_key(claim)
        admission_context = admissions.get(key)
        if admission_context is None:
            excluded.append(
                {
                    "claim_id": claim_id,
                    "pattern_key": key,
                    "reason": "NO_CURRENT_L12_SHADOW_ADMISSION",
                }
            )
            continue
        decision = admission_context["decision"]
        identity = decision["pattern_identity"]
        admission = decision["admission_contract"]
        direction = _direction(forecast.get("expected_direction"))
        if direction is None:
            raise ChallengerIntegrationError(
                f"unsupported_pattern_direction:{claim_id}"
            )
        if forecast.get("target_id") != identity.get("target_id"):
            raise ChallengerIntegrationError(
                f"claim_target_mismatch_admission:{claim_id}"
            )
        if int(forecast.get("horizon_sessions")) != int(
            identity.get("horizon_sessions")
        ):
            raise ChallengerIntegrationError(
                f"claim_horizon_mismatch_admission:{claim_id}"
            )
        if forecast.get("baseline") != identity.get("baseline"):
            raise ChallengerIntegrationError(
                f"claim_baseline_mismatch_admission:{claim_id}"
            )
        if direction != _direction(admission.get("direction")):
            raise ChallengerIntegrationError(
                f"claim_direction_mismatch_admission:{claim_id}"
            )
        active.append(
            {
                "claim_id": claim_id,
                "claim_hash": claim.get("claim_hash"),
                "pattern_id": identity["pattern_id"],
                "pattern_version": identity["pattern_version"],
                "pattern_spec_hash": identity["pattern_spec_hash"],
                "pattern_key": key,
                "direction": direction,
                "target_id": forecast.get("target_id"),
                "baseline": forecast.get("baseline"),
                "horizon_sessions": horizon,
                "l12_decision_id": decision.get("decision_id"),
                "l12_decision_hash": decision.get("decision_hash"),
                "l12_probability_attachment": admission.get(
                    "probability_attachment"
                ),
                "dependency_handling": admission.get("dependency_handling"),
            }
        )
    active.sort(
        key=lambda item: (
            item["pattern_id"],
            item["pattern_version"],
            item["claim_id"],
        )
    )
    scopes = {
        (
            item["target_id"],
            item["baseline"],
            item["horizon_sessions"],
        )
        for item in active
    }
    if len(scopes) > 1:
        raise ChallengerIntegrationError(
            "active_pattern_claims_target_scope_mismatch"
        )
    return active, excluded


def _single_dependency_graph(
    active: Sequence[Mapping[str, Any]],
    admissions: Mapping[str, Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    if not active:
        return None
    graphs: dict[str, Mapping[str, Any]] = {}
    for item in active:
        graph = admissions[item["pattern_key"]]["dependency_graph"]
        graph_hash = _text(graph.get("graph_hash"), "dependency_graph_hash")
        graphs[graph_hash] = graph
    if len(graphs) != 1:
        raise ChallengerIntegrationError(
            "active_admissions_require_single_current_dependency_graph"
        )
    return next(iter(graphs.values()))


def _pattern_clusters(
    active: Sequence[Mapping[str, Any]],
    graph: Mapping[str, Any] | None,
    contract: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not active:
        return [], []
    assert graph is not None
    nodes = {
        (
            str(node.get("pattern_id")),
            str(node.get("pattern_version")),
            str(node.get("pattern_spec_hash")),
        ): str(node.get("node_id"))
        for node in graph.get("nodes") or []
        if isinstance(node, Mapping)
    }
    node_to_key: dict[str, str] = {}
    parent: dict[str, str] = {}
    for item in active:
        identity = (
            item["pattern_id"],
            item["pattern_version"],
            item["pattern_spec_hash"],
        )
        node_id = nodes.get(identity)
        if not node_id:
            raise ChallengerIntegrationError(
                f"active_pattern_missing_from_l6_graph:{item['pattern_key']}"
            )
        node_to_key[node_id] = item["pattern_key"]
        parent[item["pattern_key"]] = item["pattern_key"]

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)

    collapsed_edges: list[dict[str, Any]] = []
    collapse_rel = set(contract["relation_graph"]["collapse_relationships"])
    collapse_sev = set(contract["relation_graph"]["collapse_severities"])
    for edge in graph.get("edges") or []:
        left = node_to_key.get(str(edge.get("source_node_id")))
        right = node_to_key.get(str(edge.get("target_node_id")))
        if left is None or right is None:
            continue
        relation = str(edge.get("relationship") or "")
        severity = str(edge.get("dependency_severity") or "")
        collapse = relation in collapse_rel or severity in collapse_sev
        collapsed_edges.append(
            {
                "left_pattern_key": left,
                "right_pattern_key": right,
                "relationship": relation,
                "dependency_severity": severity,
                "collapsed_into_same_evidence_cluster": collapse,
            }
        )
        if collapse:
            union(left, right)

    groups: dict[str, list[Mapping[str, Any]]] = {}
    for item in active:
        groups.setdefault(find(item["pattern_key"]), []).append(item)

    clusters: list[dict[str, Any]] = []
    for root in sorted(groups):
        members = sorted(
            groups[root],
            key=lambda item: (item["pattern_id"], item["pattern_version"]),
        )
        directions = {str(item["direction"]) for item in members}
        state = _state(directions)
        clusters.append(
            {
                "cluster_id": "PDC-" + _hash(
                    [item["pattern_key"] for item in members]
                )[:20].upper(),
                "member_pattern_keys": [
                    str(item["pattern_key"]) for item in members
                ],
                "member_claim_ids": [
                    str(item["claim_id"]) for item in members
                ],
                "direction_state": state,
                "direction": (
                    next(iter(directions))
                    if len(directions) == 1
                    else None
                ),
                "member_count": len(members),
                "counts_as_numeric_vote": False,
            }
        )
    return clusters, collapsed_edges


def _pattern_state(clusters: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not clusters:
        return {
            "state": "insufficient_evidence",
            "direction": None,
            "cluster_ids": [],
            "numeric_vote_counting_used": False,
        }
    if any(item.get("direction_state") == "conflicted" for item in clusters):
        return {
            "state": "conflicted",
            "direction": None,
            "cluster_ids": [str(x["cluster_id"]) for x in clusters],
            "numeric_vote_counting_used": False,
        }
    directions = {
        str(item.get("direction"))
        for item in clusters
        if item.get("direction") in {"positive", "negative"}
    }
    state = _state(directions)
    return {
        "state": state,
        "direction": next(iter(directions)) if len(directions) == 1 else None,
        "cluster_ids": [str(x["cluster_id"]) for x in clusters],
        "numeric_vote_counting_used": False,
    }


def _fuse_shadow(
    baseline: Mapping[str, Any],
    pattern: Mapping[str, Any],
) -> dict[str, Any]:
    b = str(baseline.get("state"))
    p = str(pattern.get("state"))
    if b == "conflicted":
        state = "conflicted"
        relation = "BASELINE_CONFLICT_PRESERVED"
    elif p == "conflicted":
        state = "conflicted"
        relation = "PATTERN_CONFLICT_EXPOSED"
    elif p == "insufficient_evidence":
        state = b
        relation = "NO_PATTERN_DIRECTION"
    elif b == "insufficient_evidence":
        state = p
        relation = "PATTERN_ADDS_SHADOW_DIRECTION"
    elif b == p:
        state = b
        relation = "PATTERN_CORROBORATES_TIMING"
    else:
        state = "conflicted"
        relation = "PATTERN_CONFLICTS_WITH_TIMING"
    return {
        "state": state,
        "direction": state if state in {"positive", "negative"} else None,
        "relation": relation,
        "action_change_allowed": False,
    }


def build_challenger_trace(
    decision_packet: Mapping[str, Any],
    l7_claims: Sequence[Mapping[str, Any]],
    admission_contexts: Sequence[Mapping[str, Any]],
    *,
    promotion_registry: PromotionRegistry,
    horizon_sessions: int,
    generated_at: str,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one same-snapshot, same-horizon research-only challenger trace."""
    spec = dict(contract) if contract is not None else load_challenger_contract()
    horizon = int(horizon_sessions)
    if horizon not in set(spec["inputs"]["allowed_horizons_sessions"]):
        raise ChallengerIntegrationError(f"unsupported_horizon:{horizon}")
    packet = validate_input_packet(decision_packet)
    relation = packet_relation_graph(packet)
    decision_context = compute_universal_stance(packet)
    baseline_stance = decision_context.get("universal_stance") or {}

    admissions: dict[str, dict[str, Any]] = {}
    for raw in admission_contexts:
        key, validated = _validate_admission_context(
            raw, promotion_registry
        )
        if key in admissions:
            raise ChallengerIntegrationError(
                f"duplicate_l12_admission_context:{key}"
            )
        admissions[key] = validated

    active, excluded = _active_claims(
        l7_claims,
        admissions=admissions,
        symbol=str(packet["symbol"]),
        snapshot_id=str(packet["source_snapshot_id"]),
        packet_as_of=str(packet["as_of"]),
        horizon=horizon,
    )
    graph = _single_dependency_graph(active, admissions)
    clusters, dependency_edges = _pattern_clusters(active, graph, spec)
    pattern_state = _pattern_state(clusters)
    baseline_timing = _timing_baseline(packet, relation, horizon)
    fused = _fuse_shadow(baseline_timing, pattern_state)

    baseline_direction = baseline_stance.get("direction")
    if baseline_direction in {"positive", "negative"}:
        if pattern_state["direction"] == baseline_direction:
            decision_relation = "CORROBORATES_BASELINE_DECISION_DIRECTION"
        elif pattern_state["direction"] in {"positive", "negative"}:
            decision_relation = "CONFLICTS_WITH_BASELINE_DECISION_DIRECTION"
        elif pattern_state["state"] == "conflicted":
            decision_relation = "PATTERN_EVIDENCE_CONFLICTED"
        else:
            decision_relation = "NO_PATTERN_DIRECTION"
    else:
        decision_relation = "BASELINE_DECISION_NONDIRECTIONAL"

    target_scope = (
        {
            "target_id": active[0]["target_id"],
            "baseline": active[0]["baseline"],
            "horizon_sessions": horizon,
        }
        if active
        else {
            "target_id": None,
            "baseline": None,
            "horizon_sessions": horizon,
        }
    )
    trace: dict[str, Any] = {
        "schema_version": TRACE_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L13",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "generated_at": _timestamp(generated_at, "generated_at"),
        "symbol": packet["symbol"],
        "observation_as_of": _timestamp(packet["as_of"], "packet.as_of"),
        "snapshot_id": packet["source_snapshot_id"],
        "horizon_sessions": horizon,
        "target_scope": target_scope,
        "baseline_decision_context": {
            "state": baseline_stance.get("state"),
            "direction": baseline_direction,
            "source_schema": decision_context.get("schema_version"),
            "packet_hash": _hash(packet),
            "relation_graph_hash": _hash(relation),
            "mutated_by_l13": False,
        },
        "timing_ablation": {
            "comparison": spec["ablation"]["comparison"],
            "baseline_timing": baseline_timing,
            "pattern_challenger": pattern_state,
            "shadow_fused_timing": fused,
        },
        "relation_graph": {
            "pattern_clusters": clusters,
            "pattern_dependency_edges": dependency_edges,
            "pattern_vs_baseline_decision": decision_relation,
            "numeric_vote_counting_used": False,
            "correlated_pattern_double_counting_allowed": False,
        },
        "active_pattern_claims": active,
        "excluded_pattern_claims": excluded,
        "admission_decision_ids": sorted(
            {
                str(item["l12_decision_id"])
                for item in active
            }
        ),
        "boundaries": {
            "decision_packet_mutated": False,
            "existing_relation_graph_mutated": False,
            "universal_stance_mutated": False,
            "portfolio_action_mutated": False,
            "scanner_score_change_performed": False,
            "productive_decision_change_performed": False,
            "execution_effect_created": False,
            "automatic_regular_integration_performed": False,
        },
        "l13_contract_hash": challenger_contract_hash(spec),
    }
    PatternDiscoveryBoundary().assert_research_payload(trace)
    trace["trace_hash"] = _hash(trace)
    return trace


def verify_challenger_trace(
    trace: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = dict(contract) if contract is not None else load_challenger_contract()
    if (
        not isinstance(trace, Mapping)
        or trace.get("schema_version") != TRACE_SCHEMA_VERSION
    ):
        raise ChallengerIntegrationError("challenger_trace_schema_invalid")
    if trace.get("research_only") is not True:
        raise ChallengerIntegrationError("challenger_trace_research_only_missing")
    if trace.get("productive_integration_enabled") is not False:
        raise ChallengerIntegrationError(
            "challenger_trace_productive_integration_forbidden"
        )
    if trace.get("execution_allowed") is not False:
        raise ChallengerIntegrationError(
            "challenger_trace_execution_forbidden"
        )
    if trace.get("l13_contract_hash") != challenger_contract_hash(spec):
        raise ChallengerIntegrationError(
            "challenger_trace_contract_hash_mismatch"
        )
    if trace.get("relation_graph", {}).get("numeric_vote_counting_used") is not False:
        raise ChallengerIntegrationError("challenger_vote_counting_forbidden")
    if (
        trace.get("relation_graph", {}).get(
            "correlated_pattern_double_counting_allowed"
        )
        is not False
    ):
        raise ChallengerIntegrationError(
            "challenger_correlated_double_counting_forbidden"
        )
    for field, value in (trace.get("boundaries") or {}).items():
        if value is not False:
            raise ChallengerIntegrationError(
                f"challenger_boundary_invalid:{field}"
            )
    stored = _text(trace.get("trace_hash"), "trace_hash")
    body = dict(trace)
    body.pop("trace_hash", None)
    if _hash(body) != stored:
        raise ChallengerIntegrationError("challenger_trace_hash_mismatch")
    PatternDiscoveryBoundary().assert_research_payload(trace)
    return {
        "valid": True,
        "trace_hash": stored,
        "symbol": trace.get("symbol"),
        "horizon_sessions": trace.get("horizon_sessions"),
    }


def _aligned(value: float, direction: str) -> float:
    return value if direction == "positive" else -value


def _support_regions(dates: Sequence[str], block_length: int) -> int:
    unique = sorted(set(dates))
    if not unique:
        return 0
    positions = list(range(len(unique)))
    count = 0
    cursor = 0
    for position in positions:
        if position < cursor:
            continue
        count += 1
        cursor = position + block_length
    return count


def _bootstrap_intervals(
    rows: Sequence[Mapping[str, Any]],
    *,
    horizon: int,
    repetitions: int,
    seed: int,
    interval: Sequence[float],
) -> dict[str, Any]:
    dates = sorted({str(row["observation_date"]) for row in rows})
    if not dates or not rows:
        return {
            "hit_rate_lift_95": None,
            "mean_aligned_outcome_lift_95": None,
        }
    by_date: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        by_date.setdefault(str(row["observation_date"]), []).append(row)
    block_length = max(1, 2 * horizon)
    blocks = [
        [dates[(start + offset) % len(dates)] for offset in range(block_length)]
        for start in range(len(dates))
    ]
    rng = random.Random(seed + horizon)
    draws = max(1, math.ceil(len(dates) / block_length))
    hit_lifts: list[float] = []
    outcome_lifts: list[float] = []
    for _ in range(repetitions):
        sampled_dates: list[str] = []
        for _draw in range(draws):
            sampled_dates.extend(blocks[rng.randrange(len(blocks))])
        sampled_dates = sampled_dates[: len(dates)]
        sample: list[Mapping[str, Any]] = []
        for day in sampled_dates:
            sample.extend(by_date.get(day, []))
        if not sample:
            continue
        hit_lifts.append(
            mean(float(row["challenger_hit"]) for row in sample)
            - mean(float(row["baseline_hit"]) for row in sample)
        )
        outcome_lifts.append(
            mean(float(row["challenger_aligned"]) for row in sample)
            - mean(float(row["baseline_aligned"]) for row in sample)
        )

    def quantile(values: list[float], q: float) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        position = q * (len(ordered) - 1)
        low = math.floor(position)
        high = math.ceil(position)
        if low == high:
            return float(ordered[low])
        weight = position - low
        return float(
            ordered[low] * (1.0 - weight) + ordered[high] * weight
        )

    low_q, high_q = float(interval[0]), float(interval[1])
    return {
        "hit_rate_lift_95": [
            quantile(hit_lifts, low_q),
            quantile(hit_lifts, high_q),
        ],
        "mean_aligned_outcome_lift_95": [
            quantile(outcome_lifts, low_q),
            quantile(outcome_lifts, high_q),
        ],
    }


def evaluate_incremental_value(
    traces: Sequence[Mapping[str, Any]],
    matured_outcomes: Sequence[Mapping[str, Any]],
    *,
    horizon_sessions: int,
    evaluated_at: str,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate pattern-only challenger value versus existing Timing evidence."""
    spec = dict(contract) if contract is not None else load_challenger_contract()
    horizon = int(horizon_sessions)
    if horizon not in set(spec["inputs"]["allowed_horizons_sessions"]):
        raise ChallengerIntegrationError(f"unsupported_horizon:{horizon}")

    valid_traces: list[Mapping[str, Any]] = []
    seen_traces: set[str] = set()
    for trace in traces:
        verify_challenger_trace(trace, contract=spec)
        if int(trace.get("horizon_sessions")) != horizon:
            continue
        trace_hash = str(trace["trace_hash"])
        if trace_hash in seen_traces:
            raise ChallengerIntegrationError(
                f"duplicate_challenger_trace:{trace_hash}"
            )
        seen_traces.add(trace_hash)
        valid_traces.append(trace)

    outcomes_by_claim: dict[str, Mapping[str, Any]] = {}
    for outcome in matured_outcomes:
        verification = verify_matured_outcome(outcome)
        claim_id = str(verification["claim_id"])
        if claim_id in outcomes_by_claim:
            raise ChallengerIntegrationError(
                f"duplicate_matured_outcome_claim:{claim_id}"
            )
        outcomes_by_claim[claim_id] = outcome

    paired: list[dict[str, Any]] = []
    immature_trace_hashes: list[str] = []
    added_direction_n = 0
    conflict_created_n = 0
    for trace in valid_traces:
        active = list(trace.get("active_pattern_claims") or [])
        if not active:
            continue
        claim_ids = [str(item["claim_id"]) for item in active]
        selected = [outcomes_by_claim.get(cid) for cid in claim_ids]
        if any(item is None for item in selected):
            immature_trace_hashes.append(str(trace["trace_hash"]))
            continue
        records = [item for item in selected if item is not None]
        values = [float(item["outcome"]["target_value"]) for item in records]
        first = records[0]
        first_target = first.get("target") or {}
        first_claim = first.get("claim") or {}
        for item, value in zip(records[1:], values[1:]):
            target = item.get("target") or {}
            claim = item.get("claim") or {}
            if (
                int(target.get("horizon_sessions") or -1) != horizon
                or target.get("target_id") != first_target.get("target_id")
                or target.get("baseline") != first_target.get("baseline")
                or claim.get("symbol") != first_claim.get("symbol")
                or claim.get("capture_snapshot_id")
                != first_claim.get("capture_snapshot_id")
                or not math.isclose(value, values[0], rel_tol=0.0, abs_tol=1e-12)
            ):
                raise ChallengerIntegrationError(
                    "l8_outcomes_disagree_within_challenger_trace"
                )
        if first_claim.get("symbol") != trace.get("symbol"):
            raise ChallengerIntegrationError(
                "l8_outcome_symbol_mismatch_trace"
            )
        if first_claim.get("capture_snapshot_id") != trace.get("snapshot_id"):
            raise ChallengerIntegrationError(
                "l8_outcome_snapshot_mismatch_trace"
            )

        baseline = trace["timing_ablation"]["baseline_timing"]
        challenger = trace["timing_ablation"]["pattern_challenger"]
        bdir = baseline.get("direction")
        cdir = challenger.get("direction")
        fused_state = trace["timing_ablation"]["shadow_fused_timing"]["state"]
        if bdir not in {"positive", "negative"} and cdir in {
            "positive",
            "negative",
        }:
            added_direction_n += 1
        if bdir in {"positive", "negative"} and fused_state == "conflicted":
            conflict_created_n += 1
        if bdir not in {"positive", "negative"} or cdir not in {
            "positive",
            "negative",
        }:
            continue
        target_value = values[0]
        paired.append(
            {
                "trace_hash": trace["trace_hash"],
                "symbol": trace["symbol"],
                "observation_date": str(trace["observation_as_of"])[:10],
                "baseline_direction": bdir,
                "challenger_direction": cdir,
                "disagreement": bdir != cdir,
                "target_value": target_value,
                "baseline_hit": _aligned(target_value, bdir) > 0,
                "challenger_hit": _aligned(target_value, cdir) > 0,
                "baseline_aligned": _aligned(target_value, bdir),
                "challenger_aligned": _aligned(target_value, cdir),
            }
        )

    disagreement = [row for row in paired if row["disagreement"]]
    block_length = int(
        spec["ablation"]["block_length_sessions_multiplier"]
    ) * horizon
    support_regions = _support_regions(
        [str(row["observation_date"]) for row in paired],
        block_length,
    )
    paired_n = len(paired)
    disagreement_n = len(disagreement)
    baseline_hit = (
        mean(float(row["baseline_hit"]) for row in paired)
        if paired
        else None
    )
    challenger_hit = (
        mean(float(row["challenger_hit"]) for row in paired)
        if paired
        else None
    )
    baseline_aligned = (
        mean(float(row["baseline_aligned"]) for row in paired)
        if paired
        else None
    )
    challenger_aligned = (
        mean(float(row["challenger_aligned"]) for row in paired)
        if paired
        else None
    )
    hit_lift = (
        challenger_hit - baseline_hit
        if challenger_hit is not None and baseline_hit is not None
        else None
    )
    aligned_lift = (
        challenger_aligned - baseline_aligned
        if challenger_aligned is not None and baseline_aligned is not None
        else None
    )
    bootstrap = _bootstrap_intervals(
        paired,
        horizon=horizon,
        repetitions=int(spec["ablation"]["bootstrap_repetitions"]),
        seed=int(spec["ablation"]["random_seed"]),
        interval=spec["ablation"]["bootstrap_interval"],
    )
    min_n = int(spec["ablation"]["minimum_paired_n"])
    min_disagreement = int(spec["ablation"]["minimum_disagreement_n"])
    min_support = int(
        spec["ablation"]["minimum_temporal_support_regions"]
    )
    sufficient = (
        paired_n >= min_n
        and disagreement_n >= min_disagreement
        and support_regions >= min_support
    )
    hit_interval = bootstrap["hit_rate_lift_95"]
    outcome_interval = bootstrap["mean_aligned_outcome_lift_95"]
    positive = (
        sufficient
        and hit_lift is not None
        and aligned_lift is not None
        and hit_lift > 0
        and aligned_lift > 0
        and isinstance(hit_interval, list)
        and isinstance(outcome_interval, list)
        and hit_interval[0] is not None
        and outcome_interval[0] is not None
        and float(hit_interval[0]) > 0
        and float(outcome_interval[0]) > 0
    )
    status = (
        "INSUFFICIENT_SUPPORT"
        if not sufficient
        else (
            "INCREMENTAL_VALUE_CANDIDATE"
            if positive
            else "NO_INCREMENTAL_VALUE"
        )
    )
    evaluation: dict[str, Any] = {
        "schema_version": EVALUATION_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L13",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "evaluated_at": _timestamp(evaluated_at, "evaluated_at"),
        "horizon_sessions": horizon,
        "comparison": spec["ablation"]["comparison"],
        "support": {
            "trace_count": len(valid_traces),
            "paired_directional_n": paired_n,
            "disagreement_n": disagreement_n,
            "added_direction_n": added_direction_n,
            "conflict_created_n": conflict_created_n,
            "immature_trace_count": len(immature_trace_hashes),
            "immature_trace_hashes": sorted(immature_trace_hashes),
            "temporal_support_regions": support_regions,
            "block_length_sessions": block_length,
        },
        "metrics": {
            "baseline_directional_hit_rate": baseline_hit,
            "challenger_directional_hit_rate": challenger_hit,
            "directional_hit_rate_lift": hit_lift,
            "baseline_mean_aligned_outcome": baseline_aligned,
            "challenger_mean_aligned_outcome": challenger_aligned,
            "mean_aligned_outcome_lift": aligned_lift,
            "bootstrap": bootstrap,
        },
        "incremental_value_status": status,
        "positive_incremental_evidence": positive,
        "regular_integration": {
            "approved": False,
            "automatic": False,
            "status": "SEPARATE_POSITIVE_REVIEW_REQUIRED",
        },
        "trace_hashes": sorted(str(x["trace_hash"]) for x in valid_traces),
        "outcome_hashes": sorted(
            str(item.get("outcome_hash")) for item in matured_outcomes
        ),
        "boundaries": {
            "decision_packet_mutated": False,
            "existing_relation_graph_mutated": False,
            "universal_stance_mutated": False,
            "portfolio_action_mutated": False,
            "scanner_score_change_performed": False,
            "productive_decision_change_performed": False,
            "execution_effect_created": False,
            "automatic_regular_integration_performed": False,
        },
        "l13_contract_hash": challenger_contract_hash(spec),
    }
    PatternDiscoveryBoundary().assert_research_payload(evaluation)
    evaluation["evaluation_hash"] = _hash(evaluation)
    return evaluation


def verify_incremental_evaluation(
    evaluation: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = dict(contract) if contract is not None else load_challenger_contract()
    if (
        not isinstance(evaluation, Mapping)
        or evaluation.get("schema_version") != EVALUATION_SCHEMA_VERSION
    ):
        raise ChallengerIntegrationError(
            "incremental_evaluation_schema_invalid"
        )
    if evaluation.get("productive_integration_enabled") is not False:
        raise ChallengerIntegrationError(
            "incremental_evaluation_productive_integration_forbidden"
        )
    if evaluation.get("execution_allowed") is not False:
        raise ChallengerIntegrationError(
            "incremental_evaluation_execution_forbidden"
        )
    if evaluation.get("incremental_value_status") not in set(
        spec["ablation"]["statuses"]
    ):
        raise ChallengerIntegrationError(
            "incremental_evaluation_status_invalid"
        )
    regular = evaluation.get("regular_integration") or {}
    if regular.get("approved") is not False or regular.get("automatic") is not False:
        raise ChallengerIntegrationError(
            "l13_regular_integration_must_remain_closed"
        )
    if evaluation.get("l13_contract_hash") != challenger_contract_hash(spec):
        raise ChallengerIntegrationError(
            "incremental_evaluation_contract_hash_mismatch"
        )
    for field, value in (evaluation.get("boundaries") or {}).items():
        if value is not False:
            raise ChallengerIntegrationError(
                f"incremental_evaluation_boundary_invalid:{field}"
            )
    stored = _text(evaluation.get("evaluation_hash"), "evaluation_hash")
    body = dict(evaluation)
    body.pop("evaluation_hash", None)
    if _hash(body) != stored:
        raise ChallengerIntegrationError(
            "incremental_evaluation_hash_mismatch"
        )
    PatternDiscoveryBoundary().assert_research_payload(evaluation)
    return {
        "valid": True,
        "evaluation_hash": stored,
        "status": evaluation["incremental_value_status"],
    }


def trace_repo_path(
    trace: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = dict(contract) if contract is not None else load_challenger_contract()
    return str(spec["storage"]["trace_path_template"]).format(
        snapshot_id=_text(trace.get("snapshot_id"), "snapshot_id"),
        symbol=_text(trace.get("symbol"), "symbol"),
        horizon_sessions=int(trace.get("horizon_sessions")),
        trace_hash=_text(trace.get("trace_hash"), "trace_hash"),
    )


def evaluation_repo_path(
    evaluation: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = dict(contract) if contract is not None else load_challenger_contract()
    return str(spec["storage"]["evaluation_path_template"]).format(
        horizon_sessions=int(evaluation.get("horizon_sessions")),
        evaluation_hash=_text(
            evaluation.get("evaluation_hash"), "evaluation_hash"
        ),
    )


def _persist(
    repo_root: str | Path,
    repo_path: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    PatternDiscoveryBoundary().assert_write_path_allowed(repo_path)
    path = (root / repo_path).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ChallengerIntegrationError(
            "challenger_path_outside_repo"
        ) from exc
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(
        dict(payload),
        indent=2,
        sort_keys=True,
        ensure_ascii=True,
        allow_nan=False,
    ) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") != text:
        raise ChallengerIntegrationError(
            "challenger_artifact_identity_collision"
        )
    if not path.exists():
        path.write_text(text, encoding="utf-8", newline="\n")
    return {"valid": True, "path": repo_path}


def persist_challenger_trace(
    repo_root: str | Path,
    trace: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = dict(contract) if contract is not None else load_challenger_contract()
    verify_challenger_trace(trace, contract=spec)
    return _persist(repo_root, trace_repo_path(trace, contract=spec), trace)


def persist_incremental_evaluation(
    repo_root: str | Path,
    evaluation: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = dict(contract) if contract is not None else load_challenger_contract()
    verify_incremental_evaluation(evaluation, contract=spec)
    return _persist(
        repo_root,
        evaluation_repo_path(evaluation, contract=spec),
        evaluation,
    )
