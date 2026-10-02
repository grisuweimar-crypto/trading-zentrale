"""Phase 8I-C outcome-blind external aggregation design.

8I-C freezes only deterministic aggregation semantics.  Real component direction
state generation and real bound-component aggregation remain disabled.  The
implementation below is intentionally usable only for synthetic contract tests.
It never reads forward outcomes, changes Phase 7, changes portfolio actions, or
authorizes orders/trades.
"""
from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
import re
from typing import Any, Mapping, Sequence

from scanner.research.external_evidence.decision_binding_8i import (
    BINDING_RESULT_SCHEMA,
    BOUND,
    digest as binding_digest,
    validate_binding_contract,
)


AGGREGATION_CONTRACT_SCHEMA = "external_evidence_8i_external_aggregation_design_v1"
AGGREGATION_RESULT_SCHEMA = "external_evidence_8i_external_aggregation_result_v1"

POSITIVE = "POSITIVE"
NEGATIVE = "NEGATIVE"
MIXED = "MIXED"
UNKNOWN = "UNKNOWN"
INSUFFICIENT = "INSUFFICIENT_EXTERNAL"

USABLE = "USABLE"
UNKNOWN_VALID = "UNKNOWN_VALID"
UNAVAILABLE = "UNAVAILABLE"

ALLOWED_COMPONENT_TYPES = {"8g_main_effect", "8h_interaction"}
ALLOWED_OBSERVATION_STATUSES = {USABLE, UNKNOWN_VALID, UNAVAILABLE}
ALLOWED_RAW_DIRECTIONS = {POSITIVE, NEGATIVE, UNKNOWN}
ALLOWED_EXTERNAL_STATES = {POSITIVE, NEGATIVE, MIXED, UNKNOWN, INSUFFICIENT}
FORBIDDEN_OBSERVATION_KEY_PARTS = (
    "peer_excess",
    "adverse_excursion",
    "path_max_drawdown",
    "future_return",
    "forward_return",
    "outcome",
    "label_",
    "portfolio_action",
    "trade_decision",
    "order_instruction",
    "target_weight",
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class ExternalEvidence8IAggregationError(ValueError):
    """Raised when the frozen 8I-C aggregation boundary is violated."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _parse_time(value: object, error: str) -> datetime:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        raise ExternalEvidence8IAggregationError(error)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ExternalEvidence8IAggregationError(error) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExternalEvidence8IAggregationError(error + "_timezone_required")
    return parsed


def _require_sha256(value: object, error: str) -> str:
    text = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(text):
        raise ExternalEvidence8IAggregationError(error)
    return text


def _verify_binding(binding: Mapping[str, Any]) -> str:
    if binding.get("schema_version") != BINDING_RESULT_SCHEMA:
        raise ExternalEvidence8IAggregationError("8i_c_binding_schema_mismatch")
    if binding.get("phase") != "8I-B" or binding.get("state") != BOUND:
        raise ExternalEvidence8IAggregationError("8i_c_component_not_bound_for_8i_research_only")
    recorded = _require_sha256(binding.get("binding_sha256"), "8i_c_binding_sha256_required")
    payload = dict(binding)
    payload.pop("binding_sha256", None)
    if binding_digest(payload) != recorded:
        raise ExternalEvidence8IAggregationError("8i_c_binding_digest_mismatch")
    for key in (
        "external_decision_influence_enabled",
        "decision_outcome_access_authorized",
        "phase7_mutation_authorized",
        "productive_integration_enabled",
        "portfolio_action_change_authorized",
        "orders_or_trades_authorized",
    ):
        if binding.get(key) is not False:
            raise ExternalEvidence8IAggregationError(f"8i_c_binding_scope_drift:{key}")
    return recorded


def validate_aggregation_contract(
    contract: Mapping[str, Any],
    binding_contract: Mapping[str, Any],
    conflict_matrix: Mapping[str, Any],
) -> None:
    if contract.get("schema_version") != AGGREGATION_CONTRACT_SCHEMA or contract.get("phase") != "8I-C":
        raise ExternalEvidence8IAggregationError("unsupported_8i_c_contract")
    if contract.get("research_only") is not True or contract.get("shadow_mode") is not True:
        raise ExternalEvidence8IAggregationError("8i_c_must_be_research_only_shadow")
    for key in (
        "productive_integration_enabled",
        "phase7_mutation_enabled",
        "external_state_engine_enabled",
        "extended_reliability_enabled",
        "extended_stance_enabled",
        "portfolio_action_change_enabled",
        "orders_or_trades_enabled",
        "real_decision_outcome_read_allowed",
    ):
        if contract.get(key) is not False:
            raise ExternalEvidence8IAggregationError(f"8i_c_boundary_must_remain_false:{key}")

    validate_binding_contract(binding_contract)
    if binding_contract.get("outcome_blind_boundary", {}).get("aggregation_belongs_to") != "8I-C":
        raise ExternalEvidence8IAggregationError("8i_b_does_not_delegate_aggregation_to_8i_c")
    if binding_contract.get("authorization_boundary", {}).get("maximum_8i_b_state") != BOUND:
        raise ExternalEvidence8IAggregationError("8i_b_bound_state_drift")

    if conflict_matrix.get("schema_version") != "external_conflict_matrix_v1":
        raise ExternalEvidence8IAggregationError("8i_c_conflict_matrix_required")
    if conflict_matrix.get("status") != "descriptive_not_policy":
        raise ExternalEvidence8IAggregationError("8i_c_conflict_matrix_must_remain_descriptive")
    relation = contract.get("relation_mapping") or {}
    if list(relation.get("allowed_core_states") or ()) != list(conflict_matrix.get("core_states") or ()):
        raise ExternalEvidence8IAggregationError("8i_c_core_state_vocabulary_drift")
    if list(relation.get("allowed_external_states") or ()) != list(conflict_matrix.get("external_states") or ()):
        raise ExternalEvidence8IAggregationError("8i_c_external_state_vocabulary_drift")
    matrix_relations = sorted({str(row.get("relation")) for row in conflict_matrix.get("relations") or ()})
    if sorted(relation.get("allowed_relations") or ()) != matrix_relations:
        raise ExternalEvidence8IAggregationError("8i_c_relation_vocabulary_drift")
    if relation.get("relation_is_descriptive_only") is not True:
        raise ExternalEvidence8IAggregationError("8i_c_relation_must_remain_descriptive")
    for key in ("confirming_may_promote_phase7_stance", "conflicting_may_demote_phase7_stance", "external_only_is_trade_signal"):
        if relation.get(key) is not False:
            raise ExternalEvidence8IAggregationError(f"8i_c_relation_policy_leak:{key}")

    reducer = contract.get("state_reducer") or {}
    if reducer.get("method") != "SET_SEMANTICS_NO_COUNTS_NO_WEIGHTS":
        raise ExternalEvidence8IAggregationError("8i_c_set_reducer_required")
    for key in (
        "multiplicity_changes_result",
        "component_order_changes_result",
        "majority_vote_allowed",
        "numeric_vote_counting_allowed",
        "arbitrary_weights_allowed",
        "significance_weighting_allowed",
        "confidence_weighting_allowed",
        "p_value_weighting_allowed",
    ):
        if reducer.get(key) is not False:
            raise ExternalEvidence8IAggregationError(f"8i_c_forbidden_reducer_property:{key}")

    direction = contract.get("component_direction_boundary") or {}
    for key in (
        "8i_c_derives_direction_from_raw_factor_values",
        "8i_c_derives_direction_from_model_coefficients",
        "8i_c_derives_direction_from_p_values",
        "8i_c_selects_zero_or_nonzero_threshold",
        "8i_c_selects_sign_convention",
    ):
        if direction.get(key) is not False:
            raise ExternalEvidence8IAggregationError(f"8i_c_direction_boundary_drift:{key}")
    if direction.get("direction_state_must_be_supplied_by_versioned_outcome_blind_adapter") is not True:
        raise ExternalEvidence8IAggregationError("8i_c_versioned_direction_adapter_required")
    if direction.get("real_direction_adapter_current_state") != "NOT_YET_FROZEN":
        raise ExternalEvidence8IAggregationError("8i_c_real_direction_adapter_must_remain_unfrozen")

    guards = contract.get("outcome_blind_guards") or {}
    if any(value is not False for value in guards.values()):
        raise ExternalEvidence8IAggregationError("8i_c_outcome_blind_guards_must_all_remain_false")


def reduce_direction_states(states: Sequence[str]) -> tuple[str, list[str], bool]:
    """Reduce states using set semantics only; multiplicity and order are irrelevant."""
    known: set[str] = set()
    unknown_present = False
    any_included = False
    for raw in states:
        state = str(raw)
        if state == INSUFFICIENT:
            continue
        any_included = True
        if state == POSITIVE:
            known.add(POSITIVE)
        elif state == NEGATIVE:
            known.add(NEGATIVE)
        elif state == MIXED:
            known.update((POSITIVE, NEGATIVE))
        elif state == UNKNOWN:
            unknown_present = True
        else:
            raise ExternalEvidence8IAggregationError(f"8i_c_unknown_direction_state:{state}")

    ordered_known = [state for state in (POSITIVE, NEGATIVE) if state in known]
    if not any_included:
        return INSUFFICIENT, ordered_known, False
    if known == {POSITIVE, NEGATIVE}:
        return MIXED, ordered_known, unknown_present
    if unknown_present:
        return UNKNOWN, ordered_known, True
    if known == {POSITIVE}:
        return POSITIVE, ordered_known, False
    if known == {NEGATIVE}:
        return NEGATIVE, ordered_known, False
    return INSUFFICIENT, ordered_known, False


def classify_relation(
    *, core_state: str, external_state: str, conflict_matrix: Mapping[str, Any]
) -> str:
    if external_state not in ALLOWED_EXTERNAL_STATES:
        raise ExternalEvidence8IAggregationError("8i_c_external_state_not_in_vocabulary")
    matches = [
        row for row in conflict_matrix.get("relations") or ()
        if str(row.get("core")) == str(core_state) and str(row.get("external")) == external_state
    ]
    if len(matches) != 1:
        raise ExternalEvidence8IAggregationError("8i_c_exact_conflict_matrix_relation_required")
    return str(matches[0]["relation"])


def current_aggregation_status(
    contract: Mapping[str, Any],
    binding_contract: Mapping[str, Any],
    conflict_matrix: Mapping[str, Any],
) -> dict[str, Any]:
    validate_aggregation_contract(contract, binding_contract, conflict_matrix)
    current = dict(contract.get("current_repository_state") or {})
    result = {
        "schema_version": AGGREGATION_RESULT_SCHEMA,
        "phase": "8I-C",
        "state": str(current.get("state") or ""),
        "synthetic": False,
        "bound_component_ids": list(current.get("bound_component_ids") or ()),
        "dependency_bundles": [],
        "bundle_states": {},
        "included_component_ids": [],
        "excluded_component_ids_with_reasons": {},
        "known_direction_set": [],
        "unknown_present": False,
        "external_direction_state": INSUFFICIENT,
        "external_evidence_state": INSUFFICIENT,
        "relation_state_if_core_state_supplied": None,
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
        "real_aggregation_authorized": False,
        "external_decision_influence_enabled": False,
        "phase7_mutation_authorized": False,
        "extended_reliability_enabled": False,
        "extended_stance_enabled": False,
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
    }
    result["aggregation_sha256"] = _digest(result)
    return result


def _validate_observation_keys(row: Mapping[str, Any]) -> None:
    for key in row:
        if key == "binding":
            continue
        lowered = str(key).lower()
        if any(part in lowered for part in FORBIDDEN_OBSERVATION_KEY_PARTS):
            raise ExternalEvidence8IAggregationError(f"8i_c_outcome_or_decision_input_forbidden:{key}")


def _component_state(row: Mapping[str, Any], generated_at: datetime) -> tuple[str | None, str | None]:
    _validate_observation_keys(row)
    status = str(row.get("component_observation_status") or "")
    if status not in ALLOWED_OBSERVATION_STATUSES:
        raise ExternalEvidence8IAggregationError("8i_c_component_observation_status_invalid")

    adapter_id = str(row.get("direction_adapter_id") or "").strip()
    adapter_version = str(row.get("direction_adapter_version") or "").strip()
    _require_sha256(row.get("direction_adapter_sha256"), "8i_c_direction_adapter_sha256_required")
    if not adapter_id or not adapter_version:
        raise ExternalEvidence8IAggregationError("8i_c_direction_adapter_identity_required")
    valid_from = _parse_time(row.get("direction_valid_from"), "8i_c_direction_valid_from_invalid")
    if valid_from > generated_at:
        raise ExternalEvidence8IAggregationError("8i_c_direction_valid_from_after_snapshot")

    direction = row.get("direction_state")
    if status == USABLE:
        if direction not in {POSITIVE, NEGATIVE}:
            raise ExternalEvidence8IAggregationError("8i_c_usable_component_requires_known_direction")
        return str(direction), None
    if status == UNKNOWN_VALID:
        if direction != UNKNOWN:
            raise ExternalEvidence8IAggregationError("8i_c_unknown_valid_must_preserve_unknown")
        return UNKNOWN, None

    if direction not in {None, "", INSUFFICIENT}:
        raise ExternalEvidence8IAggregationError("8i_c_unavailable_component_cannot_carry_direction")
    reason = str(row.get("unavailable_reason") or "").strip()
    if not reason:
        raise ExternalEvidence8IAggregationError("8i_c_unavailable_component_reason_required")
    return None, reason


def _find(parent: dict[str, str], item: str) -> str:
    root = item
    while parent[root] != root:
        root = parent[root]
    while parent[item] != item:
        nxt = parent[item]
        parent[item] = root
        item = nxt
    return root


def _union(parent: dict[str, str], left: str, right: str) -> None:
    a, b = _find(parent, left), _find(parent, right)
    if a != b:
        parent[max(a, b)] = min(a, b)


def aggregate_external_evidence(
    *,
    contract: Mapping[str, Any],
    binding_contract: Mapping[str, Any],
    conflict_matrix: Mapping[str, Any],
    component_inputs: Sequence[Mapping[str, Any]],
    synthetic: bool,
    core_state: str | None = None,
) -> dict[str, Any]:
    """Exercise the frozen 8I-C design on synthetic component states only."""
    validate_aggregation_contract(contract, binding_contract, conflict_matrix)
    if synthetic is not True:
        raise ExternalEvidence8IAggregationError("8i_c_real_aggregation_not_authorized")
    rows = [dict(row) for row in component_inputs]
    if not rows:
        result = current_aggregation_status(contract, binding_contract, conflict_matrix)
        result["state"] = "SYNTHETIC_AGGREGATION_COMPLETE"
        result["synthetic"] = True
        result["aggregation_sha256"] = _digest({k: v for k, v in result.items() if k != "aggregation_sha256"})
        return result

    identities: set[tuple[str, str, int, str]] = set()
    component_ids: set[str] = set()
    main_by_factor: dict[str, Mapping[str, Any]] = {}
    binding_hashes: dict[str, str] = {}
    adapter_hashes: dict[str, str] = {}
    states_by_component: dict[str, str | None] = {}
    exclusions: dict[str, str] = {}
    interactions: list[tuple[str, list[str]]] = []
    generated_times: set[str] = set()

    for row in rows:
        binding = row.get("binding")
        if not isinstance(binding, Mapping):
            raise ExternalEvidence8IAggregationError("8i_c_binding_required")
        binding_sha = _verify_binding(binding)
        component_type = str(binding.get("component_type") or "")
        component_id = str(binding.get("component_id") or "")
        factor_id = str(binding.get("factor_id") or "")
        if component_type not in ALLOWED_COMPONENT_TYPES or not component_id or not factor_id:
            raise ExternalEvidence8IAggregationError("8i_c_component_identity_invalid")
        if component_id in component_ids:
            raise ExternalEvidence8IAggregationError("8i_c_duplicate_component_id")
        component_ids.add(component_id)

        snapshot_id = str(row.get("snapshot_id") or "").strip()
        symbol = str(row.get("symbol") or "").strip()
        try:
            horizon = int(row.get("horizon_sessions"))
        except (TypeError, ValueError) as exc:
            raise ExternalEvidence8IAggregationError("8i_c_horizon_required") from exc
        if horizon != int(binding.get("horizon_sessions")):
            raise ExternalEvidence8IAggregationError("8i_c_binding_horizon_mismatch")
        generated_at = _parse_time(row.get("generated_at"), "8i_c_generated_at_invalid")
        if not snapshot_id or not symbol:
            raise ExternalEvidence8IAggregationError("8i_c_snapshot_symbol_identity_required")
        identities.add((snapshot_id, symbol, horizon, generated_at.isoformat()))
        generated_times.add(generated_at.isoformat())

        state, unavailable_reason = _component_state(row, generated_at)
        states_by_component[component_id] = state
        if unavailable_reason is not None:
            exclusions[component_id] = unavailable_reason
        binding_hashes[component_id] = binding_sha
        adapter_hashes[component_id] = _require_sha256(
            row.get("direction_adapter_sha256"), "8i_c_direction_adapter_sha256_required"
        )

        if component_type == "8g_main_effect":
            if factor_id in main_by_factor:
                raise ExternalEvidence8IAggregationError("8i_c_duplicate_main_effect_factor")
            parents = list(binding.get("parent_factor_ids_if_applicable") or ())
            if parents:
                raise ExternalEvidence8IAggregationError("8i_c_main_effect_cannot_have_parent_factors")
            main_by_factor[factor_id] = row
        else:
            parents = [str(x) for x in binding.get("parent_factor_ids_if_applicable") or ()]
            if len(parents) < 2 or len(parents) != len(set(parents)):
                raise ExternalEvidence8IAggregationError("8i_c_interaction_parent_identity_invalid")
            interactions.append((component_id, parents))

    if len(identities) != 1 or len(generated_times) != 1:
        raise ExternalEvidence8IAggregationError("8i_c_single_snapshot_symbol_horizon_required")
    snapshot_id, symbol, horizon, generated_at_text = next(iter(identities))

    for interaction_id, parents in interactions:
        missing = sorted(set(parents).difference(main_by_factor))
        if missing:
            raise ExternalEvidence8IAggregationError(
                "8i_c_orphan_interaction_missing_bound_parent:" + interaction_id + ":" + ",".join(missing)
            )

    parent = {factor_id: factor_id for factor_id in main_by_factor}
    for _, factors in interactions:
        anchor = factors[0]
        for factor in factors[1:]:
            _union(parent, anchor, factor)

    bundles_by_root: dict[str, dict[str, Any]] = {}
    for factor_id, row in main_by_factor.items():
        root = _find(parent, factor_id)
        bundle = bundles_by_root.setdefault(root, {"factor_ids": set(), "component_ids": []})
        bundle["factor_ids"].add(factor_id)
        bundle["component_ids"].append(str(row["binding"]["component_id"]))
    for interaction_id, factors in interactions:
        root = _find(parent, factors[0])
        bundle = bundles_by_root.setdefault(root, {"factor_ids": set(), "component_ids": []})
        bundle["factor_ids"].update(factors)
        bundle["component_ids"].append(interaction_id)

    dependency_bundles: list[dict[str, Any]] = []
    bundle_states: dict[str, str] = {}
    for root in sorted(bundles_by_root):
        raw = bundles_by_root[root]
        factor_ids = sorted(raw["factor_ids"])
        ids = sorted(set(raw["component_ids"]))
        bundle_id = "bundle::" + "+".join(factor_ids)
        included_states = [states_by_component[cid] for cid in ids if states_by_component[cid] is not None]
        bundle_state, known_set, unknown_present = reduce_direction_states(
            [str(state) for state in included_states]
        )
        bundle_states[bundle_id] = bundle_state
        dependency_bundles.append({
            "bundle_id": bundle_id,
            "factor_ids": factor_ids,
            "component_ids": ids,
            "state": bundle_state,
            "known_direction_set": known_set,
            "unknown_present": unknown_present,
        })

    external_state, known_set, unknown_present = reduce_direction_states(list(bundle_states.values()))
    relation_state = None
    if core_state is not None:
        relation_state = classify_relation(
            core_state=str(core_state), external_state=external_state, conflict_matrix=conflict_matrix
        )

    included = sorted(cid for cid, state in states_by_component.items() if state is not None)
    result: dict[str, Any] = {
        "schema_version": AGGREGATION_RESULT_SCHEMA,
        "phase": "8I-C",
        "state": "SYNTHETIC_AGGREGATION_COMPLETE",
        "synthetic": True,
        "snapshot_id": snapshot_id,
        "symbol": symbol,
        "horizon_sessions": horizon,
        "generated_at": generated_at_text,
        "included_component_ids": included,
        "excluded_component_ids_with_reasons": dict(sorted(exclusions.items())),
        "dependency_bundles": dependency_bundles,
        "bundle_states": dict(sorted(bundle_states.items())),
        "known_direction_set": known_set,
        "unknown_present": unknown_present,
        "external_direction_state": external_state,
        "external_evidence_state": external_state,
        "relation_state_if_core_state_supplied": relation_state,
        "binding_sha256s": dict(sorted(binding_hashes.items())),
        "direction_adapter_sha256s": dict(sorted(adapter_hashes.items())),
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
        "real_aggregation_authorized": False,
        "external_decision_influence_enabled": False,
        "phase7_mutation_authorized": False,
        "extended_reliability_enabled": False,
        "extended_stance_enabled": False,
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subblock": "8I-D_COMPONENT_DIRECTION_STATE_ENGINE_PREREGISTRATION",
    }
    result["aggregation_sha256"] = _digest(result)
    return result
