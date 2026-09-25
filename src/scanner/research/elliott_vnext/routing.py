"""Research-only swing routing for Elliott vNext Module 6D.

6D converts already-existing 6B/6C structural and geometric states into review
contexts.  It does **not** decide whether to buy, hold, reduce, sell, or place
an order.  Conflicting routes are preserved for the later global Decision Layer.

The pipeline remains one-way::

    6A causal pivots -> 6B scenarios -> 6C geometry -> 6D review routing

Routing may never change scenario identity, rank scenarios by expected return,
or claim that a geometrically interesting level has historical edge.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date
import math
from typing import Mapping, Sequence

from .fibonacci import wave2_depth_trigger


ALLOWED_REVIEW_CONTEXTS: tuple[str, ...] = (
    "entry_or_add_review",
    "hold_review",
    "partial_reduce_review",
    "reentry_or_add_review",
    "profit_protection_review",
    "larger_reduce_or_exit_review",
)

# Frozen research routing policy.  These mappings define *what deserves a
# review*, not what trade should be executed.  All routes require confirmation
# by the later system-level decision layer.
TRIGGER_POLICY: dict[str, dict[str, object]] = {
    "EW_PREWATCH_382": {
        "review_context": "hold_review",
        "reason": "W2 retracement reached the 38.2% prewatch depth; review structure without assuming completion.",
    },
    "EW_DEEP_SCAN_500": {
        "review_context": "entry_or_add_review",
        "reason": "W2 retracement reached the 50% research depth; review possible entry/add conditions.",
    },
    "EW_W2_CORE": {
        "review_context": "entry_or_add_review",
        "reason": "W2 retracement reached the 61.8% core research zone; review possible entry/add conditions.",
    },
    "EW_W2_DEEP": {
        "review_context": "entry_or_add_review",
        "reason": "W2 retracement reached the 78.6% deep research zone; review structure and risk before any entry/add decision.",
    },
    "EW_W2_DANGER": {
        "review_context": "hold_review",
        "reason": "W2 retracement reached the 88.7% danger zone; review structural risk without treating 88.7% as hard invalidation.",
    },
    "EW_INVALIDATED": {
        "review_context": "larger_reduce_or_exit_review",
        "reason": "W2 crossed the W1 origin and the structural hypothesis is hard-invalidated; escalate to reduce/exit review.",
    },
    "EW_W3_TARGET_APPROACH": {
        "review_context": "partial_reduce_review",
        "reason": "Price is approaching/inside the active W3 projection frontier; review partial reduction only with external confirmation.",
    },
    "EW_W3_EXHAUSTION": {
        "review_context": "partial_reduce_review",
        "reason": "A causal W3 endpoint is structurally confirmed; review partial reduction versus continued holding.",
    },
    "EW_W4_TARGET_ZONE": {
        "review_context": "reentry_or_add_review",
        "reason": "Price is approaching/inside the active W4 retracement frontier; review reentry/add conditions.",
    },
    "EW_W4_COMPLETION": {
        "review_context": "reentry_or_add_review",
        "reason": "A causal W4 endpoint is structurally confirmed; review reentry/add conditions before W5.",
    },
    "EW_W5_TARGET_APPROACH": {
        "review_context": "profit_protection_review",
        "reason": "Price is approaching/inside the active W5 research projection frontier; review profit protection.",
    },
    "EW_W5_COMPLETION_RISK": {
        "review_context": "larger_reduce_or_exit_review",
        "reason": "A causal W5 endpoint is structurally confirmed; review stronger profit protection or larger reduce/exit actions.",
    },
}

_TRIGGER_ORDER = {trigger: index for index, trigger in enumerate(TRIGGER_POLICY)}
_COST_SENSITIVE_CONTEXTS = {
    "entry_or_add_review",
    "partial_reduce_review",
    "reentry_or_add_review",
    "profit_protection_review",
    "larger_reduce_or_exit_review",
}


class RoutingInputError(ValueError):
    """Raised when 6D receives malformed or non-6C routing input."""


@dataclass(frozen=True)
class ExecutionCostSpec:
    """Simple research cost envelope for round-trip swing comparisons.

    ``spread_bps`` is the quoted full spread, paid once across a round trip
    (half-spread on entry plus half-spread on exit).  Commission and slippage
    are per side and therefore counted twice.
    """

    spread_bps: float = 0.0
    commission_bps_per_side: float = 0.0
    slippage_bps_per_side: float = 0.0
    validated: bool = False

    def __post_init__(self) -> None:
        for name, value in (
            ("spread_bps", self.spread_bps),
            ("commission_bps_per_side", self.commission_bps_per_side),
            ("slippage_bps_per_side", self.slippage_bps_per_side),
        ):
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and >= 0")

    @property
    def estimated_round_trip_cost_bps(self) -> float:
        return self.spread_bps + 2.0 * (self.commission_bps_per_side + self.slippage_bps_per_side)

    def as_dict(self) -> dict[str, object]:
        return {
            "spread_bps": self.spread_bps,
            "commission_bps_per_side": self.commission_bps_per_side,
            "slippage_bps_per_side": self.slippage_bps_per_side,
            "estimated_round_trip_cost_bps": self.estimated_round_trip_cost_bps,
            "parameter_validated": self.validated,
        }


def _iso_date(value: object, field: str) -> str:
    text = str(value or "")
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise RoutingInputError(f"invalid_{field}") from exc


def _policy(trigger: str) -> Mapping[str, object]:
    if trigger not in TRIGGER_POLICY:
        raise RoutingInputError(f"unsupported_routing_trigger:{trigger}")
    return TRIGGER_POLICY[trigger]


def _route(
    trigger: str,
    *,
    scenario_id: str | None,
    scenario_role: str,
    available_from: str,
    source: str,
    evidence: Mapping[str, object] | None,
    cost_spec: ExecutionCostSpec | None,
) -> dict[str, object]:
    policy = _policy(trigger)
    review_context = str(policy["review_context"])
    if review_context not in ALLOWED_REVIEW_CONTEXTS:
        raise RoutingInputError("routing_policy_has_invalid_review_context")
    cost_sensitive = review_context in _COST_SENSITIVE_CONTEXTS
    return {
        "trigger": trigger,
        "review_context": review_context,
        "reason": str(policy["reason"]),
        "scenario_id": scenario_id,
        "scenario_role": scenario_role,
        "available_from": _iso_date(available_from, "routing_available_from"),
        "source": source,
        "evidence": dict(evidence or {}),
        "requires_external_confirmation": True,
        "final_decision_owned_by_global_layer": True,
        "cost_check_required": cost_sensitive,
        "cost_model_status": (
            "estimated_round_trip_cost_attached"
            if cost_sensitive and cost_spec is not None
            else "required_not_supplied"
            if cost_sensitive
            else "not_required_for_hold_review"
        ),
        "estimated_round_trip_cost_bps": (
            cost_spec.estimated_round_trip_cost_bps if cost_sensitive and cost_spec is not None else None
        ),
        "implementation_feasibility_required": cost_sensitive,
        "historical_net_benefit_evaluated": False,
        "historical_outperformance_claimed": False,
        "actionability": "review_only_not_trade_instruction",
        "research_only": True,
    }


def route_wave2_monitor(
    wave2_map: Mapping[str, object],
    *,
    observed_at: str,
    cost_spec: ExecutionCostSpec | None = None,
) -> list[dict[str, object]]:
    """Route the optional pre-confirmation W2 monitor produced by 6C.

    The trigger becomes available at ``observed_at`` because the W2 monitor may
    use a current/as-of price.  The original geometry creation date is retained
    as evidence and is never misrepresented as the trigger-observation date.
    """

    trigger_state = wave2_map.get("trigger_state")
    if not isinstance(trigger_state, Mapping):
        return []
    trigger = trigger_state.get("trigger")
    if not trigger:
        return []
    geometry_available = wave2_map.get("available_from")
    evidence = {
        "retracement_ratio": trigger_state.get("retracement_ratio"),
        "hard_invalidation": bool(trigger_state.get("hard_invalidation", False)),
        "danger_zone_887_is_hard_invalidation": bool(
            trigger_state.get("danger_zone_887_is_hard_invalidation", False)
        ),
        "geometry_available_from": geometry_available,
        "observed_at": _iso_date(observed_at, "observed_at"),
    }
    return [
        _route(
            str(trigger),
            scenario_id=str(wave2_map.get("scenario_id")) if wave2_map.get("scenario_id") else None,
            scenario_role="wave2_monitor",
            available_from=observed_at,
            source="6c_wave2_retracement_monitor",
            evidence=evidence,
            cost_spec=cost_spec,
        )
    ]


def _scenario_index(geometry_set: Mapping[str, object]) -> list[tuple[str, Mapping[str, object]]]:
    result: list[tuple[str, Mapping[str, object]]] = []
    primary = geometry_set.get("primary_scenario")
    if isinstance(primary, Mapping):
        result.append(("primary", primary))
    alternatives = geometry_set.get("alternative_scenarios")
    if isinstance(alternatives, list):
        for index, scenario in enumerate(alternatives, start=1):
            if isinstance(scenario, Mapping):
                result.append((f"alternative_{index}", scenario))
    return result


def _geometry_index(geometry_set: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    items = geometry_set.get("fibonacci_geometry")
    if not isinstance(items, list):
        raise RoutingInputError("6c_fibonacci_geometry_missing")
    result: dict[str, Mapping[str, object]] = {}
    for item in items:
        if isinstance(item, Mapping) and item.get("scenario_id"):
            result[str(item["scenario_id"])] = item
    return result


def _pivot_roles(scenario: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    pivots = scenario.get("pivots")
    if not isinstance(pivots, list):
        return {}
    result: dict[str, Mapping[str, object]] = {}
    for pivot in pivots:
        if isinstance(pivot, Mapping) and pivot.get("role"):
            result[str(pivot["role"])] = pivot
    return result


def _active_zone_evidence(
    zones: Sequence[Mapping[str, object]],
    wave_role: str,
) -> dict[str, object] | None:
    relevant = [zone for zone in zones if str(zone.get("wave_role")) == wave_role]
    if not relevant:
        return None

    active = [zone for zone in relevant if str(zone.get("status")) in {"approaching", "inside"}]
    mode = "approaching_or_inside"
    if not active:
        # Avoid a permanently active signal merely because an early candidate
        # has been passed.  Only treat the entire research frontier as reached
        # when every currently mapped candidate is already beyond price.
        if relevant and all(str(zone.get("status")) == "reached" for zone in relevant):
            active = relevant
            mode = "all_mapped_candidates_reached"
        else:
            return None

    return {
        "wave_role": wave_role,
        "mode": mode,
        "zone_ids": [str(zone.get("zone_id")) for zone in active if zone.get("zone_id")],
        "projection_types": sorted({str(zone.get("projection_type")) for zone in active}),
        "statuses": sorted({str(zone.get("status")) for zone in active}),
        "projection_available_from": min(
            (str(zone.get("available_from")) for zone in active if zone.get("available_from")),
            default=None,
        ),
    }


def _confirmed_wave2_route(
    scenario: Mapping[str, object],
    *,
    scenario_role: str,
    cost_spec: ExecutionCostSpec | None,
) -> dict[str, object] | None:
    pivots = _pivot_roles(scenario)
    if not all(role in pivots for role in ("origin", "wave_1", "wave_2")):
        return None
    origin = float(pivots["origin"]["price"])
    wave1 = float(pivots["wave_1"]["price"])
    wave2 = float(pivots["wave_2"]["price"])
    trigger_state = wave2_depth_trigger(origin, wave1, wave2)
    trigger = trigger_state.get("trigger")
    if not trigger:
        return None
    return _route(
        str(trigger),
        scenario_id=str(scenario.get("scenario_id")) if scenario.get("scenario_id") else None,
        scenario_role=scenario_role,
        available_from=str(scenario.get("available_from")),
        source="6b_confirmed_wave2_endpoint_via_6c_depth_geometry",
        evidence={
            "retracement_ratio": trigger_state.get("retracement_ratio"),
            "wave2_endpoint_price": wave2,
            "hard_invalidation": bool(trigger_state.get("hard_invalidation", False)),
            "uses_current_market_price": False,
        },
        cost_spec=cost_spec,
    )


def _routes_for_scenario(
    scenario: Mapping[str, object],
    geometry: Mapping[str, object] | None,
    *,
    scenario_role: str,
    observed_at: str,
    cost_spec: ExecutionCostSpec | None,
) -> list[dict[str, object]]:
    if str(scenario.get("status", "")).startswith("invalidated"):
        return []
    if scenario.get("family") != "motive":
        return []
    scenario_id = str(scenario.get("scenario_id", ""))
    if not scenario_id:
        raise RoutingInputError("scenario_id_required_for_routing")
    stage = str(scenario.get("stage", ""))
    routes: list[dict[str, object]] = []

    if stage == "wave_2_complete":
        confirmed = _confirmed_wave2_route(scenario, scenario_role=scenario_role, cost_spec=cost_spec)
        if confirmed is not None:
            routes.append(confirmed)
    elif stage == "wave_3_complete":
        routes.append(
            _route(
                "EW_W3_EXHAUSTION",
                scenario_id=scenario_id,
                scenario_role=scenario_role,
                available_from=str(scenario.get("available_from")),
                source="6b_confirmed_wave3_endpoint",
                evidence={"stage": stage},
                cost_spec=cost_spec,
            )
        )
    elif stage == "wave_4_complete":
        routes.append(
            _route(
                "EW_W4_COMPLETION",
                scenario_id=scenario_id,
                scenario_role=scenario_role,
                available_from=str(scenario.get("available_from")),
                source="6b_confirmed_wave4_endpoint",
                evidence={"stage": stage},
                cost_spec=cost_spec,
            )
        )
    elif stage == "wave_5_complete":
        routes.append(
            _route(
                "EW_W5_COMPLETION_RISK",
                scenario_id=scenario_id,
                scenario_role=scenario_role,
                available_from=str(scenario.get("available_from")),
                source="6b_confirmed_wave5_endpoint",
                evidence={
                    "stage": stage,
                    "truncated_fifth": bool(scenario.get("truncated_fifth", False)),
                },
                cost_spec=cost_spec,
            )
        )

    if geometry and bool(geometry.get("supported", False)):
        zones = geometry.get("projection_zones")
        zones = zones if isinstance(zones, list) else []
        zone_trigger: tuple[str, str] | None = None
        if stage == "wave_2_complete":
            zone_trigger = ("wave_3", "EW_W3_TARGET_APPROACH")
        elif stage == "wave_3_complete":
            zone_trigger = ("wave_4", "EW_W4_TARGET_ZONE")
        elif stage == "wave_4_complete":
            zone_trigger = ("wave_5", "EW_W5_TARGET_APPROACH")
        if zone_trigger is not None:
            wave_role, trigger = zone_trigger
            evidence = _active_zone_evidence(zones, wave_role)
            if evidence is not None:
                # A current-price geometric state is observed only at the
                # scenario set's as_of date.  Do not backdate the routing event
                # to the earlier date on which the projection was created.
                routes.append(
                    _route(
                        trigger,
                        scenario_id=scenario_id,
                        scenario_role=scenario_role,
                        available_from=observed_at,
                        source="6c_current_price_vs_projection_frontier",
                        evidence=evidence,
                        cost_spec=cost_spec,
                    )
                )

    return routes


def _sort_routes(routes: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    def key(route: Mapping[str, object]) -> tuple[object, ...]:
        role = str(route.get("scenario_role", ""))
        if role == "primary":
            role_rank = 0
        elif role.startswith("alternative_"):
            try:
                role_rank = int(role.split("_", 1)[1])
            except ValueError:
                role_rank = 999
        else:
            role_rank = 1000
        return (
            role_rank,
            _TRIGGER_ORDER.get(str(route.get("trigger")), 999),
            str(route.get("scenario_id") or ""),
        )

    return [dict(route) for route in sorted(routes, key=key)]


def attach_swing_routing(
    geometry_set: Mapping[str, object],
    *,
    cost_spec: ExecutionCostSpec | None = None,
) -> dict[str, object]:
    """Attach deterministic 6D review routes to a 6C geometry set.

    No scenario is reordered or selected based on the routes.  If primary and
    alternative scenarios imply different review contexts, the conflict is
    surfaced rather than collapsed into one action.
    """

    if geometry_set.get("fibonacci_used") is not True:
        raise RoutingInputError("6d_requires_6c_fibonacci_geometry")
    if geometry_set.get("fibonacci_selects_wave_count") is not False:
        raise RoutingInputError("fibonacci_count_selection_guard_failed")
    observed_at = _iso_date(geometry_set.get("as_of"), "as_of")
    result = deepcopy(dict(geometry_set))
    geometries = _geometry_index(geometry_set)

    routes: list[dict[str, object]] = []
    for scenario_role, scenario in _scenario_index(geometry_set):
        scenario_id = str(scenario.get("scenario_id", ""))
        routes.extend(
            _routes_for_scenario(
                scenario,
                geometries.get(scenario_id),
                scenario_role=scenario_role,
                observed_at=observed_at,
                cost_spec=cost_spec,
            )
        )

    routes = _sort_routes(routes)
    primary_contexts = sorted(
        {str(route["review_context"]) for route in routes if route.get("scenario_role") == "primary"}
    )
    alternative_contexts = sorted(
        {
            str(route["review_context"])
            for route in routes
            if str(route.get("scenario_role", "")).startswith("alternative_")
        }
    )
    per_scenario: dict[str, set[str]] = {}
    for route in routes:
        scenario_id = str(route.get("scenario_id") or route.get("scenario_role"))
        per_scenario.setdefault(scenario_id, set()).add(str(route["review_context"]))
    within_scenario_conflict = any(len(contexts) > 1 for contexts in per_scenario.values())
    cross_scenario_conflict = bool(primary_contexts and alternative_contexts and set(primary_contexts) != set(alternative_contexts))

    warnings = list(result.get("warnings", [])) if isinstance(result.get("warnings"), list) else []
    if within_scenario_conflict:
        warnings.append("within_scenario_routing_context_conflict_preserved")
    if cross_scenario_conflict:
        warnings.append("scenario_routing_conflict_preserved")
    if any(bool(route.get("cost_check_required")) for route in routes) and cost_spec is None:
        warnings.append("transaction_cost_model_missing_for_cost_sensitive_reviews")

    result["routing_triggers"] = list(dict.fromkeys(str(route["trigger"]) for route in routes))
    result["swing_routing"] = routes
    result["routing_summary"] = {
        "primary_review_contexts": primary_contexts,
        "alternative_review_contexts": alternative_contexts,
        "within_scenario_conflict": within_scenario_conflict,
        "cross_scenario_conflict": cross_scenario_conflict,
        "conflicts_preserved_not_resolved": within_scenario_conflict or cross_scenario_conflict,
        "final_decision_required": True,
        "historical_net_benefit_evaluated": False,
    }
    result["execution_cost_model"] = (
        {"status": "supplied_research_assumption", **cost_spec.as_dict()}
        if cost_spec is not None
        else {"status": "not_supplied"}
    )
    result["routing_is_trade_decision"] = False
    result["routing_changes_scenario_order"] = False
    result["research_only"] = True
    result["warnings"] = sorted(set(warnings))
    result.pop("trade_decision", None)
    result.pop("order_instruction", None)
    return result
