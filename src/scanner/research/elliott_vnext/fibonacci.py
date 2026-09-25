"""Fibonacci geometry and prospective wave mapping for Elliott vNext Module 6C.

The core contract is intentionally one-way:

    structural scenario / externally supplied structural anchors -> Fibonacci

Fibonacci never creates, selects, ranks or repairs an Elliott count.  All
projection levels are research-only zones, never point forecasts or trade
orders.  Every scenario projection carries the upstream ``scenario_id`` and a
causal ``available_from`` timestamp.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date
import hashlib
import json
import math
from typing import Iterable, Mapping, Sequence


W2_LEVELS: tuple[tuple[float, str], ...] = (
    (0.382, "EW_PREWATCH_382"),
    (0.500, "EW_DEEP_SCAN_500"),
    (0.618, "EW_W2_CORE"),
    (0.786, "EW_W2_DEEP"),
    (0.887, "EW_W2_DANGER"),
)

W3_LEVELS: tuple[float, ...] = (1.000, 1.618, 2.000, 2.618, 3.236)
W4_LEVELS: tuple[float, ...] = (0.146, 0.236, 0.382, 0.500)

# Foundation v2 deliberately did not freeze numeric W5 levels.  These are
# research hypotheses sourced from the verified ElliottWaver.live Kompendium:
# W5 may equal W1 or approximate 61.8% of the W1-through-W3 structure.  They
# remain explicitly unvalidated and non-production until Module 6G compares
# them out-of-sample.
W5_RESEARCH_CANDIDATES: tuple[dict[str, object], ...] = (
    {
        "basis_name": "wave_1_length_relative_to_wave_4",
        "level": 1.000,
        "length_source": "wave_1",
        "validated": False,
        "numeric_level_frozen": False,
    },
    {
        "basis_name": "wave_1_to_wave_3_structure_relative_to_wave_4",
        "level": 0.618,
        "length_source": "origin_to_wave_3",
        "validated": False,
        "numeric_level_frozen": False,
    },
)


@dataclass(frozen=True)
class ZoneWidthSpec:
    degree: str
    atr_multiplier: float
    price_fraction_floor: float
    validated: bool = False

    def __post_init__(self) -> None:
        if self.atr_multiplier <= 0 or not math.isfinite(self.atr_multiplier):
            raise ValueError("atr_multiplier must be finite and > 0")
        if self.price_fraction_floor <= 0 or not math.isfinite(self.price_fraction_floor):
            raise ValueError("price_fraction_floor must be finite and > 0")


# These widths are deliberately frozen as *research candidates*, not calibrated
# production values.  The only contract guaranteed by 6C is that width depends
# on both volatility (ATR) and wave degree, with a price-level floor.
DEFAULT_ZONE_WIDTH_SPECS: tuple[ZoneWidthSpec, ...] = (
    ZoneWidthSpec("minor", atr_multiplier=0.20, price_fraction_floor=0.0010),
    ZoneWidthSpec("intermediate", atr_multiplier=0.30, price_fraction_floor=0.0015),
    ZoneWidthSpec("major", atr_multiplier=0.45, price_fraction_floor=0.0025),
    ZoneWidthSpec("primary", atr_multiplier=0.60, price_fraction_floor=0.0040),
)


class FibonacciInputError(ValueError):
    """Raised for malformed provenance or unsupported geometry inputs."""


def _iso_date(value: object, field: str) -> str:
    text = str(value or "")
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise FibonacciInputError(f"invalid_{field}") from exc


def _finite_positive(value: object, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise FibonacciInputError(f"invalid_{field}") from exc
    if not math.isfinite(result) or result <= 0:
        raise FibonacciInputError(f"invalid_{field}")
    return result


def _optional_positive(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) and result > 0 else None


def _width_spec(degree: str, specs: Sequence[ZoneWidthSpec]) -> ZoneWidthSpec:
    by_degree = {spec.degree: spec for spec in specs}
    if degree not in by_degree:
        raise FibonacciInputError("unsupported_degree_zone_width")
    return by_degree[degree]


def _half_width(center: float, atr: float | None, spec: ZoneWidthSpec) -> tuple[float, dict[str, object]]:
    if atr is None or not math.isfinite(atr) or atr <= 0:
        raise FibonacciInputError("atr_required_for_projection_zone")
    atr_component = atr * spec.atr_multiplier
    price_component = abs(center) * spec.price_fraction_floor
    width = max(atr_component, price_component)
    return width, {
        "degree": spec.degree,
        "atr": atr,
        "atr_multiplier": spec.atr_multiplier,
        "atr_component": atr_component,
        "price_fraction_floor": spec.price_fraction_floor,
        "price_component": price_component,
        "half_width": width,
        "parameter_validated": spec.validated,
    }


def _hash_id(payload: Mapping[str, object]) -> str:
    frozen = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(frozen.encode("utf-8")).hexdigest()


def _pivot_key(symbol: str, timeframe: str, degree: str, pivot: Mapping[str, object]) -> tuple[object, ...]:
    return (
        symbol,
        timeframe,
        degree,
        str(pivot.get("pivot_time")),
        str(pivot.get("confirmed_time")),
        round(float(pivot.get("price")), 12),
        str(pivot.get("kind")),
    )


def _source_lookup(source_pivots: Iterable[Mapping[str, object]]) -> dict[tuple[object, ...], dict[str, object]]:
    result: dict[tuple[object, ...], dict[str, object]] = {}
    for raw in source_pivots:
        required = ("symbol", "timeframe", "degree", "pivot_time", "confirmed_time", "price", "kind")
        if any(raw.get(field) in (None, "") for field in required):
            continue
        symbol = str(raw["symbol"]).strip()
        timeframe = str(raw["timeframe"]).strip()
        degree = str(raw["degree"]).strip()
        pivot = {
            "pivot_time": _iso_date(raw["pivot_time"], "pivot_time"),
            "confirmed_time": _iso_date(raw["confirmed_time"], "confirmed_time"),
            "price": _finite_positive(raw["price"], "pivot_price"),
            "kind": str(raw["kind"]),
        }
        if pivot["kind"] not in {"high", "low"}:
            continue
        result[_pivot_key(symbol, timeframe, degree, pivot)] = {
            **pivot,
            "atr_at_pivot": _optional_positive(raw.get("atr_at_pivot")),
            "price_basis": str(raw.get("price_basis", "unknown")),
            "available_from": _iso_date(raw.get("available_from", raw["confirmed_time"]), "available_from"),
        }
    return result


def _scenario_pivots(scenario: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    pivots = scenario.get("pivots")
    if not isinstance(pivots, list):
        raise FibonacciInputError("scenario_pivots_missing")
    by_role: dict[str, Mapping[str, object]] = {}
    for pivot in pivots:
        if not isinstance(pivot, Mapping):
            raise FibonacciInputError("invalid_scenario_pivot")
        role = str(pivot.get("role", ""))
        if not role:
            raise FibonacciInputError("scenario_pivot_role_missing")
        by_role[role] = pivot
    return by_role


def _atr_for(
    lookup: Mapping[tuple[object, ...], Mapping[str, object]],
    symbol: str,
    timeframe: str,
    degree: str,
    pivot: Mapping[str, object],
) -> float | None:
    try:
        source = lookup.get(_pivot_key(symbol, timeframe, degree, pivot))
    except (ValueError, TypeError):
        return None
    if not source:
        return None
    return _optional_positive(source.get("atr_at_pivot"))


def _direction_sign(direction: str) -> int:
    if direction == "up":
        return 1
    if direction == "down":
        return -1
    raise FibonacciInputError("unsupported_scenario_direction")


def _zone_status(price_low: float, price_high: float, direction: str, current_price: float | None, half_width: float) -> tuple[str, float | None]:
    if current_price is None:
        return "projected", None
    current = _finite_positive(current_price, "current_price")
    if price_low <= current <= price_high:
        return "inside", 0.0
    nearest = price_low if current < price_low else price_high
    distance = abs(current - nearest)
    distance_pct = 100.0 * distance / current
    sign = _direction_sign(direction)
    passed = current > price_high if sign > 0 else current < price_low
    if passed:
        return "reached", distance_pct
    if distance <= 2.0 * half_width:
        return "approaching", distance_pct
    return "projected", distance_pct


def _projection_zone(
    *,
    scenario_id: str,
    wave_role: str,
    projection_type: str,
    center: float,
    direction: str,
    degree: str,
    atr: float | None,
    available_from: str,
    basis: Mapping[str, object],
    width_specs: Sequence[ZoneWidthSpec],
    current_price: float | None,
) -> dict[str, object]:
    spec = _width_spec(degree, width_specs)
    half_width, width_basis = _half_width(center, atr, spec)
    low = max(0.0, center - half_width)
    high = center + half_width
    status, distance = _zone_status(low, high, direction, current_price, half_width)
    identity = {
        "scenario_id": scenario_id,
        "wave_role": wave_role,
        "projection_type": projection_type,
        "center": round(center, 12),
        "available_from": available_from,
        "basis": basis,
    }
    return {
        "zone_id": _hash_id(identity),
        "scenario_id": scenario_id,
        "wave_role": wave_role,
        "projection_type": projection_type,
        "price_low": low,
        "price_high": high,
        "center_price": center,
        "basis": dict(basis),
        "zone_width_basis": width_basis,
        "available_from": available_from,
        "status": status,
        "distance_to_zone_pct": distance,
        "research_only": True,
    }


def _retracement_price(start: float, end: float, level: float) -> float:
    return end - level * (end - start)


def _extension_from(anchor: float, length: float, level: float, direction: str) -> float:
    return anchor + _direction_sign(direction) * length * level


def _retracement_from(end: float, length: float, level: float, direction: str) -> float:
    return end - _direction_sign(direction) * length * level


def wave2_depth_trigger(origin_price: float, wave1_price: float, current_price: float) -> dict[str, object]:
    """Classify W2 retracement depth without changing any structural count."""

    origin = _finite_positive(origin_price, "origin_price")
    wave1 = _finite_positive(wave1_price, "wave1_price")
    current = _finite_positive(current_price, "current_price")
    if math.isclose(origin, wave1):
        raise FibonacciInputError("zero_length_wave_1")
    direction = "up" if wave1 > origin else "down"
    wave1_length = abs(wave1 - origin)
    retracement = (wave1 - current) / wave1_length if direction == "up" else (current - wave1) / wave1_length
    if retracement >= 1.0:
        trigger = "EW_INVALIDATED"
    elif retracement >= 0.887:
        trigger = "EW_W2_DANGER"
    elif retracement >= 0.786:
        trigger = "EW_W2_DEEP"
    elif retracement >= 0.618:
        trigger = "EW_W2_CORE"
    elif retracement >= 0.500:
        trigger = "EW_DEEP_SCAN_500"
    elif retracement >= 0.382:
        trigger = "EW_PREWATCH_382"
    else:
        trigger = None
    return {
        "direction": direction,
        "retracement_ratio": retracement,
        "trigger": trigger,
        "hard_invalidation": retracement >= 1.0,
        "danger_zone_887_is_hard_invalidation": False,
    }


def build_wave2_retracement_map(
    origin: Mapping[str, object],
    wave_1: Mapping[str, object],
    *,
    degree: str,
    scenario_id: str | None = None,
    current_price: float | None = None,
    width_specs: Sequence[ZoneWidthSpec] = DEFAULT_ZONE_WIDTH_SPECS,
) -> dict[str, object]:
    """Build W2 zones from an upstream-selected origin/W1 anchor pair.

    The function does not search for anchors.  Callers must supply a structural
    origin and W1 endpoint chosen independently of Fibonacci.
    """

    origin_price = _finite_positive(origin.get("price"), "origin_price")
    wave1_price = _finite_positive(wave_1.get("price"), "wave1_price")
    origin_kind = str(origin.get("kind", ""))
    wave1_kind = str(wave_1.get("kind", ""))
    if (origin_kind, wave1_kind) not in {("low", "high"), ("high", "low")}:
        raise FibonacciInputError("wave2_anchors_must_be_opposite_structural_pivots")
    origin_confirmed = _iso_date(origin.get("confirmed_time"), "origin_confirmed_time")
    wave1_confirmed = _iso_date(wave_1.get("confirmed_time"), "wave1_confirmed_time")
    available_from = max(origin_confirmed, wave1_confirmed)
    direction = "up" if wave1_price > origin_price else "down"
    atr = _optional_positive(wave_1.get("atr_at_pivot"))
    if atr is None:
        raise FibonacciInputError("atr_required_for_projection_zone")
    spec = _width_spec(degree, width_specs)

    zones: list[dict[str, object]] = []
    for level, trigger in W2_LEVELS:
        center = _retracement_price(origin_price, wave1_price, level)
        half_width, width_basis = _half_width(center, atr, spec)
        low, high = max(0.0, center - half_width), center + half_width
        status, distance = _zone_status(low, high, "down" if direction == "up" else "up", current_price, half_width)
        zones.append(
            {
                "level": level,
                "trigger": trigger,
                "price_low": low,
                "price_high": high,
                "center_price": center,
                "zone_width_basis": width_basis,
                "available_from": available_from,
                "status": status,
                "distance_to_zone_pct": distance,
                "research_only": True,
            }
        )

    trigger_state = None
    if current_price is not None:
        trigger_state = wave2_depth_trigger(origin_price, wave1_price, current_price)
    return {
        "scenario_id": scenario_id,
        "anchor_start": dict(origin),
        "anchor_end": dict(wave_1),
        "direction": direction,
        "available_from": available_from,
        "zones": zones,
        "hard_invalidation_price": origin_price,
        "hard_invalidation_rule": "wave_2_must_not_cross_wave_1_origin",
        "danger_level_887_is_hard_invalidation": False,
        "trigger_state": trigger_state,
        "anchor_selection_by_fibonacci": False,
        "research_only": True,
    }


def _scenario_geometry(
    scenario: Mapping[str, object],
    *,
    symbol: str,
    timeframe: str,
    degree: str,
    source_lookup: Mapping[tuple[object, ...], Mapping[str, object]],
    current_price: float | None,
    width_specs: Sequence[ZoneWidthSpec],
) -> dict[str, object]:
    scenario_id = str(scenario.get("scenario_id", ""))
    if not scenario_id:
        raise FibonacciInputError("scenario_id_required")
    if scenario.get("family") != "motive":
        return {
            "scenario_id": scenario_id,
            "supported": False,
            "reason": "6c_projection_map_currently_motive_only",
            "projection_zones": [],
            "warnings": [],
            "research_only": True,
        }
    if str(scenario.get("status", "")).startswith("invalidated"):
        return {
            "scenario_id": scenario_id,
            "supported": False,
            "reason": "invalidated_scenario_not_projected",
            "projection_zones": [],
            "warnings": [],
            "research_only": True,
        }

    direction = str(scenario.get("direction", ""))
    _direction_sign(direction)
    stage = str(scenario.get("stage", ""))
    pivots = _scenario_pivots(scenario)
    warnings: list[str] = []
    zones: list[dict[str, object]] = []
    fibonacci: dict[str, object] = {
        "anchor_selection_by_fibonacci": False,
        "scenario_id": scenario_id,
        "zones": [],
        "research_only": True,
    }

    # Descriptive W2 geometry is allowed for the current scenario but its
    # available_from is never backdated before the scenario itself existed.
    if "origin" in pivots and "wave_1" in pivots:
        origin = pivots["origin"]
        wave1 = pivots["wave_1"]
        atr_w1 = _atr_for(source_lookup, symbol, timeframe, degree, wave1)
        if atr_w1 is not None:
            w2_map = build_wave2_retracement_map(
                {**origin, "atr_at_pivot": _atr_for(source_lookup, symbol, timeframe, degree, origin)},
                {**wave1, "atr_at_pivot": atr_w1},
                degree=degree,
                scenario_id=scenario_id,
                current_price=current_price,
                width_specs=width_specs,
            )
            scenario_available = _iso_date(scenario.get("available_from"), "scenario_available_from")
            w2_map["available_from"] = scenario_available
            for item in w2_map["zones"]:
                item["available_from"] = scenario_available
            fibonacci["anchor_start"] = dict(origin)
            fibonacci["anchor_end"] = dict(wave1)
            fibonacci["zones"] = w2_map["zones"]
            fibonacci["w2_trigger_state"] = w2_map["trigger_state"]
            fibonacci["hard_invalidation_price"] = w2_map["hard_invalidation_price"]
        else:
            warnings.append("w2_geometry_suppressed_missing_wave1_atr")

    if stage == "wave_2_complete":
        required = ("origin", "wave_1", "wave_2")
        if all(role in pivots for role in required):
            origin, wave1, wave2 = (pivots[role] for role in required)
            length = abs(float(wave1["price"]) - float(origin["price"]))
            atr = _atr_for(source_lookup, symbol, timeframe, degree, wave2)
            if atr is None:
                warnings.append("wave3_projection_suppressed_missing_wave2_atr")
            else:
                for level in W3_LEVELS:
                    center = _extension_from(float(wave2["price"]), length, level, direction)
                    zones.append(
                        _projection_zone(
                            scenario_id=scenario_id,
                            wave_role="wave_3",
                            projection_type="wave1_extension_from_wave2",
                            center=center,
                            direction=direction,
                            degree=degree,
                            atr=atr,
                            available_from=str(wave2["confirmed_time"]),
                            basis={
                                "anchor_roles": ["origin", "wave_1", "wave_2"],
                                "length_source": "wave_1",
                                "level": level,
                                "level_source": "elliott_vnext_contract_v2.soft_guidelines.wave_3_extension_research_candidates",
                                "level_validated": False,
                            },
                            width_specs=width_specs,
                            current_price=current_price,
                        )
                    )

    elif stage == "wave_3_complete":
        if all(role in pivots for role in ("wave_2", "wave_3")):
            wave2, wave3 = pivots["wave_2"], pivots["wave_3"]
            length = abs(float(wave3["price"]) - float(wave2["price"]))
            atr = _atr_for(source_lookup, symbol, timeframe, degree, wave3)
            if atr is None:
                warnings.append("wave4_projection_suppressed_missing_wave3_atr")
            else:
                for level in W4_LEVELS:
                    center = _retracement_from(float(wave3["price"]), length, level, direction)
                    zones.append(
                        _projection_zone(
                            scenario_id=scenario_id,
                            wave_role="wave_4",
                            projection_type="wave3_retracement",
                            center=center,
                            direction="down" if direction == "up" else "up",
                            degree=degree,
                            atr=atr,
                            available_from=str(wave3["confirmed_time"]),
                            basis={
                                "anchor_roles": ["wave_2", "wave_3"],
                                "length_source": "wave_3",
                                "level": level,
                                "level_source": "elliott_vnext_contract_v2.soft_guidelines.wave_4_retracement",
                                "level_validated": False,
                            },
                            width_specs=width_specs,
                            current_price=current_price,
                        )
                    )

    elif stage == "wave_4_complete":
        if all(role in pivots for role in ("origin", "wave_1", "wave_3", "wave_4")):
            origin, wave1, wave3, wave4 = (pivots[role] for role in ("origin", "wave_1", "wave_3", "wave_4"))
            atr = _atr_for(source_lookup, symbol, timeframe, degree, wave4)
            if atr is None:
                warnings.append("wave5_projection_suppressed_missing_wave4_atr")
            else:
                lengths = {
                    "wave_1": abs(float(wave1["price"]) - float(origin["price"])),
                    "origin_to_wave_3": abs(float(wave3["price"]) - float(origin["price"])),
                }
                for candidate in W5_RESEARCH_CANDIDATES:
                    length = lengths[str(candidate["length_source"])]
                    level = float(candidate["level"])
                    center = _extension_from(float(wave4["price"]), length, level, direction)
                    zones.append(
                        _projection_zone(
                            scenario_id=scenario_id,
                            wave_role="wave_5",
                            projection_type=str(candidate["basis_name"]),
                            center=center,
                            direction=direction,
                            degree=degree,
                            atr=atr,
                            available_from=str(wave4["confirmed_time"]),
                            basis={
                                "anchor_roles": ["origin", "wave_1", "wave_3", "wave_4"],
                                "length_source": candidate["length_source"],
                                "level": level,
                                "level_source": "elliottwaver_live_kompendium_verified_2026-09-25",
                                "level_validated": candidate["validated"],
                                "numeric_level_frozen": candidate["numeric_level_frozen"],
                            },
                            width_specs=width_specs,
                            current_price=current_price,
                        )
                    )

    elif stage == "wave_5_complete":
        warnings.append("cycle_complete_no_historical_projection_backfill")
    else:
        warnings.append("scenario_stage_has_no_6c_prospective_projection")

    return {
        "scenario_id": scenario_id,
        "supported": True,
        "pattern_class": scenario.get("pattern_class"),
        "stage": stage,
        "fibonacci": fibonacci,
        "projection_zones": zones,
        "warnings": warnings,
        "research_only": True,
    }


def _current_stage(stage: str) -> str:
    return {
        "wave_2_complete": "possible_wave_2_completion",
        "wave_3_complete": "possible_wave_3_exhaustion",
        "wave_4_complete": "possible_wave_4_completion",
        "wave_5_complete": "possible_wave_5_completion",
    }.get(stage, "uncertain")


def _next_expected(stage: str) -> list[str]:
    return {
        "wave_2_complete": ["wave_3_in_progress", "wave_3_target_approach", "possible_wave_3_exhaustion"],
        "wave_3_complete": ["wave_4_in_progress", "wave_4_target_zone", "possible_wave_4_completion"],
        "wave_4_complete": ["wave_5_in_progress", "wave_5_target_approach", "possible_wave_5_completion"],
        "wave_5_complete": ["post_wave_5_higher_degree_correction_risk"],
    }.get(stage, ["uncertain"])


def attach_fibonacci_geometry(
    scenario_set: Mapping[str, object],
    *,
    source_pivots: Iterable[Mapping[str, object]],
    current_price: float | None = None,
    width_specs: Sequence[ZoneWidthSpec] = DEFAULT_ZONE_WIDTH_SPECS,
) -> dict[str, object]:
    """Attach 6C geometry without changing 6B scenario identity or ordering."""

    result = deepcopy(dict(scenario_set))
    symbol = str(result.get("symbol", ""))
    timeframe = str(result.get("timeframe", ""))
    degree = str(result.get("degree", ""))
    if not symbol or not timeframe or not degree:
        raise FibonacciInputError("scenario_set_identity_missing")
    _width_spec(degree, width_specs)
    lookup = _source_lookup(source_pivots)

    scenario_sequence: list[Mapping[str, object]] = []
    primary = result.get("primary_scenario")
    if isinstance(primary, Mapping):
        scenario_sequence.append(primary)
    alternatives = result.get("alternative_scenarios", [])
    if isinstance(alternatives, list):
        scenario_sequence.extend(s for s in alternatives if isinstance(s, Mapping))

    geometry: list[dict[str, object]] = []
    all_zones: list[dict[str, object]] = []
    warnings: list[str] = list(result.get("warnings", [])) if isinstance(result.get("warnings"), list) else []
    for scenario in scenario_sequence:
        item = _scenario_geometry(
            scenario,
            symbol=symbol,
            timeframe=timeframe,
            degree=degree,
            source_lookup=lookup,
            current_price=current_price,
            width_specs=width_specs,
        )
        geometry.append(item)
        all_zones.extend(item["projection_zones"])
        warnings.extend(item["warnings"])

    primary_stage = str(primary.get("stage", "")) if isinstance(primary, Mapping) else ""
    result["fibonacci_geometry"] = geometry
    result["projection_zones"] = all_zones
    result["wave_cycle_map"] = {
        "current_stage": _current_stage(primary_stage),
        "next_expected_structures": _next_expected(primary_stage),
        "scenario_maps": [
            {
                "scenario_id": item["scenario_id"],
                "stage": item.get("stage"),
                "projection_zone_ids": [zone["zone_id"] for zone in item["projection_zones"]],
                "warnings": item["warnings"],
            }
            for item in geometry
        ],
    }
    result["current_wave_stage"] = _current_stage(primary_stage)
    result["warnings"] = sorted(set(warnings))
    result["fibonacci_used"] = True
    result["fibonacci_selects_wave_count"] = False
    result["numeric_w5_levels_frozen"] = False
    result["zone_width_parameters_validated"] = all(spec.validated for spec in width_specs)
    result["routing_triggers"] = []
    result["research_only"] = True
    result.pop("trade_decision", None)
    result.pop("order_instruction", None)
    return result
