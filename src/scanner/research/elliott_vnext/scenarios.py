"""Structural Elliott scenario generation for Module 6B.

Module 6B consumes only causally available pivots produced by Module 6A.  It
creates *multiple* structural hypotheses and a deterministic presentation
ordering; it never asserts that one count is the true count.

Fibonacci geometry, target zones, historical expectancy, scanner state and
trade actions are deliberately absent from this module.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
import hashlib
import json
import math
from typing import Iterable, Mapping, Sequence


FULL_SUPPORT = {"impulse", "zigzag", "flat"}
CONSERVATIVE_SUPPORT = {
    "leading_diagonal",
    "ending_diagonal",
    "triangle",
    "double_three",
    "triple_three",
}

CLASSIC_IMPULSE_RULES = (
    "wave_2_must_not_cross_wave_1_origin",
    "wave_3_must_not_be_shortest_of_1_3_5",
    "wave_4_must_not_overlap_wave_1_price_territory",
)

_PATTERN_ORDER = {
    "impulse": 0,
    "zigzag": 1,
    "flat": 2,
    "leading_diagonal": 3,
    "ending_diagonal": 4,
    "triangle": 5,
    "double_three": 6,
    "triple_three": 7,
}


class ScenarioInputError(ValueError):
    """Raised when pivot provenance is malformed rather than merely uncertain."""


def _iso_date(value: object, field: str) -> str:
    text = str(value or "")
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ScenarioInputError(f"invalid_{field}") from exc


def _finite_price(value: object) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ScenarioInputError("invalid_pivot_price") from exc
    if not math.isfinite(result) or result <= 0:
        raise ScenarioInputError("invalid_pivot_price")
    return result


def _canonical_pivot(raw: Mapping[str, object]) -> dict[str, object]:
    required = ("symbol", "timeframe", "degree", "pivot_time", "confirmed_time", "price", "kind")
    missing = [field for field in required if raw.get(field) in (None, "")]
    if missing:
        raise ScenarioInputError("missing_pivot_fields:" + ",".join(missing))
    kind = str(raw["kind"])
    if kind not in {"high", "low"}:
        raise ScenarioInputError("invalid_pivot_kind")
    pivot_time = _iso_date(raw["pivot_time"], "pivot_time")
    confirmed_time = _iso_date(raw["confirmed_time"], "confirmed_time")
    if confirmed_time < pivot_time:
        raise ScenarioInputError("confirmed_time_before_pivot_time")
    available_from = _iso_date(raw.get("available_from", confirmed_time), "available_from")
    if available_from != confirmed_time:
        raise ScenarioInputError("available_from_must_equal_confirmed_time_in_6b")
    return {
        "symbol": str(raw["symbol"]).strip(),
        "timeframe": str(raw["timeframe"]).strip(),
        "degree": str(raw["degree"]).strip(),
        "pivot_time": pivot_time,
        "confirmed_time": confirmed_time,
        "available_from": available_from,
        "price": _finite_price(raw["price"]),
        "kind": kind,
        "sequence_ambiguous": bool(raw.get("sequence_ambiguous", False)),
        "price_basis": str(raw.get("price_basis", "unknown")),
        "research_only": bool(raw.get("research_only", True)),
    }


def _scenario_id(payload: Mapping[str, object]) -> str:
    frozen = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(frozen.encode("utf-8")).hexdigest()


def _pivot_ref(pivot: Mapping[str, object], role: str) -> dict[str, object]:
    return {
        "role": role,
        "pivot_time": pivot["pivot_time"],
        "confirmed_time": pivot["confirmed_time"],
        "price": pivot["price"],
        "kind": pivot["kind"],
    }


def _available_from(pivots: Sequence[Mapping[str, object]]) -> str:
    return max(str(p["confirmed_time"]) for p in pivots)


def _end_time(pivots: Sequence[Mapping[str, object]]) -> str:
    return max(str(p["pivot_time"]) for p in pivots)


def _direction(p0: Mapping[str, object], p1: Mapping[str, object]) -> str | None:
    if p0["kind"] == "low" and p1["kind"] == "high":
        return "up"
    if p0["kind"] == "high" and p1["kind"] == "low":
        return "down"
    return None


def _wave_moves_are_directional(window: Sequence[Mapping[str, object]], direction: str) -> bool:
    prices = [float(p["price"]) for p in window]
    for index in range(1, len(prices)):
        move_up = prices[index] > prices[index - 1]
        expected_up = (index % 2 == 1) if direction == "up" else (index % 2 == 0)
        if move_up != expected_up:
            return False
    return True


def _wave_lengths(window: Sequence[Mapping[str, object]]) -> dict[str, float]:
    result: dict[str, float] = {}
    for index in range(1, len(window)):
        result[f"wave_{index}"] = abs(float(window[index]["price"]) - float(window[index - 1]["price"]))
    return result


def _wave_2_crosses_origin(window: Sequence[Mapping[str, object]], direction: str) -> bool:
    if len(window) < 3:
        return False
    origin, wave_2 = float(window[0]["price"]), float(window[2]["price"])
    return wave_2 < origin if direction == "up" else wave_2 > origin


def _wave_3_extends_past_wave_1(window: Sequence[Mapping[str, object]], direction: str) -> bool:
    if len(window) < 4:
        return True
    wave_1, wave_3 = float(window[1]["price"]), float(window[3]["price"])
    return wave_3 > wave_1 if direction == "up" else wave_3 < wave_1


def _wave_4_overlaps_wave_1(window: Sequence[Mapping[str, object]], direction: str) -> bool:
    if len(window) < 5:
        return False
    wave_1, wave_4 = float(window[1]["price"]), float(window[4]["price"])
    return wave_4 <= wave_1 if direction == "up" else wave_4 >= wave_1


def _wave_3_is_shortest(window: Sequence[Mapping[str, object]]) -> bool:
    if len(window) < 6:
        return False
    lengths = _wave_lengths(window)
    w1, w3, w5 = lengths["wave_1"], lengths["wave_3"], lengths["wave_5"]
    return w3 < w1 and w3 < w5


def _future_invalidation_conditions(stage: str, pattern_class: str) -> list[str]:
    conditions: list[str] = []
    if stage in {"wave_2_complete", "wave_3_complete", "wave_4_complete", "wave_5_complete"}:
        conditions.append("wave_1_origin_crossed")
    if pattern_class == "impulse" and stage in {"wave_4_complete", "wave_5_complete"}:
        conditions.append("wave_4_enters_wave_1_price_territory")
    if stage == "wave_5_complete":
        conditions.append("wave_3_becomes_shortest_of_waves_1_3_5")
    return conditions


def evaluate_motive_window(window: Sequence[Mapping[str, object]]) -> dict[str, object]:
    """Evaluate one 3-6-pivot motive skeleton.

    The first pivot is the wave-1 origin and subsequent pivots are endpoints of
    waves 1..5.  Rules are applied only when the needed endpoint is already
    causally available.  A fifth wave is explicitly allowed to truncate.
    """

    if len(window) not in {3, 4, 5, 6}:
        raise ValueError("motive windows require origin plus wave 1..2/3/4/5 endpoints")
    direction = _direction(window[0], window[1])
    if direction is None:
        return {"valid": False, "structural_violations": ["non_alternating_start"], "hard_rule_violations": []}

    hard: list[str] = []
    structural: list[str] = []
    if not _wave_moves_are_directional(window, direction):
        structural.append("wave_moves_not_directionally_alternating")
    if _wave_2_crosses_origin(window, direction):
        hard.append("wave_2_must_not_cross_wave_1_origin")
    if len(window) >= 4 and not _wave_3_extends_past_wave_1(window, direction):
        structural.append("wave_3_does_not_extend_past_wave_1_endpoint")
    if len(window) >= 5 and _wave_4_overlaps_wave_1(window, direction):
        hard.append("wave_4_must_not_overlap_wave_1_price_territory")
    if len(window) >= 6 and _wave_3_is_shortest(window):
        hard.append("wave_3_must_not_be_shortest_of_1_3_5")

    stage = {
        3: "wave_2_complete",
        4: "wave_3_complete",
        5: "wave_4_complete",
        6: "wave_5_complete",
    }[len(window)]
    truncated_fifth = False
    if len(window) == 6:
        wave_3, wave_5 = float(window[3]["price"]), float(window[5]["price"])
        truncated_fifth = wave_5 <= wave_3 if direction == "up" else wave_5 >= wave_3

    return {
        "valid": not hard and not structural,
        "direction": direction,
        "stage": stage,
        "hard_rule_violations": hard,
        "structural_violations": structural,
        "wave_lengths": _wave_lengths(window),
        "truncated_fifth": truncated_fifth,
        "wave_4_overlap": _wave_4_overlaps_wave_1(window, direction) if len(window) >= 5 else None,
    }


def _motive_scenario(
    window: Sequence[Mapping[str, object]],
    *,
    pattern_class: str,
    evaluation: Mapping[str, object],
    status: str,
    support_level: str,
    diagonal_overlap_exception: bool = False,
) -> dict[str, object]:
    roles = ["origin", "wave_1", "wave_2", "wave_3", "wave_4", "wave_5"][: len(window)]
    identity = {
        "pattern_class": pattern_class,
        "direction": evaluation.get("direction"),
        "timeframe": window[0]["timeframe"],
        "degree": window[0]["degree"],
        "stage": evaluation.get("stage"),
        "pivots": [(role, p["pivot_time"], p["confirmed_time"], p["price"], p["kind"]) for role, p in zip(roles, window)],
    }
    hard_violations = list(evaluation.get("hard_rule_violations", []))
    if diagonal_overlap_exception:
        hard_violations = [rule for rule in hard_violations if rule != "wave_4_must_not_overlap_wave_1_price_territory"]
    scenario = {
        "scenario_id": _scenario_id(identity),
        "pattern_class": pattern_class,
        "family": "motive",
        "direction": evaluation.get("direction"),
        "timeframe": window[0]["timeframe"],
        "degree": window[0]["degree"],
        "stage": evaluation.get("stage"),
        "status": status,
        "support_level": support_level,
        "available_from": _available_from(window),
        "end_pivot_time": _end_time(window),
        "pivots": [_pivot_ref(p, role) for role, p in zip(roles, window)],
        "hard_rule_violations": hard_violations,
        "structural_violations": list(evaluation.get("structural_violations", [])),
        "rule_violations": hard_violations + list(evaluation.get("structural_violations", [])),
        "hard_invalidations": [
            {"rule": rule, "observed_by": _available_from(window)} for rule in hard_violations
        ],
        "future_invalidation_conditions": _future_invalidation_conditions(str(evaluation.get("stage")), pattern_class),
        "wave_lengths": dict(evaluation.get("wave_lengths", {})),
        "truncated_fifth": bool(evaluation.get("truncated_fifth", False)),
        "diagonal_overlap_exception_applied": diagonal_overlap_exception,
        "structural_fit": None,
        "confirmation_strength": None,
        "historical_expectancy": None,
        "fibonacci": None,
        "projection_zones": [],
        "trade_decision": None,
        "research_only": True,
    }
    return scenario


def _correction_direction(window: Sequence[Mapping[str, object]]) -> str | None:
    first = _direction(window[0], window[1])
    if first == "up":
        return "up"
    if first == "down":
        return "down"
    return None


def evaluate_correction_window(window: Sequence[Mapping[str, object]]) -> dict[str, object]:
    """Evaluate an origin-A-B-C corrective skeleton without Fibonacci geometry."""

    if len(window) != 4:
        raise ValueError("correction windows require origin plus A/B/C endpoints")
    direction = _correction_direction(window)
    if direction is None:
        return {"valid": False, "structural_violations": ["non_alternating_start"]}
    structural: list[str] = []
    if not _wave_moves_are_directional(window, direction):
        structural.append("a_b_c_moves_not_directionally_alternating")

    origin, a, b, c = (float(p["price"]) for p in window)
    if direction == "down":
        b_crosses_origin = b > origin
        c_extends_a = c < a
    else:
        b_crosses_origin = b < origin
        c_extends_a = c > a

    return {
        "valid": not structural,
        "direction": direction,
        "structural_violations": structural,
        "b_crosses_origin": b_crosses_origin,
        "c_extends_past_a": c_extends_a,
        "leg_lengths": {"a": abs(a - origin), "b": abs(b - a), "c": abs(c - b)},
    }


def _correction_scenario(
    window: Sequence[Mapping[str, object]],
    *,
    pattern_class: str,
    evaluation: Mapping[str, object],
    valid: bool,
    reason: str,
) -> dict[str, object]:
    roles = ["origin", "wave_a", "wave_b", "wave_c"]
    identity = {
        "pattern_class": pattern_class,
        "direction": evaluation.get("direction"),
        "timeframe": window[0]["timeframe"],
        "degree": window[0]["degree"],
        "pivots": [(role, p["pivot_time"], p["confirmed_time"], p["price"], p["kind"]) for role, p in zip(roles, window)],
    }
    violations = list(evaluation.get("structural_violations", []))
    if not valid and reason:
        violations.append(reason)
    return {
        "scenario_id": _scenario_id(identity),
        "pattern_class": pattern_class,
        "family": "correction",
        "direction": evaluation.get("direction"),
        "timeframe": window[0]["timeframe"],
        "degree": window[0]["degree"],
        "stage": "correction_complete_candidate",
        "status": "valid_structural_candidate_geometry_unresolved" if valid else "invalidated",
        "support_level": "full",
        "available_from": _available_from(window),
        "end_pivot_time": _end_time(window),
        "pivots": [_pivot_ref(p, role) for role, p in zip(roles, window)],
        "hard_rule_violations": [],
        "structural_violations": violations,
        "rule_violations": violations,
        "hard_invalidations": [],
        "future_invalidation_conditions": [],
        "b_crosses_origin": bool(evaluation.get("b_crosses_origin", False)),
        "c_extends_past_a": bool(evaluation.get("c_extends_past_a", False)),
        "leg_lengths": dict(evaluation.get("leg_lengths", {})),
        "geometry_status": "unresolved_until_6c",
        "structural_fit": None,
        "confirmation_strength": None,
        "historical_expectancy": None,
        "fibonacci": None,
        "projection_zones": [],
        "trade_decision": None,
        "research_only": True,
    }


def _uncertain_complex_correction(
    segment: Sequence[Mapping[str, object]],
    pattern_class: str,
    minimum_pivots: int,
) -> dict[str, object] | None:
    if len(segment) < minimum_pivots:
        return None
    window = segment[-minimum_pivots:]
    identity = {
        "pattern_class": pattern_class,
        "timeframe": window[0]["timeframe"],
        "degree": window[0]["degree"],
        "pivots": [(p["pivot_time"], p["confirmed_time"], p["price"], p["kind"]) for p in window],
        "status": "uncertain_complex_correction",
    }
    return {
        "scenario_id": _scenario_id(identity),
        "pattern_class": pattern_class,
        "family": "correction",
        "direction": None,
        "timeframe": window[0]["timeframe"],
        "degree": window[0]["degree"],
        "stage": "uncertain_complex_correction",
        "status": "conservative_uncertainty_alternative",
        "support_level": "conservative",
        "available_from": _available_from(window),
        "end_pivot_time": _end_time(window),
        "pivots": [_pivot_ref(p, f"pivot_{index}") for index, p in enumerate(window)],
        "hard_rule_violations": [],
        "structural_violations": [],
        "rule_violations": [],
        "hard_invalidations": [],
        "future_invalidation_conditions": [],
        "classification_reason": "represented_as_uncertainty_only;_subwave_structure_not_resolved_by_6a_pivots",
        "structural_fit": None,
        "confirmation_strength": None,
        "historical_expectancy": None,
        "fibonacci": None,
        "projection_zones": [],
        "trade_decision": None,
        "research_only": True,
    }


def _split_and_collapse(pivots: Sequence[Mapping[str, object]]) -> tuple[list[list[dict[str, object]]], list[dict[str, object]]]:
    """Split on ambiguous timestamps, then collapse consecutive same-kind runs.

    A same-kind run keeps only its more extreme confirmed pivot.  This is a
    causal state update because the replacement can happen only after the newer
    pivot's own ``confirmed_time``.  A bar carrying both high/low ambiguity is a
    hard sequence boundary: 6B does not invent intrabar ordering.
    """

    by_time: dict[str, list[dict[str, object]]] = defaultdict(list)
    for pivot in pivots:
        by_time[str(pivot["pivot_time"])].append(dict(pivot))

    segments: list[list[dict[str, object]]] = []
    current: list[dict[str, object]] = []
    ambiguities: list[dict[str, object]] = []

    for pivot_time in sorted(by_time):
        same_time = sorted(by_time[pivot_time], key=lambda p: (p["confirmed_time"], p["kind"], p["price"]))
        kinds = {p["kind"] for p in same_time}
        ambiguous = len(kinds) > 1 or any(bool(p.get("sequence_ambiguous")) for p in same_time)
        if ambiguous:
            if current:
                segments.append(current)
                current = []
            ambiguities.append(
                {
                    "pivot_time": pivot_time,
                    "confirmed_time": max(str(p["confirmed_time"]) for p in same_time),
                    "reason": "intrabar_high_low_sequence_unresolved",
                    "kinds": sorted(kinds),
                }
            )
            continue

        pivot = same_time[-1]
        if current and current[-1]["kind"] == pivot["kind"]:
            previous = current[-1]
            replace = pivot["price"] > previous["price"] if pivot["kind"] == "high" else pivot["price"] < previous["price"]
            if replace or (pivot["price"] == previous["price"] and pivot["pivot_time"] > previous["pivot_time"]):
                current[-1] = pivot
        else:
            current.append(pivot)

    if current:
        segments.append(current)
    return segments, ambiguities


def _group_pivots(pivots: Iterable[Mapping[str, object]], as_of: str | None) -> dict[tuple[str, str, str], list[dict[str, object]]]:
    cutoff = _iso_date(as_of, "as_of") if as_of is not None else None
    groups: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    for raw in pivots:
        pivot = _canonical_pivot(raw)
        if not pivot["symbol"] or not pivot["timeframe"] or not pivot["degree"]:
            raise ScenarioInputError("empty_pivot_group_key")
        if cutoff is not None and pivot["confirmed_time"] > cutoff:
            continue
        groups[(str(pivot["symbol"]), str(pivot["timeframe"]), str(pivot["degree"]))].append(pivot)
    return groups


def _scenario_rank(scenario: Mapping[str, object]) -> tuple[object, ...]:
    support_rank = 0 if scenario.get("support_level") == "full" else 1
    status_rank = 0 if str(scenario.get("status", "")).startswith("valid_structural") else 1
    pivot_count = len(scenario.get("pivots", []))
    return (
        str(scenario.get("end_pivot_time", "")),
        str(scenario.get("available_from", "")),
        -support_rank,
        -status_rank,
        pivot_count,
        -_PATTERN_ORDER.get(str(scenario.get("pattern_class")), 99),
        str(scenario.get("scenario_id", "")),
    )


def _insufficient_primary(symbol: str, timeframe: str, degree: str, as_of: str | None) -> dict[str, object]:
    identity = {
        "symbol": symbol,
        "timeframe": timeframe,
        "degree": degree,
        "as_of": as_of,
        "status": "insufficient_structure",
    }
    return {
        "scenario_id": _scenario_id(identity),
        "pattern_class": "unknown",
        "family": "unknown",
        "direction": None,
        "timeframe": timeframe,
        "degree": degree,
        "stage": "insufficient_structure",
        "status": "insufficient_structure",
        "support_level": "none",
        "available_from": as_of,
        "end_pivot_time": None,
        "pivots": [],
        "hard_rule_violations": [],
        "structural_violations": [],
        "rule_violations": [],
        "hard_invalidations": [],
        "future_invalidation_conditions": [],
        "structural_fit": None,
        "confirmation_strength": None,
        "historical_expectancy": None,
        "fibonacci": None,
        "projection_zones": [],
        "trade_decision": None,
        "research_only": True,
    }


def generate_scenario_sets(
    pivots: Iterable[Mapping[str, object]],
    *,
    as_of: str | None = None,
    max_alternatives: int = 50,
    max_invalidated: int = 100,
) -> list[dict[str, object]]:
    """Generate deterministic scenario sets per symbol/timeframe/degree.

    ``primary_scenario`` is merely the first item under a frozen structural
    recency ordering.  It is not a probability, recommendation, or assertion of
    truth.  Other plausible counts remain in ``alternative_scenarios``.
    """

    if max_alternatives < 0 or max_invalidated < 0:
        raise ValueError("scenario caps must be non-negative")
    groups = _group_pivots(pivots, as_of)
    outputs: list[dict[str, object]] = []

    for (symbol, timeframe, degree), group in sorted(groups.items()):
        segments, ambiguities = _split_and_collapse(group)
        candidates: dict[str, dict[str, object]] = {}
        invalidated: dict[str, dict[str, object]] = {}
        uncertainty: dict[str, dict[str, object]] = {}

        for segment in segments:
            for size in (3, 4, 5, 6):
                for start in range(0, len(segment) - size + 1):
                    window = segment[start : start + size]
                    evaluation = evaluate_motive_window(window)
                    if evaluation.get("valid"):
                        scenario = _motive_scenario(
                            window,
                            pattern_class="impulse",
                            evaluation=evaluation,
                            status="valid_structural_candidate",
                            support_level="full",
                        )
                        candidates[scenario["scenario_id"]] = scenario
                    else:
                        hard = set(evaluation.get("hard_rule_violations", []))
                        structural = set(evaluation.get("structural_violations", []))
                        overlap_only = (
                            "wave_4_must_not_overlap_wave_1_price_territory" in hard
                            and not structural
                            and not (hard - {"wave_4_must_not_overlap_wave_1_price_territory"})
                        )
                        if overlap_only:
                            for diagonal in ("leading_diagonal", "ending_diagonal"):
                                scenario = _motive_scenario(
                                    window,
                                    pattern_class=diagonal,
                                    evaluation=evaluation,
                                    status="conservative_structural_candidate",
                                    support_level="conservative",
                                    diagonal_overlap_exception=True,
                                )
                                candidates[scenario["scenario_id"]] = scenario
                        invalid = _motive_scenario(
                            window,
                            pattern_class="impulse",
                            evaluation=evaluation,
                            status="invalidated",
                            support_level="full",
                        )
                        invalidated[invalid["scenario_id"]] = invalid

            for start in range(0, len(segment) - 4 + 1):
                window = segment[start : start + 4]
                evaluation = evaluate_correction_window(window)
                if not evaluation.get("valid"):
                    for pattern_class in ("zigzag", "flat"):
                        scenario = _correction_scenario(
                            window,
                            pattern_class=pattern_class,
                            evaluation=evaluation,
                            valid=False,
                            reason="invalid_a_b_c_structure",
                        )
                        invalidated[scenario["scenario_id"]] = scenario
                    continue

                zigzag_valid = not bool(evaluation["b_crosses_origin"]) and bool(evaluation["c_extends_past_a"])
                zigzag = _correction_scenario(
                    window,
                    pattern_class="zigzag",
                    evaluation=evaluation,
                    valid=zigzag_valid,
                    reason="zigzag_b_crossed_origin_or_c_failed_to_extend_a",
                )
                (candidates if zigzag_valid else invalidated)[zigzag["scenario_id"]] = zigzag

                # Without 6C geometry, a structurally alternating A-B-C can be
                # represented as a flat candidate but must remain geometry-unresolved.
                flat = _correction_scenario(
                    window,
                    pattern_class="flat",
                    evaluation=evaluation,
                    valid=True,
                    reason="",
                )
                candidates[flat["scenario_id"]] = flat

            for pattern_class, minimum in (("triangle", 6), ("double_three", 7), ("triple_three", 9)):
                scenario = _uncertain_complex_correction(segment, pattern_class, minimum)
                if scenario is not None:
                    uncertainty[scenario["scenario_id"]] = scenario

        ordered = sorted(candidates.values(), key=_scenario_rank, reverse=True)
        ordered_invalid = sorted(invalidated.values(), key=_scenario_rank, reverse=True)
        ordered_uncertainty = sorted(uncertainty.values(), key=_scenario_rank, reverse=True)
        effective_as_of = _iso_date(as_of, "as_of") if as_of is not None else (
            max((p["confirmed_time"] for p in group), default=None)
        )
        primary = ordered[0] if ordered else _insufficient_primary(symbol, timeframe, degree, effective_as_of)
        alternatives = ordered[1 : 1 + max_alternatives] if ordered else []

        outputs.append(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "degree": degree,
                "as_of": effective_as_of,
                "selection_policy": "deterministic_structural_recency_not_probability_or_truth",
                "single_true_count_claimed": False,
                "primary_scenario": primary,
                "alternative_scenarios": alternatives,
                "invalidated_scenarios": ordered_invalid[:max_invalidated],
                "uncertainty_alternatives": ordered_uncertainty,
                "ambiguities": ambiguities,
                "segment_count": len(segments),
                "input_confirmed_pivot_count": len(group),
                "warnings": ["ambiguous_intrabar_sequence_excluded"] if ambiguities else [],
                "fibonacci_used": False,
                "historical_performance_used": False,
                "scanner_state_used": False,
                "trade_decision": None,
                "research_only": True,
            }
        )

    return outputs
