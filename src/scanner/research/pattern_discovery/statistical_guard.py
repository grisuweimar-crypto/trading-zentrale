"""Phase L4 Statistical Discovery Guard.

This module converts L3 discovery candidates into guarded Discovery Evidence.
It controls multiplicity, temporal dependence, support, concentration,
baseline lift and robust uncertainty while keeping discovery strictly separate
from candidate freeze, prospective confirmation, ratings and production.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from hashlib import sha256
import json
from math import ceil, erfc, isfinite, sqrt
from pathlib import Path
import random
from statistics import median
from typing import Any, Mapping, Sequence

from .boundary import PatternDiscoveryBoundary
from .feature_library import FeatureLibrary
from .run_contract import verify_run_manifest
from .search_engine import (
    _build_atoms,
    _normalize_observations,
    run_discovery_search,
    verify_search_result,
)


SCHEMA_VERSION = "pattern_discovery_l4_statistical_guard_v1"
EVIDENCE_SCHEMA_VERSION = "pattern_discovery_l4_evidence_v1"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "l4_statistical_guard_v1.json"
)


class DiscoveryGuardError(ValueError):
    """Raised when L4 statistical guard invariants are violated."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise DiscoveryGuardError(f"value_required:{field}")
    return result


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DiscoveryGuardError(f"finite_number_required:{field}")
    result = float(value)
    if not isfinite(result):
        raise DiscoveryGuardError(f"finite_number_required:{field}")
    return result


def _percentile(values: Sequence[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lo = int(position)
    hi = min(lo + 1, len(ordered) - 1)
    fraction = position - lo
    return ordered[lo] * (1.0 - fraction) + ordered[hi] * fraction


def load_guard_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DiscoveryGuardError(
            f"statistical_guard_contract_unreadable:{target}"
        ) from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != SCHEMA_VERSION
    ):
        raise DiscoveryGuardError("statistical_guard_contract_schema_invalid")
    if payload.get("research_only") is not True:
        raise DiscoveryGuardError("statistical_guard_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise DiscoveryGuardError(
            "statistical_guard_productive_integration_forbidden"
        )
    if payload.get("execution_allowed") is not False:
        raise DiscoveryGuardError("statistical_guard_execution_forbidden")
    return payload


def guard_contract_hash(contract: Mapping[str, Any] | None = None) -> str:
    value = dict(contract) if contract is not None else load_guard_contract()
    return _hash(value)


def _validate_method_contract(
    prereg: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> None:
    primary = _text(
        prereg.get("statistical_primary_method"),
        "preregistration.statistical_primary_method",
    )
    if primary not in set(contract["supported_statistical_primary_methods"]):
        raise DiscoveryGuardError(
            f"l4_statistical_primary_method_not_supported:{primary}"
        )

    requested_checks = set(str(x) for x in prereg["robustness_checks"])
    implemented = set(str(x) for x in contract["implemented_robustness_checks"])
    missing = sorted(requested_checks - implemented)
    if missing:
        raise DiscoveryGuardError(
            "l4_preregistered_robustness_check_not_implemented:"
            + ",".join(missing)
        )

    mt = prereg["multiple_testing"]
    method = str(mt["primary_method"])
    supported = set(contract["multiple_testing"]["supported_primary_methods"])
    if method not in supported:
        if method == "PREDECLARED_SINGLE_PRIMARY":
            raise DiscoveryGuardError(
                "l4_single_primary_fails_closed_without_preregistered_alpha"
            )
        if method == "CUSTOM_PREDECLARED":
            raise DiscoveryGuardError(
                "l4_custom_multiple_testing_not_implemented"
            )
        raise DiscoveryGuardError(
            f"l4_multiple_testing_method_not_supported:{method}"
        )


def _aligned(value: float, expected_direction: str) -> float:
    return value if expected_direction == "POSITIVE" else -value


def _date_key(row: Mapping[str, Any]) -> str:
    return str(row["as_of"])[:10]


def _condition_match_sets(
    manifest: Mapping[str, Any],
    observations: Sequence[Mapping[str, Any]],
    *,
    feature_library: FeatureLibrary,
    cutoff,
) -> tuple[list[dict[str, Any]], dict[str, set[int]]]:
    atoms, _ = _build_atoms(
        feature_library,
        manifest["preregistration"],
        {
            "atom_generation": {
                "included_transformations": [
                    "change_direction",
                    "threshold_crossing",
                    "state_transition",
                    "regime_context",
                    "raw",
                ]
            }
        },
        observations,
        cutoff,
    )
    return atoms, {
        str(atom["atom_id"]): set(atom["matching_observation_indices"])
        for atom in atoms
    }


def _candidate_match_indices(
    candidate: Mapping[str, Any],
    atom_support: Mapping[str, set[int]],
    total_rows: int,
) -> set[int]:
    matched = set(range(total_rows))
    for condition in candidate["conditions"]:
        atom_id = str(condition["atom_id"])
        if atom_id not in atom_support:
            raise DiscoveryGuardError(
                f"l4_candidate_atom_not_replayable:{candidate['candidate_id']}:{atom_id}"
            )
        matched &= atom_support[atom_id]
        if not matched:
            break
    return matched


def _support_regions(
    occurrence_indices: Sequence[int],
    observations: Sequence[Mapping[str, Any]],
    baseline_indices: Sequence[int],
    block_length: int,
) -> tuple[int, dict[int, int], dict[str, int], list[str]]:
    baseline_dates = sorted(
        {_date_key(observations[index]) for index in baseline_indices}
    )
    positions = {day: idx for idx, day in enumerate(baseline_dates)}
    occurrence_dates = sorted(
        {
            _date_key(observations[index])
            for index in occurrence_indices
            if _date_key(observations[index]) in positions
        },
        key=lambda day: positions[day],
    )

    region_by_date: dict[str, int] = {}
    region_starts: list[int] = []
    current_region = -1
    current_start: int | None = None
    for day in occurrence_dates:
        pos = positions[day]
        if current_start is None or pos - current_start >= block_length:
            current_region += 1
            current_start = pos
            region_starts.append(pos)
        region_by_date[day] = current_region

    region_by_index: dict[int, int] = {}
    for index in occurrence_indices:
        day = _date_key(observations[index])
        if day in region_by_date:
            region_by_index[index] = region_by_date[day]

    return len(region_starts), region_by_index, region_by_date, baseline_dates


def _hhi(counts: Counter) -> float | None:
    total = sum(counts.values())
    if total <= 0:
        return None
    return float(sum((count / total) ** 2 for count in counts.values()))


def _concentration(
    occurrence_indices: Sequence[int],
    observations: Sequence[Mapping[str, Any]],
    region_by_index: Mapping[int, int],
) -> dict[str, Any]:
    symbol = Counter(str(observations[index]["symbol"]) for index in occurrence_indices)
    date = Counter(_date_key(observations[index]) for index in occurrence_indices)
    region = Counter(
        str(region_by_index[index])
        for index in occurrence_indices
        if index in region_by_index
    )

    def top_share(counter: Counter) -> float | None:
        total = sum(counter.values())
        return max(counter.values()) / total if total and counter else None

    return {
        "top_symbol_share": top_share(symbol),
        "symbol_hhi": _hhi(symbol),
        "top_observation_date_share": top_share(date),
        "observation_date_hhi": _hhi(date),
        "top_support_region_share": top_share(region),
        "support_region_hhi": _hhi(region),
    }


def _effective_n_proxy(
    occurrence_indices: Sequence[int],
    observations: Sequence[Mapping[str, Any]],
    region_by_index: Mapping[int, int],
) -> int:
    clusters = {
        (str(observations[index]["symbol"]), int(region_by_index[index]))
        for index in occurrence_indices
        if index in region_by_index
    }
    return len(clusters)


def _split_diagnostics(
    occurrence_indices: Sequence[int],
    observations: Sequence[Mapping[str, Any]],
    *,
    expected_direction: str,
    fields: Sequence[str],
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field in fields:
        grouped: dict[str, list[float]] = defaultdict(list)
        for index in occurrence_indices:
            row = observations[index]
            value = row.get(field)
            if value is None or (isinstance(value, str) and not value.strip()):
                continue
            grouped[str(value)].append(
                _aligned(
                    float(next(iter(row["outcomes"].values()))["value"]),
                    expected_direction,
                )
            )
        if not grouped:
            continue
        total = sum(len(values) for values in grouped.values())
        out[field] = {
            key: {
                "raw_n": len(values),
                "share": len(values) / total,
                "hit_rate": sum(1 for value in values if value > 0) / len(values),
                "mean_aligned_outcome": sum(values) / len(values),
            }
            for key, values in sorted(grouped.items())
        }
    return out


def _bootstrap_intervals(
    *,
    candidate_indices: Sequence[int],
    baseline_indices: Sequence[int],
    observations: Sequence[Mapping[str, Any]],
    target_id: str,
    expected_direction: str,
    horizon: int,
    run_id: str,
    candidate_id: str,
    contract: Mapping[str, Any],
    support_region_count: int,
) -> dict[str, Any]:
    multiplier = int(contract["dependence"]["block_length_sessions_multiplier"])
    block_length = max(1, multiplier * int(horizon))
    baseline_dates = sorted(
        {_date_key(observations[index]) for index in baseline_indices}
    )
    repetitions = int(contract["dependence"]["bootstrap_repetitions"])
    low_q, high_q = [
        float(x) for x in contract["dependence"]["bootstrap_interval"]
    ]

    diagnostics = {
        "method": contract["dependence"]["bootstrap_method"],
        "block_length_sessions": block_length,
        "baseline_date_count": len(baseline_dates),
        "support_region_count": support_region_count,
        "repetitions_requested": repetitions,
        "repetitions_completed": 0,
    }
    if (
        repetitions <= 0
        or len(baseline_dates) < 2
        or not candidate_indices
        or support_region_count < 2
    ):
        return {
            **diagnostics,
            "aligned_effect_interval_95": None,
            "probability_lift_interval_95": None,
        }

    span = min(block_length, len(baseline_dates))
    blocks = [
        [
            baseline_dates[(start + offset) % len(baseline_dates)]
            for offset in range(span)
        ]
        for start in range(len(baseline_dates))
    ]
    candidate_by_day: dict[str, list[float]] = defaultdict(list)
    baseline_by_day: dict[str, list[float]] = defaultdict(list)

    for index in candidate_indices:
        outcome = observations[index]["outcomes"].get(target_id)
        if outcome:
            candidate_by_day[_date_key(observations[index])].append(
                _aligned(float(outcome["value"]), expected_direction)
            )
    for index in baseline_indices:
        outcome = observations[index]["outcomes"].get(target_id)
        if outcome:
            baseline_by_day[_date_key(observations[index])].append(
                _aligned(float(outcome["value"]), expected_direction)
            )

    seed_material = f"{run_id}|{candidate_id}|L4-MBB-v1"
    seed = int.from_bytes(
        sha256(seed_material.encode("utf-8")).digest()[:8],
        "big",
    )
    rng = random.Random(seed)
    draws_per_rep = int(ceil(len(baseline_dates) / span))
    effect_estimates: list[float] = []
    lift_estimates: list[float] = []

    for _ in range(repetitions):
        chosen = [rng.randrange(len(blocks)) for _ in range(draws_per_rep)]
        sampled_dates = [
            day
            for block_index in chosen
            for day in blocks[block_index]
        ][: len(baseline_dates)]

        candidate_values = [
            value
            for day in sampled_dates
            for value in candidate_by_day.get(day, [])
        ]
        baseline_values = [
            value
            for day in sampled_dates
            for value in baseline_by_day.get(day, [])
        ]
        if not candidate_values or not baseline_values:
            continue

        effect_estimates.append(
            sum(candidate_values) / len(candidate_values)
        )
        candidate_rate = (
            sum(1 for value in candidate_values if value > 0)
            / len(candidate_values)
        )
        baseline_rate = (
            sum(1 for value in baseline_values if value > 0)
            / len(baseline_values)
        )
        lift_estimates.append(candidate_rate - baseline_rate)

    diagnostics["repetitions_completed"] = len(effect_estimates)
    return {
        **diagnostics,
        "aligned_effect_interval_95": (
            [
                _percentile(effect_estimates, low_q),
                _percentile(effect_estimates, high_q),
            ]
            if effect_estimates
            else None
        ),
        "probability_lift_interval_95": (
            [
                _percentile(lift_estimates, low_q),
                _percentile(lift_estimates, high_q),
            ]
            if lift_estimates
            else None
        ),
    }


def _raw_dependency_aware_p(
    hit_rate: float | None,
    baseline_probability: float | None,
    effective_n_proxy: int,
) -> float:
    if (
        hit_rate is None
        or baseline_probability is None
        or effective_n_proxy <= 0
    ):
        return 1.0
    p0 = min(1.0, max(0.0, float(baseline_probability)))
    observed = min(1.0, max(0.0, float(hit_rate)))
    variance = p0 * (1.0 - p0) / effective_n_proxy
    if variance <= 0:
        return 0.0 if observed > p0 else 1.0
    z = (observed - p0) / sqrt(variance)
    p = 0.5 * erfc(z / sqrt(2.0))
    return float(min(1.0, max(0.0, p)))


def _adjust_family_pvalues(
    raw: Sequence[tuple[str, float]],
    *,
    method: str,
    parameters: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    ordered = sorted(
        ((candidate_id, min(1.0, max(0.0, float(p)))) for candidate_id, p in raw),
        key=lambda item: (item[1], item[0]),
    )
    m = len(ordered)
    if not m:
        return {}

    if method == "BONFERRONI_FWER":
        alpha = _finite(parameters.get("family_alpha"), "family_alpha")
        return {
            candidate_id: {
                "raw_p_value": p,
                "adjusted_p_value": min(1.0, p * m),
                "threshold": alpha,
                "passed": min(1.0, p * m) <= alpha,
            }
            for candidate_id, p in ordered
        }

    if method == "HOLM_FWER":
        alpha = _finite(parameters.get("family_alpha"), "family_alpha")
        adjusted_sorted: list[tuple[str, float, float]] = []
        running = 0.0
        for rank, (candidate_id, p) in enumerate(ordered, start=1):
            adjusted = min(1.0, (m - rank + 1) * p)
            running = max(running, adjusted)
            adjusted_sorted.append((candidate_id, p, min(1.0, running)))
        return {
            candidate_id: {
                "raw_p_value": p,
                "adjusted_p_value": adjusted,
                "threshold": alpha,
                "passed": adjusted <= alpha,
            }
            for candidate_id, p, adjusted in adjusted_sorted
        }

    if method == "BENJAMINI_HOCHBERG_FDR":
        q = _finite(parameters.get("fdr_q"), "fdr_q")
        adjusted_values = [1.0] * m
        running = 1.0
        for zero_index in range(m - 1, -1, -1):
            rank = zero_index + 1
            p = ordered[zero_index][1]
            running = min(running, p * m / rank)
            adjusted_values[zero_index] = min(1.0, running)
        return {
            candidate_id: {
                "raw_p_value": p,
                "adjusted_p_value": adjusted_values[index],
                "threshold": q,
                "passed": adjusted_values[index] <= q,
            }
            for index, (candidate_id, p) in enumerate(ordered)
        }

    raise DiscoveryGuardError(
        f"l4_multiple_testing_method_not_supported:{method}"
    )


def _base_candidate_evidence(
    *,
    candidate: Mapping[str, Any],
    matched_indices: Sequence[int],
    occurrence_indices: Sequence[int],
    baseline_indices: Sequence[int],
    observations: Sequence[Mapping[str, Any]],
    contract: Mapping[str, Any],
    prereg: Mapping[str, Any],
    run_id: str,
) -> dict[str, Any]:
    target_id = str(candidate["target_id"])
    expected = str(candidate["expected_direction"])
    horizon = int(candidate["horizon_sessions"])
    block_length = max(
        1,
        int(contract["dependence"]["block_length_sessions_multiplier"])
        * horizon,
    )

    values = [
        _aligned(
            float(observations[index]["outcomes"][target_id]["value"]),
            expected,
        )
        for index in occurrence_indices
    ]
    baseline_values = [
        _aligned(
            float(observations[index]["outcomes"][target_id]["value"]),
            expected,
        )
        for index in baseline_indices
    ]

    raw_n = len(values)
    mean_aligned = sum(values) / raw_n if raw_n else None
    median_aligned = float(median(values)) if values else None
    hit_rate = (
        sum(1 for value in values if value > 0) / raw_n
        if raw_n
        else None
    )
    baseline_probability = (
        sum(1 for value in baseline_values if value > 0)
        / len(baseline_values)
        if baseline_values
        else None
    )
    baseline_lift = (
        hit_rate - baseline_probability
        if hit_rate is not None and baseline_probability is not None
        else None
    )

    support_count, region_by_index, _, _ = _support_regions(
        occurrence_indices,
        observations,
        baseline_indices,
        block_length,
    )
    concentration = _concentration(
        occurrence_indices,
        observations,
        region_by_index,
    )
    effective_n = _effective_n_proxy(
        occurrence_indices,
        observations,
        region_by_index,
    )
    robust = _bootstrap_intervals(
        candidate_indices=occurrence_indices,
        baseline_indices=baseline_indices,
        observations=observations,
        target_id=target_id,
        expected_direction=expected,
        horizon=horizon,
        run_id=run_id,
        candidate_id=str(candidate["candidate_id"]),
        contract=contract,
        support_region_count=support_count,
    )
    coverage = len(occurrence_indices) / len(matched_indices) if matched_indices else 0.0

    split_fields = [
        str(field) for field in contract["diagnostic_split_fields"]
    ]
    split_diagnostics: dict[str, Any] = {}
    for field in split_fields:
        grouped: dict[str, list[float]] = defaultdict(list)
        for index in occurrence_indices:
            row = observations[index]
            state = row.get(field)
            if state is None or (isinstance(state, str) and not state.strip()):
                continue
            grouped[str(state)].append(
                _aligned(
                    float(row["outcomes"][target_id]["value"]),
                    expected,
                )
            )
        if grouped:
            total = sum(len(items) for items in grouped.values())
            split_diagnostics[field] = {
                state: {
                    "raw_n": len(items),
                    "share": len(items) / total,
                    "hit_rate": (
                        sum(1 for value in items if value > 0) / len(items)
                    ),
                    "mean_aligned_outcome": sum(items) / len(items),
                }
                for state, items in sorted(grouped.items())
            }

    raw_p = _raw_dependency_aware_p(
        hit_rate,
        baseline_probability,
        effective_n,
    )

    raw_outcomes = [
        float(observations[index]["outcomes"][target_id]["value"])
        for index in occurrence_indices
    ]
    peer_mean = (
        sum(raw_outcomes) / len(raw_outcomes)
        if candidate["pattern_type"] == "RELATIVE_ALPHA" and raw_outcomes
        else None
    )
    peer_median = (
        float(median(raw_outcomes))
        if candidate["pattern_type"] == "RELATIVE_ALPHA" and raw_outcomes
        else None
    )

    return {
        "candidate_id": candidate["candidate_id"],
        "spec_hash": candidate["spec_hash"],
        "family_id": candidate["family_id"],
        "pattern_type": candidate["pattern_type"],
        "target_id": target_id,
        "horizon_sessions": horizon,
        "expected_direction": expected,
        "baseline": candidate["baseline"],
        "conditions": candidate["conditions"],
        "l3_gate_status": candidate["l3_gate_status"],
        "l3_rejection_reasons": list(candidate["rejection_reasons"]),
        "l3_family_rank": candidate["family_rank"],
        "l3_shortlist_status": candidate["shortlist_status"],
        "discovery_evidence": {
            "raw_n": raw_n,
            "effective_n_proxy": effective_n,
            "effective_n_method": contract["dependence"]["effective_n_method"],
            "symbol_count": len(
                {str(observations[index]["symbol"]) for index in occurrence_indices}
            ),
            "observation_date_count": len(
                {_date_key(observations[index]) for index in occurrence_indices}
            ),
            "support_region_count": support_count,
            "support_region_method": contract["dependence"]["support_region_method"],
            "condition_matched_count": len(matched_indices),
            "mature_target_count": len(occurrence_indices),
            "mature_target_coverage": coverage,
            "hit_rate": hit_rate,
            "direction_probability": hit_rate,
            "baseline_probability": baseline_probability,
            "probability_advantage_lift": baseline_lift,
            "mean_aligned_outcome": mean_aligned,
            "median_aligned_outcome": median_aligned,
            "mean_outcome": (
                sum(raw_outcomes) / len(raw_outcomes)
                if raw_outcomes else None
            ),
            "median_outcome": (
                float(median(raw_outcomes)) if raw_outcomes else None
            ),
            "mean_peer_excess": peer_mean,
            "median_peer_excess": peer_median,
            "robust_uncertainty": robust,
            "concentration": concentration,
            "diagnostic_splits": split_diagnostics,
            "raw_dependency_aware_p_value": raw_p,
            "multiple_testing": None,
            "confirmation_period": None,
            "prospective_confirmation_used": False,
            "open_blockers": [],
        },
        "l4_gate_status": None,
        "l4_rejection_reasons": [],
    }


def _l4_gate_reasons(
    record: Mapping[str, Any],
    *,
    prereg: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> list[str]:
    evidence = record["discovery_evidence"]
    reasons: list[str] = []

    if record["l3_gate_status"] != "ELIGIBLE_FOR_L4":
        reasons.append("L3_NOT_ELIGIBLE")
        return reasons

    minimum = prereg["minimum_criteria"]
    if int(evidence["raw_n"]) < int(minimum["minimum_raw_n"]):
        reasons.append("MINIMUM_RAW_N_NOT_MET")
    effect = evidence["mean_aligned_outcome"]
    if effect is None or float(effect) < float(minimum["minimum_effect_size"]):
        reasons.append("MINIMUM_EFFECT_SIZE_NOT_MET")
    if int(evidence["support_region_count"]) < int(
        minimum["minimum_temporal_support_regions"]
    ):
        reasons.append("MINIMUM_TEMPORAL_SUPPORT_REGIONS_NOT_MET")
    lift = evidence["probability_advantage_lift"]
    if lift is None or float(lift) < float(minimum["minimum_baseline_lift"]):
        reasons.append("MINIMUM_BASELINE_LIFT_NOT_MET")

    coverage_min = float(
        contract["coverage_gates"]["minimum_mature_target_coverage"]
    )
    if float(evidence["mature_target_coverage"]) < coverage_min:
        reasons.append("MATURE_TARGET_COVERAGE_NOT_MET")

    concentration = evidence["concentration"]
    concentration_cfg = contract["concentration_gates"]
    if int(evidence["symbol_count"]) < int(
        concentration_cfg["minimum_symbol_count"]
    ):
        reasons.append("MINIMUM_SYMBOL_BREADTH_NOT_MET")
    checks = (
        ("top_symbol_share", "maximum_top_symbol_share", "SYMBOL_CONCENTRATION_EXCEEDED"),
        (
            "top_observation_date_share",
            "maximum_top_observation_date_share",
            "DATE_CONCENTRATION_EXCEEDED",
        ),
        (
            "top_support_region_share",
            "maximum_top_support_region_share",
            "SUPPORT_REGION_CONCENTRATION_EXCEEDED",
        ),
    )
    for evidence_key, config_key, reason in checks:
        value = concentration.get(evidence_key)
        if value is None or float(value) > float(concentration_cfg[config_key]):
            reasons.append(reason)

    robust = evidence["robust_uncertainty"]
    effect_interval = robust.get("aligned_effect_interval_95")
    lift_interval = robust.get("probability_lift_interval_95")
    if (
        contract["robustness_gates"]["aligned_effect_interval_must_exclude_zero"]
        and (
            not effect_interval
            or effect_interval[0] is None
            or float(effect_interval[0]) <= 0.0
        )
    ):
        reasons.append("ROBUST_EFFECT_INTERVAL_NOT_DIRECTIONAL")
    if (
        contract["robustness_gates"]["probability_lift_interval_must_exclude_zero"]
        and (
            not lift_interval
            or lift_interval[0] is None
            or float(lift_interval[0]) <= 0.0
        )
    ):
        reasons.append("ROBUST_PROBABILITY_LIFT_INTERVAL_NOT_POSITIVE")

    mt = evidence["multiple_testing"]
    if not isinstance(mt, Mapping) or mt.get("passed") is not True:
        reasons.append("MULTIPLE_TESTING_NOT_PASSED")

    return sorted(set(reasons))


def apply_statistical_guard(
    manifest: Mapping[str, Any],
    l3_result: Mapping[str, Any],
    observations: Sequence[Mapping[str, Any]],
    *,
    repo_root: str | Path,
    feature_library: FeatureLibrary | None = None,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Apply L4 guards to every tested L3 candidate."""
    verify_run_manifest(manifest)
    verify_search_result(l3_result)
    guard_contract = (
        dict(contract) if contract is not None else load_guard_contract()
    )
    prereg = manifest["preregistration"]
    _validate_method_contract(prereg, guard_contract)

    if l3_result.get("run_id") != manifest.get("run_id"):
        raise DiscoveryGuardError("l4_l3_run_id_mismatch")
    if l3_result.get("l1_manifest_hash") != manifest.get("manifest_hash"):
        raise DiscoveryGuardError("l4_l3_manifest_hash_mismatch")

    library = feature_library or FeatureLibrary()
    replay = run_discovery_search(
        manifest,
        observations,
        repo_root=repo_root,
        feature_library=library,
    )
    if replay["result_hash"] != l3_result["result_hash"]:
        raise DiscoveryGuardError("l4_l3_replay_hash_mismatch")

    cutoff = prereg["data_cutoff"]
    from .search_engine import _timestamp as _l3_timestamp
    normalized = _normalize_observations(
        observations,
        cutoff=_l3_timestamp(cutoff, "data_cutoff"),
        target_ids=list(prereg["targets"]),
    )
    atoms, atom_support = _condition_match_sets(
        manifest,
        normalized,
        feature_library=library,
        cutoff=_l3_timestamp(cutoff, "data_cutoff"),
    )
    _ = atoms

    baseline_by_target = {
        str(target_id): [
            index
            for index, row in enumerate(normalized)
            if str(target_id) in row["outcomes"]
        ]
        for target_id in prereg["targets"]
    }

    records: list[dict[str, Any]] = []
    family_raw_p: dict[str, list[tuple[str, float]]] = defaultdict(list)

    for candidate in l3_result["candidates"]:
        matched = sorted(
            _candidate_match_indices(
                candidate,
                atom_support,
                len(normalized),
            )
        )
        baseline_indices = baseline_by_target[str(candidate["target_id"])]
        baseline_set = set(baseline_indices)
        occurrences = [index for index in matched if index in baseline_set]
        record = _base_candidate_evidence(
            candidate=candidate,
            matched_indices=matched,
            occurrence_indices=occurrences,
            baseline_indices=baseline_indices,
            observations=normalized,
            contract=guard_contract,
            prereg=prereg,
            run_id=str(manifest["run_id"]),
        )
        records.append(record)
        family_raw_p[str(candidate["family_id"])].append(
            (
                str(candidate["candidate_id"]),
                float(
                    record["discovery_evidence"][
                        "raw_dependency_aware_p_value"
                    ]
                ),
            )
        )

    mt_method = str(prereg["multiple_testing"]["primary_method"])
    mt_parameters = dict(prereg["multiple_testing"]["parameters"])
    adjusted_by_candidate: dict[str, dict[str, Any]] = {}
    family_mt_summary: list[dict[str, Any]] = []
    for family_id in sorted(family_raw_p):
        adjusted = _adjust_family_pvalues(
            family_raw_p[family_id],
            method=mt_method,
            parameters=mt_parameters,
        )
        adjusted_by_candidate.update(adjusted)
        family_mt_summary.append(
            {
                "family_id": family_id,
                "method": mt_method,
                "tested_candidate_count": len(family_raw_p[family_id]),
                "passed_candidate_count": sum(
                    1 for item in adjusted.values() if item["passed"]
                ),
                "family_includes_all_tested_l3_candidates": True,
            }
        )

    for record in records:
        candidate_id = str(record["candidate_id"])
        mt = adjusted_by_candidate[candidate_id]
        record["discovery_evidence"]["multiple_testing"] = {
            "method": mt_method,
            **mt,
            "family_scope": guard_contract["multiple_testing"]["family_scope"],
            "p_value_method": guard_contract["multiple_testing"][
                "raw_p_value_method"
            ],
        }
        reasons = _l4_gate_reasons(
            record,
            prereg=prereg,
            contract=guard_contract,
        )
        record["l4_rejection_reasons"] = reasons
        record["discovery_evidence"]["open_blockers"] = list(reasons)
        if record["l3_gate_status"] != "ELIGIBLE_FOR_L4":
            record["l4_gate_status"] = "REJECTED_BEFORE_L4"
        elif reasons:
            record["l4_gate_status"] = "REJECTED_L4"
        else:
            record["l4_gate_status"] = "ELIGIBLE_FOR_L5"

    records.sort(
        key=lambda record: (
            str(record["family_id"]),
            str(record["candidate_id"]),
        )
    )

    result: dict[str, Any] = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L4",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "run_id": manifest["run_id"],
        "run_identity_hash": manifest["run_identity_hash"],
        "l1_manifest_hash": manifest["manifest_hash"],
        "l3_result_hash": l3_result["result_hash"],
        "l4_guard_contract_hash": guard_contract_hash(guard_contract),
        "feature_library_version": library.version,
        "feature_library_hash": library.library_hash,
        "data_cutoff": prereg["data_cutoff"],
        "discovery_only": True,
        "prospective_confirmation_used": False,
        "l5_candidate_freeze_applied": False,
        "statistical_primary_method": prereg["statistical_primary_method"],
        "multiple_testing_plan": prereg["multiple_testing"],
        "dependence_method": {
            "block_length_sessions_multiplier": guard_contract["dependence"][
                "block_length_sessions_multiplier"
            ],
            "effective_n_method": guard_contract["dependence"][
                "effective_n_method"
            ],
            "bootstrap_method": guard_contract["dependence"][
                "bootstrap_method"
            ],
        },
        "family_multiple_testing": family_mt_summary,
        "candidate_evidence": records,
        "counts": {
            "l3_tested_candidates": len(records),
            "rejected_before_l4": sum(
                1
                for record in records
                if record["l4_gate_status"] == "REJECTED_BEFORE_L4"
            ),
            "rejected_l4": sum(
                1
                for record in records
                if record["l4_gate_status"] == "REJECTED_L4"
            ),
            "eligible_for_l5": sum(
                1
                for record in records
                if record["l4_gate_status"] == "ELIGIBLE_FOR_L5"
            ),
            "negative_or_rejected_results_retained": sum(
                1
                for record in records
                if record["l4_gate_status"] != "ELIGIBLE_FOR_L5"
            ),
        },
        "boundaries": {
            "candidate_freeze_performed": False,
            "qm_c_registration_performed": False,
            "prospective_confirmation_performed": False,
            "rating_assigned": False,
            "promotion_performed": False,
            "productive_semantics_changed": False,
        },
    }
    PatternDiscoveryBoundary().assert_research_payload(result)
    result["evidence_hash"] = _hash(result)
    return result


def verify_statistical_evidence(
    result: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(result, Mapping):
        raise DiscoveryGuardError("l4_evidence_must_be_object")
    if result.get("schema_version") != EVIDENCE_SCHEMA_VERSION:
        raise DiscoveryGuardError("l4_evidence_schema_invalid")
    if result.get("research_only") is not True:
        raise DiscoveryGuardError("l4_evidence_research_only_guard_missing")
    if result.get("productive_integration_enabled") is not False:
        raise DiscoveryGuardError("l4_evidence_productive_integration_forbidden")
    if result.get("execution_allowed") is not False:
        raise DiscoveryGuardError("l4_evidence_execution_forbidden")
    if result.get("prospective_confirmation_used") is not False:
        raise DiscoveryGuardError("l4_confirmation_boundary_violation")
    if result.get("l5_candidate_freeze_applied") is not False:
        raise DiscoveryGuardError("l4_freeze_boundary_violation")

    stored = _text(result.get("evidence_hash"), "evidence_hash")
    body = dict(result)
    body.pop("evidence_hash", None)
    if _hash(body) != stored:
        raise DiscoveryGuardError("l4_evidence_hash_mismatch")
    PatternDiscoveryBoundary().assert_research_payload(result)
    return {
        "valid": True,
        "run_id": result.get("run_id"),
        "evidence_hash": stored,
        "candidate_count": len(result.get("candidate_evidence", [])),
        "eligible_for_l5": result.get("counts", {}).get("eligible_for_l5"),
    }


def evidence_repo_path(
    run_id: str,
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = dict(contract) if contract is not None else load_guard_contract()
    return str(spec["identity"]["evidence_path_template"]).format(
        run_id=_text(run_id, "run_id")
    )


def write_statistical_evidence(
    repo_root: str | Path,
    result: Mapping[str, Any],
    *,
    boundary: PatternDiscoveryBoundary | None = None,
) -> Path:
    """Persist one immutable L4 evidence artifact in the L0 namespace."""
    verify_statistical_evidence(result)
    guard = boundary or PatternDiscoveryBoundary()
    repo_path = evidence_repo_path(str(result["run_id"]))
    guard.assert_write_path_allowed(repo_path)

    root = Path(repo_root).resolve()
    target = (root / repo_path).resolve()
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise DiscoveryGuardError("l4_evidence_path_outside_repo") from exc
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with target.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(
                dict(result),
                handle,
                indent=2,
                sort_keys=True,
                ensure_ascii=True,
                allow_nan=False,
            )
            handle.write("\n")
    except FileExistsError as exc:
        raise DiscoveryGuardError(
            f"l4_evidence_already_exists:{repo_path}"
        ) from exc
    return target
