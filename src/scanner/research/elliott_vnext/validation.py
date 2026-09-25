from __future__ import annotations

"""Module 6G — PIT historical/prospective validation for Elliott vNext.

6G validates *frozen upstream claims*.  It does not discover a new Elliott
count, tune Fibonacci levels, invent an execution policy, or emit a trading
decision.  Legacy evidence through the Module-6 rule-freeze date is retained as
research/descriptive evidence only; formal promotion evidence must be unspent
and prospective.
"""

from dataclasses import dataclass
from datetime import date
from hashlib import sha256
import json
import math
from typing import Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from .cross_system import extract_elliott_events
from .market_context import resolve_context_assignments


VALIDATION_HORIZONS: tuple[int, ...] = (5, 10, 20, 40, 60)
STAGE_RANK = {
    "wave_2_complete": 2,
    "wave_3_complete": 3,
    "wave_4_complete": 4,
    "wave_5_complete": 5,
}


class ValidationInputError(ValueError):
    """Raised when validation input violates PIT/provenance contracts."""


@dataclass(frozen=True)
class ValidationConfig:
    stable_start: str = "2026-04-15"
    rules_frozen_through: str = "2026-09-25"
    horizons: tuple[int, ...] = VALIDATION_HORIZONS
    bootstrap_reps: int = 1000
    random_seed: int = 20260925
    min_support_regions: int = 2
    replay_price_basis: str = "adjusted"

    def __post_init__(self) -> None:
        _iso_date(self.stable_start, "stable_start")
        _iso_date(self.rules_frozen_through, "rules_frozen_through")
        if not self.horizons or any(int(h) <= 0 for h in self.horizons):
            raise ValueError("horizons must contain positive session counts")
        if len(set(map(int, self.horizons))) != len(self.horizons):
            raise ValueError("horizons must be unique")
        if self.bootstrap_reps < 0:
            raise ValueError("bootstrap_reps must be >= 0")
        if self.min_support_regions < 2:
            raise ValueError("min_support_regions must be >= 2")
        if self.replay_price_basis not in {"adjusted", "raw", "auto"}:
            raise ValueError("unsupported replay_price_basis")


def _iso_date(value: object, field: str) -> str:
    text = str(value or "")
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValidationInputError(f"invalid_{field}") from exc


def _finite(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _hash(payload: Mapping[str, object]) -> str:
    frozen = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)
    return sha256(frozen.encode("utf-8")).hexdigest()


def validation_partition(value: object, config: ValidationConfig = ValidationConfig()) -> str:
    day = _iso_date(value, "claim_available_from")
    return (
        "prospective_unspent"
        if day > config.rules_frozen_through
        else "legacy_development_descriptive_only"
    )


def _scenario_lookup(snapshot: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    scenarios: list[Mapping[str, object]] = []
    primary = snapshot.get("primary_scenario")
    if isinstance(primary, Mapping):
        scenarios.append(primary)
    alternatives = snapshot.get("alternative_scenarios")
    if isinstance(alternatives, list):
        scenarios.extend(item for item in alternatives if isinstance(item, Mapping))
    return {
        str(item["scenario_id"]): item
        for item in scenarios
        if item.get("scenario_id") not in (None, "")
    }


def _pivot_identity(pivot: Mapping[str, object]) -> tuple[object, ...]:
    return (
        str(pivot.get("role", "")),
        str(pivot.get("pivot_time", "")),
        str(pivot.get("confirmed_time", "")),
        _finite(pivot.get("price")),
        str(pivot.get("kind", "")),
    )


def _lineage_id(scenario: Mapping[str, object]) -> str | None:
    if scenario.get("family") != "motive":
        return None
    pivots = scenario.get("pivots")
    if not isinstance(pivots, list):
        return None
    prefix = [p for p in pivots if isinstance(p, Mapping) and str(p.get("role")) in {"origin", "wave_1", "wave_2"}]
    if len(prefix) < 3:
        return None
    prefix = sorted(prefix, key=lambda p: {"origin": 0, "wave_1": 1, "wave_2": 2}.get(str(p.get("role")), 9))[:3]
    payload = {
        "pattern_class": scenario.get("pattern_class"),
        "direction": scenario.get("direction"),
        "timeframe": scenario.get("timeframe"),
        "degree": scenario.get("degree"),
        "prefix": [_pivot_identity(p) for p in prefix],
    }
    return _hash(payload)


def extract_structure_claims(
    routed_snapshots: Iterable[Mapping[str, object]],
    config: ValidationConfig = ValidationConfig(),
) -> list[dict[str, object]]:
    """Extract first-observed motive-stage claims without performance data."""

    records: dict[tuple[str, str], dict[str, object]] = {}
    for snapshot in routed_snapshots:
        if snapshot.get("research_only") is not True:
            raise ValidationInputError("6g_requires_research_only_snapshot")
        symbol = str(snapshot.get("symbol", "")).strip()
        if not symbol:
            raise ValidationInputError("snapshot_symbol_required")
        as_of = _iso_date(snapshot.get("as_of"), "snapshot_as_of")
        for role, scenario in (
            [("primary", snapshot.get("primary_scenario"))]
            + [
                (f"alternative_{idx}", item)
                for idx, item in enumerate(snapshot.get("alternative_scenarios", []) or [], start=1)
            ]
        ):
            if not isinstance(scenario, Mapping) or scenario.get("family") != "motive":
                continue
            stage = str(scenario.get("stage", ""))
            if stage not in STAGE_RANK or str(scenario.get("status", "")).startswith("invalidated"):
                continue
            available = _iso_date(scenario.get("available_from"), "scenario_available_from")
            if available > as_of:
                raise ValidationInputError("scenario_available_after_snapshot")
            lineage = _lineage_id(scenario)
            if lineage is None:
                continue
            key = (lineage, stage)
            if key in records:
                continue
            identity = {
                "symbol": symbol,
                "lineage_id": lineage,
                "stage": stage,
                "available_from": available,
            }
            records[key] = {
                "claim_id": _hash(identity),
                "claim_type": "structure",
                "symbol": symbol,
                "available_from": available,
                "partition": validation_partition(available, config),
                "lineage_id": lineage,
                "scenario_id": scenario.get("scenario_id"),
                "scenario_role": role,
                "pattern_class": scenario.get("pattern_class"),
                "timeframe": snapshot.get("timeframe", scenario.get("timeframe")),
                "degree": snapshot.get("degree", scenario.get("degree")),
                "wave_stage": stage,
                "stage_rank": STAGE_RANK[stage],
                "elliott_direction": scenario.get("direction"),
                "rule_violations_at_claim": list(scenario.get("rule_violations", [])),
                "performance_used_to_select_claim": False,
                "research_only": True,
            }
    return sorted(records.values(), key=lambda r: (str(r["symbol"]), str(r["available_from"]), str(r["degree"]), int(r["stage_rank"])))


def evaluate_structure_progression(
    claims: Sequence[Mapping[str, object]],
    routed_snapshots: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Evaluate later structural progression/invalidation separately from returns.

    Unresolved claims stay unresolved.  They are never silently counted as
    failures merely because the available history ends.
    """

    observations: dict[str, list[dict[str, object]]] = {}
    invalidations: dict[str, list[dict[str, object]]] = {}
    for snapshot in routed_snapshots:
        as_of = _iso_date(snapshot.get("as_of"), "snapshot_as_of")
        for scenario in [snapshot.get("primary_scenario"), *(snapshot.get("alternative_scenarios", []) or [])]:
            if not isinstance(scenario, Mapping):
                continue
            lineage = _lineage_id(scenario)
            stage = str(scenario.get("stage", ""))
            if lineage and stage in STAGE_RANK and not str(scenario.get("status", "")).startswith("invalidated"):
                observations.setdefault(lineage, []).append({"as_of": as_of, "stage_rank": STAGE_RANK[stage], "stage": stage})
        for scenario in snapshot.get("invalidated_scenarios", []) or []:
            if not isinstance(scenario, Mapping):
                continue
            lineage = _lineage_id(scenario)
            if lineage:
                invalidations.setdefault(lineage, []).append({
                    "as_of": as_of,
                    "rules": list(scenario.get("rule_violations", [])),
                })

    result: list[dict[str, object]] = []
    for raw in claims:
        claim = dict(raw)
        lineage = str(claim.get("lineage_id", ""))
        start = str(claim.get("available_from", ""))
        rank = int(claim.get("stage_rank", 0))
        later_progress = sorted(
            [row for row in observations.get(lineage, []) if row["as_of"] >= start and int(row["stage_rank"]) > rank],
            key=lambda row: (row["as_of"], row["stage_rank"]),
        )
        later_invalid = sorted(
            [row for row in invalidations.get(lineage, []) if row["as_of"] >= start],
            key=lambda row: row["as_of"],
        )
        progress = later_progress[0] if later_progress else None
        invalid = later_invalid[0] if later_invalid else None
        if progress and invalid:
            progressed_first = str(progress["as_of"]) <= str(invalid["as_of"])
        else:
            progressed_first = progress is not None
        if progressed_first:
            resolution = "progressed"
            resolved_at = progress["as_of"] if progress else None
            structural_fit = 1.0
        elif invalid:
            resolution = "invalidated"
            resolved_at = invalid["as_of"]
            structural_fit = 0.0
        else:
            resolution = "unresolved"
            resolved_at = None
            structural_fit = None
        claim.update({
            "structure_resolution": resolution,
            "resolved_at": resolved_at,
            "next_stage": progress.get("stage") if progressed_first and progress else None,
            "later_invalidation_rules": invalid.get("rules", []) if invalid else [],
            "structural_fit": structural_fit,
            "performance_used_for_structure_resolution": False,
        })
        result.append(claim)
    return result


def extract_projection_claims(
    routed_snapshots: Iterable[Mapping[str, object]],
    config: ValidationConfig = ValidationConfig(),
) -> list[dict[str, object]]:
    """Extract each prospective Fibonacci zone at its first causal appearance."""

    records: dict[str, dict[str, object]] = {}
    for snapshot in routed_snapshots:
        if snapshot.get("fibonacci_used") is not True:
            continue
        symbol = str(snapshot.get("symbol", "")).strip()
        as_of = _iso_date(snapshot.get("as_of"), "snapshot_as_of")
        scenarios = _scenario_lookup(snapshot)
        for zone in snapshot.get("projection_zones", []) or []:
            if not isinstance(zone, Mapping):
                continue
            zone_id = str(zone.get("zone_id", "")).strip()
            if not zone_id or zone_id in records:
                continue
            available = _iso_date(zone.get("available_from"), "projection_available_from")
            if available > as_of:
                raise ValidationInputError("projection_available_after_snapshot")
            low = _finite(zone.get("price_low"))
            high = _finite(zone.get("price_high"))
            if low is None or high is None or low <= 0 or high <= 0 or low >= high:
                raise ValidationInputError("invalid_projection_zone_bounds")
            scenario_id = str(zone.get("scenario_id", ""))
            scenario = scenarios.get(scenario_id, {})
            status = str(zone.get("status", "projected"))
            basis = dict(zone.get("basis", {})) if isinstance(zone.get("basis"), Mapping) else {}
            records[zone_id] = {
                "claim_id": zone_id,
                "claim_type": "projection",
                "symbol": symbol,
                "available_from": available,
                "partition": validation_partition(available, config),
                "snapshot_as_of_first_observed": as_of,
                "scenario_id": scenario_id or None,
                "scenario_role": "primary" if scenario_id and scenario_id == str(snapshot.get("primary_scenario", {}).get("scenario_id", "")) else "alternative_or_unknown",
                "pattern_class": scenario.get("pattern_class"),
                "timeframe": snapshot.get("timeframe", scenario.get("timeframe")),
                "degree": snapshot.get("degree", scenario.get("degree")),
                "wave_stage": scenario.get("stage"),
                "wave_role": zone.get("wave_role"),
                "projection_type": zone.get("projection_type"),
                "price_low": low,
                "price_high": high,
                "center_price": _finite(zone.get("center_price")),
                "elliott_direction": scenario.get("direction"),
                "status_at_creation": status,
                "clean_future_hit_claim": status not in {"inside", "reached"},
                "basis": basis,
                "level": _finite(basis.get("level")),
                "level_validated_before_6g": bool(basis.get("level_validated", False)),
                "numeric_level_frozen_before_6g": bool(basis.get("numeric_level_frozen", False)),
                "fibonacci_selected_count": False,
                "research_only": True,
            }
    return sorted(records.values(), key=lambda r: (str(r["symbol"]), str(r["available_from"]), str(r.get("wave_role")), str(r.get("projection_type")), str(r["claim_id"])))


def extract_route_claims(
    routed_snapshots: Iterable[Mapping[str, object]],
    config: ValidationConfig = ValidationConfig(),
) -> list[dict[str, object]]:
    """Convert causal 6D routes to validation claims without execution semantics."""

    events = extract_elliott_events(routed_snapshots)
    result: list[dict[str, object]] = []
    for event in events:
        available = _iso_date(event.get("available_from"), "route_available_from")
        result.append({
            "claim_id": str(event["event_id"]),
            "source_event_id": str(event["event_id"]),
            "claim_type": "route_review",
            "symbol": event["symbol"],
            "available_from": available,
            "partition": validation_partition(available, config),
            "scenario_id": event.get("scenario_id"),
            "scenario_role": event.get("scenario_role"),
            "pattern_class": event.get("pattern_class"),
            "timeframe": event.get("timeframe"),
            "degree": event.get("degree"),
            "wave_stage": event.get("wave_stage"),
            "trigger": event.get("trigger"),
            "review_context": event.get("review_context"),
            "review_orientation": event.get("review_orientation"),
            "elliott_direction": event.get("elliott_direction"),
            "execution_policy_frozen": False,
            "round_trip_pnl_testable": False,
            "research_only": True,
        })
    return result


def _prepare_price_frame(prices: pd.DataFrame | Iterable[Mapping[str, object]]) -> pd.DataFrame:
    frame = prices.copy() if isinstance(prices, pd.DataFrame) else pd.DataFrame(list(prices))
    required = {"date", "symbol", "close", "adj_close", "high", "low"}
    missing = required - set(frame.columns)
    if missing:
        raise ValidationInputError(f"missing_price_columns:{','.join(sorted(missing))}")
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    frame["symbol"] = frame["symbol"].astype(str)
    for column in ("close", "adj_close", "high", "low"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
        finite = np.isfinite(frame[column].to_numpy(dtype=float, na_value=np.nan))
        frame.loc[~finite, column] = np.nan
    frame = frame.dropna(subset=["date", "symbol", "close"]).copy()
    frame = frame.loc[frame["close"] > 0].copy()
    factor = frame["adj_close"] / frame["close"]
    valid_adj = frame["adj_close"].notna() & (frame["adj_close"] > 0) & np.isfinite(factor)
    frame["adjusted_high"] = np.where(valid_adj, frame["high"] * factor, np.nan)
    frame["adjusted_low"] = np.where(valid_adj, frame["low"] * factor, np.nan)
    bad_range = (
        frame["adjusted_high"].notna()
        & frame["adjusted_low"].notna()
        & (frame["adjusted_high"] < frame["adjusted_low"])
    )
    frame.loc[bad_range, ["adjusted_high", "adjusted_low"]] = np.nan
    return (
        frame.sort_values(["symbol", "date"], kind="mergesort")
        .drop_duplicates(["symbol", "date"], keep="last")
        .reset_index(drop=True)
    )


def _direction_sign(direction: object) -> int | None:
    if direction == "up":
        return 1
    if direction == "down":
        return -1
    return None


def _review_correct(orientation: object, signed_return: float | None) -> bool | None:
    if signed_return is None:
        return None
    if orientation == "supportive_review":
        return signed_return > 0
    if orientation == "defensive_review":
        return signed_return < 0
    return None


def attach_forward_outcomes(
    claims: Sequence[Mapping[str, object]],
    prices: pd.DataFrame | Iterable[Mapping[str, object]],
    config: ValidationConfig = ValidationConfig(),
) -> list[dict[str, object]]:
    """Attach PIT forward outcomes using exact observed sessions.

    Raw close determines the session calendar.  Returns require adjusted close;
    there is no raw fallback.  Projection-zone hits begin on the *next* observed
    session so an intraday touch before an end-of-day claim cannot be counted as
    a future success.
    """

    frame = _prepare_price_frame(prices)
    groups = {
        symbol: group.reset_index(drop=True)
        for symbol, group in frame.groupby("symbol", sort=False)
    }
    result: list[dict[str, object]] = []
    for claim in claims:
        symbol = str(claim.get("symbol", ""))
        series = groups.get(symbol)
        available = pd.Timestamp(_iso_date(claim.get("available_from"), "claim_available_from"))
        for horizon in config.horizons:
            row = dict(claim)
            row["horizon_sessions"] = int(horizon)
            row["matured"] = False
            row["outcome_available"] = False
            row["forward_return"] = None
            row["signed_forward_return"] = None
            row["review_correct"] = None
            row["max_favorable_excursion"] = None
            row["max_adverse_excursion"] = None
            row["projection_hit"] = None
            row["first_hit_session"] = None
            if series is None or series.empty:
                row["outcome_missing_reason"] = "symbol_price_history_missing"
                result.append(row)
                continue
            dates = series["date"].values.astype("datetime64[ns]")
            anchor = int(np.searchsorted(dates, np.datetime64(available), side="left"))
            if anchor >= len(series):
                row["outcome_missing_reason"] = "claim_after_last_price_session"
                result.append(row)
                continue
            target = anchor + int(horizon)
            row["anchor_session"] = pd.Timestamp(series.iloc[anchor]["date"]).date().isoformat()
            row["anchor_gap_calendar_days"] = int((pd.Timestamp(series.iloc[anchor]["date"]) - available).days)
            if target >= len(series):
                row["outcome_missing_reason"] = "forward_window_not_mature"
                result.append(row)
                continue
            row["matured"] = True
            row["end_session"] = pd.Timestamp(series.iloc[target]["date"]).date().isoformat()
            start_adj = _finite(series.iloc[anchor]["adj_close"])
            end_adj = _finite(series.iloc[target]["adj_close"])
            if start_adj is None or end_adj is None or start_adj <= 0 or end_adj <= 0:
                row["outcome_missing_reason"] = "adjusted_close_missing_no_raw_fallback"
                result.append(row)
                continue
            forward = end_adj / start_adj - 1.0
            sign = _direction_sign(claim.get("elliott_direction"))
            signed = forward * sign if sign is not None else None
            row["forward_return"] = float(forward)
            row["signed_forward_return"] = float(signed) if signed is not None else None
            row["review_correct"] = _review_correct(claim.get("review_orientation"), signed)
            row["outcome_available"] = True
            row["outcome_missing_reason"] = None

            path = series.iloc[anchor + 1 : target + 1].copy()
            path_high = pd.to_numeric(path["adjusted_high"], errors="coerce")
            path_low = pd.to_numeric(path["adjusted_low"], errors="coerce")
            if len(path) and path_high.notna().all() and path_low.notna().all() and sign is not None:
                if sign > 0:
                    favorable = float((path_high / start_adj - 1.0).max())
                    adverse = float((path_low / start_adj - 1.0).min())
                else:
                    favorable = float((1.0 - path_low / start_adj).max())
                    adverse = float((1.0 - path_high / start_adj).min())
                row["max_favorable_excursion"] = favorable
                row["max_adverse_excursion"] = adverse

            if claim.get("claim_type") == "projection":
                if not bool(claim.get("clean_future_hit_claim", False)):
                    row["projection_hit"] = None
                    row["projection_hit_missing_reason"] = "zone_already_inside_or_reached_at_creation"
                elif len(path) and path_high.notna().all() and path_low.notna().all():
                    low = float(claim["price_low"])
                    high = float(claim["price_high"])
                    intersects = (path_high >= low) & (path_low <= high)
                    row["projection_hit"] = bool(intersects.any())
                    row["projection_hit_missing_reason"] = None
                    if intersects.any():
                        first_position = int(np.flatnonzero(intersects.to_numpy())[0]) + 1
                        row["first_hit_session"] = first_position
                else:
                    row["projection_hit"] = None
                    row["projection_hit_missing_reason"] = "adjusted_intraday_path_missing"
            result.append(row)
    return result


def effective_block_length(horizon: int) -> int:
    return max(1, 2 * int(horizon))


def _date_blocks(frame: pd.DataFrame, date_col: str, horizon: int) -> tuple[list[pd.Timestamp], list[list[pd.Timestamp]], int]:
    block_length = effective_block_length(horizon)
    if frame.empty or date_col not in frame.columns:
        return [], [], block_length
    dates = sorted(pd.Timestamp(day) for day in pd.to_datetime(frame[date_col], errors="coerce").dropna().unique())
    if not dates:
        return [], [], block_length
    span = min(block_length, len(dates))
    blocks = [[dates[(start + offset) % len(dates)] for offset in range(span)] for start in range(len(dates))]
    return dates, blocks, block_length


def _support_regions(frame: pd.DataFrame, date_col: str, baseline_dates: Sequence[pd.Timestamp], block_length: int) -> int:
    if frame.empty or not baseline_dates:
        return 0
    positions = {pd.Timestamp(day): idx for idx, day in enumerate(baseline_dates)}
    observed = sorted(
        positions[pd.Timestamp(day)]
        for day in pd.to_datetime(frame[date_col], errors="coerce").dropna().unique()
        if pd.Timestamp(day) in positions
    )
    count = 0
    last: int | None = None
    for pos in observed:
        if last is None or pos - last >= block_length:
            count += 1
            last = pos
    return count


def _stable_seed(base: int, *parts: object) -> int:
    digest = sha256("|".join(map(str, parts)).encode("utf-8")).digest()
    return int((base + int.from_bytes(digest[:4], "big")) % (2**32 - 1))


def block_bootstrap_mean(
    frame: pd.DataFrame,
    *,
    metric: str,
    date_col: str,
    horizon: int,
    config: ValidationConfig = ValidationConfig(),
    seed_key: Sequence[object] = (),
) -> dict[str, object]:
    """Circular moving observation-date block interval for a mean metric."""

    values = frame[[date_col, metric]].copy() if not frame.empty else pd.DataFrame(columns=[date_col, metric])
    values[metric] = pd.to_numeric(values[metric], errors="coerce")
    values = values.dropna(subset=[date_col, metric])
    dates, blocks, block_length = _date_blocks(values, date_col, horizon)
    regions = _support_regions(values, date_col, dates, block_length)
    point = float(values[metric].mean()) if not values.empty else None
    diagnostics = {
        "N": int(len(values)),
        "date_count": len(dates),
        "block_count": len(blocks),
        "block_length": block_length,
        "support_regions": regions,
        "mean": point,
        "mean_95": None,
        "robust_interval_available": False,
    }
    if (
        values.empty
        or config.bootstrap_reps <= 0
        or len(blocks) < 2
        or regions < config.min_support_regions
    ):
        return diagnostics

    values[date_col] = pd.to_datetime(values[date_col])
    by_day = {
        day: values.loc[values[date_col].eq(day), metric].to_numpy(dtype=float)
        for day in dates
    }
    rng = np.random.default_rng(_stable_seed(config.random_seed, *seed_key, metric, horizon))
    draws_per_rep = int(np.ceil(len(dates) / block_length))
    estimates: list[float] = []
    for _ in range(config.bootstrap_reps):
        chosen = rng.integers(0, len(blocks), size=draws_per_rep)
        sampled_dates = [day for idx in chosen for day in blocks[idx]][: len(dates)]
        parts = [by_day[day] for day in sampled_dates if len(by_day[day])]
        if parts:
            estimates.append(float(np.concatenate(parts).mean()))
    if estimates:
        lo, hi = np.quantile(np.asarray(estimates, dtype=float), [0.025, 0.975])
        diagnostics["mean_95"] = [float(lo), float(hi)]
        diagnostics["robust_interval_available"] = True
    return diagnostics


def block_bootstrap_difference(
    values: pd.DataFrame,
    baseline: pd.DataFrame,
    *,
    metric: str,
    date_col: str,
    horizon: int,
    config: ValidationConfig = ValidationConfig(),
    seed_key: Sequence[object] = (),
) -> dict[str, object]:
    """Synchronized date-block difference versus a same-class baseline."""

    v = values[[date_col, metric]].copy() if not values.empty else pd.DataFrame(columns=[date_col, metric])
    b = baseline[[date_col, metric]].copy() if not baseline.empty else pd.DataFrame(columns=[date_col, metric])
    v[metric] = pd.to_numeric(v[metric], errors="coerce")
    b[metric] = pd.to_numeric(b[metric], errors="coerce")
    v = v.dropna(subset=[date_col, metric])
    b = b.dropna(subset=[date_col, metric])
    dates, blocks, block_length = _date_blocks(b, date_col, horizon)
    regions = _support_regions(v, date_col, dates, block_length)
    point = float(v[metric].mean() - b[metric].mean()) if not v.empty and not b.empty else None
    result = {
        "N": int(len(v)),
        "baseline_N": int(len(b)),
        "difference": point,
        "difference_95": None,
        "block_length": block_length,
        "support_regions": regions,
        "robust_interval_available": False,
    }
    if (
        v.empty or b.empty or config.bootstrap_reps <= 0 or len(blocks) < 2
        or regions < config.min_support_regions
    ):
        return result
    v[date_col] = pd.to_datetime(v[date_col])
    b[date_col] = pd.to_datetime(b[date_col])
    v_by_day = {day: v.loc[v[date_col].eq(day), metric].to_numpy(dtype=float) for day in dates}
    b_by_day = {day: b.loc[b[date_col].eq(day), metric].to_numpy(dtype=float) for day in dates}
    rng = np.random.default_rng(_stable_seed(config.random_seed, *seed_key, "diff", metric, horizon))
    draws_per_rep = int(np.ceil(len(dates) / block_length))
    estimates: list[float] = []
    for _ in range(config.bootstrap_reps):
        chosen = rng.integers(0, len(blocks), size=draws_per_rep)
        sampled_dates = [day for idx in chosen for day in blocks[idx]][: len(dates)]
        vp = [v_by_day[day] for day in sampled_dates if len(v_by_day[day])]
        bp = [b_by_day[day] for day in sampled_dates if len(b_by_day[day])]
        if vp and bp:
            estimates.append(float(np.concatenate(vp).mean() - np.concatenate(bp).mean()))
    if estimates:
        lo, hi = np.quantile(np.asarray(estimates, dtype=float), [0.025, 0.975])
        result["difference_95"] = [float(lo), float(hi)]
        result["robust_interval_available"] = True
    return result


def summarize_projection_outcomes(
    outcomes: Sequence[Mapping[str, object]],
    config: ValidationConfig = ValidationConfig(),
) -> list[dict[str, object]]:
    frame = pd.DataFrame(list(outcomes))
    if frame.empty:
        return []
    frame = frame.loc[frame["claim_type"].eq("projection")].copy()
    if frame.empty:
        return []
    frame["event_date"] = pd.to_datetime(frame["available_from"], errors="coerce")
    frame["projection_hit_numeric"] = frame["projection_hit"].map({True: 1.0, False: 0.0})
    records: list[dict[str, object]] = []
    group_cols = ["partition", "horizon_sessions", "wave_role", "projection_type", "degree"]
    for key, group in frame.groupby(group_cols, dropna=False, sort=True):
        partition, horizon, wave_role, projection_type, degree = key
        hit_stats = block_bootstrap_mean(
            group,
            metric="projection_hit_numeric",
            date_col="event_date",
            horizon=int(horizon),
            config=config,
            seed_key=("projection", partition, wave_role, projection_type, degree),
        )
        ret_stats = block_bootstrap_mean(
            group,
            metric="signed_forward_return",
            date_col="event_date",
            horizon=int(horizon),
            config=config,
            seed_key=("projection_return", partition, wave_role, projection_type, degree),
        )
        records.append({
            "partition": partition,
            "horizon_sessions": int(horizon),
            "wave_role": wave_role,
            "projection_type": projection_type,
            "degree": degree,
            "zone_hit": hit_stats,
            "signed_return": ret_stats,
            "formal_promotion_evidence": partition == "prospective_unspent",
            "numeric_level_promoted": False,
            "research_only": True,
        })
    return records


def summarize_route_outcomes(
    outcomes: Sequence[Mapping[str, object]],
    config: ValidationConfig = ValidationConfig(),
) -> list[dict[str, object]]:
    frame = pd.DataFrame(list(outcomes))
    if frame.empty:
        return []
    frame = frame.loc[frame["claim_type"].eq("route_review")].copy()
    if frame.empty:
        return []
    frame["event_date"] = pd.to_datetime(frame["available_from"], errors="coerce")
    frame["review_correct_numeric"] = frame["review_correct"].map({True: 1.0, False: 0.0})
    records: list[dict[str, object]] = []
    group_cols = ["partition", "horizon_sessions", "review_context", "wave_stage", "degree"]
    for key, group in frame.groupby(group_cols, dropna=False, sort=True):
        partition, horizon, review_context, wave_stage, degree = key
        correctness = block_bootstrap_mean(
            group,
            metric="review_correct_numeric",
            date_col="event_date",
            horizon=int(horizon),
            config=config,
            seed_key=("route", partition, review_context, wave_stage, degree),
        )
        signed = block_bootstrap_mean(
            group,
            metric="signed_forward_return",
            date_col="event_date",
            horizon=int(horizon),
            config=config,
            seed_key=("route_return", partition, review_context, wave_stage, degree),
        )
        records.append({
            "partition": partition,
            "horizon_sessions": int(horizon),
            "review_context": review_context,
            "wave_stage": wave_stage,
            "degree": degree,
            "directional_review_correctness": correctness,
            "signed_return": signed,
            "round_trip_pnl_evaluated": False,
            "reason_round_trip_not_evaluated": "no_frozen_execution_fraction_or_reentry_policy",
            "formal_promotion_evidence": partition == "prospective_unspent",
            "research_only": True,
        })
    return records


def validate_cross_system_relations(
    relations: Sequence[Mapping[str, object]],
    route_outcomes: Sequence[Mapping[str, object]],
    config: ValidationConfig = ValidationConfig(),
) -> list[dict[str, object]]:
    """Test 6E relation groups against same-class Elliott event baselines."""

    rel = pd.DataFrame(list(relations))
    out = pd.DataFrame(list(route_outcomes))
    if rel.empty or out.empty:
        return []
    out = out.loc[out["claim_type"].eq("route_review") & out["outcome_available"].eq(True)].copy()
    if out.empty:
        return []
    rel = rel.rename(columns={"event_id": "source_event_id"})
    merged = rel.merge(out, on="source_event_id", how="inner", suffixes=("_relation", ""))
    if merged.empty:
        return []
    merged["event_date"] = pd.to_datetime(merged["available_from"], errors="coerce")
    records: list[dict[str, object]] = []
    for key, group in merged.groupby(["model", "relation", "partition", "horizon_sessions"], dropna=False, sort=True):
        model, relation, partition, horizon = key
        baseline = merged.loc[
            merged["model"].eq(model)
            & merged["partition"].eq(partition)
            & merged["horizon_sessions"].eq(horizon)
            & merged["review_orientation"].eq(group["review_orientation"].iloc[0])
        ].copy()
        stats = block_bootstrap_difference(
            group,
            baseline,
            metric="signed_forward_return",
            date_col="event_date",
            horizon=int(horizon),
            config=config,
            seed_key=("cross_system", model, relation, partition),
        )
        records.append({
            "model": model,
            "relation": relation,
            "partition": partition,
            "horizon_sessions": int(horizon),
            "incremental_signed_return_vs_same_orientation_baseline": stats,
            "formal_promotion_evidence": partition == "prospective_unspent",
            "research_only": True,
        })
    return records


def attach_context_alpha(
    outcomes: Sequence[Mapping[str, object]],
    context_history: pd.DataFrame,
    assignments: pd.DataFrame,
    registry: pd.DataFrame,
) -> list[dict[str, object]]:
    """Attach alpha only where 6F resolves causal usable external context.

    The same asset start/end calendar interval is used.  Missing external context
    stays missing; scanner peers are never substituted.
    """

    history = context_history.copy()
    if history.empty:
        return []
    history["date"] = pd.to_datetime(history["date"], errors="coerce").dt.normalize()
    records: list[dict[str, object]] = []
    for outcome in outcomes:
        if not outcome.get("outcome_available") or not outcome.get("anchor_session") or not outcome.get("end_session"):
            continue
        resolved = resolve_context_assignments(
            assignments,
            registry,
            symbol=str(outcome["symbol"]),
            as_of=str(outcome["available_from"]),
            include_unusable=False,
        )
        for _, assignment in resolved.iterrows():
            context_id = str(assignment["context_id"])
            rows = history.loc[history["context_id"].astype(str).eq(context_id)].sort_values("date", kind="mergesort")
            if rows.empty:
                continue
            start_day = pd.Timestamp(outcome["anchor_session"])
            end_day = pd.Timestamp(outcome["end_session"])
            start_rows = rows.loc[rows["date"] <= start_day]
            end_rows = rows.loc[rows["date"] <= end_day]
            if start_rows.empty or end_rows.empty:
                continue
            start = start_rows.iloc[-1]
            end = end_rows.iloc[-1]
            basis = str(start.get("price_basis", assignment.get("price_basis", "raw")))
            start_value = _finite(start.get("adj_close")) if basis == "adjusted" else _finite(start.get("close"))
            end_value = _finite(end.get("adj_close")) if basis == "adjusted" else _finite(end.get("close"))
            if start_value is None or end_value is None or start_value <= 0 or end_value <= 0:
                continue
            context_return = end_value / start_value - 1.0
            asset_return = float(outcome["forward_return"])
            sign = _direction_sign(outcome.get("elliott_direction"))
            records.append({
                "claim_id": outcome["claim_id"],
                "symbol": outcome["symbol"],
                "horizon_sessions": outcome["horizon_sessions"],
                "context_id": context_id,
                "relationship": assignment["relationship"],
                "context_quality": assignment["context_quality"],
                "asset_return": asset_return,
                "context_return": float(context_return),
                "alpha": float(asset_return - context_return),
                "signed_alpha": float((asset_return - context_return) * sign) if sign is not None else None,
                "context_start_date": pd.Timestamp(start["date"]).date().isoformat(),
                "context_end_date": pd.Timestamp(end["date"]).date().isoformat(),
                "scanner_peer_fallback_used": False,
                "research_only": True,
            })
    return records


def build_validation_report(
    routed_snapshots: Sequence[Mapping[str, object]],
    prices: pd.DataFrame | Iterable[Mapping[str, object]],
    *,
    relations: Sequence[Mapping[str, object]] = (),
    context_history: pd.DataFrame | None = None,
    context_assignments: pd.DataFrame | None = None,
    context_registry: pd.DataFrame | None = None,
    config: ValidationConfig = ValidationConfig(),
) -> dict[str, object]:
    """Build a deterministic 6G report without automatic promotion."""

    structure = extract_structure_claims(routed_snapshots, config)
    structure_eval = evaluate_structure_progression(structure, routed_snapshots)
    projections = extract_projection_claims(routed_snapshots, config)
    routes = extract_route_claims(routed_snapshots, config)
    projection_outcomes = attach_forward_outcomes(projections, prices, config)
    route_outcomes = attach_forward_outcomes(routes, prices, config)
    prospective_mature = sum(
        1 for row in [*projection_outcomes, *route_outcomes]
        if row.get("partition") == "prospective_unspent" and row.get("outcome_available")
    )
    prospective_structure = sum(
        1 for row in structure_eval
        if row.get("partition") == "prospective_unspent" and row.get("structure_resolution") != "unresolved"
    )
    cross = validate_cross_system_relations(relations, route_outcomes, config) if relations else []
    if context_history is not None and context_assignments is not None and context_registry is not None:
        context_alpha = attach_context_alpha(
            [*projection_outcomes, *route_outcomes],
            context_history,
            context_assignments,
            context_registry,
        )
        context_status = "available" if context_alpha else "not_testable_no_verified_external_context_or_mature_outcomes"
    else:
        context_alpha = []
        context_status = "not_supplied_missing_context_stays_missing"
    promotion_status = (
        "prospective_evidence_accumulating_no_automatic_promotion"
        if prospective_mature + prospective_structure > 0
        else "awaiting_unspent_prospective_evidence"
    )
    return {
        "schema_version": "elliott_vnext_validation_v1",
        "module": "6G_historical_validation",
        "rules_frozen_through": config.rules_frozen_through,
        "horizons_sessions": list(map(int, config.horizons)),
        "evidence_policy": {
            "legacy_data_can_support_promotion": False,
            "formal_claims_require_available_from_after_freeze": True,
            "prospective_unspent_mature_outcomes": prospective_mature,
            "prospective_unspent_resolved_structure_claims": prospective_structure,
        },
        "coverage": {
            "routed_snapshots": len(routed_snapshots),
            "structure_claims": len(structure),
            "projection_claims": len(projections),
            "route_review_claims": len(routes),
            "projection_outcome_rows": len(projection_outcomes),
            "route_outcome_rows": len(route_outcomes),
            "cross_system_rows": len(cross),
            "context_alpha_rows": len(context_alpha),
        },
        "structure_validation": structure_eval,
        "projection_summary": summarize_projection_outcomes(projection_outcomes, config),
        "route_summary": summarize_route_outcomes(route_outcomes, config),
        "cross_system_validation": cross,
        "market_context_validation": {
            "status": context_status,
            "alpha_rows": context_alpha,
            "scanner_peer_fallback_used": False,
        },
        "promotion_status": promotion_status,
        "automatic_promotion_allowed": False,
        "technical_completion_is_empirical_validation": False,
        "round_trip_pnl_evaluated": False,
        "numeric_w5_levels_promoted": False,
        "trade_decision": None,
        "order_instruction": None,
        "research_only": True,
    }
