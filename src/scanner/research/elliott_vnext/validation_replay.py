from __future__ import annotations

"""Causal prefix replay for Module 6G.

This module re-runs the frozen 6A→6D chain against historical *prefixes*.
It never paints today's count onto yesterday.  Every replay date sees only rows
whose market date is on or before that date, and upstream pivot/projection
confirmation rules remain unchanged.
"""

from hashlib import sha256
import json
from typing import Iterable, Mapping, Sequence

import pandas as pd

from .fibonacci import DEFAULT_ZONE_WIDTH_SPECS, ZoneWidthSpec, attach_fibonacci_geometry
from .pivots import DEFAULT_PIVOT_SPECS, PivotSpec, detect_multidegree_pivots, prepare_daily_ohlcv
from .routing import ExecutionCostSpec, attach_swing_routing
from .scenarios import generate_scenario_sets
from .validation import ValidationConfig, ValidationInputError, validation_partition


def _fingerprint(snapshot: Mapping[str, object]) -> str:
    primary = snapshot.get("primary_scenario")
    primary_id = primary.get("scenario_id") if isinstance(primary, Mapping) else None
    primary_stage = primary.get("stage") if isinstance(primary, Mapping) else None
    zones = []
    for zone in snapshot.get("projection_zones", []) or []:
        if isinstance(zone, Mapping):
            zones.append((
                str(zone.get("zone_id", "")),
                str(zone.get("status", "")),
                zone.get("distance_to_zone_pct"),
            ))
    routes = []
    for route in snapshot.get("swing_routing", []) or []:
        if isinstance(route, Mapping):
            routes.append((
                str(route.get("trigger", "")),
                str(route.get("review_context", "")),
                str(route.get("scenario_id", "")),
                str(route.get("available_from", "")),
            ))
    invalidated = []
    for scenario in snapshot.get("invalidated_scenarios", []) or []:
        if isinstance(scenario, Mapping):
            invalidated.append((
                str(scenario.get("scenario_id", "")),
                tuple(scenario.get("rule_violations", []) or []),
            ))
    payload = {
        "symbol": snapshot.get("symbol"),
        "timeframe": snapshot.get("timeframe"),
        "degree": snapshot.get("degree"),
        "primary_id": primary_id,
        "primary_stage": primary_stage,
        "alternative_ids": [
            str(item.get("scenario_id", ""))
            for item in snapshot.get("alternative_scenarios", []) or []
            if isinstance(item, Mapping)
        ],
        "zones": sorted(zones),
        "routes": sorted(routes),
        "invalidated": sorted(invalidated),
    }
    frozen = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)
    return sha256(frozen.encode("utf-8")).hexdigest()


def _price_rows_for_symbol(
    rows: pd.DataFrame | Iterable[Mapping[str, object]],
    symbol: str,
) -> pd.DataFrame:
    frame = rows.copy() if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))
    if "symbol" not in frame.columns or "date" not in frame.columns:
        raise ValidationInputError("replay_requires_symbol_and_date")
    frame["symbol"] = frame["symbol"].astype(str)
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    frame = frame.loc[frame["symbol"].eq(str(symbol)) & frame["date"].notna()].copy()
    return frame.sort_values("date", kind="mergesort").reset_index(drop=True)


def replay_symbol_states(
    rows: pd.DataFrame | Iterable[Mapping[str, object]],
    symbol: str,
    *,
    config: ValidationConfig = ValidationConfig(),
    pivot_specs: Sequence[PivotSpec] = DEFAULT_PIVOT_SPECS,
    width_specs: Sequence[ZoneWidthSpec] = DEFAULT_ZONE_WIDTH_SPECS,
    cost_spec: ExecutionCostSpec | None = None,
    as_of_dates: Sequence[str] | None = None,
    keep_unchanged: bool = False,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Replay 6A→6D for one symbol with strict prefix-only inputs.

    ``keep_unchanged=False`` stores only state transitions.  This is a storage
    optimisation, not a sampling shortcut: every requested/observed session is
    still evaluated, and a changed zone status or route therefore creates a new
    snapshot.
    """

    source = _price_rows_for_symbol(rows, symbol)
    if source.empty:
        return [], {
            "symbol": str(symbol),
            "status": "price_history_missing",
            "evaluated_dates": 0,
            "snapshots": 0,
            "errors": [],
        }

    if as_of_dates is None:
        dates = [
            day.date().isoformat()
            for day in source["date"]
            if day >= pd.Timestamp(config.stable_start)
        ]
    else:
        dates = sorted({pd.Timestamp(day).normalize().date().isoformat() for day in as_of_dates})

    snapshots: list[dict[str, object]] = []
    previous: dict[tuple[str, str], str] = {}
    errors: list[dict[str, str]] = []
    evaluated = 0

    for day in dates:
        cutoff = pd.Timestamp(day).normalize()
        prefix = source.loc[source["date"] <= cutoff].copy()
        if prefix.empty:
            continue
        evaluated += 1
        try:
            daily = prepare_daily_ohlcv(
                prefix.to_dict("records"),
                str(symbol),
                as_of=day,
                price_basis=config.replay_price_basis,
            )
        except ValueError as exc:
            errors.append({"as_of": day, "error": str(exc)})
            continue
        if daily.empty:
            continue
        pivots = detect_multidegree_pivots(daily, specs=pivot_specs, as_of=day)
        scenario_sets = generate_scenario_sets(pivots, as_of=day)
        current_price = float(daily.iloc[-1]["close"])
        for scenario_set in scenario_sets:
            geometry = attach_fibonacci_geometry(
                scenario_set,
                source_pivots=pivots,
                current_price=current_price,
                width_specs=width_specs,
            )
            routed = attach_swing_routing(geometry, cost_spec=cost_spec)
            routed["validation_replay"] = {
                "mode": "prefix_only",
                "as_of": day,
                "partition": validation_partition(day, config),
                "price_basis": config.replay_price_basis,
                "future_rows_used": False,
                "performance_used_for_state": False,
            }
            routed["validation_partition"] = validation_partition(day, config)
            routed["research_only"] = True
            routed.pop("trade_decision", None)
            routed.pop("order_instruction", None)
            key = (str(routed.get("timeframe", "")), str(routed.get("degree", "")))
            state_hash = _fingerprint(routed)
            if keep_unchanged or previous.get(key) != state_hash:
                snapshots.append(routed)
                previous[key] = state_hash

    snapshots.sort(key=lambda item: (
        str(item.get("as_of", "")),
        str(item.get("timeframe", "")),
        str(item.get("degree", "")),
    ))
    coverage = {
        "symbol": str(symbol),
        "status": "ok" if snapshots else "no_elliott_state_emitted",
        "evaluated_dates": evaluated,
        "snapshots": len(snapshots),
        "errors": errors,
        "prefix_only": True,
        "future_rows_used": False,
        "replay_price_basis": config.replay_price_basis,
    }
    return snapshots, coverage


def replay_universe_states(
    rows: pd.DataFrame | Iterable[Mapping[str, object]],
    *,
    symbols: Sequence[str] | None = None,
    config: ValidationConfig = ValidationConfig(),
    pivot_specs: Sequence[PivotSpec] = DEFAULT_PIVOT_SPECS,
    width_specs: Sequence[ZoneWidthSpec] = DEFAULT_ZONE_WIDTH_SPECS,
    cost_spec: ExecutionCostSpec | None = None,
    keep_unchanged: bool = False,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Replay a universe without converting failures into synthetic evidence."""

    frame = rows.copy() if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))
    if "symbol" not in frame.columns:
        raise ValidationInputError("replay_requires_symbol")
    wanted = list(symbols) if symbols is not None else sorted(frame["symbol"].dropna().astype(str).unique())
    all_snapshots: list[dict[str, object]] = []
    details: list[dict[str, object]] = []
    for symbol in wanted:
        snapshots, coverage = replay_symbol_states(
            frame,
            str(symbol),
            config=config,
            pivot_specs=pivot_specs,
            width_specs=width_specs,
            cost_spec=cost_spec,
            keep_unchanged=keep_unchanged,
        )
        all_snapshots.extend(snapshots)
        details.append(coverage)
    return all_snapshots, {
        "symbols_requested": len(wanted),
        "symbols_with_snapshots": sum(1 for item in details if int(item.get("snapshots", 0)) > 0),
        "snapshots": len(all_snapshots),
        "details": details,
        "failures_are_missing_evidence_not_imputed": True,
        "research_only": True,
    }
